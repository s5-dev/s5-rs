//! Publish task: encrypt the vault's Transparent Node for recipients and publish.
//!
//! 1. Loads the raw CBOR of the vault's Transparent Node (decrypted from local storage).
//! 2. Resolves recipient keys from config → age public keys.
//! 3. Fetches the previously published encrypted TN (if any) from the blob store,
//!    decrypts it, and extracts accumulated history entries.
//! 4. Builds a new Node with the current snapshot at `""` plus history entries:
//!    - All history entries from the previous TN are carried forward.
//!    - A new history entry keyed by ISO 8601 UTC timestamp points to the
//!      previous encrypted blob hash.
//! 5. Age-encrypts the enriched Node CBOR for those recipients.
//! 6. Uploads the encrypted blob to the vault's first blob store.
//! 7. Signs a registry entry pointing to the encrypted blob's hash.
//! 8. Publishes the registry entry.
//!
//! Remote nodes fetch the encrypted Transparent Node via the registry hash,
//! decrypt it with their age identity, and recover the full `TraversalContext`
//! (keys, pipelines) needed to traverse the vault.

use std::io::Read;
use std::sync::Arc;

use anyhow::{Context, anyhow};
use bytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use s5_core::blob::BlobStore;
use s5_core::{BlobsRead, BlobsWrite, Hash, RegistryApi, StreamKey, StreamMessage};
use s5_fs_v2::layer::ReadableLayer;
use s5_fs_v2::node::{ContentRef, Node, NodeEntry, NodeKind, Structural};
use s5_fs_v2::pipeline::Pipeline;
use s5_fs_v2::snapshot::Snapshot;

use super::vault_persist::{
    age_decrypt_with_identity_files, age_encrypt_for_recipients, load_vault_root_cbor,
    node_to_snapshot_parts, save_node, vault_root_path,
};
use super::{
    TaskExecutorContext, resolve_key, resolve_store, resolve_vault, resolve_vault_key_info,
};

/// Derive the Ed25519 signing key the device uses to sign every vault
/// registry entry it writes — i.e., the device's iroh transport key
/// reused as the registry signer per the v3 model. Same key for every
/// vault; per-vault disambiguation happens via the `VAULT_ID` field in
/// the wire format.
pub fn device_signing_key(node_secret: &[u8; 32]) -> SigningKey {
    SigningKey::from_bytes(node_secret)
}

/// Derive a vault's `vault_id` from the recovery secret carried in its
/// vault root (`KEY_SLOT_RECOVERY` slot, see
/// `docs/reference/snapshot-publication.md` § Vault ID derivation):
///
/// ```text
/// vault_id = blake3("s5-vault-id" || recovery_secret)[..16]
/// ```
pub fn derive_vault_id(recovery_secret: &[u8; 32]) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"s5-vault-id");
    hasher.update(recovery_secret);
    let mut id = [0u8; 16];
    id.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    id
}

/// Derive the Ed25519 recovery signing key from a vault's
/// `recovery_secret`:
///
/// ```text
/// recovery_signing_seed = blake3("s5-recovery-sig" || recovery_secret)
/// recovery_signing_key  = ed25519_keypair_from_seed(recovery_signing_seed)
/// ```
///
/// Anyone with the vault root can derive this key (no device or DID
/// state needed) and verify the recovery registry entry — the canonical
/// disaster-recovery lookup path.
pub fn recovery_signing_key(recovery_secret: &[u8; 32]) -> SigningKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"s5-recovery-sig");
    hasher.update(recovery_secret);
    let seed: [u8; 32] = *hasher.finalize().as_bytes();
    SigningKey::from_bytes(&seed)
}

/// Extract the `recovery_secret` (32 bytes) from a vault root's
/// `KEY_SLOT_RECOVERY` slot. Returns an error if the slot is missing
/// (which would indicate a corrupted or pre-v3 vault root).
fn recovery_secret_from_vault_root(cbor: &[u8]) -> anyhow::Result<[u8; 32]> {
    let node = Node::from_bytes(cbor).map_err(|e| anyhow!("CBOR decode vault root: {e}"))?;
    recovery_secret_from_node(&node)
}

/// Same as `recovery_secret_from_vault_root` but takes a parsed `Node`.
fn recovery_secret_from_node(node: &Node) -> anyhow::Result<[u8; 32]> {
    let (_, _, ctx) = node_to_snapshot_parts(node)?;
    ctx.keys
        .as_ref()
        .and_then(|map| map.get(&s5_fs_v2::snapshot::KEY_SLOT_RECOVERY))
        .copied()
        .ok_or_else(|| {
            anyhow!(
                "vault root TraversalContext has no KEY_SLOT_RECOVERY slot — \
                 was this vault created before the v3 schema?"
            )
        })
}

/// Vault-root substitution defense (per
/// `docs/reference/snapshot-publication.md` § Vault-root substitution
/// defense). Compares the `KEY_SLOT_RECOVERY` value carried in an
/// incoming vault root against the local reference. A legitimate update
/// never changes `recovery_secret` (it's fixed at vault-creation time
/// and committed to `vault_id` derivation), so a mismatch means an
/// attacker is advertising a fresh vault under our `(pubkey, vault_id)`
/// lookup. Reject in that case.
fn verify_recovery_secret_invariant(
    local_secret: &[u8; 32],
    incoming_node: &Node,
) -> anyhow::Result<()> {
    let incoming = recovery_secret_from_node(incoming_node).context(
        "incoming vault root has no KEY_SLOT_RECOVERY slot — cannot \
         verify substitution defense",
    )?;
    if &incoming != local_secret {
        return Err(anyhow!(
            "vault root substitution detected — incoming KEY_SLOT_RECOVERY \
             does not match local; rejecting (see snapshot-publication.md \
             § Vault-root substitution defense)"
        ));
    }
    Ok(())
}

/// Ensure the one-time recovery registry entry exists.
///
/// The recovery entry maps `recovery_pubkey → device_signing_pubkey` so
/// that a restorer with just the vault root (paper passphrase →
/// recovery_age_secret → age-decrypt vault root → `KEY_SLOT_RECOVERY`)
/// can discover one of the device's vault registry entries (which in
/// turn points to the latest encrypted vault root).
///
/// Both the recovery entry and the per-device entry share the same
/// `vault_id`, so the lookup is `(recovery_pubkey, vault_id)` →
/// payload contains the device's signing pubkey, then
/// `(device_pubkey, vault_id)` → current HEAD.
///
/// Only writes if no entry exists yet (revision == 0).
async fn ensure_recovery_entry(
    registry: &dyn RegistryApi,
    vault_name: &str,
    recovery_secret: &[u8; 32],
    vault_id: [u8; 16],
    device_pubkey: &VerifyingKey,
) -> anyhow::Result<()> {
    let recovery_key = recovery_signing_key(recovery_secret);
    let recovery_verifying: VerifyingKey = (&recovery_key).into();
    let recovery_stream_key = StreamKey::Vault {
        pubkey: recovery_verifying.to_bytes(),
        vault_id,
    };

    // Check if entry already exists
    match registry.get(&recovery_stream_key).await {
        Ok(Some(_)) => {
            tracing::debug!(vault = vault_name, "recovery registry entry already exists");
            return Ok(());
        }
        Ok(None) => {} // Need to create it
        Err(e) => {
            tracing::warn!(
                vault = vault_name,
                error = %e,
                "could not check recovery registry entry"
            );
            return Ok(()); // Don't fail the publish
        }
    }

    // The recovery entry's "hash" stores the device's signing pubkey
    // (32 bytes). Slight abuse of the Hash field — it's the simplest way
    // to carry 32 bytes in a registry entry payload without a new field.
    let device_pubkey_hash = Hash::from(device_pubkey.to_bytes());

    let message = sign_registry_entry(&recovery_key, vault_id, device_pubkey_hash, 1)?;

    registry
        .set(message)
        .await
        .context("publishing recovery registry entry")?;

    let recovery_hex = hex::encode(recovery_verifying.to_bytes());
    let device_hex = hex::encode(device_pubkey.to_bytes());
    let vault_hex = hex::encode(vault_id);
    tracing::info!(
        vault = vault_name,
        recovery_pubkey = recovery_hex,
        device_pubkey = device_hex,
        vault_id = vault_hex,
        "recovery registry entry published"
    );

    Ok(())
}

/// Sign a v3 vault registry entry for the given vault, hash, and revision.
///
/// Wire format (per `s5_core::stream::types`):
/// `[0x5c (Registry), 0xed (Vault), pub_key(32), vault_id(16),
///   revision(8 BE), 0x21 (LEN=33), 0x?? (multihash tag) || hash(32),
///   sig(64)]`
fn sign_registry_entry(
    signing_key: &SigningKey,
    vault_id: [u8; 16],
    hash: Hash,
    revision: u64,
) -> anyhow::Result<StreamMessage> {
    StreamMessage::sign_ed25519_registry(signing_key, vault_id, hash, revision)
        .map_err(|e| anyhow!("creating signed registry entry: {e}"))
}

/// Age-decrypt bytes using a raw age secret key string (e.g. `AGE-SECRET-KEY-1...`).
///
/// Used for disaster recovery: the user has only their paper key, no identity
/// files on disk. Parses the secret into an `age::x25519::Identity` and
/// decrypts directly.
pub(crate) fn age_decrypt_with_secret_key(
    ciphertext: &[u8],
    age_secret: &str,
) -> anyhow::Result<Vec<u8>> {
    let identity: age::x25519::Identity = age_secret
        .trim()
        .parse()
        .map_err(|e| anyhow!("parsing age secret key: {e}"))?;

    let decryptor = age::Decryptor::new(ciphertext).map_err(|e| anyhow!("age decryptor: {e}"))?;

    let mut plaintext = vec![];
    let mut reader = decryptor
        .decrypt(std::iter::once(&identity as &dyn age::Identity))
        .map_err(|e| anyhow!("age decrypt with paper key: {e}"))?;
    reader
        .read_to_end(&mut plaintext)
        .context("reading age plaintext")?;
    Ok(plaintext)
}

/// Fetch the previously published encrypted Transparent Node, decrypt it,
/// and return it as a parsed `Node` along with the encrypted blob hash.
///
/// Returns `None` if nothing was previously published (no registry entry).
pub(crate) async fn fetch_previous_published_node(
    registry: &dyn RegistryApi,
    blob_store: &BlobStore,
    stream_key: &StreamKey,
    identity_files: &[String],
) -> anyhow::Result<Option<(Node, Hash, u64)>> {
    let entry = match registry.get(stream_key).await? {
        Some(e) => e,
        None => return Ok(None),
    };

    let encrypted_bytes = blob_store
        .blob_download(entry.hash)
        .await
        .map_err(|e| anyhow!("downloading previous encrypted TN: {e}"))?;

    let cbor = age_decrypt_with_identity_files(&encrypted_bytes, identity_files)
        .context("decrypting previous published Transparent Node")?;

    let node = Node::from_bytes(&cbor).map_err(|e| anyhow!("CBOR decode previous TN: {e}"))?;

    Ok(Some((node, entry.hash, entry.revision)))
}

/// Build an enriched Node with current snapshot + accumulated history.
///
/// The current local TN is parsed to get the `""` entry (current snapshot).
/// History entries from the previous published TN are carried forward.
/// A new history entry is added keyed by the current UTC timestamp,
/// pointing to the previous encrypted blob hash.
fn build_published_node(
    current_cbor: &[u8],
    prev_node: Option<&Node>,
    prev_encrypted_hash: Option<Hash>,
) -> anyhow::Result<Node> {
    let current_node =
        Node::from_bytes(current_cbor).map_err(|e| anyhow!("CBOR decode current TN: {e}"))?;

    let current_entry = current_node
        .transparent_entry()
        .ok_or_else(|| anyhow!("current vault root is not a Transparent Node"))?
        .clone();

    let mut node = Node::new();
    node.header.kind = s5_fs_v2::node::NodeKind::Transparent;

    // Current snapshot at key ""
    node.entries.insert(String::new(), current_entry);

    // Carry forward history from previous published TN
    if let Some(prev) = prev_node {
        for (key, entry) in &prev.entries {
            if key.is_empty() {
                // Skip the old current snapshot — it becomes a history entry
                continue;
            }
            // All non-empty keys are history entries — carry forward
            node.entries.insert(key.clone(), entry.clone());
        }
    }

    // Add new history entry for the previous snapshot
    if let Some(prev_hash) = prev_encrypted_hash {
        let timestamp = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| String::from("unknown"));
        let history_entry = NodeEntry {
            content: Some(ContentRef {
                structural: Structural::Leaf,
                hash: *prev_hash.as_bytes(),
                size: 0,
                plaintext_hash: None,
                stored_blocks: None,
            }),
            semantic: None,
            child_context: None,
            tombstone: None,
        };
        node.entries.insert(timestamp, history_entry);
    }

    Ok(node)
}

/// Convergence step: if the previously published TN's tree differs
/// from the local TN's tree, merge them via `Pipeline::merge_and_persist`
/// (local entries win on collision — they're the strictly newer state)
/// and emit a fresh local TN whose `""` entry points at the merged
/// tree. The merge is symmetric "produce the union" — entries unique
/// to the registry-side tree (concurrently published by another flow)
/// are preserved, entries unique to local survive, and tombstones in
/// either side resolve via the standard merge rule.
///
/// Returns `None` when no merge is needed (no prev TN, or local + prev
/// already point at the same tree root). Otherwise `Some` with the
/// CBOR bytes of the merged TN — caller should `save_node` them so the
/// local cache mirrors registry truth before the next flush.
///
/// # Why this lives here
///
/// Without this step, a publish that targets `prev_revision + 1`
/// silently discards any tree state the previous publication had that
/// our local snapshot didn't include — i.e. concurrent producers
/// (rw-mount-flush + backup, two snaps from sibling devices that
/// pre-paired the same vault) lose data on the slower writer's path.
/// The merge primitives reconcile divergence at the data layer; this
/// is the operational call site that actually invokes them.
async fn maybe_merge_with_prev_published(
    local_cbor: &[u8],
    prev_node: Option<&Node>,
    blob_store: &BlobStore,
) -> anyhow::Result<Option<Vec<u8>>> {
    let Some(prev) = prev_node else {
        return Ok(None);
    };

    let local =
        Node::from_bytes(local_cbor).map_err(|e| anyhow!("CBOR decode local TN for merge: {e}"))?;

    let local_entry = local
        .transparent_entry()
        .ok_or_else(|| anyhow!("local TN has no \"\" entry — not a Transparent node?"))?;
    let local_content = local_entry
        .content
        .as_ref()
        .ok_or_else(|| anyhow!("local TN \"\" entry has no content"))?;

    let prev_entry = prev
        .transparent_entry()
        .ok_or_else(|| anyhow!("prev TN has no \"\" entry"))?;
    let prev_content = prev_entry
        .content
        .as_ref()
        .ok_or_else(|| anyhow!("prev TN \"\" entry has no content"))?;

    // Already aligned: local TN already includes (or equals) prev's tree.
    if local_content.hash == prev_content.hash {
        return Ok(None);
    }

    // Build read-only Snapshot views over each tree. Both share the same
    // vault keys via TraversalContext (recipient set is per-vault), so a
    // single Pipeline built from local's context decrypts entries from
    // either side.
    let read_store: Arc<dyn BlobsRead> = Arc::new(blob_store.clone());
    let local_ctx = local_entry
        .child_context
        .as_ref()
        .map(|c| (**c).clone())
        .unwrap_or_default();
    let prev_ctx = prev_entry
        .child_context
        .as_ref()
        .map(|c| (**c).clone())
        .unwrap_or_default();

    let local_snap = Snapshot::new(
        Hash::from(local_content.hash),
        Arc::clone(&read_store),
        local_ctx.clone(),
        local_content.plaintext_hash,
    );
    let prev_snap = Snapshot::new(
        Hash::from(prev_content.hash),
        Arc::clone(&read_store),
        prev_ctx,
        prev_content.plaintext_hash,
    );

    let pipeline = Pipeline::new(Arc::clone(&read_store), local_ctx.clone());
    let chunk_mask = prev_snap.chunk_mask().await;

    let merge_result = pipeline
        .merge_and_persist(&prev_snap, chunk_mask, &local_snap, blob_store)
        .await
        .context("merging local TN with previously published TN")?;

    let Some((unified_root, unified_plaintext_hash, _stats)) = merge_result else {
        // Both sides empty after tombstone filter — emit an empty TN.
        // Fall through to local_cbor unchanged; nothing to merge.
        return Ok(None);
    };

    // Wrap the merged tree in a fresh Transparent Node. Carry local's
    // semantic / non-content fields if any.
    let unified_entry = NodeEntry {
        content: Some(ContentRef {
            structural: Structural::Link,
            hash: *unified_root.as_bytes(),
            size: 0,
            plaintext_hash: Some(unified_plaintext_hash),
            stored_blocks: None,
        }),
        semantic: local_entry.semantic.clone(),
        child_context: Some(Box::new(local_ctx)),
        tombstone: None,
    };

    let mut unified_tn = Node::new();
    unified_tn.header.kind = NodeKind::Transparent;
    unified_tn.entries.insert(String::new(), unified_entry);

    let cbor = unified_tn
        .to_vec()
        .map_err(|e| anyhow!("CBOR encode merged TN: {e}"))?;
    Ok(Some(cbor))
}

/// Run a publish task.
///
/// 1. Load raw CBOR of the vault's Transparent Node (decrypted from local file).
/// 2. Resolve key names → age recipient public keys + identity files.
/// 3. Fetch previous published TN (if any) → decrypt → extract history.
/// 4. Merge local + prev published trees if they diverge (convergence step).
/// 5. Build enriched Node: current snapshot + accumulated history entries.
/// 6. Age-encrypt the enriched Node CBOR for recipients.
/// 7. Upload the encrypted blob to the vault's first blob store.
/// 8. Derive Ed25519 signing key from node secret + vault name.
/// 9. Sign a registry entry pointing to the encrypted blob's hash.
/// 10. Publish to registry.
pub async fn run_publish(
    ctx: &TaskExecutorContext,
    vault_name: &str,
    key_names: &[String],
) -> anyhow::Result<()> {
    let (vault, key_configs, vault_identity_files) = {
        let config = ctx.config.read().await;
        let vault = resolve_vault(&config, vault_name)?.clone();
        let mut key_configs = Vec::new();
        for name in key_names {
            let kc = resolve_key(&config, name)
                .with_context(|| format!("resolving key '{name}'"))?
                .clone();
            key_configs.push(kc);
        }
        let (_, id_files) = resolve_vault_key_info(&config, vault_name)
            .context("resolving vault key for local decryption")?;
        (vault, key_configs, id_files)
    };
    let registry = ctx
        .registry
        .as_ref()
        .ok_or_else(|| anyhow!("no registry configured — cannot publish snapshot"))?;

    // -- Load the raw CBOR of the Transparent Node --
    let current_path = vault_root_path(&vault.root_path);
    let cbor = load_vault_root_cbor(&current_path, &vault_identity_files)
        .context("reading vault root for publish")?
        .ok_or_else(|| {
            anyhow!(
                "vault '{}' has no snapshot to publish (run ingest first)",
                vault_name
            )
        })?;

    // -- Resolve recipient keys (public keys + identity files) --
    let mut recipient_strings = Vec::new();
    let mut identity_files = Vec::new();
    for key_config in &key_configs {
        recipient_strings.push(key_config.public_key.clone());
        if let Some(ref id_file) = key_config.identity_file {
            identity_files.push(id_file.clone());
        }
    }

    if recipient_strings.is_empty() {
        return Err(anyhow!(
            "publish requires at least one key recipient (specify keys in task config)"
        ));
    }

    // -- Resolve blob store --
    let blob_store_name = vault.blob_stores.first().ok_or_else(|| {
        anyhow!(
            "vault '{}' has no blob_stores configured — cannot upload encrypted snapshot",
            vault_name
        )
    })?;
    let blob_store: &BlobStore = resolve_store(&ctx.stores, blob_store_name)?;

    // -- Derive vault_id from the vault root's KEY_SLOT_RECOVERY slot --
    let recovery_secret = recovery_secret_from_vault_root(&cbor)?;
    let vault_id = derive_vault_id(&recovery_secret);

    // -- Per-device signing key + stream key --
    // The device's iroh transport key signs every vault registry entry
    // this device writes — `VAULT_ID` in the wire format disambiguates
    // entries across vaults.
    let signing_key = device_signing_key(&ctx.node_secret);
    let verifying_key: VerifyingKey = (&signing_key).into();
    let stream_key = StreamKey::Vault {
        pubkey: verifying_key.to_bytes(),
        vault_id,
    };

    // ---- Convergence retry loop ----------------------------------------
    //
    // Two flows publishing to the same `(pubkey, vault_id)` stream key
    // race on `prev_revision + 1`. The registry's `should_store`
    // deterministically picks one winner on a tie, but the loser's call
    // returns `Ok(())` without storing — silent data loss unless the
    // loser converges.
    //
    // On every attempt:
    //   1. Fetch the latest registry-published TN (`prev_node`).
    //   2. Merge our local intent with `prev_node`'s tree if they diverge
    //      (`maybe_merge_with_prev_published`). This handles both
    //      back-to-back ("we lost the race; their state is now `prev`")
    //      and concurrent ("our `prev` will be overwritten before we
    //      `set`") cases — the next iteration's fetch sees the winning
    //      state and merges against it.
    //   3. Encrypt + upload the merged TN.
    //   4. Sign + `registry.set` at `prev_revision + 1`.
    //   5. Read back: if the registry still holds *our* hash, we won —
    //      break. Otherwise loop with backoff.
    //
    // `MAX_PUBLISH_RETRIES` caps unbounded contention. With N
    // contestants racing, only one can win per iteration — so the
    // worst case needs ~N attempts to fully serialise. 16 covers
    // realistic single-device load (handful of concurrent flushes /
    // snaps) and a comfortable margin for daemon-side bursts.
    const MAX_PUBLISH_RETRIES: u32 = 16;
    const BASE_BACKOFF_MS: u64 = 25;

    let mut encrypted_bytes: Option<Bytes> = None;
    let mut encrypted_hash: Option<Hash> = None;
    let mut won = false;

    for attempt in 0..MAX_PUBLISH_RETRIES {
        // -- Fetch the latest registry-published TN --
        let (prev_node, prev_encrypted_hash, prev_revision) = if !identity_files.is_empty() {
            match fetch_previous_published_node(
                registry.as_ref(),
                blob_store,
                &stream_key,
                &identity_files,
            )
            .await
            {
                Ok(Some((node, hash, rev))) => {
                    // Substitution defense (publish-side guard).
                    //
                    // TODO(load-bearing check): the documented threat model
                    // (see snapshot-publication.md § Vault-root substitution
                    // defense) is an authorized peer/relay-writer publishing
                    // a fresh vault under our (pubkey, vault_id). That attack
                    // is intercepted on the **consumer/sync read path**, not
                    // here — the read path doesn't exist in code yet.
                    //
                    // What this call covers is the narrower case of the
                    // device fetching its own previous publication: if the
                    // bytes there ever come back with a different
                    // KEY_SLOT_RECOVERY, something is wrong (registry-level
                    // overwrite, key reuse) and we'd rather fail the publish
                    // than splice foreign history into our next snapshot.
                    verify_recovery_secret_invariant(&recovery_secret, &node).with_context(
                        || {
                            format!(
                                "vault '{vault_name}': previous published vault root \
                                 failed substitution-defense check"
                            )
                        },
                    )?;
                    (Some(node), Some(hash), rev)
                }
                Ok(None) => (None, None, 0),
                Err(e) => {
                    tracing::warn!(
                        vault = vault_name,
                        attempt,
                        error = %e,
                        "could not fetch previous published TN for history — \
                         publishing without history this attempt"
                    );
                    let rev = match registry.get(&stream_key).await {
                        Ok(Some(entry)) => entry.revision,
                        _ => 0,
                    };
                    (None, None, rev)
                }
            }
        } else {
            if attempt == 0 {
                tracing::info!(
                    vault = vault_name,
                    "no identity files configured — publishing without history"
                );
            }
            let rev = match registry.get(&stream_key).await {
                Ok(Some(entry)) => entry.revision,
                _ => 0,
            };
            (None, None, rev)
        };

        // -- Convergence merge: union local + previously published --
        let merged_cbor =
            match maybe_merge_with_prev_published(&cbor, prev_node.as_ref(), blob_store).await {
                Ok(Some(merged)) => {
                    tracing::info!(
                        vault = vault_name,
                        attempt,
                        "merged local TN with previously published TN — \
                     publishing the union"
                    );
                    let merged_node = Node::from_bytes(&merged)
                        .map_err(|e| anyhow!("CBOR decode merged TN before disk write: {e}"))?;
                    if let Err(e) = save_node(&current_path, &merged_node, &recipient_strings) {
                        tracing::warn!(
                            vault = vault_name,
                            error = %e,
                            "could not persist merged TN to local cache — \
                             publish proceeds with merged bytes; the local file \
                             remains pre-merge until the next snap"
                        );
                    }
                    merged
                }
                Ok(None) => cbor.clone(),
                Err(e) => {
                    tracing::warn!(
                        vault = vault_name,
                        attempt,
                        error = %e,
                        "convergence merge failed — falling back to publishing \
                         local-only TN; the next attempt will retry the merge"
                    );
                    cbor.clone()
                }
            };

        // -- Build enriched Node with history --
        let published_node =
            build_published_node(&merged_cbor, prev_node.as_ref(), prev_encrypted_hash)?;
        let history_count = published_node.entries.len() - 1;

        let enriched_cbor = published_node
            .to_vec()
            .map_err(|e| anyhow!("CBOR encode enriched TN: {e}"))?;

        // -- Age-encrypt + upload --
        let encrypted = age_encrypt_for_recipients(&enriched_cbor, &recipient_strings)
            .context("encrypting for recipients")?;
        let bytes = Bytes::from(encrypted);
        let blob_id = blob_store
            .blob_upload_bytes(bytes.clone())
            .await
            .map_err(|e| anyhow!("uploading encrypted Transparent Node: {e}"))?;
        let this_hash = blob_id.hash;

        // -- Already-published short-circuit (idempotent) --
        if prev_encrypted_hash.is_some_and(|h| h == this_hash) {
            tracing::info!(
                vault = vault_name,
                revision = prev_revision,
                "snapshot already published at current revision"
            );
            return Ok(());
        }

        tracing::info!(
            vault = vault_name,
            attempt,
            encrypted_blob = %this_hash.fmt_short(),
            size = blob_id.size,
            recipients = key_names.len(),
            history_entries = history_count,
            "encrypted Transparent Node uploaded"
        );

        // -- Sign + publish at prev_revision + 1 --
        let new_revision = prev_revision + 1;
        let message = sign_registry_entry(&signing_key, vault_id, this_hash, new_revision)?;

        registry
            .set(message)
            .await
            .context("publishing registry entry")?;

        // -- Verify our entry won the registry race --
        match registry
            .get(&stream_key)
            .await
            .context("verifying our registry entry won")?
        {
            Some(after) if after.hash == this_hash => {
                let pub_key_hex = hex::encode(verifying_key.to_bytes());
                tracing::info!(
                    vault = vault_name,
                    revision = new_revision,
                    encrypted_blob = %this_hash.fmt_short(),
                    public_key = pub_key_hex,
                    attempt,
                    "snapshot published to registry"
                );
                encrypted_bytes = Some(bytes);
                encrypted_hash = Some(this_hash);
                won = true;
                break;
            }
            Some(other) => {
                tracing::info!(
                    vault = vault_name,
                    attempt,
                    our_hash = %this_hash.fmt_short(),
                    winning_hash = %other.hash.fmt_short(),
                    winning_revision = other.revision,
                    "registry race: another publish won — retrying with their state as prev"
                );
                // Fall through to backoff + next iteration.
            }
            None => {
                tracing::warn!(
                    vault = vault_name,
                    attempt,
                    "registry returned None immediately after set — registry not converged?"
                );
            }
        }

        // Backoff before retry: exponential ceiling + jitter. Jitter
        // breaks the thundering-herd pattern where N contestants all
        // wake on the same multiple-of-base interval and race
        // immediately — without it, 10-way contention serialises into
        // a synchronised retry storm and the retry budget runs out
        // before all writers converge.
        let exp = BASE_BACKOFF_MS.saturating_mul(1u64 << attempt.min(8));
        let jitter = (rand::random::<u64>() % exp.max(1)).max(1);
        tokio::time::sleep(std::time::Duration::from_millis(jitter)).await;
    }

    if !won {
        return Err(anyhow!(
            "publish for vault '{vault_name}' did not converge after \
             {MAX_PUBLISH_RETRIES} attempts — registry contention is \
             extreme or another writer is overwriting our entries"
        ));
    }

    // SAFETY: encrypted_bytes / encrypted_hash are Some when won == true.
    let encrypted_bytes = encrypted_bytes.expect("set in the win branch");
    let encrypted_hash = encrypted_hash.expect("set in the win branch");

    // -- Mirror encrypted TN to meta_targets (relay stores for cross-device sync) --
    // Skips any target equal to the primary blob_store (already written above).
    // A failure to mirror to one target does not fail the publish: the encrypted
    // TN is on the primary, the registry entry has its hash, and a later publish
    // can re-attempt the mirror. Loud-warn so operators notice.
    for meta_target in &vault.meta_targets {
        if meta_target == blob_store_name {
            continue;
        }
        match resolve_store(&ctx.stores, meta_target) {
            Ok(target_store) => {
                match target_store
                    .blob_upload_bytes(encrypted_bytes.clone())
                    .await
                {
                    Ok(mirror_id) => {
                        debug_assert_eq!(mirror_id.hash, encrypted_hash);
                        tracing::info!(
                            vault = vault_name,
                            meta_target = %meta_target,
                            encrypted_blob = %encrypted_hash.fmt_short(),
                            "mirrored encrypted Transparent Node to meta_target"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            vault = vault_name,
                            meta_target = %meta_target,
                            error = %e,
                            "could not mirror encrypted Transparent Node — primary upload still succeeded"
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    vault = vault_name,
                    meta_target = %meta_target,
                    error = %e,
                    "skipping unknown meta_target store"
                );
            }
        }
    }

    // -- Ensure one-time recovery entry exists --
    if let Err(e) = ensure_recovery_entry(
        registry.as_ref(),
        vault_name,
        &recovery_secret,
        vault_id,
        &verifying_key,
    )
    .await
    {
        tracing::warn!(
            vault = vault_name,
            error = %e,
            "could not ensure recovery registry entry — publish still succeeded"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use s5_fs_v2::node::{NodeKind, TraversalContext};

    #[test]
    fn device_signing_key_is_deterministic_and_vault_independent() {
        // The device's signing key is the same iroh transport key for
        // every vault — per-vault disambiguation lives in `VAULT_ID`,
        // not in a derived per-vault keypair.
        let secret = [42u8; 32];
        let k1 = device_signing_key(&secret);
        let k2 = device_signing_key(&secret);
        assert_eq!(k1.to_bytes(), k2.to_bytes());
    }

    #[test]
    fn vault_id_derivation_is_deterministic() {
        let recovery_secret = [7u8; 32];
        let v1 = derive_vault_id(&recovery_secret);
        let v2 = derive_vault_id(&recovery_secret);
        assert_eq!(v1, v2);
        assert_eq!(v1.len(), 16);

        let other_secret = [9u8; 32];
        let v3 = derive_vault_id(&other_secret);
        assert_ne!(v1, v3);
    }

    #[test]
    fn recovery_signing_key_is_deterministic() {
        let recovery_secret = [3u8; 32];
        let k1 = recovery_signing_key(&recovery_secret);
        let k2 = recovery_signing_key(&recovery_secret);
        assert_eq!(k1.to_bytes(), k2.to_bytes());

        let other = [4u8; 32];
        let k3 = recovery_signing_key(&other);
        assert_ne!(k1.to_bytes(), k3.to_bytes());
    }

    #[test]
    fn sign_and_verify_round_trip() {
        let secret = [1u8; 32];
        let signing_key = device_signing_key(&secret);
        let recovery_secret = [2u8; 32];
        let vault_id = derive_vault_id(&recovery_secret);
        let root_hash = s5_core::Hash::from([99u8; 32]);

        let message = sign_registry_entry(&signing_key, vault_id, root_hash, 1).unwrap();
        assert_eq!(message.revision, 1);
        assert_eq!(message.hash, root_hash);
        assert_eq!(message.signature.len(), 64); // Ed25519 signature
        assert!(matches!(
            message.key,
            StreamKey::Vault { vault_id: vid, .. } if vid == vault_id
        ));
    }

    #[test]
    fn age_recipient_encrypt_round_trip() {
        // Generate a test identity
        let identity = age::x25519::Identity::generate();
        let recipient = identity.to_public();
        let recipient_str = recipient.to_string();

        let plaintext = b"test transparent node cbor data";
        let encrypted = age_encrypt_for_recipients(plaintext, &[recipient_str]).unwrap();

        // Decrypt with the identity
        let decryptor = age::Decryptor::new(&encrypted[..]).unwrap();
        let mut decrypted = vec![];
        let mut reader = decryptor
            .decrypt(std::iter::once(&identity as &dyn age::Identity))
            .unwrap();
        std::io::Read::read_to_end(&mut reader, &mut decrypted).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn age_encrypt_no_recipients_fails() {
        let result = age_encrypt_for_recipients(b"data", &[]);
        assert!(result.is_err());
    }

    #[test]
    fn age_decrypt_with_secret_key_round_trip() {
        use age::secrecy::ExposeSecret;

        let identity = age::x25519::Identity::generate();
        let recipient_str = identity.to_public().to_string();
        let secret_str = identity.to_string().expose_secret().to_string();

        let plaintext = b"disaster recovery test data";
        let encrypted = age_encrypt_for_recipients(plaintext, &[recipient_str]).unwrap();

        let decrypted = age_decrypt_with_secret_key(&encrypted, &secret_str).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn age_decrypt_with_wrong_secret_key_fails() {
        let identity = age::x25519::Identity::generate();
        let recipient_str = identity.to_public().to_string();

        // Encrypt for one identity
        let plaintext = b"test";
        let encrypted = age_encrypt_for_recipients(plaintext, &[recipient_str]).unwrap();

        // Try to decrypt with a different identity
        let other = age::x25519::Identity::generate();
        let other_secret = {
            use age::secrecy::ExposeSecret;
            other.to_string().expose_secret().to_string()
        };
        let result = age_decrypt_with_secret_key(&encrypted, &other_secret);
        assert!(result.is_err());
    }

    /// Helper: build a simple Transparent Node with a Link entry at "".
    fn make_transparent_node(hash: [u8; 32]) -> Node {
        let entry = NodeEntry {
            content: Some(ContentRef {
                structural: Structural::Link,
                hash,
                size: 0,
                plaintext_hash: None,
                stored_blocks: None,
            }),
            semantic: None,
            child_context: Some(Box::new(TraversalContext::default())),
            tombstone: None,
        };
        Node::transparent(entry)
    }

    /// Helper: build a Transparent Node carrying a `KEY_SLOT_RECOVERY`
    /// slot (the v3 vault-root shape). Used to exercise the substitution
    /// defense.
    fn make_vault_root_with_recovery(hash: [u8; 32], recovery_secret: [u8; 32]) -> Node {
        use s5_fs_v2::snapshot::KEY_SLOT_RECOVERY;
        let mut keys = std::collections::BTreeMap::new();
        keys.insert(KEY_SLOT_RECOVERY, recovery_secret);
        let ctx = TraversalContext {
            keys: Some(keys),
            ..Default::default()
        };
        let entry = NodeEntry {
            content: Some(ContentRef {
                structural: Structural::Link,
                hash,
                size: 0,
                plaintext_hash: None,
                stored_blocks: None,
            }),
            semantic: None,
            child_context: Some(Box::new(ctx)),
            tombstone: None,
        };
        Node::transparent(entry)
    }

    #[test]
    fn substitution_defense_accepts_matching_recovery_secret() {
        let recovery_secret = [7u8; 32];
        let local = recovery_secret;
        let incoming = make_vault_root_with_recovery([1u8; 32], recovery_secret);

        verify_recovery_secret_invariant(&local, &incoming).expect("matching secret accepted");
    }

    #[test]
    fn substitution_defense_rejects_mismatched_recovery_secret() {
        let local = [7u8; 32];
        let attacker_secret = [8u8; 32];
        let incoming = make_vault_root_with_recovery([1u8; 32], attacker_secret);

        let err = verify_recovery_secret_invariant(&local, &incoming)
            .expect_err("mismatched secret must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("substitution"),
            "error should mention substitution: {msg}"
        );
    }

    #[test]
    fn substitution_defense_rejects_missing_recovery_slot() {
        let local = [7u8; 32];
        // Plain Transparent Node — no KEY_SLOT_RECOVERY slot
        let incoming = make_transparent_node([1u8; 32]);

        let err = verify_recovery_secret_invariant(&local, &incoming)
            .expect_err("incoming root without KEY_SLOT_RECOVERY must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("KEY_SLOT_RECOVERY"),
            "error should mention the missing slot: {msg}"
        );
    }

    #[test]
    fn build_published_node_first_publish_no_history() {
        let current = make_transparent_node([1u8; 32]);
        let cbor = current.to_vec().unwrap();

        let node = build_published_node(&cbor, None, None).unwrap();

        assert_eq!(node.header.kind, NodeKind::Transparent);
        assert_eq!(node.entries.len(), 1); // only ""
        assert!(node.entries.contains_key(""));
    }

    #[test]
    fn build_published_node_accumulates_history() {
        let current = make_transparent_node([2u8; 32]);
        let cbor = current.to_vec().unwrap();

        // Simulate a previous published TN with existing history
        let mut prev = make_transparent_node([1u8; 32]);
        prev.entries.insert(
            "2025-01-01T00:00:00Z".to_string(),
            NodeEntry {
                content: Some(ContentRef {
                    structural: Structural::Leaf,
                    hash: [99u8; 32],
                    size: 0,
                    plaintext_hash: None,
                    stored_blocks: None,
                }),
                semantic: None,
                child_context: None,
                tombstone: None,
            },
        );

        let prev_hash = Hash::from([1u8; 32]);
        let node = build_published_node(&cbor, Some(&prev), Some(prev_hash)).unwrap();

        // "" (current) + "2025-01-01..." (carried forward) + new timestamp entry
        assert_eq!(node.entries.len(), 3);
        assert!(node.entries.contains_key(""));
        assert!(node.entries.contains_key("2025-01-01T00:00:00Z"));

        // The new timestamp entry should be there (exact key depends on current time)
        let new_history: Vec<_> = node
            .entries
            .keys()
            .filter(|k| !k.is_empty() && k.as_str() != "2025-01-01T00:00:00Z")
            .collect();
        assert_eq!(new_history.len(), 1);

        // The new history entry should point to the previous encrypted hash
        let entry = node.entries.get(new_history[0]).unwrap();
        assert_eq!(entry.content.as_ref().unwrap().hash, [1u8; 32]);
    }

    #[test]
    fn build_published_node_second_publish_adds_one_entry() {
        let current = make_transparent_node([3u8; 32]);
        let cbor = current.to_vec().unwrap();

        // Previous TN with no history (first publish)
        let prev = make_transparent_node([2u8; 32]);
        let prev_hash = Hash::from([50u8; 32]);

        let node = build_published_node(&cbor, Some(&prev), Some(prev_hash)).unwrap();

        // "" (current) + 1 new history entry
        assert_eq!(node.entries.len(), 2);
        assert!(node.entries.contains_key(""));
    }
}
