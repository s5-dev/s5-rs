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

use std::sync::Arc;

use anyhow::{Context, anyhow};
use bytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use s5_core::blob::Blobs;
use s5_core::{BlobsRead, Hash, RegistryApi, StreamKey, StreamMessage};
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

/// `S5_CONVERGENCE_TREEDIFF` — compute the convergence merge by pruning
/// byte-identical subtrees (tree-diff) instead of re-folding the whole corpus.
/// Byte-identical to the full path; default off until soaked.
fn convergence_treediff_enabled() -> bool {
    std::env::var("S5_CONVERGENCE_TREEDIFF")
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

/// `S5_CONVERGENCE_TREEDIFF_VERIFY` — run BOTH the treediff and full convergence
/// merge, assert the roots match, and publish the FULL (proven) result. The
/// validation soak before trusting treediff alone; only consulted when
/// `S5_CONVERGENCE_TREEDIFF` is also set.
fn convergence_treediff_verify_enabled() -> bool {
    std::env::var("S5_CONVERGENCE_TREEDIFF_VERIFY")
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

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

/// The fixed 16-byte vault id for a *well-known* vault — one located by a
/// constant domain rather than a per-vault `recovery_secret`:
///
/// ```text
/// well_known_vault_id(domain) = blake3(domain)[..16]
/// ```
///
/// Used by the master-anchored special vaults (`identity_secrets`, `stores`)
/// and the identity bundle, all of which are found at `(master_pubkey,
/// well_known_vault_id(...))` — no secret needed to locate them (confidentiality
/// is the age layer; see `docs/reference/special-vaults.md`). Any peer can
/// compute it from the public domain string.
pub fn well_known_vault_id(domain: &str) -> [u8; 16] {
    let h = blake3::hash(domain.as_bytes());
    let mut id = [0u8; 16];
    id.copy_from_slice(&h.as_bytes()[..16]);
    id
}

/// Derive a vault's per-vault, **non-authoritative** *discovery* signing key
/// from the identity-wide recovery `seed` (held in the `config` vault) and the
/// vault's `vault_id`:
///
/// ```text
/// discovery_signing_seed = blake3("s5-vault-discovery" || seed || vault_id)
/// discovery_signing_key  = ed25519_keypair_from_seed(discovery_signing_seed)
/// ```
///
/// It is derived from the `config`-vault `seed`, so a recovering or
/// freshly-paired device can derive it from the config vault alone (`seed` + the
/// `vault_id` directory), with no vault root and no per-vault secret. It carries
/// **no write/merge authority**: `discovery_pubkey`
/// appears in no identity bundle's `signers[]`, so the membership ACL never
/// accepts it as a merge source. Its sole job is the owner's private
/// `(discovery_pubkey, vault_id) → current HEAD` breadcrumb, mirrored into the
/// owner's own durable store on each publish and read back on recovery
/// (`docs/reference/registry-durability.md`). The locator is unguessable without
/// `seed`.
pub fn discovery_signing_key(seed: &[u8; 32], vault_id: &[u8; 16]) -> SigningKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"s5-vault-discovery");
    hasher.update(seed);
    hasher.update(vault_id);
    let signing_seed: [u8; 32] = *hasher.finalize().as_bytes();
    SigningKey::from_bytes(&signing_seed)
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

/// Mirror the just-published HEAD under the vault's non-authoritative discovery
/// key, so a paper recovery or a freshly-paired device can find it from the
/// `config` vault alone (`seed` + the vault directory) — no vault root, no
/// `recovery_secret`. The entry lives at `(discovery_pubkey, vault_id)` and
/// points at the same encrypted-TN hash as the device-keyed HEAD; it carries no
/// write/merge authority (peers never trust `discovery_pubkey` as a writer), so
/// it is a pure owner-private breadcrumb in the owner's durable store.
///
/// Best-effort: a failure only means the breadcrumb is stale/absent, not that
/// the publish failed. Writes only when the pointer actually changed, so it adds
/// no churn on a re-publish of unchanged content.
async fn publish_discovery_entry(
    registry: &dyn RegistryApi,
    seed: &[u8; 32],
    vault_id: [u8; 16],
    head_hash: Hash,
) -> anyhow::Result<()> {
    let discovery_key = discovery_signing_key(seed, &vault_id);
    let pubkey = VerifyingKey::from(&discovery_key).to_bytes();
    let stream_key = StreamKey::Vault { pubkey, vault_id };

    let revision = match registry.get(&stream_key).await? {
        Some(prev) if prev.hash == head_hash => return Ok(()), // already current
        Some(prev) => prev.revision + 1,
        None => 1,
    };
    let message = sign_registry_entry(&discovery_key, vault_id, head_hash, revision)?;
    registry
        .set(message)
        .await
        .context("publishing discovery registry entry")?;
    Ok(())
}

/// Derive a vault's `vault_id` from a loaded vault-root `TraversalContext`
/// (its `KEY_SLOT_RECOVERY` slot). The read-side counterpart to
/// [`recovery_secret_from_vault_root`] for callers that already hold the
/// decrypted root context (e.g. the restore/list-snapshots paths).
pub(crate) fn vault_id_from_context(
    ctx: &s5_fs_v2::node::TraversalContext,
) -> anyhow::Result<[u8; 16]> {
    let recovery_secret = ctx
        .keys
        .as_ref()
        .and_then(|map| map.get(&s5_fs_v2::snapshot::KEY_SLOT_RECOVERY))
        .copied()
        .ok_or_else(|| {
            anyhow!(
                "vault root TraversalContext has no KEY_SLOT_RECOVERY slot — \
                 was this vault created before the v3 schema?"
            )
        })?;
    Ok(derive_vault_id(&recovery_secret))
}

/// Download a published Transparent Node blob by hash and parse it, trying an
/// age-decrypt with the supplied identity files first and falling back to a
/// direct CBOR parse (the read-side counterpart to `plaintext_published_tn`).
///
/// Used to follow a vault's published history chain: each history entry in a
/// published TN points at a *previous* encrypted-TN blob, and resolving a
/// `#snap` selector walks back along those pointers.
pub(crate) async fn download_published_node(
    blob_store: &dyn Blobs,
    hash: Hash,
    identity_files: &[String],
) -> anyhow::Result<Node> {
    let blob_bytes = blob_store
        .blob_download(hash)
        .await
        .map_err(|e| anyhow!("downloading published TN {}: {e}", hash.fmt_short()))?;

    if !identity_files.is_empty() {
        match age_decrypt_with_identity_files(&blob_bytes, identity_files) {
            Ok(plaintext) => {
                Node::from_bytes(&plaintext).map_err(|e| anyhow!("CBOR decode published TN: {e}"))
            }
            Err(decrypt_err) => match Node::from_bytes(&blob_bytes) {
                Ok(node) => Ok(node),
                Err(_) => Err(decrypt_err).context("decrypting published Transparent Node"),
            },
        }
    } else {
        Node::from_bytes(&blob_bytes).map_err(|e| anyhow!("CBOR decode published TN: {e}"))
    }
}

/// Fetch the previously published encrypted Transparent Node, decrypt it,
/// and return it as a parsed `Node` along with the encrypted blob hash.
///
/// Returns `None` if nothing was previously published (no registry entry).
pub(crate) async fn fetch_previous_published_node(
    registry: &dyn RegistryApi,
    blob_store: &dyn Blobs,
    stream_key: &StreamKey,
    identity_files: &[String],
) -> anyhow::Result<Option<(Node, Hash, u64)>> {
    let entry = match registry.get(stream_key).await? {
        Some(e) => e,
        None => return Ok(None),
    };

    let node = download_published_node(blob_store, entry.hash, identity_files).await?;
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
    tn_history_keep: Option<usize>,
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

    // Bound the history chain to the last `n` entries, if configured. The
    // `""` current-snapshot entry is always kept and never counted. History
    // keys are RFC3339 timestamps and `node.entries` is a `BTreeMap`, so
    // iteration is ascending == chronological — the oldest `len - n` keys
    // are the front of the list. Dropping them unbinds their old
    // encrypted-TN blobs (their last referrer), making those blobs
    // unreachable and thus eligible for the cold-store GC. `None` =>
    // unbounded (the historical behavior; zero impact on other vaults).
    if let Some(n) = tn_history_keep {
        let mut hist: Vec<String> = node
            .entries
            .keys()
            .filter(|k| !k.is_empty())
            .cloned()
            .collect();
        let drop_count = hist.len().saturating_sub(n);
        for k in hist.drain(..drop_count) {
            node.entries.remove(&k);
        }
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
    blob_store: Arc<dyn Blobs>,
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
    let read_store: Arc<dyn BlobsRead> = blob_store.clone();
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

    // Convergence merge (the ~25 s publish floor: it re-derives the whole
    // 2.58 M-entry tree every cycle — entries_changed≈corpus). Mode by env:
    //  - off (default): full O(corpus) `merge_and_persist`.
    //  - S5_CONVERGENCE_TREEDIFF: `merge_and_persist_treediff` — prune subtrees
    //    byte-identical between prev-published and local (by content hash), diff
    //    only the rest, feed the small change-layer to the oracle-tested
    //    structural merge. Byte-identical to the full path, O(changed leaves).
    //  - S5_CONVERGENCE_TREEDIFF_VERIFY: run BOTH, assert roots match, and
    //    PUBLISH THE FULL (proven) result — a treediff bug is loud-but-harmless
    //    during the soak. Drop the verify after a clean soak for the real win.
    let t_merge = std::time::Instant::now();
    let treediff = convergence_treediff_enabled();
    let merge_result = if treediff && convergence_treediff_verify_enabled() {
        let t_td = std::time::Instant::now();
        let td = pipeline
            .merge_and_persist_treediff(&prev_snap, chunk_mask, &local_snap, blob_store.as_ref())
            .await
            .context("convergence treediff merge")?;
        let td_ms = t_td.elapsed().as_millis() as u64;
        let full = pipeline
            .merge_and_persist(&prev_snap, chunk_mask, &local_snap, blob_store.as_ref())
            .await
            .context("merging local TN with previously published TN")?;
        match (&td, &full) {
            (Some((th, tph, _)), Some((fh, fph, _))) => {
                if th != fh || tph != fph {
                    tracing::error!(
                        td_root = ?th, full_root = ?fh, td_ms,
                        "S5_CONVERGENCE_TREEDIFF_VERIFY: ROOT MISMATCH — publishing the FULL result (treediff has a bug)"
                    );
                } else {
                    tracing::info!(
                        td_ms,
                        "convergence treediff verify OK (root matches full path)"
                    );
                }
            }
            (None, None) => {
                tracing::info!(td_ms, "convergence treediff verify OK (both empty)");
            }
            _ => tracing::error!(
                td_ok = td.is_some(),
                full_ok = full.is_some(),
                "S5_CONVERGENCE_TREEDIFF_VERIFY: shape divergence — publishing the FULL result"
            ),
        }
        full
    } else if treediff {
        pipeline
            .merge_and_persist_treediff(&prev_snap, chunk_mask, &local_snap, blob_store.as_ref())
            .await
            .context("convergence treediff merge")?
    } else {
        pipeline
            .merge_and_persist(&prev_snap, chunk_mask, &local_snap, blob_store.as_ref())
            .await
            .context("merging local TN with previously published TN")?
    };
    let merge_ms = t_merge.elapsed().as_millis() as u64;

    let Some((unified_root, unified_plaintext_hash, stats)) = merge_result else {
        // Both sides empty after tombstone filter — emit an empty TN.
        // Fall through to local_cbor unchanged; nothing to merge.
        tracing::info!(merge_ms, "convergence merge: empty result (no-op union)");
        return Ok(None);
    };
    tracing::info!(
        merge_ms,
        treediff,
        entries_changed = stats.entries_changed,
        entries_reused = stats.entries_reused,
        leaf_nodes = stats.leaf_nodes,
        nodes_uploaded = stats.nodes_uploaded,
        nodes_deduped = stats.nodes_deduped,
        "convergence merge: local vs prev-published"
    );

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

/// Load a configured vault's current *local* root and derive its `vault_id`
/// (`derive_vault_id(recovery_secret)`), reading the working snapshot directly —
/// no registry round-trip. Returns `Ok(None)` when the vault has no local
/// snapshot yet (nothing to record in the recovery directory). Used by the
/// `config` vault bootstrap ([`crate::config_vault::publish_bootstrap_config`])
/// to build the vault directory at startup.
pub(crate) fn vault_id_for_config(
    config: &crate::config::S5NodeConfig,
    vault_name: &str,
) -> anyhow::Result<Option<[u8; 16]>> {
    let vault = resolve_vault(config, vault_name)?;
    let (_, identity_files) = resolve_vault_key_info(config, vault_name)
        .context("resolving vault key for local decryption")?;
    let current_path = vault_root_path(&vault.root_path);
    let Some(cbor) = load_vault_root_cbor(&current_path, &identity_files)? else {
        return Ok(None);
    };
    let recovery_secret = recovery_secret_from_vault_root(&cbor)?;
    Ok(Some(derive_vault_id(&recovery_secret)))
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
    let (vault, key_configs, vault_identity_files, data_store_name, meta_store_name) = {
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
        let data_store_name = config.vault_data_store(vault_name, &vault)?.to_string();
        let meta_store_name = config.vault_meta_store(vault_name, &vault)?.to_string();
        (
            vault,
            key_configs,
            id_files,
            data_store_name,
            meta_store_name,
        )
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
    //
    // Two sources, unioned:
    //   1. The hand-listed `vault.recipients = [...]` config (legacy
    //      path; each name resolves to a `[key.<name>]` block).
    //   2. The `keyAgreement` recipients of every member's published
    //      DidDocument (step 5 — auto-derived from the bundle layer).
    //
    // (2) is the production-shape path: adding a friend to a vault is
    //     `members = [..., "alice"]` + `[friend.alice].id = "did:s5:..."`,
    //     no recipient hand-edit needed. (1) remains because some
    //     recipients (e.g. paper recovery) live as standalone keys
    //     without a published DID.
    let mut recipient_strings = Vec::new();
    let mut identity_files = Vec::new();
    let mut seen = std::collections::HashSet::<String>::new();
    for key_config in &key_configs {
        if seen.insert(key_config.public_key.clone()) {
            recipient_strings.push(key_config.public_key.clone());
        }
        if let Some(ref id_file) = key_config.identity_file {
            identity_files.push(id_file.clone());
        }
    }
    if let Some(membership) = ctx.membership.as_ref() {
        let state = membership.read().await;
        if let Some(vm) = state.vaults.get(vault_name) {
            for r in &vm.age_recipients {
                if seen.insert(r.clone()) {
                    recipient_strings.push(r.clone());
                }
            }
        }
    }

    // `plaintext_published_tn` skips the age envelope on publication —
    // see NodeConfigVault docs. A recipient-less, plaintext publish is the
    // public-publisher pattern (one writer, many anonymous read-only
    // consumers); a recipient list with the flag set is harmless (we'd
    // just ignore the recipients below, but flag a config bug).
    let plaintext_publish = vault.plaintext_published_tn;
    if plaintext_publish && !recipient_strings.is_empty() {
        tracing::warn!(
            vault = vault_name,
            recipient_count = recipient_strings.len(),
            "plaintext_published_tn = true but recipients/members resolved to a non-empty set — \
             ignoring recipients for the publish envelope (TN will be plaintext)"
        );
    }
    if !plaintext_publish && recipient_strings.is_empty() {
        return Err(anyhow!(
            "publish requires at least one recipient (vault.recipients or vault.members), \
             unless `plaintext_published_tn = true` is set on the vault"
        ));
    }

    // -- Resolve blob store: the encrypted TN goes to the vault's META
    // primary (which defaults to the data store) — see decision D1.
    let blob_store_name = &meta_store_name;
    let blob_store = resolve_store(&ctx.stores, blob_store_name)?;

    // -- Derive vault_id from the vault root's KEY_SLOT_RECOVERY slot --
    let recovery_secret = recovery_secret_from_vault_root(&cbor)?;
    let vault_id = derive_vault_id(&recovery_secret);

    // Register `vault_name → vault_id` in shared membership state so
    // the per-vault registry ACL can resolve wire-level `vault_id`
    // back to a name. First publish for each vault populates the
    // mapping; subsequent publishes are a no-op. The membership
    // subscriber reads this mapping to know which (peer, vault_id)
    // data keys to subscribe to.
    if let Some(membership) = ctx.membership.as_ref() {
        let mut state = membership.write().await;
        let changed = state.register_vault_id(vault_name, vault_id);
        if changed {
            tracing::info!(
                vault = vault_name,
                vault_id = %hex::encode(vault_id),
                "registered vault_id in membership state"
            );
            drop(state);
            if let Some(refresh) = ctx.membership_refresh.as_ref() {
                refresh.notify_one();
            }
        }
    }

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

    // The plaintext snapshot tree root of our local TN (the `""` entry's CAS
    // hash). Stable for identical content — the fs_v2 encryption is
    // deterministic, so an unchanged tree always hashes the same. Used below to
    // detect a true no-op snap: `build_published_node` appends a
    // fresh-timestamped history entry on every publish, so the published TN's
    // *encrypted* hash always changes and the in-loop
    // `this_hash == prev_encrypted_hash` guard can never fire — but the
    // underlying tree root does not, so compare that instead.
    let local_root_hash: Option<[u8; 32]> = Node::from_bytes(&cbor)
        .ok()
        .as_ref()
        .and_then(|n| n.transparent_entry())
        .and_then(|e| e.content.as_ref())
        .map(|c| c.hash);

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
        // DIAGNOSTIC 2026-06-18: split the ~28 s publish gap into fetch vs the
        // convergence merge (timed in maybe_merge_with_prev_published).
        let t_fetch = std::time::Instant::now();
        let (prev_node, prev_encrypted_hash, prev_revision) = if !identity_files.is_empty() {
            match fetch_previous_published_node(
                registry.as_ref(),
                blob_store.as_ref(),
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

        tracing::info!(
            attempt,
            fetch_ms = t_fetch.elapsed().as_millis() as u64,
            has_prev = prev_node.is_some(),
            "publish: fetched previous published TN"
        );

        // -- True no-op short-circuit --
        // If our local snapshot tree root matches the already-published one,
        // there is genuinely nothing new to publish. Skip the history append,
        // encrypt, upload, durability `sync()` (one Sia slab ≈ 40 s on the
        // packed path), and the registry bump entirely. Only fires once a vault
        // has a published prev to compare against (first publish always
        // proceeds); a peer-diverged tree has a different root and falls
        // through to the merge below.
        if let (Some(local), Some(prev)) = (local_root_hash, prev_node.as_ref()) {
            let prev_root = prev
                .transparent_entry()
                .and_then(|e| e.content.as_ref())
                .map(|c| c.hash);
            if prev_root == Some(local) {
                tracing::info!(
                    vault = vault_name,
                    revision = prev_revision,
                    "snapshot unchanged from published revision — skipping publish (no-op)"
                );
                return Ok(());
            }
        }

        // -- Convergence merge: union local + previously published --
        let merged_cbor =
            match maybe_merge_with_prev_published(&cbor, prev_node.as_ref(), blob_store.clone())
                .await
            {
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
        let published_node = build_published_node(
            &merged_cbor,
            prev_node.as_ref(),
            prev_encrypted_hash,
            vault.tn_history_keep,
        )?;
        let history_count = published_node.entries.len() - 1;

        let enriched_cbor = published_node
            .to_vec()
            .map_err(|e| anyhow!("CBOR encode enriched TN: {e}"))?;

        // -- Encrypt (or skip) + upload --
        // With `plaintext_published_tn`, the published TN goes onto the wire
        // as raw CBOR. Authenticity still hinges on the signed registry
        // entry (BLAKE3 hash of the published blob).
        let bytes = if plaintext_publish {
            Bytes::from(enriched_cbor.clone())
        } else {
            let encrypted = age_encrypt_for_recipients(&enriched_cbor, &recipient_strings)
                .context("encrypting for recipients")?;
            Bytes::from(encrypted)
        };
        let blob_id = blob_store
            .blob_upload_bytes(bytes.clone())
            .await
            .map_err(|e| anyhow!("uploading published Transparent Node: {e}"))?;
        let this_hash = blob_id.hash;

        // (The idempotent "already published" short-circuit lives at the top of
        // the loop now — a plaintext snapshot-root compare. An encrypted-hash
        // compare here is unreachable: we only get past that skip when content
        // changed or there's no prev, and `build_published_node` appends a
        // fresh-timestamped history entry, so `this_hash` never equals
        // `prev_encrypted_hash`.)

        tracing::info!(
            vault = vault_name,
            attempt,
            encrypted_blob = %this_hash.fmt_short(),
            size = blob_id.size,
            recipients = key_names.len(),
            history_entries = history_count,
            "encrypted Transparent Node uploaded"
        );

        // -- Durability barrier (unconditional) --
        // Flush the blob store ONCE before the registry entry is set, so a root
        // is never published ahead of its data — the content-addressed
        // crash-safety contract (`blob_store` holds the full published vault:
        // file blobs, tree nodes via the meta-tee, and the encrypted TN).
        //
        // REQUIRED for the packing store: a packed vault's chunks sit in local
        // staging until `sync()` force-flushes them into Sia packs — publishing
        // the HEAD first would reference non-durable data. Also covers the
        // batched-fjall case (writes only in the write buffer until now). Cheap
        // for stores that already persist per write (a near-no-op fsync).
        // meta_store is a re-derivable local cache (nodes also live here via the
        // tee), so it needs no separate barrier.
        {
            let t = std::time::Instant::now();
            blob_store
                .blob_sync()
                .await
                .context("syncing blob store before publishing registry entry")?;
            tracing::info!(
                vault = vault_name,
                sync_ms = t.elapsed().as_millis() as u64,
                "publish: blob store synced (durability barrier before registry.set)"
            );
        }

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

    // -- Mirror encrypted TN to the data store when meta rides a separate
    // store (relay/cross-device sync setups) — so a reader following only
    // the data store still finds the TN. A failure to mirror does not fail
    // the publish: the TN is on the meta primary, the registry entry has
    // its hash, and a later publish re-attempts. Loud-warn so operators
    // notice.
    for meta_target in [&data_store_name] {
        if *meta_target == *blob_store_name {
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

    // -- Mirror the HEAD under the discovery key (paper/add-device recovery) --
    // Reachable from the config vault alone (seed + vault directory). Skipped
    // until the bootstrap publish has populated the seed.
    if let Some(seed) = ctx.discovery_seed.get() {
        if let Err(e) =
            publish_discovery_entry(registry.as_ref(), seed, vault_id, encrypted_hash).await
        {
            tracing::warn!(
                vault = vault_name,
                error = %e,
                "could not publish discovery entry — publish still succeeded"
            );
        }
    } else {
        tracing::debug!(
            vault = vault_name,
            "discovery seed not yet available; skipping discovery entry"
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
    fn discovery_signing_key_is_deterministic_and_scoped() {
        let seed = [5u8; 32];
        let vault_id = [6u8; 16];

        // Same (seed, vault_id) → same key.
        let k1 = discovery_signing_key(&seed, &vault_id);
        let k2 = discovery_signing_key(&seed, &vault_id);
        assert_eq!(k1.to_bytes(), k2.to_bytes());

        // Distinct per vault under the same identity seed...
        let other_vault = discovery_signing_key(&seed, &[7u8; 16]);
        assert_ne!(k1.to_bytes(), other_vault.to_bytes());

        // ...and distinct per identity seed for the same vault.
        let other_seed = discovery_signing_key(&[8u8; 32], &vault_id);
        assert_ne!(k1.to_bytes(), other_seed.to_bytes());
    }

    /// The recovery contract: with only the config-vault `seed` and a vault's
    /// `vault_id`, a fresh device re-derives the discovery locator and finds the
    /// current HEAD — no device pubkey, no `recovery_secret`. Idempotent on an
    /// unchanged HEAD; bumps on a new one.
    #[tokio::test]
    async fn discovery_entry_is_findable_from_seed_alone() {
        use s5_registry::MemoryRegistry;

        let registry = MemoryRegistry::new();
        let seed = [11u8; 32];
        let vault_id = derive_vault_id(&[22u8; 32]);
        let head = Hash::from([33u8; 32]);

        publish_discovery_entry(&registry, &seed, vault_id, head)
            .await
            .unwrap();

        let discovery_pubkey =
            VerifyingKey::from(&discovery_signing_key(&seed, &vault_id)).to_bytes();
        let locator = StreamKey::Vault {
            pubkey: discovery_pubkey,
            vault_id,
        };
        let entry = registry.get(&locator).await.unwrap().unwrap();
        assert_eq!(entry.hash, head, "the breadcrumb points at the HEAD");
        let rev1 = entry.revision;

        // Re-publishing the same HEAD is a no-op (no churn).
        publish_discovery_entry(&registry, &seed, vault_id, head)
            .await
            .unwrap();
        assert_eq!(
            registry.get(&locator).await.unwrap().unwrap().revision,
            rev1,
            "unchanged HEAD must not bump the discovery revision"
        );

        // A new HEAD advances the pointer.
        let head2 = Hash::from([44u8; 32]);
        publish_discovery_entry(&registry, &seed, vault_id, head2)
            .await
            .unwrap();
        let entry2 = registry.get(&locator).await.unwrap().unwrap();
        assert_eq!(entry2.hash, head2);
        assert_eq!(entry2.revision, rev1 + 1);
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

        let node = build_published_node(&cbor, None, None, None).unwrap();

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
        let node = build_published_node(&cbor, Some(&prev), Some(prev_hash), None).unwrap();

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

        let node = build_published_node(&cbor, Some(&prev), Some(prev_hash), None).unwrap();

        // "" (current) + 1 new history entry
        assert_eq!(node.entries.len(), 2);
        assert!(node.entries.contains_key(""));
    }

    /// Insert `keys.len()` history entries into `node` under the given
    /// RFC3339 timestamp keys (test helper for the history-bound tests).
    fn add_history_entries(node: &mut Node, keys: &[&str]) {
        for (i, k) in keys.iter().enumerate() {
            node.entries.insert(
                (*k).to_string(),
                NodeEntry {
                    content: Some(ContentRef {
                        structural: Structural::Leaf,
                        hash: [i as u8; 32],
                        size: 0,
                        plaintext_hash: None,
                        stored_blocks: None,
                    }),
                    semantic: None,
                    child_context: None,
                    tombstone: None,
                },
            );
        }
    }

    #[test]
    fn build_published_node_keeps_only_last_n_history() {
        let current = make_transparent_node([9u8; 32]);
        let cbor = current.to_vec().unwrap();

        // Previous TN carrying 6 history entries. `prev_encrypted_hash =
        // None` so no new timestamp entry is added — this isolates the
        // history-bound prune from the carry-forward+append logic.
        let mut prev = make_transparent_node([1u8; 32]);
        add_history_entries(
            &mut prev,
            &[
                "2020-01-01T00:00:00Z",
                "2021-01-01T00:00:00Z",
                "2022-01-01T00:00:00Z",
                "2023-01-01T00:00:00Z",
                "2024-01-01T00:00:00Z",
                "2025-01-01T00:00:00Z",
            ],
        );

        let node = build_published_node(&cbor, Some(&prev), None, Some(3)).unwrap();

        // Exactly the 3 newest history keys + "" survive.
        assert_eq!(node.entries.len(), 4);
        assert!(node.entries.contains_key(""));
        assert!(node.entries.contains_key("2023-01-01T00:00:00Z"));
        assert!(node.entries.contains_key("2024-01-01T00:00:00Z"));
        assert!(node.entries.contains_key("2025-01-01T00:00:00Z"));
        // The 3 oldest were dropped.
        assert!(!node.entries.contains_key("2020-01-01T00:00:00Z"));
        assert!(!node.entries.contains_key("2021-01-01T00:00:00Z"));
        assert!(!node.entries.contains_key("2022-01-01T00:00:00Z"));
    }

    #[test]
    fn build_published_node_none_is_unbounded() {
        let current = make_transparent_node([9u8; 32]);
        let cbor = current.to_vec().unwrap();

        let mut prev = make_transparent_node([1u8; 32]);
        add_history_entries(
            &mut prev,
            &[
                "2020-01-01T00:00:00Z",
                "2021-01-01T00:00:00Z",
                "2022-01-01T00:00:00Z",
                "2023-01-01T00:00:00Z",
                "2024-01-01T00:00:00Z",
                "2025-01-01T00:00:00Z",
            ],
        );

        // None => unbounded: every history entry preserved (regression
        // guard for vaults that never set tn_history_keep).
        let node = build_published_node(&cbor, Some(&prev), None, None).unwrap();
        assert_eq!(node.entries.len(), 7); // "" + 6 carried
    }
}
