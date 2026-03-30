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

use std::io::{Read, Write};

use anyhow::{Context, anyhow};
use bytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey};
use s5_core::blob::BlobStore;
use s5_core::{BlobsRead, BlobsWrite, Hash, RegistryApi, StreamKey, StreamMessage};
use s5_fs_v2::node::{ContentRef, Node, NodeEntry, Structural};

use super::vault_persist::{load_vault_root_cbor, vault_root_path};
use super::{TaskExecutorContext, resolve_key, resolve_store, resolve_vault};

/// Derive the Ed25519 signing key for a vault's snapshot publishing.
///
/// Uses `blake3::derive_key("s5/snapshot/ed25519/{name}", node_secret)` — the
/// same derivation path used by the legacy `SnapshotTimer`.
pub(crate) fn vault_signing_key(node_secret: &[u8; 32], vault_name: &str) -> SigningKey {
    let context = format!("s5/snapshot/ed25519/{vault_name}");
    let derived = blake3::derive_key(&context, node_secret);
    SigningKey::from_bytes(&derived)
}

/// Derive the Ed25519 recovery signing key from an age secret key string.
///
/// `blake3::derive_key("s5/recovery/ed25519/{vault_name}", age_secret.as_bytes())`
/// → Ed25519 signing key. The public key becomes the registry lookup key
/// for disaster recovery.
///
/// The vault name is included so each vault gets its own recovery entry,
/// avoiding collisions when multiple vaults share the same age key.
pub fn recovery_signing_key(age_secret: &str, vault_name: &str) -> SigningKey {
    let context = format!("s5/recovery/ed25519/{vault_name}");
    let derived = blake3::derive_key(&context, age_secret.as_bytes());
    SigningKey::from_bytes(&derived)
}

/// Ensure the one-time recovery registry entry exists.
///
/// The recovery entry maps `recovery_pubkey → vault_signing_pubkey` so that
/// a restorer with just the age paper key can discover the vault's registry
/// entry (which in turn points to the latest encrypted TN).
///
/// Only writes if no entry exists yet (revision == 0).
async fn ensure_recovery_entry(
    registry: &dyn RegistryApi,
    identity_files: &[String],
    vault_name: &str,
    vault_pubkey: &VerifyingKey,
) -> anyhow::Result<()> {
    // We need an age secret key to derive the recovery key.
    // Read the first identity file that contains an age x25519 secret.
    let age_secret = find_age_secret_from_identity_files(identity_files)?;
    let recovery_key = recovery_signing_key(&age_secret, vault_name);
    let recovery_verifying: VerifyingKey = (&recovery_key).into();
    let recovery_stream_key = StreamKey::PublicKeyEd25519(recovery_verifying.to_bytes());

    // Check if entry already exists
    match registry.get(&recovery_stream_key).await {
        Ok(Some(_)) => {
            tracing::debug!(
                vault = vault_name,
                "recovery registry entry already exists"
            );
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

    // The recovery entry's "hash" stores the vault's verifying key (32 bytes).
    // This is a slight abuse of the Hash field, but it's the simplest way to
    // store 32 bytes in a registry entry without adding a new field.
    let vault_pubkey_hash = Hash::from(vault_pubkey.to_bytes());

    let message = sign_registry_entry(&recovery_key, vault_pubkey_hash, 1)?;

    registry
        .set(message)
        .await
        .context("publishing recovery registry entry")?;

    let recovery_hex = hex::encode(recovery_verifying.to_bytes());
    let vault_hex = hex::encode(vault_pubkey.to_bytes());
    tracing::info!(
        vault = vault_name,
        recovery_key = recovery_hex,
        vault_key = vault_hex,
        "recovery registry entry published"
    );

    Ok(())
}

/// Extract the raw age secret key string from identity files.
///
/// Reads each file looking for a line starting with `AGE-SECRET-KEY-1`.
fn find_age_secret_from_identity_files(identity_files: &[String]) -> anyhow::Result<String> {
    for path in identity_files {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("reading identity file '{path}'"))?;
        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("AGE-SECRET-KEY-1") {
                return Ok(trimmed.to_string());
            }
        }
    }
    Err(anyhow!(
        "no AGE-SECRET-KEY found in any of the {} identity files",
        identity_files.len()
    ))
}

/// Sign a registry entry for the given hash and revision.
///
/// Wire format:
/// `[MessageType::Registry, 0x00 (Ed25519), pub_key(32), revision(8), 0x21 (Blake3), hash(32)]`
fn sign_registry_entry(
    signing_key: &SigningKey,
    hash: Hash,
    revision: u64,
) -> anyhow::Result<StreamMessage> {
    StreamMessage::sign_ed25519_registry(signing_key, hash, revision)
        .map_err(|e| anyhow!("creating signed registry entry: {e}"))
}

/// Age-encrypt bytes for the given recipients (age public key strings).
///
/// Parses each string as an `age::x25519::Recipient` and encrypts for all of them.
fn age_encrypt_for_recipients(
    plaintext: &[u8],
    recipient_strings: &[String],
) -> anyhow::Result<Vec<u8>> {
    if recipient_strings.is_empty() {
        return Err(anyhow!("no recipients specified for publish encryption"));
    }

    let recipients: Vec<age::x25519::Recipient> = recipient_strings
        .iter()
        .map(|s| {
            s.parse::<age::x25519::Recipient>()
                .map_err(|e| anyhow!("invalid age recipient '{}': {}", s, e))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let encryptor =
        age::Encryptor::with_recipients(recipients.iter().map(|r| r as &dyn age::Recipient))
            .map_err(|e| anyhow!("creating age encryptor: {e}"))?;

    let mut ciphertext = vec![];
    let mut writer = encryptor
        .wrap_output(&mut ciphertext)
        .map_err(|e| anyhow!("age encrypt: {e}"))?;
    writer
        .write_all(plaintext)
        .context("writing age ciphertext")?;
    writer.finish().context("finishing age encryption")?;
    Ok(ciphertext)
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

    let decryptor =
        age::Decryptor::new(ciphertext).map_err(|e| anyhow!("age decryptor: {e}"))?;

    let mut plaintext = vec![];
    let mut reader = decryptor
        .decrypt(std::iter::once(&identity as &dyn age::Identity))
        .map_err(|e| anyhow!("age decrypt with paper key: {e}"))?;
    reader
        .read_to_end(&mut plaintext)
        .context("reading age plaintext")?;
    Ok(plaintext)
}

/// Age-decrypt bytes using identity files from the configured keys.
///
/// Tries each key that has an `identity_file` until one succeeds.
/// Returns the decrypted plaintext, or an error if no identity could decrypt.
fn age_decrypt_with_identity_files(
    ciphertext: &[u8],
    identity_files: &[String],
) -> anyhow::Result<Vec<u8>> {
    if identity_files.is_empty() {
        return Err(anyhow!(
            "no identity files available for decryption — \
             at least one key must have identity_file set"
        ));
    }

    for path in identity_files {
        let file_content = std::fs::read_to_string(path)
            .with_context(|| format!("reading identity file '{path}'"))?;

        let identity_file =
            age::IdentityFile::from_buffer(std::io::BufReader::new(file_content.as_bytes()))
                .with_context(|| format!("parsing identity file '{path}'"))?;

        let identities = identity_file
            .into_identities()
            .map_err(|e| anyhow!("loading identities from '{path}': {e}"))?;

        let decryptor = match age::Decryptor::new(ciphertext) {
            Ok(d) => d,
            Err(e) => return Err(anyhow!("age decryptor: {e}")),
        };

        let identity_refs: Vec<&dyn age::Identity> =
            identities.iter().map(|i| i.as_ref()).collect();

        match decryptor.decrypt(identity_refs.into_iter()) {
            Ok(mut reader) => {
                let mut plaintext = vec![];
                reader
                    .read_to_end(&mut plaintext)
                    .context("reading age plaintext")?;
                return Ok(plaintext);
            }
            Err(_) => {
                // This identity didn't work, try the next one.
                continue;
            }
        }
    }

    Err(anyhow!(
        "none of the {} identity files could decrypt the blob",
        identity_files.len()
    ))
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

    let node =
        Node::from_bytes(&cbor).map_err(|e| anyhow!("CBOR decode previous TN: {e}"))?;

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

/// Run a publish task.
///
/// 1. Load raw CBOR of the vault's Transparent Node (decrypted from local file).
/// 2. Resolve key names → age recipient public keys + identity files.
/// 3. Fetch previous published TN (if any) → decrypt → extract history.
/// 4. Build enriched Node: current snapshot + accumulated history entries.
/// 5. Age-encrypt the enriched Node CBOR for recipients.
/// 6. Upload the encrypted blob to the vault's first blob store.
/// 7. Derive Ed25519 signing key from node secret + vault name.
/// 8. Sign a registry entry pointing to the encrypted blob's hash.
/// 9. Publish to registry.
pub async fn run_publish(
    ctx: &TaskExecutorContext,
    vault_name: &str,
    key_names: &[String],
) -> anyhow::Result<()> {
    let (vault, key_configs) = {
        let config = ctx.config.read().await;
        let vault = resolve_vault(&config, vault_name)?.clone();
        let mut key_configs = Vec::new();
        for name in key_names {
            let kc = resolve_key(&config, name)
                .with_context(|| format!("resolving key '{name}'"))?
                .clone();
            key_configs.push(kc);
        }
        (vault, key_configs)
    };
    let registry = ctx
        .registry
        .as_ref()
        .ok_or_else(|| anyhow!("no registry configured — cannot publish snapshot"))?;

    // -- Load the raw CBOR of the Transparent Node --
    let current_path = vault_root_path(&vault.root_path);
    let cbor = load_vault_root_cbor(&current_path, &ctx.node_secret, vault_name)
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

    // -- Derive signing key + stream key --
    let signing_key = vault_signing_key(&ctx.node_secret, vault_name);
    let verifying_key: VerifyingKey = (&signing_key).into();
    let stream_key = StreamKey::PublicKeyEd25519(verifying_key.to_bytes());

    // -- Fetch previous published TN for history accumulation --
    let (prev_node, prev_encrypted_hash, prev_revision) =
        if !identity_files.is_empty() {
            match fetch_previous_published_node(
                registry.as_ref(),
                blob_store,
                &stream_key,
                &identity_files,
            )
            .await
            {
                Ok(Some((node, hash, rev))) => (Some(node), Some(hash), rev),
                Ok(None) => (None, None, 0),
                Err(e) => {
                    tracing::warn!(
                        vault = vault_name,
                        error = %e,
                        "could not fetch previous published TN for history — publishing without history"
                    );
                    // Fall back: check registry for revision only
                    let rev = match registry.get(&stream_key).await {
                        Ok(Some(entry)) => entry.revision,
                        _ => 0,
                    };
                    (None, None, rev)
                }
            }
        } else {
            tracing::info!(
                vault = vault_name,
                "no identity files configured — publishing without history"
            );
            let rev = match registry.get(&stream_key).await {
                Ok(Some(entry)) => entry.revision,
                _ => 0,
            };
            (None, None, rev)
        };

    // -- Build enriched Node with history --
    let published_node = build_published_node(
        &cbor,
        prev_node.as_ref(),
        prev_encrypted_hash,
    )?;

    let history_count = published_node.entries.len() - 1; // exclude ""

    let enriched_cbor = published_node
        .to_vec()
        .map_err(|e| anyhow!("CBOR encode enriched TN: {e}"))?;

    // -- Age-encrypt for recipients --
    let encrypted = age_encrypt_for_recipients(&enriched_cbor, &recipient_strings)
        .context("encrypting for recipients")?;

    // -- Upload encrypted blob --
    let blob_id = blob_store
        .blob_upload_bytes(Bytes::from(encrypted))
        .await
        .map_err(|e| anyhow!("uploading encrypted Transparent Node: {e}"))?;

    let encrypted_hash = blob_id.hash;

    // -- Check if hash changed (skip if identical) --
    if prev_encrypted_hash.is_some_and(|h| h == encrypted_hash) {
        tracing::info!(
            vault = vault_name,
            revision = prev_revision,
            "snapshot already published at current revision"
        );
        return Ok(());
    }

    tracing::info!(
        vault = vault_name,
        encrypted_blob = %encrypted_hash.fmt_short(),
        size = blob_id.size,
        recipients = key_names.len(),
        history_entries = history_count,
        "encrypted Transparent Node uploaded"
    );

    // -- Sign and publish registry entry --
    let new_revision = prev_revision + 1;
    let message = sign_registry_entry(&signing_key, encrypted_hash, new_revision)?;

    registry
        .set(message)
        .await
        .context("publishing registry entry")?;

    let pub_key_hex = hex::encode(verifying_key.to_bytes());
    tracing::info!(
        vault = vault_name,
        revision = new_revision,
        encrypted_blob = %encrypted_hash.fmt_short(),
        public_key = pub_key_hex,
        "snapshot published to registry"
    );

    // -- Ensure one-time recovery entry exists --
    if !identity_files.is_empty() {
        if let Err(e) = ensure_recovery_entry(
            registry.as_ref(),
            &identity_files,
            vault_name,
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
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use s5_fs_v2::node::{NodeKind, TraversalContext};

    #[test]
    fn signing_key_is_deterministic() {
        let secret = [42u8; 32];
        let k1 = vault_signing_key(&secret, "test-vault");
        let k2 = vault_signing_key(&secret, "test-vault");
        assert_eq!(k1.to_bytes(), k2.to_bytes());

        // Different vault names → different keys
        let k3 = vault_signing_key(&secret, "other-vault");
        assert_ne!(k1.to_bytes(), k3.to_bytes());
    }

    #[test]
    fn sign_and_verify_round_trip() {
        let secret = [1u8; 32];
        let signing_key = vault_signing_key(&secret, "my-vault");
        let root_hash = s5_core::Hash::from([99u8; 32]);

        let message = sign_registry_entry(&signing_key, root_hash, 1).unwrap();
        assert_eq!(message.revision, 1);
        assert_eq!(message.hash, root_hash);
        assert_eq!(message.signature.len(), 64); // Ed25519 signature
    }

    #[test]
    fn age_recipient_encrypt_round_trip() {
        // Generate a test identity
        let identity = age::x25519::Identity::generate();
        let recipient = identity.to_public();
        let recipient_str = recipient.to_string();

        let plaintext = b"test transparent node cbor data";
        let encrypted =
            age_encrypt_for_recipients(plaintext, &[recipient_str]).unwrap();

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
        let new_history: Vec<_> = node.entries.keys()
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
