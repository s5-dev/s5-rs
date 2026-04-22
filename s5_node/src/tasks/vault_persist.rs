//! Vault root persistence — save and load encrypted Transparent Node root pointers.
//!
//! The vault's current state is a **Transparent Node** (see `s5_fs_v2` SPEC §6):
//! a single `NodeEntry` at key `""` with `Structural::Link` pointing to the
//! prolly tree root, carrying the full `TraversalContext` (encryption keys,
//! compression/encryption pipelines) in `child_context`.
//!
//! This Node is CBOR-encoded, then age-encrypted for the vault's configured
//! recipients (x25519 public keys), and written to `{vault.root_path}/root.fs5.cbor.age`.
//! On the next ingest, this file is loaded (decrypted via the corresponding
//! identity files) to reconstruct the previous `Snapshot` with full context
//! (keys, pipelines) — not just a bare hash.
//!
//! For resume support, in-progress state is saved to
//! `{vault.root_path}/inprogress.fs5.cbor.age`.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};
use s5_core::Hash;
use s5_fs_v2::node::{ContentRef, Node, NodeEntry, Structural, TraversalContext};
use s5_fs_v2::snapshot::Snapshot;

/// Build a Transparent Node from a snapshot's state.
///
/// The entry at `""` carries:
/// - `content.hash` → prolly tree root (CAS address)
/// - `content.plaintext_hash` → root plaintext hash (for KDF)
/// - `child_context` → full `TraversalContext` (keys + pipelines)
pub fn snapshot_to_node(snapshot: &Snapshot) -> Node {
    let entry = NodeEntry {
        content: Some(ContentRef {
            structural: Structural::Link,
            hash: *snapshot.root().as_bytes(),
            size: 0,
            plaintext_hash: snapshot.root_plaintext_hash().copied(),
            stored_blocks: None,
        }),
        semantic: None,
        child_context: Some(Box::new(snapshot.context().clone())),
        tombstone: None,
    };
    Node::transparent(entry)
}

/// Extract (root, root_plaintext_hash, context) from a Transparent Node's `""` entry.
pub fn node_to_snapshot_parts(
    node: &Node,
) -> anyhow::Result<(Hash, Option<[u8; 32]>, TraversalContext)> {
    let entry = node
        .transparent_entry()
        .ok_or_else(|| anyhow!("not a Transparent node or missing entry at \"\""))?;

    let content = entry
        .content
        .as_ref()
        .ok_or_else(|| anyhow!("Transparent node entry has no content"))?;

    if !matches!(content.structural, Structural::Link) {
        return Err(anyhow!(
            "Transparent node entry is {:?}, expected Link",
            content.structural
        ));
    }

    let context = entry
        .child_context
        .as_ref()
        .map(|c| (**c).clone())
        .unwrap_or_default();

    Ok((Hash::from(content.hash), content.plaintext_hash, context))
}

// ---------------------------------------------------------------------------
// Age encrypt / decrypt (recipient-based, no scrypt)
// ---------------------------------------------------------------------------

/// Age-encrypt bytes for the given recipients (age public key strings).
///
/// Parses each string as an `age::x25519::Recipient` and encrypts for all of them.
/// This is fast (~microseconds) unlike passphrase-based encryption which uses scrypt.
pub(crate) fn age_encrypt_for_recipients(
    plaintext: &[u8],
    recipient_strings: &[String],
) -> anyhow::Result<Vec<u8>> {
    if recipient_strings.is_empty() {
        return Err(anyhow!("no recipients specified for encryption"));
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

/// Age-decrypt bytes using identity files (age secret key files).
///
/// Tries each identity file until one succeeds.
/// Returns the decrypted plaintext, or an error if no identity could decrypt.
pub(crate) fn age_decrypt_with_identity_files(
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

// ---------------------------------------------------------------------------
// File paths
// ---------------------------------------------------------------------------

/// Path to the vault root file.
pub fn vault_root_path(vault_root: &str) -> PathBuf {
    PathBuf::from(vault_root).join("root.fs5.cbor.age")
}

/// Path to the in-progress vault root file (for resume support).
pub fn inprogress_root_path(vault_root: &str) -> PathBuf {
    PathBuf::from(vault_root).join("inprogress.fs5.cbor.age")
}

// ---------------------------------------------------------------------------
// Save / Load
// ---------------------------------------------------------------------------

/// Save a snapshot as an age-encrypted Transparent Node.
///
/// CBOR-encodes the Node, age-encrypts for the given recipients,
/// and writes atomically (tmp + rename) to the given path.
pub fn save_vault_root(
    path: &Path,
    snapshot: &Snapshot,
    recipients: &[String],
) -> anyhow::Result<()> {
    let node = snapshot_to_node(snapshot);
    save_node(path, &node, recipients)
}

/// Save a Transparent Node as an age-encrypted file.
pub fn save_node(path: &Path, node: &Node, recipients: &[String]) -> anyhow::Result<()> {
    let cbor = node
        .to_vec()
        .map_err(|e| anyhow!("CBOR encode Transparent Node: {e}"))?;

    let encrypted = age_encrypt_for_recipients(&cbor, recipients)?;

    // Atomic write: tmp file + rename.
    let tmp_path = path.with_extension("tmp");
    std::fs::write(&tmp_path, &encrypted)
        .with_context(|| format!("writing {}", tmp_path.display()))?;
    std::fs::rename(&tmp_path, path)
        .with_context(|| format!("renaming {} to {}", tmp_path.display(), path.display()))?;

    Ok(())
}

/// Load a Transparent Node from an age-encrypted file.
///
/// Returns `None` if the file does not exist.
pub fn load_node(path: &Path, identity_files: &[String]) -> anyhow::Result<Option<Node>> {
    if !path.exists() {
        return Ok(None);
    }

    let ciphertext = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;

    let cbor = age_decrypt_with_identity_files(&ciphertext, identity_files)
        .with_context(|| format!("decrypting {}", path.display()))?;

    let node = Node::from_bytes(&cbor).map_err(|e| anyhow!("CBOR decode Transparent Node: {e}"))?;
    Ok(Some(node))
}

/// Load a vault root, returning the snapshot parts directly.
///
/// Convenience wrapper: loads + extracts (root, plaintext_hash, context).
/// Returns `None` if the file does not exist.
#[allow(clippy::type_complexity)]
pub fn load_vault_root(
    path: &Path,
    identity_files: &[String],
) -> anyhow::Result<Option<(Hash, Option<[u8; 32]>, TraversalContext)>> {
    let node = match load_node(path, identity_files)? {
        Some(n) => n,
        None => return Ok(None),
    };
    let parts = node_to_snapshot_parts(&node)
        .with_context(|| format!("parsing Transparent Node from {}", path.display()))?;
    Ok(Some(parts))
}

/// Load the raw CBOR bytes of a vault root (decrypted but not parsed).
///
/// Used by publish to re-encrypt the same Transparent Node for recipients.
pub fn load_vault_root_cbor(
    path: &Path,
    identity_files: &[String],
) -> anyhow::Result<Option<Vec<u8>>> {
    if !path.exists() {
        return Ok(None);
    }

    let ciphertext = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;

    let cbor = age_decrypt_with_identity_files(&ciphertext, identity_files)
        .with_context(|| format!("decrypting {}", path.display()))?;

    Ok(Some(cbor))
}

/// Remove an in-progress vault root file (cleanup after successful completion).
pub fn remove_inprogress(vault_root: &str) -> anyhow::Result<()> {
    let path = inprogress_root_path(vault_root);
    if path.exists() {
        std::fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use s5_core::blob::BlobStore;
    use s5_core::Hash;
    use s5_fs_v2::snapshot::Snapshot;
    use s5_store_memory::MemoryStore;

    use super::*;

    fn test_store() -> Arc<dyn s5_core::BlobsRead> {
        Arc::new(BlobStore::new(MemoryStore::new()))
    }

    fn test_snapshot_plain(root: Hash) -> Snapshot {
        Snapshot::new_plain(root, test_store())
    }

    fn test_snapshot_encrypted(root: Hash, root_plaintext_hash: Option<[u8; 32]>) -> Snapshot {
        let master_secret = [42u8; 32];
        Snapshot::new_encrypted(root, test_store(), master_secret, root_plaintext_hash)
    }

    /// Generate a test age identity + recipient pair.
    fn test_age_keypair() -> (age::x25519::Identity, String) {
        let identity = age::x25519::Identity::generate();
        let recipient = identity.to_public().to_string();
        (identity, recipient)
    }

    /// Write a test identity to a temp file and return the path string.
    fn write_identity_file(dir: &Path, identity: &age::x25519::Identity) -> String {
        use age::secrecy::ExposeSecret;
        let path = dir.join("test-identity.txt");
        let secret_str = identity.to_string();
        std::fs::write(&path, secret_str.expose_secret()).unwrap();
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn transparent_node_round_trip_plain() {
        let root = Hash::from([99u8; 32]);
        let snapshot = test_snapshot_plain(root);
        let node = snapshot_to_node(&snapshot);
        let (got_root, got_ph, got_ctx) = node_to_snapshot_parts(&node).unwrap();
        assert_eq!(got_root, root);
        assert!(got_ph.is_none());
        assert!(got_ctx.keys.is_none());
    }

    #[test]
    fn transparent_node_round_trip_encrypted() {
        let root = Hash::from([42u8; 32]);
        let plaintext_hash = [7u8; 32];
        let snapshot = test_snapshot_encrypted(root, Some(plaintext_hash));
        let node = snapshot_to_node(&snapshot);
        let (got_root, got_ph, got_ctx) = node_to_snapshot_parts(&node).unwrap();
        assert_eq!(got_root, root);
        assert_eq!(got_ph.unwrap(), plaintext_hash);
        assert!(got_ctx.keys.is_some());
        assert!(got_ctx.keys.unwrap().contains_key(&0x0e));
    }

    #[test]
    fn cbor_round_trip() {
        let root = Hash::from([42u8; 32]);
        let snapshot = test_snapshot_encrypted(root, Some([7u8; 32]));
        let node = snapshot_to_node(&snapshot);

        let cbor = node.to_vec().unwrap();
        let decoded = Node::from_bytes(&cbor).unwrap();
        let (got_root, got_ph, got_ctx) = node_to_snapshot_parts(&decoded).unwrap();

        assert_eq!(got_root, root);
        assert_eq!(got_ph.unwrap(), [7u8; 32]);
        assert!(got_ctx.keys.is_some());
    }

    #[test]
    fn age_encrypt_decrypt_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let (identity, recipient) = test_age_keypair();
        let identity_path = write_identity_file(dir.path(), &identity);

        let root = Hash::from([42u8; 32]);
        let snapshot = test_snapshot_encrypted(root, Some([7u8; 32]));
        let node = snapshot_to_node(&snapshot);
        let cbor = node.to_vec().unwrap();

        let encrypted = age_encrypt_for_recipients(&cbor, &[recipient]).unwrap();
        let decrypted = age_decrypt_with_identity_files(&encrypted, &[identity_path]).unwrap();

        assert_eq!(cbor, decrypted);
    }

    #[test]
    fn wrong_identity_fails() {
        let dir = tempfile::tempdir().unwrap();
        let (_identity, recipient) = test_age_keypair();
        let (wrong_identity, _) = test_age_keypair();
        let wrong_path = write_identity_file(dir.path(), &wrong_identity);

        let data = b"test data";
        let encrypted = age_encrypt_for_recipients(data, &[recipient]).unwrap();
        let result = age_decrypt_with_identity_files(&encrypted, &[wrong_path]);
        assert!(result.is_err());
    }

    #[test]
    fn save_load_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("root.fs5.cbor.age");
        let (identity, recipient) = test_age_keypair();
        let identity_path = write_identity_file(dir.path(), &identity);

        let root = Hash::from([42u8; 32]);
        let snapshot = test_snapshot_encrypted(root, Some([7u8; 32]));

        save_vault_root(&path, &snapshot, &[recipient]).unwrap();
        let (got_root, got_ph, got_ctx) =
            load_vault_root(&path, &[identity_path]).unwrap().unwrap();

        assert_eq!(got_root, root);
        assert_eq!(got_ph.unwrap(), [7u8; 32]);
        assert!(got_ctx.keys.is_some());
    }

    #[test]
    fn load_missing_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nonexistent.fs5.cbor.age");
        let result = load_vault_root(&path, &[String::from("dummy")]).unwrap();
        assert!(result.is_none());
    }
}
