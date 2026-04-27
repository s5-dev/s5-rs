//! Frozen anonymous export: emit a share link that lets a recipient
//! decrypt the current snapshot of a vault without ever joining as a
//! peer or being added to the vault's standing recipient list.
//!
//! Mechanism:
//!
//! 1. Generate a fresh `age::x25519::Identity` per call. The secret
//!    becomes the URL fragment; the recipient pubkey joins the
//!    encrypted Transparent Node's recipient set for *this single
//!    blob only*.
//! 2. Load the local TN (decrypted with the vault's own identity
//!    files), re-encrypt the same CBOR with the vault's existing
//!    recipients **plus** the ephemeral recipient.
//! 3. Upload the new encrypted blob to `vault.blob_stores[0]` and
//!    every entry in `vault.meta_targets`. The blob is content-
//!    addressed by `M = BLAKE3(age_bytes)`.
//! 4. Format the URL per `docs/reference/share-links.md`.
//!
//! Frozen: the URL targets one specific blob hash, so future snaps
//! by the producer don't reach this recipient. Anonymous: no
//! identity exchange, no signature, no registry write.

use std::collections::HashMap;

use age::secrecy::ExposeSecret;
use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use s5_core::Hash;
use s5_core::blob::{BlobStore, BlobsWrite};

use crate::config::S5NodeConfig;
use crate::tasks::vault_persist::{
    age_encrypt_for_recipients, load_vault_root_cbor, vault_root_path,
};

/// Result of a frozen-anonymous export.
pub struct ExportResult {
    /// The user-facing share URL.
    pub url: String,
    /// The encrypted Transparent Node's blob hash.
    pub blob_hash: Hash,
}

/// Re-encrypt the current vault TN with an ephemeral recipient added,
/// upload to the vault's stores, and return the share URL.
///
/// The optional `path` is reserved for sub-tree exports and must be
/// `None` today; passing `Some(_)` errors out so the CLI doesn't
/// silently produce a whole-vault URL when the user asked for a subtree.
pub async fn run_export(
    config: &S5NodeConfig,
    stores: &HashMap<String, BlobStore>,
    vault_name: &str,
    path: Option<&str>,
) -> Result<ExportResult> {
    if path.is_some() {
        return Err(anyhow!(
            "sub-tree export (--path) is not yet implemented; whole-vault export only"
        ));
    }

    // -- Resolve vault config and identity files --
    let vault = config
        .vault
        .get(vault_name)
        .ok_or_else(|| anyhow!("vault '{}' not found in config", vault_name))?;

    let vault_key = config.key.get(&vault.key).ok_or_else(|| {
        anyhow!(
            "vault '{}' references unknown key '{}'",
            vault_name,
            vault.key
        )
    })?;

    let identity_files: Vec<String> = vault_key.identity_file.iter().cloned().collect();
    if identity_files.is_empty() {
        return Err(anyhow!(
            "vault '{}' key '{}' has no identity_file — cannot decrypt local TN to re-encrypt for share",
            vault_name,
            vault.key,
        ));
    }

    // -- Load local Transparent Node CBOR (decrypted) --
    let current_path = vault_root_path(&vault.root_path);
    let cbor = load_vault_root_cbor(&current_path, &identity_files)
        .context("reading vault root for export")?
        .ok_or_else(|| {
            anyhow!(
                "vault '{}' has no snapshot to export (run `snap` first)",
                vault_name,
            )
        })?;

    // -- Resolve standing recipients (the ones who already see every snap) --
    let mut recipient_strings = Vec::new();
    for name in &vault.recipients {
        let kc = config
            .key
            .get(name)
            .ok_or_else(|| anyhow!("vault '{}' recipient '{}' not in [key.*]", vault_name, name))?;
        recipient_strings.push(kc.public_key.clone());
    }

    // -- Generate ephemeral age recipient for this share --
    let ephemeral = age::x25519::Identity::generate();
    let ephemeral_secret = ephemeral.to_string().expose_secret().to_string();
    let ephemeral_recipient = ephemeral.to_public().to_string();
    recipient_strings.push(ephemeral_recipient);

    // -- Re-encrypt the same CBOR with the enlarged recipient set --
    let encrypted = age_encrypt_for_recipients(&cbor, &recipient_strings)
        .context("re-encrypting TN for share")?;
    let encrypted_bytes = Bytes::from(encrypted);

    // -- Upload to primary store --
    let blob_store_name = vault
        .blob_stores
        .first()
        .ok_or_else(|| anyhow!("vault '{}' has no blob_stores configured", vault_name))?;
    let blob_store = stores
        .get(blob_store_name)
        .ok_or_else(|| anyhow!("blob_store '{}' not configured", blob_store_name))?;

    let blob_id = blob_store
        .blob_upload_bytes(encrypted_bytes.clone())
        .await
        .map_err(|e| anyhow!("uploading export blob: {e}"))?;
    let blob_hash = blob_id.hash;

    tracing::info!(
        vault = vault_name,
        blob = %blob_hash.fmt_short(),
        "uploaded frozen-export Transparent Node"
    );

    // -- Mirror to meta_targets (best-effort, same policy as publish) --
    for meta_target in &vault.meta_targets {
        if meta_target == blob_store_name {
            continue;
        }
        match stores.get(meta_target) {
            Some(target_store) => {
                if let Err(e) = target_store
                    .blob_upload_bytes(encrypted_bytes.clone())
                    .await
                {
                    tracing::warn!(
                        vault = vault_name,
                        meta_target = %meta_target,
                        error = %e,
                        "could not mirror export blob"
                    );
                }
            }
            None => tracing::warn!(
                vault = vault_name,
                meta_target = %meta_target,
                "skipping unknown meta_target store"
            ),
        }
    }

    // -- Build URL per docs/reference/share-links.md --
    let url = format!(
        "s5://export/{}?m={}#{}",
        vault_name,
        hex::encode(blob_hash.as_bytes()),
        ephemeral_secret,
    );

    Ok(ExportResult { url, blob_hash })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::S5NodeConfig;
    use s5_core::blob::BlobsRead;
    use s5_node_api::config::{
        NodeConfigIdentity, NodeConfigKey, NodeConfigSource, NodeConfigVault,
    };
    use s5_store_local::{LocalStore, LocalStoreConfig};
    use std::collections::BTreeMap;
    use tempfile::tempdir;

    /// Smoke test: a freshly-snapped vault produces a working export URL,
    /// the encrypted blob lands in the primary store, and the ephemeral
    /// secret can decrypt it.
    #[tokio::test]
    async fn export_produces_decryptable_blob() -> Result<()> {
        // Set up a vault with a fake snapshot at root_path/root.fs5.cbor.age.
        let vault_dir = tempdir()?;
        let store_dir = tempdir()?;
        let identity_dir = tempdir()?;

        // Generate the vault's own age key (used both as the standing
        // recipient and as the local-decryption identity).
        let vault_id = age::x25519::Identity::generate();
        let vault_recipient = vault_id.to_public().to_string();
        let vault_secret = vault_id.to_string().expose_secret().to_string();
        let identity_path = identity_dir.path().join("vault.txt");
        std::fs::write(&identity_path, &vault_secret)?;

        // Author a fake "current TN" — content doesn't matter for this
        // test, only that age decrypt → CBOR → age encrypt round-trips.
        use s5_fs_v2::node::{ContentRef, Node, NodeEntry, Structural, TraversalContext};
        let node = Node::transparent(NodeEntry {
            content: Some(ContentRef {
                structural: Structural::Link,
                hash: [0xAB; 32],
                size: 0,
                plaintext_hash: None,
                stored_blocks: None,
            }),
            semantic: None,
            child_context: Some(Box::new(TraversalContext::default())),
            tombstone: None,
        });
        let cbor = node.to_vec().unwrap();
        let encrypted = age_encrypt_for_recipients(&cbor, std::slice::from_ref(&vault_recipient))?;
        std::fs::write(vault_dir.path().join("root.fs5.cbor.age"), &encrypted)?;

        // Build the minimal config + stores map.
        let mut keys = BTreeMap::new();
        keys.insert(
            "vault".to_string(),
            NodeConfigKey {
                public_key: vault_recipient.clone(),
                identity_file: Some(identity_path.to_string_lossy().into_owned()),
            },
        );
        let mut vaults = BTreeMap::new();
        vaults.insert(
            "test".to_string(),
            NodeConfigVault {
                root_path: vault_dir.path().to_string_lossy().into_owned(),
                key: "vault".to_string(),
                blob_stores: vec!["primary".to_string()],
                preset: None,
                recipients: vec!["vault".to_string()],
                sources: Vec::new(),
                meta_targets: Vec::new(),
                plaintext_tree: false,
                watch: false,
            },
        );
        let config = S5NodeConfig {
            identity: NodeConfigIdentity {
                secret_key_file: None,
                secret_key: None,
                encrypted_with: None,
            },
            key: keys,
            store: BTreeMap::new(),
            registry: BTreeMap::new(),
            source: BTreeMap::<String, NodeConfigSource>::new(),
            vault: vaults,
            task: BTreeMap::new(),
        };

        let blob_store = BlobStore::new(LocalStore::create(LocalStoreConfig {
            base_path: store_dir.path().to_string_lossy().into_owned(),
        }));
        let mut stores_map = HashMap::new();
        stores_map.insert("primary".to_string(), blob_store.clone());

        // Run export.
        let result = run_export(&config, &stores_map, "test", None).await?;

        // URL should parse: scheme s5, intent export, vault label "test",
        // m= the blob hash, fragment is the secret.
        assert!(result.url.starts_with("s5://export/test?m="));
        assert!(result.url.contains("#AGE-SECRET-KEY-1"));

        // The fragment is the ephemeral secret — extract it and use it
        // to decrypt the blob the export uploaded.
        let secret = result.url.split_once('#').unwrap().1;
        let ephemeral: age::x25519::Identity = secret.parse().unwrap();

        let downloaded = blob_store.blob_download(result.blob_hash).await?;
        let decryptor = age::Decryptor::new(&downloaded[..])?;
        let mut reader = decryptor.decrypt(std::iter::once(&ephemeral as &dyn age::Identity))?;
        let mut decrypted = Vec::new();
        std::io::Read::read_to_end(&mut reader, &mut decrypted)?;

        assert_eq!(
            decrypted, cbor,
            "decrypted bytes must match the original CBOR"
        );
        Ok(())
    }
}
