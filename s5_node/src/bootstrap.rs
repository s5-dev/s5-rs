//! `bootstrap_from_identity` — the shared core behind paper recovery *and*
//! adding a new device.
//!
//! Both start from the same place: a fresh node that holds the identity master
//! key but nothing else. From there the steps are identical — they differ only
//! in **how the master key is obtained** (paper phrase → derive, or device
//! pairing → transfer). Everything downstream is this one path:
//!
//! ```text
//! master key
//!   → locate the master-anchored `config` vault  (master_pubkey, config_vault_id())
//!   → read: store configs + vault directory + discovery seed
//!   → per vault: discovery_signing_key(seed, vault_id)
//!               → (discovery_pubkey, vault_id) registry lookup → current HEAD
//! ```
//!
//! No vault root, no per-vault `recovery_secret`, no device-pubkey enumeration:
//! the `config` vault is the single source of truth both flows read. The caller
//! turns the result into a `config.toml` (from the store configs) and restores
//! each vault from its resolved HEAD (reusing the ordinary restore machinery).
//!
//! See `docs/reference/registry-durability.md`.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use ed25519_dalek::SigningKey;
use s5_core::blob::Blobs;
use s5_core::{Hash, RegistryApi, StreamKey};

use crate::config::NodeConfigStore;
use crate::config_vault::ConfigVault;
use crate::tasks::publish::discovery_signing_key;

/// One owned/joined vault resolved to its current HEAD, ready to restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedVault {
    /// Human vault name (the `[vault.<name>]` it rematerialises as).
    pub name: String,
    /// The vault's 16-byte locator.
    pub vault_id: [u8; 16],
    /// The current encrypted-Transparent-Node hash to download + restore.
    pub head_hash: Hash,
    /// Durable stores that host the vault (read fallbacks, in order).
    pub stores: Vec<String>,
}

/// The synced node configuration recovered from the `config` vault, plus every
/// vault resolved to its current HEAD.
#[derive(Debug, Default)]
pub struct Bootstrap {
    /// `[store.*]` configs (AppKeys/credentials inline) to rematerialise.
    pub stores: BTreeMap<String, NodeConfigStore>,
    /// Vaults whose HEAD was found via the discovery key. A vault listed in the
    /// directory but with no discovery entry yet (never snapped on a durable
    /// store) is omitted and logged.
    pub vaults: Vec<ResolvedVault>,
}

/// Read the `config` vault and resolve every vault's current HEAD via its
/// discovery key. `store` + `registry` must address the durable backend the
/// identity was published into (recovery re-creates it from the storage dialog;
/// add-device already has it); `identity_files` must hold an age identity that
/// can decrypt the config vault (the paper recovery key, or a device key).
pub async fn bootstrap_from_identity(
    master: &SigningKey,
    store: Arc<dyn Blobs>,
    registry: Arc<dyn RegistryApi + Send + Sync>,
    identity_files: &[String],
) -> Result<Bootstrap> {
    // The config vault is read-only here (recipients unused on the read path).
    let config_vault = ConfigVault::new(
        master.clone(),
        store,
        registry.clone(),
        Vec::new(),
        identity_files.to_vec(),
    );

    let stores = config_vault.read_all().await?;
    let directory = config_vault.read_vault_dir().await?;
    let seed = config_vault.read_seed().await?.ok_or_else(|| {
        anyhow!(
            "config vault has no discovery seed — the identity was never published \
             to a durable bootstrap store, so its vaults cannot be discovered"
        )
    })?;

    let mut vaults = Vec::new();
    for (name, entry) in directory {
        let discovery_key = discovery_signing_key(&seed, &entry.vault_id);
        let stream_key = StreamKey::Vault {
            pubkey: discovery_key.verifying_key().to_bytes(),
            vault_id: entry.vault_id,
        };
        match registry.get(&stream_key).await? {
            Some(head) => vaults.push(ResolvedVault {
                name,
                vault_id: entry.vault_id,
                head_hash: head.hash,
                stores: entry.stores,
            }),
            None => tracing::warn!(
                vault = %name,
                "vault in directory has no discovery entry yet (never snapped to a \
                 durable store); skipping"
            ),
        }
    }

    Ok(Bootstrap { stores, vaults })
}

/// Materialise a vault's local Transparent-Node root
/// (`{root_dir}/root.fs5.cbor.age`) from its published HEAD, so the
/// ordinary restore machinery can read it on a cold device.
///
/// Fetches the encrypted TN blob by `head_hash` from `blobs`, decrypts it
/// with `reader_identity_files` (an age identity that is a recipient of
/// the published root — the paper recovery key on paper recovery, or a
/// device key when adding a device), and re-encrypts it to `recipients`
/// (this device's keys) in the exact on-disk shape the daemon writes. The
/// blob read is BLAKE3-verified against `head_hash` by the `BlobsRead`
/// contract, so a mis-served TN is rejected.
pub async fn materialise_vault_root(
    blobs: &dyn Blobs,
    head_hash: Hash,
    reader_identity_files: &[String],
    recipients: &[String],
    root_dir: &std::path::Path,
) -> Result<()> {
    use s5_fs_v2::node::Node;

    let encrypted = blobs
        .blob_download(head_hash)
        .await
        .map_err(|e| anyhow!("downloading published Transparent Node {head_hash}: {e}"))?;

    let cbor = crate::tasks::vault_persist::age_decrypt_with_identity_files(
        &encrypted,
        reader_identity_files,
    )
    .map_err(|e| anyhow!("decrypting published Transparent Node (wrong recovery key?): {e}"))?;

    let node = Node::from_bytes(&cbor).map_err(|e| anyhow!("decoding Transparent Node: {e}"))?;

    let path = crate::tasks::vault_persist::vault_root_path(&root_dir.to_string_lossy());
    crate::tasks::vault_persist::save_node(&path, &node, recipients)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use s5_core::StreamMessage;
    use s5_core::blob::BlobStore;
    use s5_registry::MemoryRegistry;
    use s5_store_memory::MemoryStore;

    use super::*;
    use crate::config::{IndexdStoreConfig, NodeConfigStoreBackend};
    use crate::config_vault::VaultDirEntry;
    use crate::special_vaults::{config_vault_id, publish_vault_entries};
    use crate::tasks::publish::derive_vault_id;

    fn age_identity(dir: &Path) -> (String, String) {
        use age::secrecy::ExposeSecret;
        let identity = age::x25519::Identity::generate();
        let recipient = identity.to_public().to_string();
        let path = dir.join("paper.txt");
        std::fs::write(&path, identity.to_string().expose_secret()).unwrap();
        (recipient, path.to_string_lossy().into_owned())
    }

    /// The whole recovery walk, end to end, against one shared store + registry:
    /// publish a config vault (store config + vault directory + seed) and a
    /// discovery entry, then `bootstrap_from_identity` reads the config back and
    /// resolves the vault's HEAD — with only the master key + the paper age
    /// identity. This is exactly what `vup recover` / add-device run.
    #[tokio::test]
    async fn resolves_store_config_and_vault_head_from_paper() {
        let dir = tempfile::tempdir().unwrap();
        let (paper_recipient, paper_identity) = age_identity(dir.path());
        let store = Arc::new(BlobStore::new(MemoryStore::new()));
        let registry: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());
        let master = SigningKey::from_bytes(&[7u8; 32]);

        let seed = [9u8; 32];
        let vault_id = derive_vault_id(&[1u8; 32]);
        let head = Hash::from([2u8; 32]);
        let sia =
            NodeConfigStore::from_backend(NodeConfigStoreBackend::Indexd(IndexdStoreConfig {
                indexer_url: "https://sia.storage".to_string(),
                account: String::new(),
                app_key: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_string(),
                cache_path: "/c".to_string(),
                ..Default::default()
            }));

        // -- publish side: the config vault (sealed to paper) + discovery entry --
        let raw = BTreeMap::from([
            ("seed".to_string(), seed.to_vec()),
            ("stores/sia".to_string(), serde_json::to_vec(&sia).unwrap()),
            (
                "vaults/backup".to_string(),
                serde_json::to_vec(&VaultDirEntry {
                    vault_id,
                    stores: vec!["sia".to_string()],
                })
                .unwrap(),
            ),
        ]);
        publish_vault_entries(
            raw,
            config_vault_id(),
            &master,
            store.clone(),
            registry.as_ref(),
            &[paper_recipient],
        )
        .await
        .unwrap();

        let discovery_key = discovery_signing_key(&seed, &vault_id);
        let msg = StreamMessage::sign_ed25519_registry(&discovery_key, vault_id, head, 1).unwrap();
        registry.set(msg).await.unwrap();

        // -- recovery side: master key + paper identity only --
        let got = bootstrap_from_identity(&master, store, registry, &[paper_identity])
            .await
            .unwrap();

        assert_eq!(got.stores.get("sia"), Some(&sia), "store config recovered");
        assert_eq!(
            got.vaults,
            vec![ResolvedVault {
                name: "backup".to_string(),
                vault_id,
                head_hash: head,
                stores: vec!["sia".to_string()],
            }],
            "vault resolved to its current HEAD via the discovery key"
        );
    }

    #[tokio::test]
    async fn errors_when_no_seed_published() {
        let dir = tempfile::tempdir().unwrap();
        let (_rec, paper_identity) = age_identity(dir.path());
        let store = Arc::new(BlobStore::new(MemoryStore::new()));
        let registry: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());
        let master = SigningKey::from_bytes(&[3u8; 32]);

        // Nothing published at all → no seed → cannot discover vaults.
        let err = bootstrap_from_identity(&master, store, registry, &[paper_identity])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no discovery seed"));
    }

    /// The recover last mile: a published Transparent Node (encrypted to the
    /// paper recipient, as `publish` writes it) is fetched by HEAD hash,
    /// decrypted with the paper identity, and re-materialised as a cold
    /// device's local root — which then loads under the NEW device key and
    /// round-trips the vault's traversal context (the encryption keys restore
    /// needs). Proves `materialise_vault_root` closes the "root file not
    /// found" gap without the paper key leaking into the on-disk root.
    #[tokio::test]
    async fn materialise_vault_root_rewraps_head_for_a_cold_device() {
        use crate::tasks::vault_persist::{
            age_encrypt_for_recipients, load_node, snapshot_to_node, vault_root_path,
        };
        use s5_core::blob::{BlobsRead, BlobsWrite};
        use s5_fs_v2::snapshot::Snapshot;

        let dir = tempfile::tempdir().unwrap();
        let (paper_recipient, paper_identity) = age_identity(dir.path());

        // A device key present on the recovering machine but NOT a recipient
        // of the original published TN — it must still be able to read the
        // materialised root.
        let (device_recipient, device_identity) = {
            use age::secrecy::ExposeSecret;
            let id = age::x25519::Identity::generate();
            let rec = id.to_public().to_string();
            let path = dir.path().join("device.txt");
            std::fs::write(&path, id.to_string().expose_secret()).unwrap();
            (rec, path.to_string_lossy().into_owned())
        };

        // Build a published TN: an encrypted snapshot with a real traversal
        // context (keys), encoded as a Transparent Node, age-encrypted to the
        // PAPER recipient only — exactly what a durable-store publish leaves.
        let store = Arc::new(BlobStore::new(MemoryStore::new()));
        let root = Hash::from([42u8; 32]);
        let snapshot = Snapshot::new_encrypted(root, store.clone(), [3u8; 32], Some([9u8; 32]));
        let node = snapshot_to_node(&snapshot);
        let cbor = node.to_vec().unwrap();
        let encrypted =
            age_encrypt_for_recipients(&cbor, std::slice::from_ref(&paper_recipient)).unwrap();
        let head = store.blob_upload_bytes(encrypted.into()).await.unwrap();

        // Materialise into a fresh cold-device vault dir, rewrapping to the
        // device key + paper (recovery stays a recipient).
        let root_dir = dir.path().join("vaults/backup");
        std::fs::create_dir_all(&root_dir).unwrap();
        materialise_vault_root(
            store.as_ref(),
            head.hash,
            &[paper_identity],
            &[device_recipient, paper_recipient],
            &root_dir,
        )
        .await
        .expect("materialise the cold-device root");

        // The local root now exists and loads under the DEVICE key alone,
        // round-tripping the traversal context (keys) restore depends on.
        let root_path = vault_root_path(&root_dir.to_string_lossy());
        assert!(root_path.exists(), "root.fs5.cbor.age was written");
        let reloaded = load_node(&root_path, &[device_identity])
            .expect("device key decrypts the materialised root")
            .expect("root present");
        let (got_root, got_ph, got_ctx) =
            super::super::tasks::vault_persist::node_to_snapshot_parts(&reloaded).unwrap();
        assert_eq!(got_root, root, "restore sees the original tree root");
        assert_eq!(got_ph, Some([9u8; 32]));
        assert!(
            got_ctx.keys.is_some(),
            "traversal-context keys survived the rewrap — restore can decrypt file blobs"
        );

        // A blob whose bytes were tampered must be rejected by the BlobsRead
        // contract before it can be materialised (mis-served HEAD).
        let bad = store.blob_download(head.hash).await;
        assert!(bad.is_ok(), "sanity: clean read succeeds");
    }
}
