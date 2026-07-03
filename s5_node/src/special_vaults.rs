//! The special-vault primitive: a tiny single-leaf KV vault.
//!
//! The `stores` and `identity_secrets` vaults (see
//! `docs/reference/special-vaults.md`) are ordinary `s5_fs_v2` prolly-tree
//! vaults used as small key→value stores. This module is the shared mechanism
//! both build on: [`KvVault`] assembles entries in memory, [`KvVault::seal`]
//! persists them to a blob store and age-encrypts the vault root to its
//! recipients (the blob shipped over the registry/relay), and [`KvVault::open`]
//! reverses it.
//!
//! **Master-anchored.** Unlike user vaults (located by a per-vault
//! `recovery_secret`), the special vaults are found from the identity alone: at
//! `(master_pubkey, well_known_vault_id(domain))`, with the HEAD signed by the
//! identity master key the daemon already holds. So any device with the
//! identity can *locate* them (the locator is public — confidentiality is the
//! age layer), and any device in the age-recipient set can *read* them, with no
//! `recovery_secret` to derive or store. The content keys (leaf/node/structural)
//! are random per vault and travel *inside* the age-encrypted root, so only the
//! recipients can read the contents even though the locator is public.
//!
//! Typed entry payloads (e.g. [`s5_core::vaults::StoreEntry`], or the config
//! vault's `VaultDirEntry`) live above this layer; here we only move opaque
//! value bytes. The daemon handle that drives the `config` vault end to end is
//! [`crate::config_vault::ConfigVault`].

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use futures_util::StreamExt;
use rand::Rng;
use s5_core::blob::Blobs;
use s5_core::{BlobsRead, Hash, RegistryApi, StreamKey, StreamMessage};
use s5_fs_v2::layer::ReadableLayer;
use s5_fs_v2::node::Node;
use s5_fs_v2::overlay::WritableOverlay;
use s5_fs_v2::pipeline::Pipeline;
use s5_fs_v2::snapshot::Snapshot;

use crate::tasks::publish::well_known_vault_id;
use crate::tasks::vault_persist::{
    age_decrypt_with_identity_files, age_encrypt_for_recipients, node_to_snapshot_parts,
    snapshot_to_node,
};

/// Domain identifying the warm `config` vault — its well-known locator
/// (`well_known_vault_id(CONFIG_VAULT_DOMAIN)`). The synced node configuration:
/// `[store.*]` configs, the vault directory (`name → vault_id → stores`), and
/// the identity-wide discovery `seed`. See `mnemonic-derivation.md` § Layer C /
/// `special-vaults.md` § 4.
pub const CONFIG_VAULT_DOMAIN: &str = "s5/config/v1";

/// Domain identifying the cold `identity_secrets` vault. See
/// `mnemonic-derivation.md` § 4.1 / `special-vaults.md` § 3.
pub const IDENTITY_SECRETS_VAULT_DOMAIN: &str = "s5/identity-secrets/v1";

/// The `config` vault's well-known 16-byte locator. Constant, public,
/// secret-free — the vault is found at `(master_pubkey, this)`.
pub fn config_vault_id() -> [u8; 16] {
    well_known_vault_id(CONFIG_VAULT_DOMAIN)
}

/// The `identity_secrets` vault's well-known 16-byte locator.
pub fn identity_secrets_vault_id() -> [u8; 16] {
    well_known_vault_id(IDENTITY_SECRETS_VAULT_DOMAIN)
}

/// A tiny single-leaf KV vault — the shared primitive behind the special
/// vaults. Build in memory with [`put`](Self::put), then [`seal`](Self::seal) /
/// [`open`](Self::open) to persist and recover.
///
/// All three content keys (leaf/node/structural) are random per vault and
/// travel *inside* the age-encrypted root, so the recipients are the only
/// readers. The vault carries no locator of its own — that is supplied at
/// publish time ([`publish_vault`]), keeping the primitive agnostic to whether
/// it is master-anchored (special vaults) or `recovery_secret`-anchored.
pub struct KvVault {
    structural_key: [u8; 32],
    leaf_key: [u8; 32],
    node_key: [u8; 32],
    entries: BTreeMap<String, Vec<u8>>,
}

impl Default for KvVault {
    fn default() -> Self {
        Self::new()
    }
}

impl KvVault {
    /// New empty vault with freshly-generated random content keys.
    pub fn new() -> Self {
        let mut structural_key = [0u8; 32];
        let mut leaf_key = [0u8; 32];
        let mut node_key = [0u8; 32];
        rand::rng().fill_bytes(&mut structural_key);
        rand::rng().fill_bytes(&mut leaf_key);
        rand::rng().fill_bytes(&mut node_key);
        Self {
            structural_key,
            leaf_key,
            node_key,
            entries: BTreeMap::new(),
        }
    }

    /// Upsert a key→value entry (the value is opaque bytes — typically a
    /// serialized config/directory entry).
    pub fn put(&mut self, key: impl Into<String>, value: Vec<u8>) {
        self.entries.insert(key.into(), value);
    }

    /// Persist every entry to `store` and age-seal the vault root to
    /// `recipients`, returning the age-encrypted root bytes (the blob shipped
    /// over the registry/relay; hand them to [`publish_vault`]). `store` must be
    /// both readable and writable (the same blob store backs the prolly tree and
    /// serves it back on [`open`](Self::open)). Errors on an empty vault — there
    /// is no leaf to persist.
    pub async fn seal(&self, store: Arc<dyn Blobs>, recipients: &[String]) -> Result<Vec<u8>> {
        let read: Arc<dyn BlobsRead> = store.clone();
        let base = Snapshot::empty_encrypted_split(
            read.clone(),
            self.leaf_key,
            self.node_key,
            self.structural_key,
        );
        let ctx = base.context().clone();
        let pipeline = Arc::new(Pipeline::new(read.clone(), ctx.clone()));
        let overlay =
            WritableOverlay::new(Arc::new(base) as Arc<dyn ReadableLayer>, pipeline.clone());

        for (key, value) in &self.entries {
            let entry = pipeline
                .import_bytes(value, store.as_ref(), None)
                .await
                .map_err(|e| anyhow!("importing vault entry {key:?}: {e}"))?;
            overlay.put(key.clone(), entry);
        }

        let (root, plaintext_hash, _stats) = overlay
            .flush(store.as_ref())
            .await?
            .ok_or_else(|| anyhow!("cannot seal an empty KvVault (no entries)"))?;

        let snapshot = Snapshot::new(root, read, ctx, Some(plaintext_hash));
        let node = snapshot_to_node(&snapshot);
        let cbor = node
            .to_vec()
            .map_err(|e| anyhow!("encoding vault root node: {e}"))?;
        age_encrypt_for_recipients(&cbor, recipients)
    }

    /// Recover a vault from its sealed root: age-decrypt with one of
    /// `identity_files`, then read every entry back out of `store` (which must
    /// hold the blobs persisted at [`seal`](Self::seal) time). Returns the live
    /// key→value map (tombstones skipped).
    pub async fn open(
        sealed_root: &[u8],
        store: Arc<dyn Blobs>,
        identity_files: &[String],
    ) -> Result<BTreeMap<String, Vec<u8>>> {
        let cbor = age_decrypt_with_identity_files(sealed_root, identity_files)?;
        let node = Node::from_bytes(&cbor).map_err(|e| anyhow!("decoding vault root node: {e}"))?;
        let (root, plaintext_hash, ctx) = node_to_snapshot_parts(&node)?;

        let read: Arc<dyn BlobsRead> = store;
        let snapshot = Snapshot::new(root, read.clone(), ctx.clone(), plaintext_hash);
        let pipeline = Pipeline::new(read, ctx);

        let mut out = BTreeMap::new();
        let mut scan = snapshot.scan(Bound::Unbounded, Bound::Unbounded);
        while let Some(item) = scan.next().await {
            let (key, entry) = item?;
            if entry.is_tombstone() {
                continue;
            }
            let value = pipeline
                .export_bytes(&entry)
                .await
                .map_err(|e| anyhow!("exporting vault entry {key:?}: {e}"))?;
            out.insert(key, value.to_vec());
        }
        Ok(out)
    }
}

/// Publish a sealed vault root over the registry/relay: upload the
/// age-encrypted root blob to `blob_store`, then write the registry HEAD under
/// `(signing_key.pubkey, vault_id)` pointing at that blob's hash, signed by
/// `signing_key`.
///
/// For the master-anchored special vaults, `signing_key` is the identity master
/// key and `vault_id` is a [`well_known_vault_id`] — so recovery is a single
/// registry lookup from the identity alone ([`recover_vault_root`]). Returns the
/// published blob hash.
pub async fn publish_vault(
    sealed_root: &[u8],
    vault_id: [u8; 16],
    signing_key: &SigningKey,
    blob_store: &dyn Blobs,
    registry: &dyn RegistryApi,
) -> Result<Hash> {
    let pubkey = signing_key.verifying_key().to_bytes();
    let stream_key = StreamKey::Vault { pubkey, vault_id };

    // Monotone revision: one past whatever is currently published.
    let revision = match registry.get(&stream_key).await? {
        Some(prev) => prev.revision + 1,
        None => 1,
    };

    let blob_id = blob_store
        .blob_upload_bytes(Bytes::from(sealed_root.to_vec()))
        .await
        .map_err(|e| anyhow!("uploading sealed vault root: {e}"))?;
    let hash = blob_id.hash;

    // Durability barrier before registry.set — the same contract as the
    // snapshot publish path (tasks/publish.rs): a HEAD must never point at a
    // blob still sitting in a local staging spool. Proven live 2026-07-02: the identity_secrets HEAD referenced a never-uploaded
    // staged blob and paper recovery bricked on a "healthy" identity. The
    // sync also covers the KvVault entry blobs sealed to this store just
    // before us. Change-gated callers make this free in steady state.
    blob_store
        .blob_sync()
        .await
        .map_err(|e| anyhow!("syncing blob store before publishing vault registry entry: {e}"))?;

    let message = StreamMessage::sign_ed25519_registry(signing_key, vault_id, hash, revision)
        .map_err(|e| anyhow!("signing vault registry entry: {e}"))?;
    registry
        .set(message)
        .await
        .context("publishing vault registry entry")?;
    Ok(hash)
}

/// Recover a published vault's sealed root: look up the registry HEAD under
/// `(pubkey, vault_id)` and download the age-encrypted root blob from
/// `blob_store`. Returns the sealed root bytes — hand them to [`KvVault::open`]
/// with an age identity — or `None` if nothing is published under this locator.
pub async fn recover_vault_root(
    pubkey: [u8; 32],
    vault_id: [u8; 16],
    blob_store: &dyn Blobs,
    registry: &dyn RegistryApi,
) -> Result<Option<Vec<u8>>> {
    let stream_key = StreamKey::Vault { pubkey, vault_id };
    let entry = match registry.get(&stream_key).await? {
        Some(e) => e,
        None => return Ok(None),
    };
    let bytes = blob_store
        .blob_download(entry.hash)
        .await
        .map_err(|e| anyhow!("downloading sealed vault root: {e}"))?;
    Ok(Some(bytes.to_vec()))
}

/// Read every key→value entry of a master-anchored vault: locate + download its
/// sealed root at `(pubkey, vault_id)`, age-decrypt with one of `identity_files`,
/// and read it back. Returns an empty map if nothing is published there. The
/// shared read half behind [`crate::config_vault::ConfigVault`] /
/// [`crate::identity_secrets_vault::IdentitySecretsVault`].
pub async fn read_vault_entries(
    pubkey: [u8; 32],
    vault_id: [u8; 16],
    store: Arc<dyn Blobs>,
    registry: &dyn RegistryApi,
    identity_files: &[String],
) -> Result<BTreeMap<String, Vec<u8>>> {
    match recover_vault_root(pubkey, vault_id, store.as_ref(), registry).await? {
        Some(root) => KvVault::open(&root, store, identity_files).await,
        None => Ok(BTreeMap::new()),
    }
}

/// Seal `entries` into a fresh master-anchored vault (random content keys),
/// age-encrypt the root to `recipients`, and publish it at `(master.pubkey,
/// vault_id)`, signed by `master`. A no-op when `entries` is empty (an empty KV
/// vault has no leaf to seal). The shared write half behind the special-vault
/// handles.
pub async fn publish_vault_entries(
    entries: BTreeMap<String, Vec<u8>>,
    vault_id: [u8; 16],
    master: &SigningKey,
    store: Arc<dyn Blobs>,
    registry: &dyn RegistryApi,
    recipients: &[String],
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let mut vault = KvVault::new();
    for (key, value) in entries {
        vault.put(key, value);
    }
    let sealed = vault.seal(store.clone(), recipients).await?;
    publish_vault(&sealed, vault_id, master, store.as_ref(), registry).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use ed25519_dalek::SigningKey;
    use s5_core::blob::BlobStore;
    use s5_core::vaults::{StoreEntry, StoreKind};
    use s5_registry::MemoryRegistry;
    use s5_store_memory::MemoryStore;

    use super::*;

    fn store() -> Arc<BlobStore> {
        Arc::new(BlobStore::new(MemoryStore::new()))
    }

    /// A fresh age identity written to a temp file; returns (recipient string,
    /// identity-file path).
    fn age_identity(dir: &Path) -> (String, String) {
        use age::secrecy::ExposeSecret;
        let identity = age::x25519::Identity::generate();
        let recipient = identity.to_public().to_string();
        let path = dir.join("id.txt");
        std::fs::write(&path, identity.to_string().expose_secret()).unwrap();
        (recipient, path.to_string_lossy().into_owned())
    }

    #[tokio::test]
    async fn seal_open_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let (recipient, id_file) = age_identity(dir.path());

        let mut vault = KvVault::new();
        let entry = StoreEntry::imported(
            StoreKind::Indexd,
            BTreeMap::from([("url".to_string(), "https://sia.storage".to_string())]),
            vec![3u8; 32],
        );
        vault.put("stores/default", entry.encode_cbor());

        let st = store();
        let sealed = vault.seal(st.clone(), &[recipient]).await.unwrap();

        let recovered = KvVault::open(&sealed, st, &[id_file]).await.unwrap();
        let got = recovered
            .get("stores/default")
            .expect("the entry must survive the seal/open round trip");
        assert_eq!(StoreEntry::decode_cbor(got).unwrap(), entry);
    }

    #[tokio::test]
    async fn publish_recover_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let (recipient, id_file) = age_identity(dir.path());

        // Master-anchored: a fixed well-known vault_id + the identity master key
        // (here a deterministic test key) locate and authorize the HEAD — no
        // recovery_secret involved.
        let master = SigningKey::from_bytes(&[7u8; 32]);
        let vault_id = config_vault_id();

        let mut vault = KvVault::new();
        let entry = StoreEntry::imported(StoreKind::Indexd, BTreeMap::new(), vec![1u8; 32]);
        vault.put("stores/default", entry.encode_cbor());

        let st = store();
        let registry = MemoryRegistry::new();

        let sealed = vault.seal(st.clone(), &[recipient]).await.unwrap();
        publish_vault(&sealed, vault_id, &master, st.as_ref(), &registry)
            .await
            .unwrap();

        // Recover from only the master pubkey + the well-known vault_id (locate +
        // download), plus the age identity (decrypt + read back).
        let recovered_root = recover_vault_root(
            master.verifying_key().to_bytes(),
            vault_id,
            st.as_ref(),
            &registry,
        )
        .await
        .unwrap()
        .expect("a HEAD was published under (master_pubkey, config_vault_id)");
        let entries = KvVault::open(&recovered_root, st, &[id_file])
            .await
            .unwrap();
        assert_eq!(
            StoreEntry::decode_cbor(entries.get("stores/default").unwrap()).unwrap(),
            entry
        );
    }

    #[tokio::test]
    async fn wrong_identity_cannot_open() {
        let dir = tempfile::tempdir().unwrap();
        let (recipient, _id_file) = age_identity(dir.path());
        let wrong_dir = tempfile::tempdir().unwrap();
        let (_r2, wrong_id) = age_identity(wrong_dir.path());

        let mut vault = KvVault::new();
        vault.put("k", b"v".to_vec());
        let st = store();
        let sealed = vault.seal(st.clone(), &[recipient]).await.unwrap();

        assert!(
            KvVault::open(&sealed, st, &[wrong_id]).await.is_err(),
            "an unrelated age identity must not decrypt the vault root"
        );
    }

    #[test]
    fn well_known_vault_ids_are_distinct_and_public() {
        assert_ne!(
            config_vault_id(),
            identity_secrets_vault_id(),
            "the two special vaults occupy distinct well-known locators"
        );
        // Secret-free: derivable from the public domain string alone.
        assert_eq!(config_vault_id(), well_known_vault_id(CONFIG_VAULT_DOMAIN));
    }

    #[tokio::test]
    async fn empty_vault_seal_errors() {
        let dir = tempfile::tempdir().unwrap();
        let (recipient, _) = age_identity(dir.path());
        let vault = KvVault::new();
        assert!(vault.seal(store(), &[recipient]).await.is_err());
    }
}
