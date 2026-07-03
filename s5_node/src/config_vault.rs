//! The `config` vault — the daemon's synced node configuration.
//!
//! A master-anchored special vault ([`crate::special_vaults`],
//! `docs/reference/special-vaults.md`) holding the configuration a fresh device
//! or a paper recovery needs to rebuild the node. Today: one [`NodeConfigStore`]
//! per synced backend, keyed `stores/<name>`. The vault entry is **the same
//! shape as the `[store.<name>]` TOML** — the AppKey/credentials live inline,
//! exactly as in `config.toml` — so syncing a store across devices is just
//! copying its config block, and `vup recover` rematerialises `config.toml`
//! verbatim. It also carries (added incrementally) the vault directory
//! (`vaults/<name> → {vault_id, stores}`) and the identity-wide discovery
//! `seed` — the single source of truth both add-device and recovery read.
//!
//! It lives at `(master_pubkey, config_vault_id())`, HEAD signed by the identity
//! master key the daemon holds, so:
//!
//! - any device with the identity can *find* it (the locator is public), and
//! - any device in its age-recipient set can *read* the configs — including an
//!   indexd backend's inline AppKey — without re-running the per-indexer auth
//!   dance.
//!
//! Writes are read-modify-republish: open the current contents, upsert, re-seal
//! to the recipient set, publish a new HEAD (revision++). Single-writer is
//! assumed (adding/removing a store is rare and operator-driven); the registry's
//! monotonic-revision rule turns a concurrent writer into a losing retry, not
//! corruption.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use ed25519_dalek::SigningKey;
use s5_core::RegistryApi;
use s5_core::blob::Blobs;

use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::config::NodeConfigStore;
use crate::special_vaults::{config_vault_id, publish_vault_entries, read_vault_entries};

/// In-vault key prefix for a named store config: `stores/<name>`.
const STORES_PREFIX: &str = "stores/";
/// In-vault key prefix for a vault directory entry: `vaults/<name>`.
const VAULTS_PREFIX: &str = "vaults/";
/// In-vault key for the identity-wide discovery seed.
const SEED_KEY: &str = "seed";

/// The in-vault key for a named store config entry: `stores/<name>`
/// (`special-vaults.md` § 4).
fn entry_key(name: &str) -> String {
    format!("{STORES_PREFIX}{name}")
}

/// A vault directory entry: the minimal, non-secret discovery record for one
/// owned/joined vault. `vault_id` (the locator) plus the durable `stores` that
/// host its HEAD + data is everything `bootstrap_from_identity` needs — combined
/// with the discovery `seed` it derives the per-vault discovery keypair and
/// finds the HEAD, with no vault root and no `recovery_secret`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultDirEntry {
    /// The vault's 16-byte locator (`derive_vault_id(recovery_secret)`).
    pub vault_id: [u8; 16],
    /// Names of the durable (syncable) stores that host this vault.
    pub stores: Vec<String>,
}

impl VaultDirEntry {
    fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("VaultDirEntry JSON encoding is infallible")
    }
    fn decode(bytes: &[u8]) -> Result<Self> {
        serde_json::from_slice(bytes).map_err(|e| anyhow!("decoding vault directory entry: {e}"))
    }
}

/// A handle to the identity's `config` vault. Cheap to construct; each method is
/// a fresh registry + blob round trip (the vault is small and writes are rare).
pub struct ConfigVault {
    /// Identity master key — signs the HEAD; its pubkey locates the vault.
    master: SigningKey,
    /// Blob store backing both the prolly-tree blobs and the sealed root. For
    /// the vault to survive total device loss this MUST be a durable, recoverable
    /// store (the one `vup recover` re-selects), not a throwaway local cache.
    store: Arc<dyn Blobs>,
    registry: Arc<dyn RegistryApi + Send + Sync>,
    /// age recipients the vault is re-sealed to on every write (the device set
    /// + paper). A reader only needs to hold one of the matching identities.
    recipients: Vec<String>,
    /// age identity files tried when decrypting on read.
    identity_files: Vec<String>,
}

impl ConfigVault {
    pub fn new(
        master: SigningKey,
        store: Arc<dyn Blobs>,
        registry: Arc<dyn RegistryApi + Send + Sync>,
        recipients: Vec<String>,
        identity_files: Vec<String>,
    ) -> Self {
        Self {
            master,
            store,
            registry,
            recipients,
            identity_files,
        }
    }

    /// Every `stores/<name>` entry, decoded to its `[store.<name>]` config and
    /// keyed by `<name>`. Empty if the vault has never been published.
    pub async fn read_all(&self) -> Result<BTreeMap<String, NodeConfigStore>> {
        let mut out = BTreeMap::new();
        for (key, value) in self.read_raw().await? {
            let Some(name) = key.strip_prefix("stores/") else {
                continue;
            };
            let cfg: NodeConfigStore = serde_json::from_slice(&value)
                .map_err(|e| anyhow!("decoding stores/{name}: {e}"))?;
            out.insert(name.to_string(), cfg);
        }
        Ok(out)
    }

    /// The config for one store, or `None` if absent.
    pub async fn get(&self, name: &str) -> Result<Option<NodeConfigStore>> {
        Ok(self.read_all().await?.remove(name))
    }

    /// Upsert `stores/<name>` and republish the vault (HEAD revision++).
    pub async fn put(&self, name: &str, store_config: &NodeConfigStore) -> Result<()> {
        let encoded =
            serde_json::to_vec(store_config).map_err(|e| anyhow!("encoding stores/{name}: {e}"))?;
        let mut raw = self.read_raw().await?;
        raw.insert(entry_key(name), encoded);
        publish_vault_entries(
            raw,
            config_vault_id(),
            &self.master,
            self.store.clone(),
            self.registry.as_ref(),
            &self.recipients,
        )
        .await
    }

    /// The vault directory: every `vaults/<name>` entry decoded, keyed by
    /// `<name>`. Empty if nothing is published. The discovery index
    /// `bootstrap_from_identity` walks.
    pub async fn read_vault_dir(&self) -> Result<BTreeMap<String, VaultDirEntry>> {
        let mut out = BTreeMap::new();
        for (key, value) in self.read_raw().await? {
            let Some(name) = key.strip_prefix(VAULTS_PREFIX) else {
                continue;
            };
            out.insert(name.to_string(), VaultDirEntry::decode(&value)?);
        }
        Ok(out)
    }

    /// The identity-wide discovery `seed`, or `None` if not yet generated. With
    /// the vault directory it lets a recovering/paired device derive each
    /// vault's discovery keypair ([`crate::tasks::publish::discovery_signing_key`]).
    pub async fn read_seed(&self) -> Result<Option<[u8; 32]>> {
        Ok(self
            .read_raw()
            .await?
            .get(SEED_KEY)
            .and_then(|v| v.as_slice().try_into().ok()))
    }

    /// The raw key→bytes contents, or an empty map if nothing is published.
    async fn read_raw(&self) -> Result<BTreeMap<String, Vec<u8>>> {
        read_vault_entries(
            self.master.verifying_key().to_bytes(),
            config_vault_id(),
            self.store.clone(),
            self.registry.as_ref(),
            &self.identity_files,
        )
        .await
    }

    /// Re-seal and publish a complete raw entry set (HEAD revision++). No-op on
    /// an empty set.
    async fn publish_raw(&self, raw: BTreeMap<String, Vec<u8>>) -> Result<()> {
        publish_vault_entries(
            raw,
            config_vault_id(),
            &self.master,
            self.store.clone(),
            self.registry.as_ref(),
            &self.recipients,
        )
        .await
    }
}

/// Remote, credential-bearing backends worth syncing for recovery. Local
/// backends (Local/Memory/Fjall/LocalLinks) are device-specific and
/// never recovered from paper.
fn is_syncable(backend: &crate::config::NodeConfigStoreBackend) -> bool {
    use crate::config::NodeConfigStoreBackend as B;
    matches!(backend, B::Indexd(_) | B::S3(_) | B::SiaRenterd(_))
}

/// Publish the daemon's synced node configuration into the `config` vault,
/// hosted in the durable `[identity].bootstrap_store`, so a fresh device or a
/// paper recovery can rebuild the node after total device loss. Writes, in one
/// HEAD bump: the `[store.*]` configs, the vault directory (`vaults/<name> →
/// {vault_id, stores}`), and the identity-wide discovery `seed`.
///
/// Best-effort and idempotent: an unset/local-only bootstrap store, or no age
/// recipients, is a no-op (recovery is simply unavailable, logged once). Merges
/// rather than overwrites — entries another device synced stay intact — and
/// republishes only on a real change, so a quiescent daemon does not churn the
/// durable store on every boot.
pub async fn publish_bootstrap_config(
    config: &crate::config::S5NodeConfig,
    master: &SigningKey,
    blobs: &std::collections::HashMap<String, Arc<dyn Blobs>>,
    registry: Arc<dyn RegistryApi + Send + Sync>,
) -> Result<Option<[u8; 32]>> {
    let Some(bootstrap_name) = config.identity.bootstrap_store.as_deref() else {
        return Ok(None);
    };
    // Resolve the bootstrap store in the `dyn Blobs` view, NOT a path-`BlobStore`
    // map: the content-addressed Sia `PackingStore` (the common durable backend)
    // has no path-`Store` view and so never lands in a `BlobStore` map. Looking
    // it up there silently skipped the config-vault publish on Sia — "recovery
    // unavailable" — so a fresh device / paper recovery could not discover any
    // vaults. The `dyn Blobs` map (`vault_blobs` in `run_node`) includes it.
    let Some(store) = blobs.get(bootstrap_name).cloned() else {
        tracing::warn!(
            "identity.bootstrap_store = \"{bootstrap_name}\" not found in [store.*]; \
             config vault not published (recovery unavailable)"
        );
        return Ok(None);
    };

    let recipients: Vec<String> = config.key.values().map(|k| k.public_key.clone()).collect();
    if recipients.is_empty() {
        tracing::info!("bootstrap publish: no [key.*] recipients; skipping config vault");
        return Ok(None);
    }
    let identity_files: Vec<String> = config
        .key
        .values()
        .filter_map(|k| k.identity_file.clone())
        .collect();

    let vault = ConfigVault::new(master.clone(), store, registry, recipients, identity_files);

    // Assemble the complete desired entry set from the current published
    // state, so the store configs, vault directory, and discovery seed publish
    // in a single HEAD bump — and another device's entries are preserved
    // (merge, not clobber). Republish only if something actually changed, so a
    // quiescent daemon does not churn the durable store on every boot.
    let mut raw = vault.read_raw().await.unwrap_or_default();
    let before = raw.clone();

    // Identity-wide discovery seed: generated once, then stable. Random is
    // fine — the config vault is the durable source of truth both add-device
    // and recovery read; the daemon has no root_master at runtime to derive it
    // from (`run_node` boots from the device keyset, not the phrase).
    if !raw.contains_key(SEED_KEY) {
        let mut seed = [0u8; 32];
        rand::rng().fill_bytes(&mut seed);
        raw.insert(SEED_KEY.to_string(), seed.to_vec());
    }

    // Synced store configs — remote, credential-bearing backends only.
    for (name, store_cfg) in &config.store {
        if is_syncable(&store_cfg.backend) {
            raw.insert(
                entry_key(name),
                serde_json::to_vec(store_cfg)
                    .map_err(|e| anyhow!("encoding stores/{name}: {e}"))?,
            );
        }
    }

    // Vault directory: locator + durable hosts per snapped vault. A vault with
    // no local snapshot yet, or hosted only on device-local stores, is skipped
    // (nothing recoverable from paper) and picked up on a later boot.
    for name in config.vault.keys() {
        let vault_id = match crate::tasks::publish::vault_id_for_config(config, name) {
            Ok(Some(id)) => id,
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!(
                    vault = name,
                    "config vault: cannot derive vault_id, skipping: {e:#}"
                );
                continue;
            }
        };
        let dir_stores: Vec<String> = config
            .vault_read_stores(name, &config.vault[name])
            .unwrap_or_default()
            .into_iter()
            .filter(|s| {
                config
                    .store
                    .get(*s)
                    .is_some_and(|sc| is_syncable(&sc.backend))
            })
            .map(str::to_string)
            .collect();
        if dir_stores.is_empty() {
            continue;
        }
        raw.insert(
            format!("{VAULTS_PREFIX}{name}"),
            VaultDirEntry {
                vault_id,
                stores: dir_stores,
            }
            .encode(),
        );
    }

    // The seed is present in `raw` by now (just generated, or already there).
    let seed: [u8; 32] = raw
        .get(SEED_KEY)
        .and_then(|v| v.as_slice().try_into().ok())
        .ok_or_else(|| anyhow!("config vault seed missing after assembly"))?;

    if raw != before {
        vault.publish_raw(raw).await?;
        tracing::info!(
            bootstrap_store = bootstrap_name,
            "bootstrap: published config vault (store configs + vault directory + seed)"
        );
    }
    Ok(Some(seed))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::config::{IndexdStoreConfig, NodeConfigStoreBackend};
    use s5_core::blob::BlobStore;
    use s5_registry::MemoryRegistry;
    use s5_store_memory::MemoryStore;

    use super::*;

    fn age_identity(dir: &Path, name: &str) -> (String, String) {
        use age::secrecy::ExposeSecret;
        let identity = age::x25519::Identity::generate();
        let recipient = identity.to_public().to_string();
        let path = dir.join(format!("{name}.txt"));
        std::fs::write(&path, identity.to_string().expose_secret()).unwrap();
        (recipient, path.to_string_lossy().into_owned())
    }

    fn indexd_store(app_key_hex: &str) -> NodeConfigStore {
        NodeConfigStore::from_backend(NodeConfigStoreBackend::Indexd(IndexdStoreConfig {
            indexer_url: "https://sia.storage".to_string(),
            account: String::new(),
            app_key: app_key_hex.to_string(),
            cache_path: "/data/s5/indexd-cache".to_string(),
            max_inflight: None,
        }))
    }

    /// Device A writes a store config; device B — same identity, its own age
    /// key, no enrollment — reads it back verbatim. The recovered entry is the
    /// exact `[store.*]` config, AppKey inline. This is the whole point.
    #[tokio::test]
    async fn second_device_reads_config_without_re_enrolling() {
        let dir = tempfile::tempdir().unwrap();
        let (rec_a, id_a) = age_identity(dir.path(), "a");
        let (rec_b, id_b) = age_identity(dir.path(), "b");

        let store = Arc::new(BlobStore::new(MemoryStore::new()));
        let registry: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());
        let master = SigningKey::from_bytes(&[7u8; 32]);

        // Device A writes, sealing to both devices' age keys.
        let a = ConfigVault::new(
            master.clone(),
            store.clone(),
            registry.clone(),
            vec![rec_a, rec_b],
            vec![id_a],
        );
        let sia = indexd_store("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
        a.put("sia", &sia).await.unwrap();

        // Device B: different age key, same identity — recovers the exact config.
        let b = ConfigVault::new(
            master.clone(),
            store.clone(),
            registry.clone(),
            vec![],
            vec![id_b],
        );
        assert_eq!(
            b.get("sia")
                .await
                .unwrap()
                .expect("device B reads device A's store config"),
            sia
        );

        // A second write (revision++) and a local store both survive.
        let local = NodeConfigStore::from_backend(NodeConfigStoreBackend::Local(
            s5_store_local::LocalStoreConfig {
                base_path: "/data/s5/store".to_string(),
            },
        ));
        a.put("local", &local).await.unwrap();
        let all = b.read_all().await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all.get("sia"), Some(&sia));
        assert_eq!(all.get("local"), Some(&local));
    }

    /// The daemon's startup publish: only **remote** stores land in the vault,
    /// the inline AppKey survives, and a second run with unchanged config is a
    /// no-op (no churn on the durable store).
    #[tokio::test]
    async fn bootstrap_publishes_remote_stores_only_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let (recipient, id_file) = age_identity(dir.path(), "main");
        let app_key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let toml = format!(
            r#"
[identity]
secret_key_file = "x"
bootstrap_store = "sia"

[key.main]
public_key = "{recipient}"
identity_file = "{id_file}"

[store.sia]
type = "indexd"
indexer_url = "https://sia.storage"
app_key = "{app_key}"
cache_path = "/c"

[store.local]
type = "local"
base_path = "/data"
"#
        );
        let config: crate::config::S5NodeConfig = toml::from_str(&toml).unwrap();
        let stores: std::collections::HashMap<String, Arc<dyn Blobs>> =
            std::collections::HashMap::from([(
                "sia".to_string(),
                Arc::new(BlobStore::new(MemoryStore::new())) as Arc<dyn Blobs>,
            )]);
        let registry: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());
        let master = SigningKey::from_bytes(&[9u8; 32]);
        let stream_key = s5_core::StreamKey::Vault {
            pubkey: master.verifying_key().to_bytes(),
            vault_id: config_vault_id(),
        };

        publish_bootstrap_config(&config, &master, &stores, registry.clone())
            .await
            .unwrap();
        let rev1 = registry.get(&stream_key).await.unwrap().map(|e| e.revision);
        assert!(rev1.is_some(), "the config vault HEAD was published");

        // Read back through a ConfigVault over the same durable store + registry.
        let bs = stores["sia"].clone();
        let vault = ConfigVault::new(
            master.clone(),
            bs,
            registry.clone(),
            vec![recipient],
            vec![id_file],
        );
        let all = vault.read_all().await.unwrap();
        assert!(all.contains_key("sia"), "the remote indexd store is synced");
        assert!(
            !all.contains_key("local"),
            "a local store is device-specific and not synced"
        );
        // The inline AppKey survived the round trip.
        match &all["sia"].backend {
            crate::config::NodeConfigStoreBackend::Indexd(c) => assert_eq!(c.app_key, app_key),
            other => panic!("expected Indexd, got {other:?}"),
        }

        // Idempotent: republishing the same config must not bump the revision.
        publish_bootstrap_config(&config, &master, &stores, registry.clone())
            .await
            .unwrap();
        let rev2 = registry.get(&stream_key).await.unwrap().map(|e| e.revision);
        assert_eq!(rev2, rev1, "unchanged config must not republish");
    }

    #[tokio::test]
    async fn get_on_unpublished_vault_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let (_rec, id) = age_identity(dir.path(), "a");
        let store = Arc::new(BlobStore::new(MemoryStore::new()));
        let registry: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());
        let v = ConfigVault::new(
            SigningKey::from_bytes(&[1u8; 32]),
            store,
            registry,
            vec![],
            vec![id],
        );
        assert!(v.get("sia").await.unwrap().is_none());
        assert!(v.read_all().await.unwrap().is_empty());
        assert!(v.read_vault_dir().await.unwrap().is_empty());
        assert!(v.read_seed().await.unwrap().is_none());
    }

    /// The three kinds of entry — store config, vault directory entry, discovery
    /// seed — coexist in one config vault and read back through their typed
    /// views. This is what `bootstrap_from_identity` consumes on recovery.
    #[tokio::test]
    async fn store_config_vault_directory_and_seed_coexist() {
        let dir = tempfile::tempdir().unwrap();
        let (rec, id) = age_identity(dir.path(), "main");
        let store = Arc::new(BlobStore::new(MemoryStore::new()));
        let registry: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());
        let master = SigningKey::from_bytes(&[5u8; 32]);
        let vault = ConfigVault::new(master, store, registry, vec![rec], vec![id]);

        let sia = indexd_store("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef");
        let backup = VaultDirEntry {
            vault_id: [9u8; 16],
            stores: vec!["sia".to_string()],
        };
        let seed = [3u8; 32];

        let raw = BTreeMap::from([
            (entry_key("sia"), serde_json::to_vec(&sia).unwrap()),
            (format!("{VAULTS_PREFIX}backup"), backup.encode()),
            (SEED_KEY.to_string(), seed.to_vec()),
        ]);
        vault.publish_raw(raw).await.unwrap();

        assert_eq!(vault.get("sia").await.unwrap(), Some(sia));
        assert_eq!(
            vault.read_vault_dir().await.unwrap().get("backup"),
            Some(&backup)
        );
        assert_eq!(vault.read_seed().await.unwrap(), Some(seed));
        // The store view ignores non-store entries (no `vaults/backup` leaking in).
        assert_eq!(vault.read_all().await.unwrap().len(), 1);
    }
}
