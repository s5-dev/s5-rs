use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

// Re-export config types from s5_node_api so downstream users can access
// everything through `s5_node::config::*` as before.
pub use s5_node_api::config::{
    BlobPipelineConfig, CompressionConfig, FileChunkingConfig, NodeConfigIdentity, NodeConfigKey,
    NodeConfigRegistry, NodeConfigSource, NodeConfigTask, NodeConfigVault, PipelineRouteConfig,
    TaskSpec, TaskTrigger,
};

/// Returns the path for the default registry.
///
/// Looks up `registry["default"]` and extracts the local path.
/// Panics if no default registry is configured or it isn't a local variant.
pub fn default_registry_path(config: &S5NodeConfig) -> PathBuf {
    let reg = config
        .registry
        .get("default")
        .expect("no [registry.default] configured");
    match reg {
        NodeConfigRegistry::Local { path } => PathBuf::from(path),
        NodeConfigRegistry::Redb { path } => PathBuf::from(path),
        NodeConfigRegistry::StoreLocal { path, .. } => PathBuf::from(path),
        _ => panic!("default registry is not a local variant"),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct S5NodeConfig {
    pub identity: NodeConfigIdentity,
    /// Named age keys for encryption (public key + optional identity file).
    #[serde(default)]
    pub key: BTreeMap<String, NodeConfigKey>,
    pub store: BTreeMap<String, NodeConfigStore>,
    /// Node-wide default store: vaults without an explicit `data_store`
    /// use this. When unset and exactly one `[store.*]` entry exists,
    /// that entry is the implied default (architecture decision D1).
    #[serde(default)]
    pub default_store: Option<String>,
    /// Named registry backends keyed by name (e.g., "default").
    /// At least one entry named "default" is expected for normal operation.
    #[serde(default)]
    pub registry: BTreeMap<String, NodeConfigRegistry>,
    /// Local directories that s5 is allowed to read (security boundary).
    #[serde(default)]
    pub source: BTreeMap<String, NodeConfigSource>,
    /// FS5 vaults — each creates a separate FS5 root.
    /// Both encrypted and unencrypted trees are vaults.
    #[serde(default)]
    pub vault: BTreeMap<String, NodeConfigVault>,
    /// Named tasks — ingest, snapshot, backup operations.
    #[serde(default)]
    pub task: BTreeMap<String, NodeConfigTask>,
    /// Paired peer identities. Each entry maps a local nickname to a
    /// `did:s5:` value; vault `members` lists reference these by name.
    #[serde(default)]
    pub friend: BTreeMap<String, s5_node_api::config::NodeConfigFriend>,
}

// ---------------------------------------------------------------------------
// Store resolution (architecture decision D1)
// ---------------------------------------------------------------------------

impl S5NodeConfig {
    /// The node-wide default store name: explicit `default_store` if set,
    /// else the sole `[store.*]` entry when exactly one exists.
    pub fn default_store_name(&self) -> Option<&str> {
        if let Some(name) = self.default_store.as_deref() {
            return Some(name);
        }
        if self.store.len() == 1 {
            return self.store.keys().next().map(String::as_str);
        }
        None
    }

    /// The primary store for a vault's content blobs: `vault.data_store`,
    /// else the node default. Every write path and the head of every read
    /// chain use this — there is no implicit multi-store write fan-out.
    pub fn vault_data_store<'a>(
        &'a self,
        vault_name: &str,
        vault: &'a NodeConfigVault,
    ) -> anyhow::Result<&'a str> {
        vault
            .data_store
            .as_deref()
            .or_else(|| self.default_store_name())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "vault '{vault_name}' has no data store — set \
                     `vault.{vault_name}.data_store` or the node-level `default_store`"
                )
            })
    }

    /// The primary store for a vault's published meta blobs (encrypted TN,
    /// exports): `vault.meta_store`, else the vault's data store.
    pub fn vault_meta_store<'a>(
        &'a self,
        vault_name: &str,
        vault: &'a NodeConfigVault,
    ) -> anyhow::Result<&'a str> {
        if let Some(meta) = vault.meta_store.as_deref() {
            return Ok(meta);
        }
        self.vault_data_store(vault_name, vault)
    }

    /// The vault's read-fallback chain: data store first, then the meta
    /// store when distinct. Reads try each in order.
    pub fn vault_read_stores<'a>(
        &'a self,
        vault_name: &str,
        vault: &'a NodeConfigVault,
    ) -> anyhow::Result<Vec<&'a str>> {
        let data = self.vault_data_store(vault_name, vault)?;
        let meta = self.vault_meta_store(vault_name, vault)?;
        let mut chain = vec![data];
        if meta != data {
            chain.push(meta);
        }
        Ok(chain)
    }
}

/// One `[store.<name>]` entry in the node config.
///
/// Combines a `backend` (which physical store to open) with a small set
/// of wrapper-level toggles that apply to the `BlobStore` built on top
/// of it — currently just whether to write Bao outboard data.
///
/// TOML shape stays flat: `type = "fjall"`, `path = "..."`, and the
/// optional `outboard = true` all sit at the same level under `[store.<name>]`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigStore {
    /// The underlying physical store backend (Local, S3, Fjall, …).
    #[serde(flatten)]
    pub backend: NodeConfigStoreBackend,

    /// Write Bao outboard data alongside each blob ≥ 64 KiB.
    ///
    /// Outboard data (`obao6/...`) is only useful for **verified
    /// streaming**: peers fetching this store via the iroh blobs
    /// protocol use it to verify chunks against the BLAKE3 root without
    /// downloading the full blob first. Single-node deployments and
    /// untrusted-peer-free deployments don't need it.
    ///
    /// Default: `false`. This is a behavior change from earlier s5
    /// versions that defaulted obao to on; existing `obao6/` trees stay
    /// readable, but new writes won't add to them. Set `outboard = true`
    /// per store if you serve verified streams.
    #[serde(default)]
    pub outboard: bool,

    /// Optional in-RAM read-through cache above this store, in bytes.
    ///
    /// When set (and non-zero), the built store is wrapped in a
    /// [`s5_core::CachingStore`] backed by a byte-budgeted
    /// `MemoryStore::with_budget`. Reads consult RAM first and populate it on
    /// a miss; writes pass through to the durable store (the cache is NOT the
    /// source of truth and is not populated on write). Blobs are
    /// content-addressed, so cached entries never go stale.
    ///
    /// Intended for a publisher serving the same hot blobs to many peers (and
    /// repeated manifest re-walks): it turns repeated serves into RAM hits
    /// instead of file `open`/`read`/`close` on the hot tier. `None` (default)
    /// or `0` disables it.
    #[serde(default)]
    pub read_cache_bytes: Option<u64>,

    /// Friend-hosted-storage push ACL: local `[friend.<nick>]` nicknames
    /// authorised to push blobs into this store when we host it for them.
    ///
    /// Currently UNENFORCED — nothing in blob serving consumes it — and not
    /// exposed in the CLI (the `vup store allow/disallow` verbs were removed
    /// 2026-07-03 for that reason). The field stays for config-format
    /// stability and as the natural enforcement hook.
    /// TODO(friend-hosted storage): wire this into blob-serving
    /// authorization, then restore `vup store allow/disallow`.
    ///
    /// Each entry names a `[friend.*]` nickname (the same by-nickname
    /// convention vault `members`/`writers` use), so the mapping to a
    /// `did:s5:` stays in the friend table. Empty (default) = nobody but
    /// this identity may push. Serialised only when non-empty so
    /// existing/local-only configs stay clean.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow: Vec<String>,
}

/// Physical store backend variants. See [`NodeConfigStore`] for the
/// wrapper that adds top-level toggles.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum NodeConfigStoreBackend {
    SiaRenterd(s5_store_sia::SiaStoreConfig),
    Local(s5_store_local::LocalStoreConfig),
    S3(s5_store_s3::S3StoreConfig),
    Memory,
    /// Local links store (references files by hash without copying.)
    LocalLinks(LocalLinksStoreConfig),
    /// Fjall LSM-tree blob store (packs small blobs into large SSTs).
    Fjall(FjallStoreConfig),
    /// Sia storage via an indexd service (`s5_store_indexd::IndexdStore`). A
    /// leaf store: it owns a local index/capability cache and unwraps object
    /// data keys with an inline registered AppKey — all data lives in the
    /// config (like the `S3` backend's access/secret keys), so the daemon
    /// builds it directly with no external lookup.
    Indexd(IndexdStoreConfig),
}

impl NodeConfigStore {
    /// Convenience constructor preserving the old enum-only ergonomics
    /// (defaults `outboard = false`). Mostly useful in tests.
    pub fn from_backend(backend: NodeConfigStoreBackend) -> Self {
        Self {
            backend,
            outboard: false,
            read_cache_bytes: None,
            allow: Vec::new(),
        }
    }
}

/// Configuration for an indexd (Sia) blob store — see
/// [`NodeConfigStoreBackend::Indexd`] and `s5_store_indexd::IndexdStore`.
///
/// Standalone: every field needed to open the store lives here, mirroring the
/// `S3` backend's inline credentials. The same shape is what the `stores` vault
/// holds for cross-device sync (`docs/reference/special-vaults.md`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct IndexdStoreConfig {
    /// Indexer URL the SDK connects to (e.g. `https://sia.storage`).
    pub indexer_url: String,
    /// Account label on this indexer (`""` = the primary account). Scopes the
    /// AppKey derivation; recorded so recovery can re-derive the same key for
    /// this `(identity, indexer, account)`.
    #[serde(default)]
    pub account: String,
    /// The registered 32-byte AppKey, hex-encoded. Inline like the `S3`
    /// backend's `access_key` / `secret_key`; the same value is mirrored into
    /// the `stores` vault so other devices need not re-run the auth dance.
    pub app_key: String,
    /// Directory for the local index + capability cache (path↔object-id index
    /// plus sealed-capability cache). Holds only rebuildable state — it may be
    /// deleted and reconstructed from the indexer. Device-local: not carried
    /// across the stores-vault sync.
    pub cache_path: String,
    /// Max concurrent upload slab-buffers (`sia_storage`'s `max_buffered_slabs`)
    /// — the device-tunable RAM/throughput knob. In-flight upload memory is
    /// roughly `max_inflight × total_shards × 4 MiB`, so lower it on phones /
    /// low-RAM devices and raise it on capable ones for more concurrency.
    /// `None` = the built-in default (8).
    #[serde(default)]
    pub max_inflight: Option<usize>,
}

/// Configuration for a fjall blob store.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct FjallStoreConfig {
    /// Path to the fjall database directory.
    pub path: String,
    /// Block cache size in MiB (default: 256).
    pub cache_mib: Option<u32>,
}

/// Configuration for a local links store.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct LocalLinksStoreConfig {
    /// Path to the local_links database directory.
    pub path: String,
}

// ---------------------------------------------------------------------------
// Config validation
// ---------------------------------------------------------------------------

impl S5NodeConfig {
    /// Validate cross-references between config sections.
    ///
    /// Checks that:
    /// - Task references (vaults, sources, stores, keys) are valid
    /// - Task `then` chains reference declared tasks
    /// - Vault `key` references a declared key (with identity file)
    /// - `default_store` and vault `data_store`/`meta_store` reference
    ///   declared stores, and every vault resolves a data store
    /// - Identity `encrypted_with` references a declared key
    ///
    /// Returns a list of all validation errors found (empty = valid).
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        // Check identity encrypted_with
        if let Some(key_name) = &self.identity.encrypted_with
            && !self.key.contains_key(key_name)
        {
            errors.push(format!(
                "identity.encrypted_with: key \"{key_name}\" not found in [key.*]"
            ));
        }

        // Check the node default store reference
        if let Some(name) = &self.default_store
            && !self.store.contains_key(name)
        {
            errors.push(format!("default_store: \"{name}\" not found in [store.*]"));
        }

        for (store_name, store_cfg) in &self.store {
            // Indexd is standalone (no store refs); validate its inline fields.
            if let NodeConfigStoreBackend::Indexd(icfg) = &store_cfg.backend {
                if icfg.indexer_url.trim().is_empty() {
                    errors.push(format!("store.{store_name}: indexd.indexer_url is empty"));
                }
                if icfg.cache_path.trim().is_empty() {
                    errors.push(format!("store.{store_name}: indexd.cache_path is empty"));
                }
                match hex::decode(icfg.app_key.trim()) {
                    Ok(bytes) if bytes.len() == 32 => {}
                    Ok(bytes) => errors.push(format!(
                        "store.{store_name}: indexd.app_key must be 32 bytes (64 hex chars), got {}",
                        bytes.len()
                    )),
                    Err(_) => errors.push(format!(
                        "store.{store_name}: indexd.app_key is not valid hex"
                    )),
                }
            }
        }

        // Check vault references
        for (vault_name, vault_config) in &self.vault {
            if !self.key.contains_key(&vault_config.key) {
                errors.push(format!(
                    "vault.{vault_name}: key \"{}\" not found in [key.*]",
                    vault_config.key
                ));
            }
            if let Some(store_name) = &vault_config.data_store
                && !self.store.contains_key(store_name)
            {
                errors.push(format!(
                    "vault.{vault_name}: data_store \"{store_name}\" not found in [store.*]"
                ));
            }
            // Every vault must resolve a data store — fail at load, not
            // on the first snap.
            if vault_config.data_store.is_none() && self.default_store_name().is_none() {
                errors.push(format!(
                    "vault.{vault_name}: no data store resolvable — set \
                     `vault.{vault_name}.data_store` or the node-level `default_store` \
                     (required when more than one [store.*] exists)"
                ));
            }
            for recipient_name in &vault_config.recipients {
                if !self.key.contains_key(recipient_name) {
                    errors.push(format!(
                        "vault.{vault_name}: recipient \"{recipient_name}\" not found in [key.*]"
                    ));
                }
            }
            for source_name in &vault_config.sources {
                if !self.source.contains_key(source_name) {
                    errors.push(format!(
                        "vault.{vault_name}: source \"{source_name}\" not found in [source.*]"
                    ));
                }
            }
            if let Some(store_name) = &vault_config.meta_store
                && !self.store.contains_key(store_name)
            {
                errors.push(format!(
                    "vault.{vault_name}: meta_store \"{store_name}\" not found in [store.*]"
                ));
            }
            // writers ⊆ members (D11): a write-capable identity must first be
            // a member (readers get the ACL/recipients; writers additionally
            // get signer authority).
            for w in &vault_config.writers {
                if !vault_config.members.iter().any(|m| m == w) {
                    errors.push(format!(
                        "vault.{vault_name}: writer \"{w}\" is not in members — \
                         grant read access first"
                    ));
                }
            }
        }

        // Check task references
        for (task_name, task_config) in &self.task {
            // Validate then-chains
            for then_name in &task_config.then {
                if !self.task.contains_key(then_name) {
                    errors.push(format!(
                        "task.{task_name}: then target \"{then_name}\" not found in [task.*]"
                    ));
                }
            }

            // Validate spec-specific references
            match &task_config.spec {
                TaskSpec::Ingest {
                    vault,
                    source,
                    blob_store,
                    ..
                } => {
                    if !self.vault.contains_key(vault) {
                        errors.push(format!(
                            "task.{task_name}: vault \"{vault}\" not found in [vault.*]"
                        ));
                    }
                    if !self.source.contains_key(source) {
                        errors.push(format!(
                            "task.{task_name}: source \"{source}\" not found in [source.*]"
                        ));
                    }
                    if !self.store.contains_key(blob_store) {
                        errors.push(format!(
                            "task.{task_name}: blob_store \"{blob_store}\" not found in [store.*]"
                        ));
                    }
                }
                TaskSpec::Publish { vault, keys } => {
                    if !self.vault.contains_key(vault) {
                        errors.push(format!(
                            "task.{task_name}: vault \"{vault}\" not found in [vault.*]"
                        ));
                    }
                    for key_name in keys {
                        if !self.key.contains_key(key_name) {
                            errors.push(format!(
                                "task.{task_name}: key \"{key_name}\" not found in [key.*]"
                            ));
                        }
                    }
                }
                TaskSpec::Backup {
                    vault,
                    source,
                    blob_store,
                    keys,
                    ..
                } => {
                    if !self.vault.contains_key(vault) {
                        errors.push(format!(
                            "task.{task_name}: vault \"{vault}\" not found in [vault.*]"
                        ));
                    }
                    if !self.source.contains_key(source) {
                        errors.push(format!(
                            "task.{task_name}: source \"{source}\" not found in [source.*]"
                        ));
                    }
                    if !self.store.contains_key(blob_store) {
                        errors.push(format!(
                            "task.{task_name}: blob_store \"{blob_store}\" not found in [store.*]"
                        ));
                    }
                    for key_name in keys {
                        if !self.key.contains_key(key_name) {
                            errors.push(format!(
                                "task.{task_name}: key \"{key_name}\" not found in [key.*]"
                            ));
                        }
                    }
                }
                TaskSpec::Restore {
                    vault, blob_store, ..
                } => {
                    if !self.vault.contains_key(vault) {
                        errors.push(format!(
                            "task.{task_name}: vault \"{vault}\" not found in [vault.*]"
                        ));
                    }
                    if let Some(store_name) = blob_store
                        && !self.store.contains_key(store_name)
                    {
                        errors.push(format!(
                            "task.{task_name}: blob_store \"{store_name}\" not found in [store.*]"
                        ));
                    }
                }
                TaskSpec::Copy {
                    src_vault,
                    dst_vault,
                    blob_store,
                    keys,
                    ..
                } => {
                    if !self.vault.contains_key(src_vault) {
                        errors.push(format!(
                            "task.{task_name}: src_vault \"{src_vault}\" not found in [vault.*]"
                        ));
                    }
                    if !self.vault.contains_key(dst_vault) {
                        errors.push(format!(
                            "task.{task_name}: dst_vault \"{dst_vault}\" not found in [vault.*]"
                        ));
                    }
                    if !self.store.contains_key(blob_store) {
                        errors.push(format!(
                            "task.{task_name}: blob_store \"{blob_store}\" not found in [store.*]"
                        ));
                    }
                    for key_name in keys {
                        if !self.key.contains_key(key_name) {
                            errors.push(format!(
                                "task.{task_name}: key \"{key_name}\" not found in [key.*]"
                            ));
                        }
                    }
                }
            }

            // Automation trigger invariants (Stage 7): `Every` needs a cadence;
            // `Watch` only makes sense for a Backup (the daemon watches its
            // source paths). `Manual` is unconstrained.
            match task_config.trigger {
                TaskTrigger::Every => {
                    if task_config.interval_secs.is_none() {
                        errors.push(format!(
                            "task.{task_name}: trigger = \"every\" requires `interval_secs`"
                        ));
                    }
                }
                TaskTrigger::Watch => {
                    if !matches!(task_config.spec, TaskSpec::Backup { .. }) {
                        errors.push(format!(
                            "task.{task_name}: trigger = \"watch\" requires a backup task \
                             (the daemon watches the source's paths)"
                        ));
                    }
                }
                TaskTrigger::Manual => {}
            }
        }

        // Check registry store references
        for (reg_name, reg_config) in &self.registry {
            self.validate_registry(reg_name, reg_config, &mut errors);
        }

        errors
    }

    /// Recursively validate a registry config entry.
    // Clippy's `collapsible_match` suggestion would convert the inner `if` into
    // a match guard, but that breaks exhaustiveness here — there's no
    // fall-through arm for Store/Remote when the guard fails.
    #[allow(clippy::collapsible_match)]
    fn validate_registry(&self, name: &str, reg: &NodeConfigRegistry, errors: &mut Vec<String>) {
        match reg {
            NodeConfigRegistry::Store { store, .. } => {
                if !self.store.contains_key(store) {
                    errors.push(format!(
                        "registry.{name}: store \"{store}\" not found in [store.*]"
                    ));
                }
            }
            NodeConfigRegistry::Multi { backends, .. } => {
                for backend in backends {
                    self.validate_registry(name, backend, errors);
                }
            }
            _ => {} // Local, Redb, StoreLocal, Memory — no cross-references
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Full backup-style config — tests that the new vault + task model
    /// deserializes correctly from TOML.
    const BACKUP_CONFIG: &str = r#"
[identity]
secret_key_file = "node.key.age"
encrypted_with = "local"

[key.local]
public_key = "age1local..."
identity_file = "local.age"

[key.yubikey]
public_key = "age1yubikey..."

[registry.default]
type = "local"
path = "/root/.config/s5/registry"

[store.hetzner]
type = "s3"
bucket_name = "my-backup"
endpoint = "https://s3.hetzner.com"
region = "eu-central"
access_key = "AKIAIOSFODNN7EXAMPLE"
secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

[store.local-ssd]
type = "local"
base_path = "/root/.local/share/s5/blobs"

[source.unsorted]
paths = ["/data/unsorted/models", "/data/unsorted/steam"]
respect_ignore_files = true

[source.photos]
paths = ["/home/user/Photos"]
exclude = ["*.tmp", "*.log"]

[vault.backup]
root_path = "/data/backups-metadata"
key = "local"
preset = "e2ee_prolly_chunked_zstd_dict_default_chacha20"
data_store = "hetzner"
recipients = ["local", "yubikey"]
sources = ["unsorted", "photos"]
meta_store = "local-ssd"

[task.ingest-unsorted]
type = "ingest"
vault = "backup"
source = "unsorted"
blob_store = "hetzner"
target_path = "unsorted"

[task.publish]
type = "publish"
vault = "backup"
keys = ["local", "yubikey"]

[task.full-backup]
type = "backup"
vault = "backup"
source = "photos"
blob_store = "hetzner"
keys = ["local", "yubikey"]
then = ["publish"]
"#;

    /// Minimal config — just identity + store, no vaults or tasks.
    const MINIMAL_CONFIG: &str = r#"
[identity]
secret_key_file = "local.secretkey"

[store.local]
type = "local"
base_path = "/blobs"
"#;

    #[test]
    fn backup_config_round_trip() {
        let config: S5NodeConfig = toml::from_str(BACKUP_CONFIG).expect("parse backup config");

        // Identity
        assert_eq!(
            config.identity.secret_key_file.as_deref(),
            Some("node.key.age")
        );
        assert_eq!(config.identity.encrypted_with.as_deref(), Some("local"));

        // Keys
        assert_eq!(config.key.len(), 2);
        assert_eq!(config.key["local"].public_key, "age1local...");
        assert_eq!(
            config.key["local"].identity_file.as_deref(),
            Some("local.age")
        );
        assert_eq!(config.key["yubikey"].public_key, "age1yubikey...");
        assert!(config.key["yubikey"].identity_file.is_none());

        // Sources
        assert_eq!(config.source.len(), 2);
        assert_eq!(
            config.source["unsorted"].paths,
            vec!["/data/unsorted/models", "/data/unsorted/steam"]
        );
        assert!(config.source["unsorted"].respect_ignore_files);
        assert_eq!(config.source["photos"].exclude, vec!["*.tmp", "*.log"]);

        // Vaults
        assert_eq!(config.vault.len(), 1);
        let backup = &config.vault["backup"];
        assert_eq!(backup.root_path, "/data/backups-metadata");
        assert_eq!(backup.key, "local");
        assert_eq!(
            backup.preset.as_deref(),
            Some("e2ee_prolly_chunked_zstd_dict_default_chacha20")
        );
        assert_eq!(backup.data_store.as_deref(), Some("hetzner"));
        assert_eq!(backup.recipients, vec!["local", "yubikey"]);
        assert_eq!(backup.sources, vec!["unsorted", "photos"]);
        assert_eq!(backup.meta_store.as_deref(), Some("local-ssd"));
        assert_eq!(
            config.vault_read_stores("backup", backup).unwrap(),
            vec!["hetzner", "local-ssd"]
        );
        assert!(!backup.plaintext_tree); // default false → tree is encrypted

        // Tasks
        assert_eq!(config.task.len(), 3);

        let ingest = &config.task["ingest-unsorted"];
        assert!(ingest.then.is_empty());
        match &ingest.spec {
            TaskSpec::Ingest {
                vault,
                source,
                blob_store,
                target_path,
            } => {
                assert_eq!(vault, "backup");
                assert_eq!(source, "unsorted");
                assert_eq!(blob_store, "hetzner");
                assert_eq!(target_path.as_deref(), Some("unsorted"));
            }
            other => panic!("expected Ingest, got {other:?}"),
        }

        let publish = &config.task["publish"];
        match &publish.spec {
            TaskSpec::Publish { vault, keys } => {
                assert_eq!(vault, "backup");
                assert_eq!(keys, &vec!["local", "yubikey"]);
            }
            other => panic!("expected Publish, got {other:?}"),
        }

        let full = &config.task["full-backup"];
        assert_eq!(full.then, vec!["publish"]);
        match &full.spec {
            TaskSpec::Backup {
                vault,
                source,
                blob_store,
                keys,
                ..
            } => {
                assert_eq!(vault, "backup");
                assert_eq!(source, "photos");
                assert_eq!(blob_store, "hetzner");
                assert_eq!(keys, &vec!["local", "yubikey"]);
            }
            other => panic!("expected Backup, got {other:?}"),
        }

        // Validation should pass
        let errors = config.validate();
        assert!(errors.is_empty(), "expected no errors, got: {errors:?}");

        // Round-trip: serialize back to TOML and re-parse
        let toml_str = toml::to_string(&config).expect("serialize");
        let config2: S5NodeConfig = toml::from_str(&toml_str).expect("re-parse");
        assert_eq!(config, config2);
    }

    #[test]
    fn minimal_config_parses() {
        let config: S5NodeConfig = toml::from_str(MINIMAL_CONFIG).expect("parse minimal config");
        assert!(config.source.is_empty());
        assert!(config.vault.is_empty());
        assert!(config.task.is_empty());
        assert!(config.key.is_empty());
    }

    /// Exercises every validation path in a single config with many dangling refs.
    #[test]
    fn validation_catches_dangling_references() {
        let toml_str = r#"
[identity]
secret_key_file = "local.secretkey"
encrypted_with = "ghost_key"

[key.local]
public_key = "age1local..."
identity_file = "local.age"

[store.local]
type = "local"
base_path = "/blobs"

[source.data]
paths = ["/data"]

[vault.v]
root_path = "/tmp/v"
key = "missing_key"
data_store = "missing_store"
recipients = ["missing_recipient"]
sources = ["missing_source_ref"]
meta_store = "missing_meta_store"

[task.ingest]
type = "ingest"
vault = "missing_vault"
source = "missing_source"
blob_store = "missing_store"
then = ["missing_task"]

[task.pub]
type = "publish"
vault = "missing_vault"
keys = ["missing_key"]
then = ["missing_task"]

[task.bak]
type = "backup"
vault = "missing_vault"
source = "missing_source"
blob_store = "missing_store"
keys = ["missing_key"]
"#;
        let config: S5NodeConfig = toml::from_str(toml_str).expect("parse");
        let errors = config.validate();

        // Check that each category of dangling ref is caught.
        let has = |needle: &str| errors.iter().any(|e| e.contains(needle));
        assert!(
            has("identity.encrypted_with"),
            "missing: identity key\n{errors:#?}"
        );
        assert!(
            has("vault.v") && has("missing_key"),
            "missing: vault key\n{errors:#?}"
        );
        assert!(
            has("missing_store"),
            "missing: vault blob_store\n{errors:#?}"
        );
        assert!(
            has("missing_recipient"),
            "missing: vault recipient\n{errors:#?}"
        );
        assert!(
            has("missing_source_ref"),
            "missing: vault source ref\n{errors:#?}"
        );
        assert!(
            has("missing_meta_store"),
            "missing: vault meta_target\n{errors:#?}"
        );
        assert!(
            has("task.ingest") && has("missing_vault"),
            "missing: task vault\n{errors:#?}"
        );
        assert!(
            has("task.ingest") && has("missing_source"),
            "missing: task source\n{errors:#?}"
        );
        assert!(has("missing_task"), "missing: then target\n{errors:#?}");
        assert!(has("task.pub"), "missing: publish refs\n{errors:#?}");
        assert!(has("task.bak"), "missing: backup refs\n{errors:#?}");
    }

    #[test]
    fn vault_requires_key() {
        let toml_str = r#"
[identity]
secret_key_file = "local.secretkey"

[store.local]
type = "local"
base_path = "/blobs"

[vault.plain]
root_path = "/tmp/plain"
"#;
        let result: Result<S5NodeConfig, _> = toml::from_str(toml_str);
        assert!(result.is_err(), "vault without key should fail to parse");
    }

    #[test]
    fn store_registry_parses_and_validates() {
        let toml_str = r#"
[identity]
secret_key_file = "local.secretkey"

[store.my-s3]
type = "s3"
bucket_name = "backups"
endpoint = "https://s3.example.com"
region = "us-east-1"
access_key = "AK"
secret_key = "SK"

[registry.default]
type = "store"
store = "my-s3"
prefix = "reg"

[registry.bad]
type = "store"
store = "nonexistent"
"#;
        let config: S5NodeConfig = toml::from_str(toml_str).expect("parse store registry config");

        // Check deserialization
        match &config.registry["default"] {
            NodeConfigRegistry::Store { store, prefix } => {
                assert_eq!(store, "my-s3");
                assert_eq!(prefix.as_deref(), Some("reg"));
            }
            other => panic!("expected Store variant, got {other:?}"),
        }

        // Validation: "default" is fine, "bad" references missing store
        let errors = config.validate();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("registry.bad") && e.contains("nonexistent")),
            "expected validation error for bad registry store ref, got: {errors:#?}"
        );
        assert!(
            !errors.iter().any(|e| e.contains("registry.default")),
            "default registry should be valid, got: {errors:#?}"
        );
    }

    #[test]
    fn indexd_store_round_trip_and_validation() {
        let toml_str = r#"
[identity]
secret_key_file = "local.secretkey"

[store.sia]
type = "indexd"
indexer_url = "https://sia.storage"
app_key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
cache_path = "/data/s5/indexd-cache"
"#;
        let config: S5NodeConfig = toml::from_str(toml_str).expect("parse indexd config");
        match &config.store["sia"].backend {
            NodeConfigStoreBackend::Indexd(icfg) => {
                assert_eq!(icfg.indexer_url, "https://sia.storage");
                assert_eq!(icfg.account, "", "account defaults to the primary account");
                assert_eq!(icfg.app_key.len(), 64, "32-byte AppKey, hex-encoded");
                assert_eq!(icfg.cache_path, "/data/s5/indexd-cache");
            }
            other => panic!("expected Indexd variant, got {other:?}"),
        }

        // Standalone: nothing to cross-reference, valid inline fields.
        assert!(
            config.validate().is_empty(),
            "got: {:#?}",
            config.validate()
        );

        // Round-trip via TOML re-emit.
        let back = toml::to_string(&config).expect("serialize");
        let config2: S5NodeConfig = toml::from_str(&back).expect("re-parse");
        assert_eq!(config, config2);
    }

    #[test]
    fn indexd_validation_rejects_bad_app_key() {
        let toml_str = r#"
[identity]
secret_key_file = "local.secretkey"

[store.sia]
type = "indexd"
indexer_url = "https://sia.storage"
app_key = "not-hex"
cache_path = "/data/s5/indexd-cache"
"#;
        let config: S5NodeConfig = toml::from_str(toml_str).expect("parse");
        let errors = config.validate();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("app_key is not valid hex")),
            "expected an app_key hex error, got: {errors:#?}"
        );
    }

    #[test]
    fn multi_registry_over_store_parses_and_validates() {
        // Mirrors what `vup onboard` generates for a Sia store: a Multi registry
        // fanning out to local redb + a StoreRegistry over the remote store, so
        // HEADs are durable for `vup recover`.
        let toml_str = r#"
[identity]
secret_key_file = "x"
bootstrap_store = "sia"

[store.sia]
type = "indexd"
indexer_url = "https://sia.storage"
app_key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
cache_path = "/c"

[registry.default]
type = "multi"
write_policy = "all"

[[registry.default.backends]]
type = "redb"
path = "/r"

[[registry.default.backends]]
type = "store"
store = "sia"
prefix = "registry"
"#;
        let config: S5NodeConfig = toml::from_str(toml_str).expect("parse multi registry config");
        assert_eq!(config.identity.bootstrap_store.as_deref(), Some("sia"));
        match &config.registry["default"] {
            NodeConfigRegistry::Multi {
                backends,
                write_policy,
            } => {
                assert_eq!(backends.len(), 2, "redb + store backends");
                assert_eq!(write_policy.as_deref(), Some("all"));
                assert!(matches!(backends[1], NodeConfigRegistry::Store { .. }));
            }
            other => panic!("expected Multi registry, got {other:?}"),
        }
        assert!(
            config.validate().is_empty(),
            "got: {:#?}",
            config.validate()
        );
    }

    /// Vault `pipelines` round-trip: glob + pipeline + chunking. Tests
    /// three representative pipeline shapes (zstd default, zstd L9,
    /// uncompressed) against the route schema.
    #[test]
    fn vault_pipelines_round_trip() {
        let toml_str = r#"
[identity]
secret_key_file = "local.secretkey"

[key.media]
public_key = "age1media..."
identity_file = "media.age"

[store.media_blobs]
type = "memory"

[vault.media]
root_path = "/data/media/.s5-vault"
key = "media"
data_store = "media_blobs"
plaintext_tree = true

[[vault.media.pipelines]]
glob = "segments/**/*.seg"
pipeline = { compression = { type = "zstd", level = 9 } }
chunking = { type = "fixed", chunk_size = 8388608 }

[[vault.media.pipelines]]
glob = "segments/**/*.eseg"
pipeline = { compression = { type = "zstd" } }
chunking = { type = "fixed", chunk_size = 67108864 }

[[vault.media.pipelines]]
glob = "{ledger,rindex,interner_packs}/**"
pipeline = { compression = { type = "uncompressed" } }
chunking = { type = "none" }
"#;
        let config: S5NodeConfig = toml::from_str(toml_str).expect("parse vault.pipelines");
        let v = &config.vault["media"];
        assert_eq!(v.pipelines.len(), 3);

        // Route 0: segments/**/*.seg with zstd L9 + 8 MiB chunks.
        let r0 = &v.pipelines[0];
        assert_eq!(r0.glob, "segments/**/*.seg");
        let p0 = r0.pipeline.as_ref().expect("pipeline 0");
        match p0.compression.as_ref().unwrap() {
            CompressionConfig::Zstd { level: Some(9) } => {}
            other => panic!("expected zstd level 9, got {other:?}"),
        }
        assert!(matches!(
            r0.chunking.as_ref().unwrap(),
            FileChunkingConfig::Fixed {
                chunk_size: 8388608
            }
        ));

        // Route 1: zstd default level (level: None).
        let r1 = &v.pipelines[1];
        let p1 = r1.pipeline.as_ref().unwrap();
        match p1.compression.as_ref().unwrap() {
            CompressionConfig::Zstd { level: None } => {}
            other => panic!("expected zstd default level, got {other:?}"),
        }

        // Route 2: uncompressed + chunking::none.
        let r2 = &v.pipelines[2];
        assert_eq!(r2.glob, "{ledger,rindex,interner_packs}/**");
        match r2.pipeline.as_ref().unwrap().compression.as_ref().unwrap() {
            CompressionConfig::Uncompressed => {}
            other => panic!("expected uncompressed, got {other:?}"),
        }
        assert!(matches!(
            r2.chunking.as_ref().unwrap(),
            FileChunkingConfig::None
        ));

        // Round-trip via TOML re-emit.
        let back = toml::to_string(&config).expect("serialize");
        let config2: S5NodeConfig = toml::from_str(&back).expect("re-parse");
        assert_eq!(config, config2);

        // Validation passes: all referenced keys/stores exist.
        let errors = config.validate();
        assert!(errors.is_empty(), "got: {errors:#?}");
    }

    /// `outboard` field is optional and defaults to false (off — opt in
    /// via `outboard = true` per store). Verifies the wrapper-vs-backend
    /// flatten behavior holds for both omitted and explicit values.
    #[test]
    fn store_outboard_defaults_off_and_round_trips() {
        let toml_str = r#"
[identity]
secret_key_file = "local.secretkey"

[store.implicit]
type = "memory"

[store.explicit_off]
type = "memory"
outboard = false

[store.explicit_on]
type = "memory"
outboard = true
"#;
        let config: S5NodeConfig = toml::from_str(toml_str).expect("parse outboard config");

        assert!(!config.store["implicit"].outboard);
        assert!(!config.store["explicit_off"].outboard);
        assert!(config.store["explicit_on"].outboard);

        // All three resolve to a Memory backend regardless of the flag.
        for name in ["implicit", "explicit_off", "explicit_on"] {
            assert!(matches!(
                &config.store[name].backend,
                NodeConfigStoreBackend::Memory
            ));
        }

        // Round-trip preserves the flag — including the `false` case
        // (serde emits it as `outboard = false`, not as omission, but
        // either form re-parses to the same struct).
        let back = toml::to_string(&config).expect("serialize");
        let config2: S5NodeConfig = toml::from_str(&back).expect("re-parse");
        assert_eq!(config, config2);
    }
}
