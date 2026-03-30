use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

// Re-export config types from s5_node_api so downstream users can access
// everything through `s5_node::config::*` as before.
pub use s5_node_api::config::{
    NodeConfigIdentity, NodeConfigKey, NodeConfigRegistry, NodeConfigSource, NodeConfigTask,
    NodeConfigVault, TaskSpec,
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
    #[serde(default)]
    pub peer: BTreeMap<String, NodeConfigPeer>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum NodeConfigStore {
    SiaRenterd(s5_store_sia::SiaStoreConfig),
    Local(s5_store_local::LocalStoreConfig),
    S3(s5_store_s3::S3StoreConfig),
    Memory,
    /// Local links store (references files by hash without copying.)
    LocalLinks(LocalLinksStoreConfig),
}

/// Configuration for a local links store.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct LocalLinksStoreConfig {
    /// Path to the local_links database directory.
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigPeer {
    /// Peer public ID string used for both ACLs and dialing.
    #[serde(default)]
    pub id: String,
    /// Optional blob ACL configuration; defaults to no access.
    #[serde(default)]
    pub blobs: s5_blobs::PeerConfigBlobs,
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
    /// - Vault `blob_stores` reference declared stores
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

        // Check vault references
        for (vault_name, vault_config) in &self.vault {
            if !self.key.contains_key(&vault_config.key) {
                errors.push(format!(
                    "vault.{vault_name}: key \"{}\" not found in [key.*]",
                    vault_config.key
                ));
            }
            for store_name in &vault_config.blob_stores {
                if !self.store.contains_key(store_name) {
                    errors.push(format!(
                        "vault.{vault_name}: blob_store \"{store_name}\" not found in [store.*]"
                    ));
                }
            }
            for peer_name in &vault_config.peers {
                if !self.peer.contains_key(peer_name) {
                    errors.push(format!(
                        "vault.{vault_name}: peer \"{peer_name}\" not found in [peer.*]"
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
                TaskSpec::RemoteRestore { blob_store, .. } => {
                    // Vault name is only used for key derivation — it doesn't
                    // need to match a configured vault (disaster recovery may
                    // happen on a fresh node). Only validate store exists.
                    if !self.store.contains_key(blob_store) {
                        errors.push(format!(
                            "task.{task_name}: blob_store \"{blob_store}\" not found in [store.*]"
                        ));
                    }
                }
            }
        }

        // Check registry store references
        for (reg_name, reg_config) in &self.registry {
            self.validate_registry(reg_name, reg_config, &mut errors);
        }

        errors
    }

    /// Recursively validate a registry config entry.
    fn validate_registry(&self, name: &str, reg: &NodeConfigRegistry, errors: &mut Vec<String>) {
        match reg {
            NodeConfigRegistry::Store { store, .. } => {
                if !self.store.contains_key(store) {
                    errors.push(format!(
                        "registry.{name}: store \"{store}\" not found in [store.*]"
                    ));
                }
            }
            NodeConfigRegistry::Remote { peer } => {
                if !self.peer.contains_key(peer) {
                    errors.push(format!(
                        "registry.{name}: peer \"{peer}\" not found in [peer.*]"
                    ));
                }
            }
            NodeConfigRegistry::Tee { local, remote_peer } => {
                self.validate_registry(name, local, errors);
                if !self.peer.contains_key(remote_peer) {
                    errors.push(format!(
                        "registry.{name}: remote_peer \"{remote_peer}\" not found in [peer.*]"
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
blob_stores = ["hetzner", "local-ssd"]

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
        assert_eq!(backup.blob_stores, vec!["hetzner", "local-ssd"]);
        assert!(backup.peers.is_empty());

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
blob_stores = ["missing_store"]
peers = ["missing_peer"]

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
        assert!(has("missing_peer"), "missing: vault peer\n{errors:#?}");
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
}
