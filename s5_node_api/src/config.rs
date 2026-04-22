//! Configuration types shared between the S5 node and its clients.
//!
//! These types are the subset of `S5NodeConfig` that can live in the
//! API crate without pulling in heavy store/blob dependencies.
//! The full `S5NodeConfig` (which adds `NodeConfigStore`, `NodeConfigPeer`,
//! and cross-references) remains in `s5_node::config`.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigIdentity {
    pub secret_key_file: Option<String>,
    pub secret_key: Option<String>,
    /// Optional key name from `[key.*]` — identity file is encrypted with this key.
    #[serde(default)]
    pub encrypted_with: Option<String>,
}

// ---------------------------------------------------------------------------
// Key
// ---------------------------------------------------------------------------

/// An age key used for vault encryption.
///
/// `public_key` is always present (age recipient string).
/// `identity_file` is optional — if present, s5 can decrypt with this key.
/// Keys without an identity file are encrypt-only (e.g. yubikey recipients
/// where the hardware key is not always available).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigKey {
    /// Age public key (recipient string, e.g. "age1abc...").
    pub public_key: String,
    /// Optional path to an age identity file for decryption.
    #[serde(default)]
    pub identity_file: Option<String>,
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Declares a local directory that s5 is *allowed* to read.
///
/// This is a security boundary — nothing outside declared sources can be
/// ingested, regardless of what remote orchestration requests.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigSource {
    /// Filesystem paths to the source directories.
    /// Supports both a single path and multiple paths.
    pub paths: Vec<String>,

    /// Whether to include cache directories (CACHEDIR.TAG, node_modules, target/, ...).
    /// Default: false — skip known cache dirs.
    #[serde(default)]
    pub include_caches: bool,

    /// Whether to skip hidden files and directories (dotfiles/dotdirs).
    /// Default: false — include hidden entries.
    #[serde(default)]
    pub skip_hidden: bool,

    /// Whether to respect .gitignore / .ignore rules.
    /// Default: false — don't follow ignore rules, include everything.
    #[serde(default)]
    pub respect_ignore_files: bool,

    /// Glob patterns to exclude from this source.
    #[serde(default)]
    pub exclude: Vec<String>,

    /// Stay on the same filesystem — do not cross mount boundaries.
    /// Default: false.
    #[serde(default)]
    pub one_file_system: bool,
}

// ---------------------------------------------------------------------------
// Vault
// ---------------------------------------------------------------------------

/// An FS5 vault — the canonical local metadata state.
///
/// A vault only advances via merges from task-produced snapshots.
/// Each vault creates a separate FS5 root at the configured `root_path`.
/// All vaults are encrypted — `key` is required.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigVault {
    /// Absolute path to the FS5 root directory (contains `root.fs5.cbor`
    /// and the metadata store). Metadata is always local.
    pub root_path: String,

    /// Key name from `[key.*]` used to encrypt this vault's local state.
    /// The node must have the identity file for this key (can decrypt).
    pub key: String,

    /// Blob store names where this vault's data blobs live (for reads).
    /// When downloading a blob, these stores are checked in order.
    /// Must reference declared `[store.*]` entries.
    #[serde(default)]
    pub blob_stores: Vec<String>,

    /// Optional preset string for FS5 pipeline configuration
    /// (e.g. "e2ee_prolly_chunked_zstd_dict_default_chacha20").
    #[serde(default)]
    pub preset: Option<String>,

    /// Peers this vault is shared with (for sync/replication).
    /// Must reference declared `[peer.*]` entries.
    // TODO: design peer-based sync — deferred, focus on backups first.
    #[serde(default)]
    pub peers: Vec<String>,

    /// Use filesystem events (inotify/FSEvents) for immediate detection.
    /// Default: false.
    #[serde(default)]
    pub watch: bool,
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum NodeConfigRegistry {
    /// Local Redb-backed registry at the given path.
    ///
    /// `type = "local"` is the recommended default — uses Redb under the hood.
    Local {
        /// Absolute path to the registry data directory.
        path: String,
    },
    /// Explicit Redb backend (alias for `local` with a different name).
    Redb {
        /// Absolute path to the registry data directory.
        path: String,
    },
    /// Registry stored in a local blob store directory via `StoreRegistry`.
    StoreLocal {
        /// Absolute path to the registry data directory.
        path: String,
        /// Optional key prefix within the store; defaults to "registry".
        #[serde(default)]
        prefix: Option<String>,
    },
    /// In-memory registry for testing or ephemeral nodes.
    Memory,
    /// Remote registry accessed via iroh/irpc from another S5 node.
    Remote {
        /// The peer name (key in `peer` map) to connect to.
        peer: String,
    },
    /// Tee registry: writes to both a local and remote registry.
    Tee {
        /// Local backend configuration (nested).
        local: Box<NodeConfigRegistry>,
        /// Remote peer name (key in `peer` map) for the remote backend.
        remote_peer: String,
    },
    /// Registry backed by a named store (e.g. S3, Sia, local).
    ///
    /// References a `[store.*]` entry by name. Uses `StoreRegistry` internally,
    /// so the registry entries are stored as individual files under `prefix/`.
    ///
    /// ```toml
    /// [registry.remote]
    /// type = "store"
    /// store = "my-s3-bucket"
    /// prefix = "registry"
    /// ```
    Store {
        /// Name of a declared `[store.*]` entry.
        store: String,
        /// Optional key prefix within the store; defaults to "registry".
        #[serde(default)]
        prefix: Option<String>,
    },
    /// Multi registry: fans out writes to N backends in parallel.
    Multi {
        /// List of backend configurations.
        backends: Vec<NodeConfigRegistry>,
        /// Write policy: "all" (default), "any", or "quorum:N".
        #[serde(default)]
        write_policy: Option<String>,
    },
}

// ---------------------------------------------------------------------------
// Task
// ---------------------------------------------------------------------------

/// A named task — the unit of work in s5.
///
/// Each task works in its own ephemeral FS5 working tree, produces a
/// snapshot, and merges it into the target vault.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigTask {
    /// Tasks to trigger on successful completion.
    #[serde(default)]
    pub then: Vec<String>,

    /// The task kind and its parameters.
    #[serde(flatten)]
    pub spec: TaskSpec,
}

/// Task specification — determines what the task does.
///
/// Used both in config (`[task.*]`) and over RPC (`RunTask`).
/// All fields reference named config entries (vaults, sources, stores, keys)
/// that the node resolves at execution time.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum TaskSpec {
    /// Ingest files from a source into a vault.
    ///
    /// Scans source dirs, imports blobs to `blob_store`, builds metadata
    /// in an ephemeral working tree, produces a snapshot, and merges it
    /// into the vault.
    Ingest {
        /// Vault name — must reference a declared `[vault.*]`.
        vault: String,
        /// Source name — must reference a declared `[source.*]`.
        source: String,
        /// Store name for writing data blobs — must reference a declared `[store.*]`.
        blob_store: String,
        /// Optional path prefix in the FS5 tree.
        #[serde(default)]
        target_path: Option<String>,
    },
    /// Publish vault state — diff, replicate meta blobs, publish to registry.
    ///
    /// Diffs the current vault snapshot against the last published one,
    /// pushes new meta blobs, and publishes the snapshot hash to the registry.
    /// The published state is encrypted for the specified key recipients.
    Publish {
        /// Vault name — must reference a declared `[vault.*]`.
        vault: String,
        /// Key names from `[key.*]` — recipients for the published state.
        /// The published snapshot is encrypted for these recipients.
        keys: Vec<String>,
    },
    /// Convenience: ingest + publish in one task.
    Backup {
        /// Vault name — must reference a declared `[vault.*]`.
        vault: String,
        /// Source name — must reference a declared `[source.*]`.
        source: String,
        /// Store name for writing data blobs — must reference a declared `[store.*]`.
        blob_store: String,
        /// Key names from `[key.*]` — recipients for the published state.
        keys: Vec<String>,
        /// Optional path prefix in the FS5 tree.
        #[serde(default)]
        target_path: Option<String>,
    },
    /// Restore files from a vault snapshot to a local directory.
    Restore {
        /// Vault name — must reference a declared `[vault.*]`.
        vault: String,
        /// Local directory to restore into.
        target_path: String,
        /// Optional blob store override for reading file content.
        /// If not set, uses the vault's `blob_stores` list.
        #[serde(default)]
        blob_store: Option<String>,
    },
    /// Disaster recovery restore from a paper age key.
    ///
    /// Derives the recovery registry key from the age secret + vault name,
    /// discovers the vault's signing key, fetches and decrypts the latest
    /// published snapshot from the remote store, and restores to target_path.
    RemoteRestore {
        /// Vault name — used for key derivation (must match original vault).
        vault: String,
        /// The age secret key string (`AGE-SECRET-KEY-1...`).
        age_secret_key: String,
        /// Blob store name for downloading blobs.
        blob_store: String,
        /// Local directory to restore into.
        target_path: String,
    },
}
