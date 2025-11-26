use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub fn registry_path(node_config_file: &Path, config: &S5NodeConfig) -> PathBuf {
    if let Some(p) = &config.registry_path {
        p.into()
    } else {
        let base = node_config_file.parent().unwrap_or_else(|| Path::new("."));
        base.join("registry")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct S5NodeConfig {
    /// Optional human-readable node name (for referencing in examples)
    #[serde(default)]
    pub name: Option<String>,
    pub identity: NodeConfigIdentity,
    pub store: BTreeMap<String, NodeConfigStore>,
    #[serde(default)]
    pub peer: BTreeMap<String, NodeConfigPeer>,
    /// File sync configurations keyed by name (e.g., "music")
    #[serde(default)]
    pub sync: BTreeMap<String, NodeConfigSync>,
    /// Optional registry data directory; defaults near config file or data dir
    #[serde(default)]
    pub registry_path: Option<String>,
    /// Optional FUSE mounts driven by this node
    // TODO rename to mounts
    #[serde(default)]
    pub fuse: BTreeMap<String, NodeConfigFuseMount>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigIdentity {
    pub secret_key_file: Option<String>,
    pub secret_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum NodeConfigStore {
    SiaRenterd(s5_store_sia::SiaStoreConfig),
    Local(s5_store_local::LocalStoreConfig),
    S3(s5_store_s3::S3StoreConfig),
    Memory,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigPeer {
    /// Peer public ID string used for both ACLs and dialing.
    /// This should be the `EndpointId` display string as printed
    /// by `endpoint.id().to_string()`.
    #[serde(default)]
    pub id: String,
    /// Optional blob ACL configuration; defaults to no access.
    #[serde(default)]
    pub blobs: s5_blobs::PeerConfigBlobs,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigSync {
    /// Local filesystem path to sync
    pub local_path: String,
    /// Route through these untrusted peers (by name)
    #[serde(default)]
    pub via_untrusted: Vec<String>,
    /// Pre-shared secret for authorization
    pub shared_secret: String,
    /// Optional continuous sync interval in seconds; if set, runs a loop
    #[serde(default)]
    pub interval_secs: Option<u64>,
    // TODO: Add hooks for each sync target (e.g. pre-sync, post-sync scripts)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigFuseMount {
    /// Path to the FS5 root directory (contains `root.fs5.cbor`)
    // TODO rename to fs5_root_path or so
    pub root_path: String,
    /// Local mount point where this FS5 root should be mounted via FUSE
    pub mount_path: String,
    /// Whether to request auto-unmount on process exit
    #[serde(default)]
    pub auto_unmount: bool,
    /// Whether to allow the system root user to access the mount
    #[serde(default)]
    pub allow_root: bool,
}
