use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct S5NodeConfig {
    /// Optional human-readable node name (for referencing in examples)
    #[serde(default)]
    pub name: Option<String>,
    pub identity: NodeConfigIdentity,
    pub store: BTreeMap<String, NodeConfigStore>,
    pub peer: BTreeMap<String, NodeConfigPeer>,
    /// File sync configurations keyed by name (e.g., "music")
    #[serde(default)]
    pub sync: BTreeMap<String, NodeConfigSync>,
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
    /// Peer public ID (iroh node id, etc.)
    #[serde(default)]
    pub id: String,
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
}
