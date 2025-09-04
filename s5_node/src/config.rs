use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct S5NodeConfig {
    pub identity: NodeConfigIdentity,
    pub store: BTreeMap<String, NodeConfigStore>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeConfigIdentity {
    pub secret_key_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum NodeConfigStore {
    SiaRenterd(s5_store_sia::SiaStoreConfig),
    Local(s5_store_local::LocalStoreConfig),
    S3(s5_store_s3::S3StoreConfig),
}
