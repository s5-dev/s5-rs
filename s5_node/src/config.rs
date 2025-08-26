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
    SiaRenterd {
        bucket: String,
        worker_api_url: String,
        bus_api_url: String,
        password: String,
    },
}
