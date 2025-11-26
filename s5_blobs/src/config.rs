use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct PeerConfigBlobs {
    /// Names of stores this peer can read and query
    #[serde(default)]
    pub readable_stores: Vec<String>,
    #[serde(default)]
    pub store_uploads_in: Option<String>,
}
