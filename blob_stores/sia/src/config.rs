#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SiaStoreConfig {
    pub bucket: String,
    pub worker_api_url: String,
    pub bus_api_url: String,
    pub password: String,
}
