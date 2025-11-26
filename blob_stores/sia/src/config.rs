#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SiaStoreConfig {
    /// Bucket name used for storing objects.
    pub bucket: String,
    /// Base worker API URL, e.g. `http://localhost:9980/api/worker`.
    pub worker_api_url: String,
    /// Base bus API URL, e.g. `http://localhost:9980/api/bus`.
    pub bus_api_url: String,
    /// Renterd API password.
    pub password: String,
}
