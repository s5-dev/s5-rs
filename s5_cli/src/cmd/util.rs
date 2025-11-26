use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use s5_core::BlobStore;
use s5_node::config::S5NodeConfig;

/// Opens a blob store by name from the node config using the same
/// configuration schema as the node itself.
pub async fn open_store(config: &S5NodeConfig, name: &str) -> Result<BlobStore> {
    let store_cfg = config
        .store
        .get(name)
        .cloned()
        .with_context(|| format!("store with name \"{name}\" not present in node config"))?;
    // TODO: consider a DroppingStore that discards writes for dry runs or testing.
    // TODO create LocalFileSystemBackedStoreLink
    // TODO ReadOnlyStore wrapper for other stores?
    let store: BlobStore = s5_node::create_store(store_cfg).await?;
    Ok(store)
}

/// Computes the registry path exactly as the node would, so CLI tools
/// see the same pins as the running node.
pub fn registry_path(node_config_file: &Path, config: &S5NodeConfig) -> PathBuf {
    s5_node::config::registry_path(node_config_file, config)
}
