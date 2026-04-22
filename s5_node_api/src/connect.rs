//! Service discovery and connection for native platforms.
//!
//! The running node writes a lock file containing its iroh [`EndpointAddr`].
//! Clients read this, create their own iroh endpoint, and connect over iroh.

use std::path::PathBuf;

use anyhow::{Context, Result};

use crate::S5NodeClient;

/// Info written to the lock file so clients can connect.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ServiceLock {
    /// The node's iroh endpoint address (id + direct addrs + relay URLs).
    pub endpoint_addr: iroh::EndpointAddr,
    /// Build version of the daemon that wrote this lock file.
    /// Clients compare this to their own [`crate::VERSION`] to detect mismatches.
    #[serde(default)]
    pub version: Option<String>,
    /// PID of the daemon process. Used as a fallback to kill a stale daemon
    /// when the shutdown RPC fails (e.g. protocol mismatch after upgrade).
    #[serde(default)]
    pub pid: Option<u32>,
}

/// Default lock file path (`~/.local/share/s5/service.lock`).
pub fn lock_path() -> Result<PathBuf> {
    let dirs = directories::ProjectDirs::from("pro", "s5", "s5")
        .context("could not determine data directory")?;
    Ok(dirs.data_dir().join("service.lock"))
}

/// Read the lock file. Returns `None` if it doesn't exist or is invalid.
pub fn read_lock() -> Result<Option<ServiceLock>> {
    let path = lock_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&path)?;
    match serde_json::from_str(&content) {
        Ok(lock) => Ok(Some(lock)),
        Err(_) => {
            // Stale or corrupt lock file — remove it
            let _ = std::fs::remove_file(&path);
            Ok(None)
        }
    }
}

/// Write a lock file (called by the node on startup).
pub fn write_lock(lock: &ServiceLock) -> Result<()> {
    let path = lock_path()?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let content = serde_json::to_string_pretty(lock)?;
    std::fs::write(&path, content)?;
    Ok(())
}

/// Remove the lock file (called by the node on shutdown).
pub fn remove_lock() {
    if let Ok(path) = lock_path() {
        let _ = std::fs::remove_file(&path);
    }
}

/// Connect to a running node using the lock file.
///
/// Returns `None` if no lock file exists (node not running).
pub async fn connect() -> Result<Option<S5NodeClient>> {
    let lock = match read_lock()? {
        Some(l) => l,
        None => return Ok(None),
    };
    connect_with_lock(&lock).await.map(Some)
}

/// Connect to a running node using an existing lock.
///
/// Creates a temporary iroh endpoint and connects to the node's endpoint
/// address using the S5 node RPC ALPN.
///
/// Uses a local-only endpoint (no relay servers) for security.
pub async fn connect_with_lock(lock: &ServiceLock) -> Result<S5NodeClient> {
    // Local-only: no relay servers, direct connection only
    let endpoint = iroh::Endpoint::empty_builder().bind().await?;
    let endpoint_handle = endpoint.clone();
    let client =
        irpc_iroh::client::<crate::S5NodeProto>(endpoint, lock.endpoint_addr.clone(), crate::ALPN);

    Ok(S5NodeClient::new(client, endpoint_handle))
}
