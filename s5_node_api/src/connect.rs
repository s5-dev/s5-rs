//! Service discovery and connection for native platforms.
//!
//! The running node writes a lock file containing the iroh [`EndpointAddr`]
//! of its **loopback-only control endpoint** plus a per-run auth token.
//! Clients read this, create their own iroh endpoint, connect, and prove
//! token possession in a one-stream preamble before issuing any RPC
//! (see [`crate::CONTROL_AUTH_MAGIC`]). The lock file is written 0600, so
//! readability of the token is the access-control boundary: same user as
//! the daemon, or root.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};

use crate::S5NodeClient;

/// Info written to the lock file so clients can connect.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ServiceLock {
    /// The node's **control** endpoint address: loopback direct addrs
    /// only, no relay URLs (the control plane is never published to
    /// pkarr/DNS or reachable via relays).
    pub endpoint_addr: iroh::EndpointAddr,
    /// Build version of the daemon that wrote this lock file.
    /// Clients compare this to their own [`crate::VERSION`] to detect mismatches.
    #[serde(default)]
    pub version: Option<String>,
    /// PID of the daemon process. Used as a fallback to kill a stale daemon
    /// when the shutdown RPC fails (e.g. protocol mismatch after upgrade).
    #[serde(default)]
    pub pid: Option<u32>,
    /// Hex-encoded per-run control auth token ([`crate::CONTROL_TOKEN_LEN`]
    /// raw bytes). Absent only in lock files from daemons that predate the
    /// local-only control plane; clients treat that as a stale daemon.
    #[serde(default)]
    pub control_token: Option<String>,
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
    write_lock_at(&path, lock)
}

/// Write the lock file at an explicit path. The file carries the control
/// auth token, so it must never be group/world-readable: any pre-existing
/// file is removed first and the new one is created 0600 on unix.
fn write_lock_at(path: &Path, lock: &ServiceLock) -> Result<()> {
    let content = serde_json::to_string_pretty(lock)?;
    // Remove first so create-with-mode applies even where an old
    // (possibly 0644, pre-token) lock file survives.
    let _ = std::fs::remove_file(path);
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(content.as_bytes())?;
    }
    #[cfg(not(unix))]
    std::fs::write(path, content)?;
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
/// Creates a temporary iroh endpoint, connects to the node's loopback
/// control endpoint, and completes the auth preamble: one bi-stream
/// carrying `CONTROL_AUTH_MAGIC ‖ token`, answered by `0x01`. Fails if
/// the lock has no token (pre-token daemon — callers treat this like a
/// version mismatch and restart it) or the daemon rejects the token.
///
/// Intentionally uses `presets::Minimal` even though the daemon's public
/// endpoint uses `presets::N0`: this client dials a known local
/// `EndpointAddr` from the service lock file, so it needs neither
/// pkarr/DNS/mDNS resolution nor the relay fallback, and skipping them
/// keeps `vup`/`s5` invocations from publishing the user's transient
/// client pubkey to n0's DNS on every CLI call. Do not unify with the
/// daemon's preset.
pub async fn connect_with_lock(lock: &ServiceLock) -> Result<S5NodeClient> {
    let token = parse_control_token(lock)?;
    let endpoint = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
        .bind()
        .await?;
    let conn = endpoint
        .connect(lock.endpoint_addr.clone(), crate::ALPN)
        .await
        .context("failed to reach the daemon's control endpoint")?;

    // Auth preamble on the first bi-stream, before irpc sees the
    // connection. The round-trip (we wait for the 0x01) guarantees no
    // RPC stream can arrive at the daemon ahead of the token check.
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(crate::CONTROL_AUTH_MAGIC).await?;
    send.write_all(&token).await?;
    send.finish()?;
    let mut ok = [0u8; 1];
    recv.read_exact(&mut ok)
        .await
        .context("daemon closed the control connection during auth")?;
    ensure!(ok[0] == 0x01, "daemon rejected the control auth token");

    // Reuse the now-authenticated connection for all RPCs. Deliberately
    // NOT `IrohLazyRemoteConnection`: a transparent reconnect would skip
    // the preamble and every RPC on it would hang in the daemon's auth
    // read. A short-lived CLI process never needs to reconnect.
    let client = irpc::Client::boxed(irpc_iroh::IrohRemoteConnection::new(conn));
    Ok(S5NodeClient::new(client, endpoint))
}

/// Decode the lock file's hex token, insisting on the exact length.
fn parse_control_token(lock: &ServiceLock) -> Result<[u8; crate::CONTROL_TOKEN_LEN]> {
    let Some(hex_token) = lock.control_token.as_deref() else {
        bail!(
            "service lock has no control token (daemon predates the local-only \
             control plane) — restart the daemon"
        );
    };
    let bytes = hex::decode(hex_token).context("invalid control token in service lock")?;
    let token: [u8; crate::CONTROL_TOKEN_LEN] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("control token in service lock has the wrong length"))?;
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_lock() -> ServiceLock {
        ServiceLock {
            endpoint_addr: iroh::EndpointAddr::new(
                iroh::SecretKey::from_bytes(&[7u8; 32]).public(),
            ),
            version: Some(crate::VERSION.to_string()),
            pid: Some(1234),
            control_token: Some(hex::encode([0xabu8; crate::CONTROL_TOKEN_LEN])),
        }
    }

    #[test]
    fn lock_roundtrip_and_token_parse() {
        let lock = dummy_lock();
        let json = serde_json::to_string(&lock).unwrap();
        let back: ServiceLock = serde_json::from_str(&json).unwrap();
        assert_eq!(
            parse_control_token(&back).unwrap(),
            [0xabu8; crate::CONTROL_TOKEN_LEN]
        );
    }

    #[test]
    fn missing_or_malformed_token_is_an_error() {
        let mut lock = dummy_lock();
        lock.control_token = None;
        assert!(parse_control_token(&lock).is_err());
        lock.control_token = Some("abcd".into()); // too short
        assert!(parse_control_token(&lock).is_err());
        lock.control_token = Some("zz".repeat(crate::CONTROL_TOKEN_LEN)); // not hex
        assert!(parse_control_token(&lock).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn lock_file_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("service.lock");
        // Simulate a stale wide-open lock from a pre-token daemon: the
        // rewrite must end up 0600, not inherit 0644.
        std::fs::write(&path, "{}").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        write_lock_at(&path, &dummy_lock()).unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}
