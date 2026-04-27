//! Auto-start daemon and connection helpers.
//!
//! `ensure_node_running()` checks for a running s5 node via the service lock
//! file. If no node is found, it spawns `vup _daemon --config <path>` as a
//! detached process and waits for the lock file to appear.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use s5_node_api::S5NodeClient;
use s5_node_api::connect;

/// Default s5 node config path (`~/.config/s5/config.toml`).
pub fn default_config_path() -> Result<PathBuf> {
    let dirs = directories::ProjectDirs::from("pro", "s5", "s5")
        .context("could not determine config directory")?;
    Ok(dirs.config_dir().join("config.toml"))
}

/// Connect to a running s5 node, or spawn one and wait for it.
///
/// 1. Try `connect()` — if the lock file exists, check the version matches.
///    If the version is stale, shut down the old daemon first.
/// 2. If no daemon is running, spawn `vup _daemon --config <path>`.
/// 3. Poll `connect()` with backoff until success or timeout.
pub async fn ensure_node_running(config_path: &Path) -> Result<S5NodeClient> {
    // Check lock file first for version comparison
    if let Some(lock) = connect::read_lock()? {
        let version_matches = lock
            .version
            .as_deref()
            .is_some_and(|v| v == s5_node_api::VERSION);

        if version_matches {
            // Same version — try to connect
            if let Ok(client) = connect::connect_with_lock(&lock).await {
                return Ok(client);
            }
            // Connection failed despite lock existing — fall through to respawn
        } else {
            // Version mismatch (or old lock without version) — shut down stale daemon
            let old_version = lock.version.as_deref().unwrap_or("unknown");
            tracing::info!(
                old = old_version,
                new = s5_node_api::VERSION,
                "daemon version mismatch, restarting"
            );

            let mut shutdown_ok = false;

            // Try graceful shutdown via RPC
            if let Ok(client) = connect::connect_with_lock(&lock).await {
                if client.shutdown().await.is_ok() {
                    shutdown_ok = true;
                    // Give it a moment to clean up
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                client.close().await;
            }

            // Fallback: kill by PID if RPC failed (e.g. protocol mismatch)
            if !shutdown_ok {
                if let Some(pid) = lock.pid {
                    tracing::info!(pid, "shutdown RPC failed, killing stale daemon by PID");
                    #[cfg(unix)]
                    {
                        use std::process::Command;
                        let _ = Command::new("kill").arg(pid.to_string()).output();
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    #[cfg(windows)]
                    {
                        use std::process::Command;
                        let _ = Command::new("taskkill")
                            .args(["/PID", &pid.to_string(), "/F"])
                            .output();
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                } else {
                    tracing::warn!(
                        "no PID in lock file and shutdown RPC failed — old daemon may still be running"
                    );
                }
            }

            // Remove stale lock file in case shutdown didn't clean up
            connect::remove_lock();
        }
    }

    // Verify config exists before attempting to spawn
    if !config_path.exists() {
        bail!(
            "s5 node config not found at {}. Run `vup onboard` to set up, or pass --config.",
            config_path.display()
        );
    }

    tracing::info!(config = %config_path.display(), "spawning s5 daemon");
    spawn_daemon(config_path)?;

    // Poll for the node to come up (lock file + successful connection)
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut interval = Duration::from_millis(100);
    loop {
        tokio::time::sleep(interval).await;

        if let Some(client) = connect::connect().await? {
            tracing::info!("connected to s5 daemon");
            return Ok(client);
        }

        if tokio::time::Instant::now() > deadline {
            bail!(
                "timed out waiting for s5 daemon to start (30s). \
                 Check logs or run `vup _daemon --config {}` manually.",
                config_path.display()
            );
        }

        // Back off: 100ms → 200ms → 400ms → … → 2s max
        interval = (interval * 2).min(Duration::from_secs(2));
    }
}

/// Spawn `vup _daemon --config <path>` as a fully detached background process.
fn spawn_daemon(config_path: &Path) -> Result<()> {
    let exe = std::env::current_exe().context("could not determine own executable path")?;
    let config_str = config_path
        .to_str()
        .context("config path is not valid UTF-8")?;

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        use std::process::{Command, Stdio};

        // process_group(0) makes the child a session leader so it survives
        // the parent's exit. Redirect stdio to /dev/null.
        let child = Command::new(&exe)
            .args(["_daemon", "--config", config_str])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .with_context(|| format!("failed to spawn daemon: {}", exe.display()))?;

        tracing::debug!(pid = child.id(), "daemon process spawned");
    }

    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        use std::process::{Command, Stdio};

        const DETACHED_PROCESS: u32 = 0x00000008;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x00000200;

        let child = Command::new(&exe)
            .args(["_daemon", "--config", config_str])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP)
            .spawn()
            .with_context(|| format!("failed to spawn daemon: {}", exe.display()))?;

        tracing::debug!(pid = child.id(), "daemon process spawned");
    }

    #[cfg(not(any(unix, windows)))]
    {
        bail!("daemon auto-start is not supported on this platform");
    }

    Ok(())
}

/// Entry point for `vup _daemon --config <path>`.
///
/// Loads the config and delegates to `s5_node::run_node()`.
pub async fn run_daemon(config_path: &Path) -> Result<()> {
    let content = std::fs::read_to_string(config_path)
        .with_context(|| format!("failed to read config: {}", config_path.display()))?;

    let config: s5_node::config::S5NodeConfig = toml::from_str(&content)
        .with_context(|| format!("failed to parse config: {}", config_path.display()))?;

    // Validate cross-references
    let errors = config.validate();
    if !errors.is_empty() {
        for e in &errors {
            tracing::error!("config error: {e}");
        }
        bail!("config validation failed ({} errors)", errors.len());
    }

    s5_node::run_node(config_path.to_path_buf(), config).await
}
