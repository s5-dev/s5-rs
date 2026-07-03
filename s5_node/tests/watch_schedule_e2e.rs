//! M5 validation E2E — automatic backups, both daemon-side triggers:
//!
//! - **watch** (`watch = true`): "daemon watches directory, file created,
//!   backup triggered automatically within 30 s" — the grant's M5
//!   validation criterion, driven against the real `WatchManager` (the
//!   daemon-resident notify loop), not the CLI's foreground `snap --watch`.
//! - **schedule** (`snap_interval_secs`): the M5 "cron-like schedule in
//!   config, daemon executes" knob — a fresh HEAD revision appears on the
//!   interval without any filesystem event.
//!
//! Both assert on the published registry HEAD (the durable, syncable
//! artifact a second device would pull), not on task bookkeeping.

mod common;

use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use common::{MemoryBackend, age_identity, build_ctx, make_config};
use ed25519_dalek::VerifyingKey;
use s5_core::StreamKey;
use s5_node::tasks::TaskExecutor;
use s5_node::tasks::publish::{derive_vault_id, device_signing_key};
use s5_node::tasks::vault_persist::{load_vault_root, vault_root_path};
use s5_node::watch::WatchManager;
use std::sync::Arc;

const NODE_SECRET: [u8; 32] = [0x11u8; 32];

/// Poll the published HEAD for the "backup" vault until `pred(revision)`
/// holds, up to `deadline`. Returns the revision that satisfied it.
/// Revision-None (no vault root / no HEAD yet) polls as revision 0.
async fn wait_for_head(
    registry: &(dyn s5_core::RegistryApi + Send + Sync),
    vault_root: &str,
    device_identity_file: &str,
    pred: impl Fn(u64) -> bool,
    deadline: Duration,
    what: &str,
) -> Result<u64> {
    let device_signing = device_signing_key(&NODE_SECRET);
    let device_pubkey = VerifyingKey::from(&device_signing).to_bytes();
    let identity_files = [device_identity_file.to_string()];
    let start = tokio::time::Instant::now();
    loop {
        // The vault_id is only derivable once a snap persisted a local root
        // (its KEY_SLOT_RECOVERY secret); until then, keep polling.
        let root_file = vault_root_path(vault_root);
        if let Some((_root, _ph, root_ctx)) = load_vault_root(&root_file, &identity_files)?
            && let Some(secret) = root_ctx
                .keys
                .as_ref()
                .and_then(|m| m.get(&s5_fs_v2::snapshot::KEY_SLOT_RECOVERY))
                .copied()
        {
            let vault_id = derive_vault_id(&secret);
            if let Some(entry) = registry
                .get(&StreamKey::Vault {
                    pubkey: device_pubkey,
                    vault_id,
                })
                .await?
                && pred(entry.revision)
            {
                return Ok(entry.revision);
            }
        }
        if start.elapsed() > deadline {
            return Err(anyhow!("timed out waiting for {what}"));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// M5 validation: file created → watch loop publishes a new HEAD ≤ 30 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_backs_up_new_file_within_30s() -> Result<()> {
    let backend = MemoryBackend::new();
    let scratch = tempfile::tempdir()?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("hello.txt"), b"first")?;

    let (paper_recipient, paper_id) = age_identity(scratch.path(), "paper");
    let (device_recipient, device_id) = age_identity(scratch.path(), "device");
    let vault_root = scratch.path().join("vault");
    std::fs::create_dir_all(&vault_root)?;
    let vault_root_str = vault_root.to_string_lossy().into_owned();

    let mut config = make_config(
        &vault_root_str,
        &paper_recipient,
        &paper_id,
        &device_recipient,
        &device_id,
        &source.path().to_string_lossy(),
    );
    config.vault.get_mut("backup").expect("vault").watch = true;

    use common::DurableBackend;
    let (blobs, registry) = backend.open();
    let ctx = build_ctx(config.clone(), blobs, registry.clone(), NODE_SECRET);
    let executor = Arc::new(TaskExecutor::new(ctx));
    let manager = Arc::new(WatchManager::new(executor));
    manager.start_from_config(&config).await;

    // Baseline backup (the watch loop's initial reconcile) publishes rev ≥ 1.
    let baseline = wait_for_head(
        registry.as_ref(),
        &vault_root_str,
        &device_id,
        |r| r >= 1,
        Duration::from_secs(30),
        "the watch loop's baseline backup",
    )
    .await?;

    // THE criterion: create a file; a fresh HEAD must land within 30 s.
    std::fs::write(source.path().join("created-later.txt"), b"the M5 file")?;
    wait_for_head(
        registry.as_ref(),
        &vault_root_str,
        &device_id,
        |r| r > baseline,
        Duration::from_secs(30),
        "an automatic backup of the newly created file (M5: ≤30 s)",
    )
    .await
    .context("M5 watch validation")?;

    manager.shutdown().await;
    Ok(())
}

/// M5 scheduled backups: `snap_interval_secs` alone (no watcher, no FS
/// event correlation) publishes fresh HEAD revisions on the interval.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduled_snap_publishes_on_interval() -> Result<()> {
    let backend = MemoryBackend::new();
    let scratch = tempfile::tempdir()?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("data.txt"), b"v1")?;

    let (paper_recipient, paper_id) = age_identity(scratch.path(), "paper");
    let (device_recipient, device_id) = age_identity(scratch.path(), "device");
    let vault_root = scratch.path().join("vault");
    std::fs::create_dir_all(&vault_root)?;
    let vault_root_str = vault_root.to_string_lossy().into_owned();

    let mut config = make_config(
        &vault_root_str,
        &paper_recipient,
        &paper_id,
        &device_recipient,
        &device_id,
        &source.path().to_string_lossy(),
    );
    config
        .vault
        .get_mut("backup")
        .expect("vault")
        .snap_interval_secs = Some(1);

    use common::DurableBackend;
    let (blobs, registry) = backend.open();
    let ctx = build_ctx(config.clone(), blobs, registry.clone(), NODE_SECRET);
    let executor = Arc::new(TaskExecutor::new(ctx));
    let manager = Arc::new(WatchManager::new(executor));
    manager.start_from_config(&config).await;

    // First scheduled snap fires after ~1 s.
    let first = wait_for_head(
        registry.as_ref(),
        &vault_root_str,
        &device_id,
        |r| r >= 1,
        Duration::from_secs(30),
        "the first scheduled snap",
    )
    .await?;

    // Change content; the next interval must publish a newer revision.
    std::fs::write(source.path().join("data.txt"), b"v2 - changed")?;
    wait_for_head(
        registry.as_ref(),
        &vault_root_str,
        &device_id,
        |r| r > first,
        Duration::from_secs(30),
        "the next scheduled snap after a content change",
    )
    .await?;

    manager.shutdown().await;
    Ok(())
}
