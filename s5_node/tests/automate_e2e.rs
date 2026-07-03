//! Stage 7 `automate` engine E2E — the daemon's `AutomationManager` reconciled
//! from `[task.*]` automations, exercised over the store-agnostic
//! [`common::DurableBackend`] seam so it runs without a live network.
//!
//! Covers the M5 "automatic backups" surface end to end:
//! - **watch** automation: file created → a fresh published HEAD ≤ 30 s;
//! - **pause / resume**: a paused automation stops publishing; resuming it
//!   captures the change made while paused;
//! - **schedule** (`trigger = every`, `interval_secs`): fresh HEADs on the
//!   interval with no filesystem event;
//! - **second-device sync** (the M5 criterion second half): a cold consumer sharing
//!   only the durable backend resolves the watch-published file's content;
//! - **failure surfacing**: a watch pointed at a missing source path
//!   surfaces `alive = false` + a climbing restart count via `status()`.
//!
//! All assertions ride the published registry HEAD (the durable, syncable
//! artifact a second device pulls), not task bookkeeping.

mod common;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use common::{DurableBackend, MemoryBackend, age_identity, build_ctx, make_config};
use ed25519_dalek::VerifyingKey;
use futures_util::StreamExt;
use s5_core::{BlobsRead, RegistryApi, StreamKey};
use s5_node::config::{NodeConfigTask, S5NodeConfig, TaskSpec, TaskTrigger};
use s5_node::tasks::TaskExecutor;
use s5_node::tasks::peer_load::load_peer_snapshot;
use s5_node::tasks::publish::{derive_vault_id, device_signing_key};
use s5_node::tasks::vault_persist::{load_vault_root, vault_root_path};
use s5_node::watch::AutomationManager;

const NODE_SECRET: [u8; 32] = [0x11u8; 32];

/// A `[task.*]` Backup automation of the `make_config` "backup" vault.
fn backup_automation(
    trigger: TaskTrigger,
    interval_secs: Option<u64>,
    paused: bool,
) -> NodeConfigTask {
    NodeConfigTask {
        then: Vec::new(),
        trigger,
        interval_secs,
        paused,
        spec: TaskSpec::Backup {
            vault: "backup".to_string(),
            source: "docs".to_string(),
            blob_store: "durable".to_string(),
            keys: vec!["device".to_string(), "paper".to_string()],
            target_path: None,
            changed_paths: None,
        },
    }
}

/// Poll the published HEAD for the "backup" vault until `pred(revision)` holds,
/// up to `deadline`. Returns the satisfying revision. (Same primitive as
/// `watch_schedule_e2e::wait_for_head`.)
async fn wait_for_head(
    registry: &(dyn RegistryApi + Send + Sync),
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

/// Standard single-vault config with a source dir; returns (config, paths).
struct Fixture {
    _scratch: tempfile::TempDir,
    source: tempfile::TempDir,
    vault_root: String,
    device_id: String,
    config: S5NodeConfig,
}

fn fixture() -> Result<Fixture> {
    let scratch = tempfile::tempdir()?;
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("hello.txt"), b"first")?;

    let (paper_recipient, paper_id) = age_identity(scratch.path(), "paper");
    let (device_recipient, device_id) = age_identity(scratch.path(), "device");
    let vault_root = scratch.path().join("vault");
    std::fs::create_dir_all(&vault_root)?;
    let vault_root_str = vault_root.to_string_lossy().into_owned();

    let config = make_config(
        &vault_root_str,
        &paper_recipient,
        &paper_id,
        &device_recipient,
        &device_id,
        &source.path().to_string_lossy(),
    );

    Ok(Fixture {
        _scratch: scratch,
        source,
        vault_root: vault_root_str,
        device_id,
        config,
    })
}

/// A `Watch` automation snaps a newly created file into a fresh HEAD ≤ 30 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn watch_automation_backs_up_new_file() -> Result<()> {
    let backend = MemoryBackend::new();
    let mut fx = fixture()?;
    fx.config.task.insert(
        "docs-watch".to_string(),
        backup_automation(TaskTrigger::Watch, None, false),
    );

    let (blobs, registry) = backend.open();
    let ctx = build_ctx(fx.config.clone(), blobs, registry.clone(), NODE_SECRET);
    let executor = Arc::new(TaskExecutor::new(ctx));
    let manager = Arc::new(AutomationManager::new(executor));
    manager.reconcile(&fx.config).await;

    let baseline = wait_for_head(
        registry.as_ref(),
        &fx.vault_root,
        &fx.device_id,
        |r| r >= 1,
        Duration::from_secs(30),
        "the watch automation's baseline backup",
    )
    .await?;

    std::fs::write(fx.source.path().join("created-later.txt"), b"the M5 file")?;
    wait_for_head(
        registry.as_ref(),
        &fx.vault_root,
        &fx.device_id,
        |r| r > baseline,
        Duration::from_secs(30),
        "an automatic backup of the new file (≤30 s)",
    )
    .await
    .context("watch automation")?;

    manager.shutdown().await;
    Ok(())
}

/// Pausing an automation stops publishing; resuming captures the change made
/// while paused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_resume_gates_publishing() -> Result<()> {
    let backend = MemoryBackend::new();
    let fx = fixture()?;
    let mut config = fx.config.clone();
    config.task.insert(
        "docs-watch".to_string(),
        backup_automation(TaskTrigger::Watch, None, false),
    );

    let (blobs, registry) = backend.open();
    let ctx = build_ctx(config.clone(), blobs, registry.clone(), NODE_SECRET);
    let executor = Arc::new(TaskExecutor::new(ctx));
    let manager = Arc::new(AutomationManager::new(executor));
    manager.reconcile(&config).await;

    let baseline = wait_for_head(
        registry.as_ref(),
        &fx.vault_root,
        &fx.device_id,
        |r| r >= 1,
        Duration::from_secs(30),
        "the baseline backup",
    )
    .await?;

    // Pause: reconcile with the task flipped to paused; the loop is cancelled.
    let mut paused_config = config.clone();
    paused_config.task.insert(
        "docs-watch".to_string(),
        backup_automation(TaskTrigger::Watch, None, true),
    );
    manager.reconcile(&paused_config).await;

    // A change while paused must NOT publish.
    std::fs::write(fx.source.path().join("while-paused.txt"), b"unseen")?;
    let paused_result = wait_for_head(
        registry.as_ref(),
        &fx.vault_root,
        &fx.device_id,
        |r| r > baseline,
        Duration::from_secs(5),
        "a (forbidden) publish while paused",
    )
    .await;
    assert!(
        paused_result.is_err(),
        "paused automation must not publish (HEAD advanced past {baseline})"
    );

    // Resume: the resume baseline captures the file written while paused.
    manager.reconcile(&config).await;
    wait_for_head(
        registry.as_ref(),
        &fx.vault_root,
        &fx.device_id,
        |r| r > baseline,
        Duration::from_secs(30),
        "a publish after resume",
    )
    .await
    .context("resume must publish the change made while paused")?;

    manager.shutdown().await;
    Ok(())
}

/// An `Every` automation publishes fresh HEADs on its interval with no FS event.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_automation_publishes_on_interval() -> Result<()> {
    let backend = MemoryBackend::new();
    let fx = fixture()?;
    let mut config = fx.config.clone();
    config.task.insert(
        "docs-1s".to_string(),
        backup_automation(TaskTrigger::Every, Some(1), false),
    );

    let (blobs, registry) = backend.open();
    let ctx = build_ctx(config.clone(), blobs, registry.clone(), NODE_SECRET);
    let executor = Arc::new(TaskExecutor::new(ctx));
    let manager = Arc::new(AutomationManager::new(executor));
    manager.reconcile(&config).await;

    let first = wait_for_head(
        registry.as_ref(),
        &fx.vault_root,
        &fx.device_id,
        |r| r >= 1,
        Duration::from_secs(30),
        "the first scheduled snap",
    )
    .await?;

    std::fs::write(fx.source.path().join("hello.txt"), b"v2 - changed")?;
    wait_for_head(
        registry.as_ref(),
        &fx.vault_root,
        &fx.device_id,
        |r| r > first,
        Duration::from_secs(30),
        "the next scheduled snap after a content change",
    )
    .await?;

    manager.shutdown().await;
    Ok(())
}

/// The watch-published file resolves at a cold second device that shares only
/// the durable backend (M5: sync to a second device succeeds).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn second_device_sees_watch_published_file() -> Result<()> {
    let backend = MemoryBackend::new();
    let mut fx = fixture()?;
    fx.config.task.insert(
        "docs-watch".to_string(),
        backup_automation(TaskTrigger::Watch, None, false),
    );

    let (blobs, registry) = backend.open();
    let ctx = build_ctx(fx.config.clone(), blobs, registry.clone(), NODE_SECRET);
    let executor = Arc::new(TaskExecutor::new(ctx));
    let manager = Arc::new(AutomationManager::new(executor));
    manager.reconcile(&fx.config).await;

    let baseline = wait_for_head(
        registry.as_ref(),
        &fx.vault_root,
        &fx.device_id,
        |r| r >= 1,
        Duration::from_secs(30),
        "the baseline backup",
    )
    .await?;

    let payload = b"second-device payload";
    std::fs::write(fx.source.path().join("created-later.txt"), payload)?;
    wait_for_head(
        registry.as_ref(),
        &fx.vault_root,
        &fx.device_id,
        |r| r > baseline,
        Duration::from_secs(30),
        "the watch publish of the new file",
    )
    .await?;

    manager.shutdown().await;

    // ---- Second device: cold handles over the SAME durable backend ----
    let device_signing = device_signing_key(&NODE_SECRET);
    let device_pubkey = VerifyingKey::from(&device_signing).to_bytes();

    // The shared vault_id is derived from the publisher's local vault root
    // (KEY_SLOT_RECOVERY) — identical across every device of the vault.
    let root_file = vault_root_path(&fx.vault_root);
    let (_root, _ph, root_ctx) = load_vault_root(&root_file, &[fx.device_id.clone()])?
        .ok_or_else(|| anyhow!("publisher vault root missing after publish"))?;
    let secret = root_ctx
        .keys
        .as_ref()
        .and_then(|m| m.get(&s5_fs_v2::snapshot::KEY_SLOT_RECOVERY))
        .copied()
        .ok_or_else(|| anyhow!("vault root has no KEY_SLOT_RECOVERY"))?;
    let vault_id = derive_vault_id(&secret);

    let (blobs2, registry2) = backend.open();
    let read_store: Arc<dyn BlobsRead> = blobs2.clone();
    let snapshot = load_peer_snapshot(
        device_pubkey,
        vault_id,
        registry2.as_ref(),
        blobs2.as_ref(),
        std::slice::from_ref(&fx.device_id),
        read_store,
    )
    .await
    .context("load_peer_snapshot on the second device")?
    .ok_or_else(|| anyhow!("second device found nothing published"))?;

    // Walk the peer snapshot, find the watch-published file, verify its bytes.
    let mut found = None;
    let mut stream = snapshot.walk();
    while let Some(item) = stream.next().await {
        let (key, entry) = item?;
        if key == "created-later.txt" {
            found = Some(entry);
            break;
        }
    }
    drop(stream);
    let entry =
        found.ok_or_else(|| anyhow!("second device: created-later.txt not in peer tree"))?;
    let bytes = snapshot.export_bytes(&entry).await?;
    assert_eq!(
        bytes.as_ref(),
        payload,
        "second device read back the wrong content for the watch-published file"
    );
    Ok(())
}

/// A watch pointed at a missing source path fails to establish, and the
/// supervisor surfaces it: `alive = false`, `last_error = Some`, restarts climb.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_source_surfaces_failure() -> Result<()> {
    let backend = MemoryBackend::new();
    let scratch = tempfile::tempdir()?;
    let (paper_recipient, paper_id) = age_identity(scratch.path(), "paper");
    let (device_recipient, device_id) = age_identity(scratch.path(), "device");
    let vault_root = scratch.path().join("vault");
    std::fs::create_dir_all(&vault_root)?;
    // A source path that does not exist — the notify watcher cannot attach.
    let missing = scratch.path().join("does-not-exist");

    let mut config = make_config(
        &vault_root.to_string_lossy(),
        &paper_recipient,
        &paper_id,
        &device_recipient,
        &device_id,
        &missing.to_string_lossy(),
    );
    config.task.insert(
        "broken-watch".to_string(),
        backup_automation(TaskTrigger::Watch, None, false),
    );

    let (blobs, registry) = backend.open();
    let ctx = build_ctx(config.clone(), blobs, registry, NODE_SECRET);
    let executor = Arc::new(TaskExecutor::new(ctx));
    let manager = Arc::new(AutomationManager::new(executor));
    manager.reconcile(&config).await;

    // The supervisor backs off ~1 s between attempts; wait for the counter to
    // climb past a single failure so we're sure it is genuinely restarting.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let status = manager.status().await;
        let broken = status.iter().find(|a| a.name == "broken-watch");
        if let Some(a) = broken
            && a.restarts >= 2
        {
            assert!(
                !a.alive,
                "a watch that never attaches must report alive = false"
            );
            assert!(
                a.last_error.is_some(),
                "a failing watch must surface a last_error"
            );
            manager.shutdown().await;
            return Ok(());
        }
        if tokio::time::Instant::now() > deadline {
            let status = manager.status().await;
            manager.shutdown().await;
            return Err(anyhow!(
                "failure was not surfaced within 15 s; status = {:?}",
                status
                    .iter()
                    .map(|a| (a.name.clone(), a.alive, a.restarts, a.last_error.clone()))
                    .collect::<Vec<_>>()
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
