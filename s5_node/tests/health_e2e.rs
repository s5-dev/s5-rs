//! Store-agnostic E2E for the `vup doctor` / `vup status` health walk
//! (Stage 5 of the D20 CLI cutover).
//!
//! Drives `s5_node::health::gather_health` — the exact function the daemon's
//! `GetHealth` RPC handler calls — against the `DurableBackend` seam, with no
//! live daemon:
//!
//!   - a reachable store answers the `blob_contains(Hash::EMPTY)` probe →
//!     `reachable = true`;
//!   - a direct store (Memory / Local `BlobStore`) has no staging layer →
//!     `staging = None` (its writes are durable on return);
//!   - a store configured but absent from the resolved registry is reported
//!     `reachable = false` with a note, not silently dropped;
//!   - a vault's legacy `snap_interval_secs` AND a D20 `[task.*]` `every`
//!     automation each surface as a `ScheduledRun`.
//!
//! Runs against the Memory and Local `DurableBackend`s (same seam as the
//! restore / share / list-tree / recovery E2Es).

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use common::{DurableBackend, LocalBackend, MemoryBackend, age_identity, build_ctx, make_config};
use s5_core::blob::Blobs;
use s5_node::health::gather_health;

async fn health_walk(backend: &dyn DurableBackend) -> Result<()> {
    let label = backend.label();
    let scratch = tempfile::tempdir()?;
    let (paper_recipient, paper_id) = age_identity(scratch.path(), "paper");
    let (device_recipient, device_id) = age_identity(scratch.path(), "device");

    let source = scratch.path().join("source");
    std::fs::create_dir_all(&source)?;
    std::fs::write(source.join("f.txt"), b"hi")?;
    let vault_root = scratch.path().join("vault");
    std::fs::create_dir_all(&vault_root)?;

    let (blobs, registry) = backend.open();
    let mut config = make_config(
        &vault_root.to_string_lossy(),
        &paper_recipient,
        &paper_id,
        &device_recipient,
        &device_id,
        &source.to_string_lossy(),
    );
    // Give the vault a scheduled backup so it surfaces in `schedules`.
    config
        .vault
        .get_mut("backup")
        .expect("backup vault present")
        .snap_interval_secs = Some(3600);

    let ctx = build_ctx(config, blobs, registry, [0x51u8; 32]);
    let cfg = ctx.config.read().await;

    // ================= Reachable store, no staging =================
    let health = gather_health(&cfg, &ctx.stores).await;
    let durable = health
        .stores
        .iter()
        .find(|s| s.name == "durable")
        .with_context(|| format!("[{label}] 'durable' store must appear in health walk"))?;
    assert!(
        durable.reachable,
        "[{label}] a live store must answer the reachability probe (err: {:?})",
        durable.error
    );
    assert!(
        durable.error.is_none(),
        "[{label}] a reachable store carries no error"
    );
    assert!(
        durable.staging.is_none(),
        "[{label}] a direct store has no staging layer — writes are durable on return"
    );

    // ================= Scheduled backup surfaces =================
    let sched = health
        .schedules
        .iter()
        .find(|r| r.vault == "backup")
        .with_context(|| format!("[{label}] the scheduled backup must surface"))?;
    assert_eq!(
        sched.interval_secs, 3600,
        "[{label}] the configured cadence must round-trip"
    );

    drop(cfg);

    // ================= Configured-but-unresolved store =================
    // A store named in config with no resolved handle is reported unreachable
    // (it failed to build), not dropped. Simulate by handing gather_health a
    // config that names a second store while the resolved map lacks it.
    let mut cfg2 = ctx.config.read().await.clone();
    cfg2.store
        .insert("ghost".to_string(), example_store_entry());
    // Resolved map still holds only "durable".
    let resolved: HashMap<String, Arc<dyn Blobs>> = ctx.stores.clone();
    let health2 = gather_health(&cfg2, &resolved).await;
    let ghost = health2
        .stores
        .iter()
        .find(|s| s.name == "ghost")
        .with_context(|| {
            format!("[{label}] an unresolved configured store must still be listed")
        })?;
    assert!(
        !ghost.reachable,
        "[{label}] a store with no resolved handle must be UNREACHABLE"
    );
    assert!(
        ghost.error.is_some(),
        "[{label}] an unreachable store must carry a reason"
    );
    // The real store is still reachable in the same walk.
    assert!(
        health2
            .stores
            .iter()
            .any(|s| s.name == "durable" && s.reachable),
        "[{label}] the resolved store stays reachable alongside the ghost"
    );

    // ============== D20 `[task.*]` `every` automation surfaces ==============
    // The real D20 surface (`automate add … --every`, `share … --live`) writes a
    // `[task.*]` with `trigger = every` — NOT `vault.snap_interval_secs`. It must
    // surface in the doctor/status "scheduled backups" list too, else a
    // configured scheduled backup is invisible to the safety glance.
    let mut cfg3 = ctx.config.read().await.clone();
    cfg3.task.insert(
        "backup-1h".to_string(),
        s5_node::config::NodeConfigTask {
            then: Vec::new(),
            trigger: s5_node::config::TaskTrigger::Every,
            interval_secs: Some(1800),
            paused: false,
            spec: s5_node::config::TaskSpec::Backup {
                vault: "backup".to_string(),
                source: "docs".to_string(),
                blob_store: "durable".to_string(),
                keys: vec!["device".to_string()],
                target_path: None,
                changed_paths: None,
            },
        },
    );
    let health3 = gather_health(&cfg3, &ctx.stores).await;
    assert!(
        health3
            .schedules
            .iter()
            .any(|r| r.vault == "backup" && r.interval_secs == 1800),
        "[{label}] a `[task.*]` every-automation must surface as a scheduled backup"
    );

    Ok(())
}

/// A throwaway `[store.*]` config entry used only to name a store that has no
/// resolved handle (the backend variant is irrelevant — it is never built).
fn example_store_entry() -> s5_node::config::NodeConfigStore {
    s5_node::config::NodeConfigStore::from_backend(s5_node::config::NodeConfigStoreBackend::Memory)
}

#[tokio::test]
async fn health_walk_memory() {
    health_walk(&MemoryBackend::new()).await.unwrap();
}

#[tokio::test]
async fn health_walk_local() {
    health_walk(&LocalBackend::new()).await.unwrap();
}
