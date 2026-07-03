//! Node health walk: per-store reachability + staging gauges + configured
//! schedules, gathered for `vup doctor` and the `vup status` durability
//! gauges.
//!
//! Kept as a plain `pub async fn` (not an RPC method) so the E2E harness can
//! drive it against the `DurableBackend` seam without a live daemon — mirrors
//! [`crate::tasks::list::list_tree`]. The `GetHealth` RPC handler is a thin
//! wrapper that reads config + `TaskExecutorContext.stores` and calls this.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use s5_core::Hash;
use s5_core::blob::Blobs;
use s5_node_api::config::{TaskSpec, TaskTrigger};
use s5_node_api::{GetHealthResponse, ScheduledRun, StagingGauges, StoreHealth};

use crate::config::S5NodeConfig;

/// Probe every configured `[store.*]` for reachability + staging state and
/// collect the configured scheduled backups.
///
/// - **Reachability** is a `blob_contains(Hash::EMPTY)` HEAD against the
///   store's vault-facing handle: a store that answers (even `Ok(false)`) is
///   reachable; one whose backend errors is not. A store present in config but
///   absent from the resolved registry is reported unreachable with a note
///   rather than silently skipped.
/// - **Staging** comes from [`s5_core::blob::BlobsWrite::staging_stats`] —
///   `Some` only for a write-buffering backend (a packing store); `None` for a
///   direct store whose writes are durable on return.
/// - **Schedules** are every `[task.*]` automation with `trigger = "every"`
///   (the D20 `automate add … --every` / `share … --live` surface) plus any
///   vault still carrying the legacy `snap_interval_secs` knob.
///
/// Store rows come back in config (name) order so the output is stable.
pub async fn gather_health(
    config: &S5NodeConfig,
    stores: &HashMap<String, Arc<dyn Blobs>>,
) -> GetHealthResponse {
    // Probe every store the daemon knows about: the union of the configured
    // `[store.*]` names and the resolved handles. A configured store missing
    // from the resolved map is reported unreachable (it failed to build)
    // rather than silently dropped; sorted for stable output.
    let names: BTreeSet<&String> = config.store.keys().chain(stores.keys()).collect();
    let mut store_health = Vec::with_capacity(names.len());
    for name in names {
        let entry = match stores.get(name) {
            Some(blobs) => {
                // A HEAD on the empty-blob hash: cheap on every backend, and a
                // truthful reachability signal (a down remote errors; a live
                // store answers Ok(true|false)).
                let (reachable, error) = match blobs.blob_contains(Hash::EMPTY).await {
                    Ok(_) => (true, None),
                    Err(e) => (false, Some(format!("{e:#}"))),
                };
                let staging = blobs.staging_stats().map(|s| StagingGauges {
                    staged_bytes: s.staged_bytes,
                    since_last_flush_secs: s.since_last_flush_secs,
                    inflight: s.inflight,
                });
                StoreHealth {
                    name: name.clone(),
                    reachable,
                    error,
                    staging,
                }
            }
            None => StoreHealth {
                name: name.clone(),
                reachable: false,
                error: Some("configured but not resolved in the daemon store registry".to_string()),
                staging: None,
            },
        };
        store_health.push(entry);
    }

    // Scheduled backups the daemon actually runs: every `[task.*]` automation
    // with `trigger = "every"` (the D20 `automate --every` / `share --live`
    // surface — this is what the CLI writes today), plus any vault still on the
    // legacy `snap_interval_secs` knob (pre-`automate` configs). Task-name order
    // then vault name keeps the output stable.
    let mut schedules: Vec<ScheduledRun> = config
        .task
        .iter()
        .filter(|(_, task)| task.trigger == TaskTrigger::Every)
        .filter_map(|(_, task)| {
            task.interval_secs.map(|interval_secs| ScheduledRun {
                vault: task_scheduled_vault(&task.spec),
                interval_secs,
            })
        })
        .collect();
    schedules.extend(config.vault.iter().filter_map(|(vault, cfg)| {
        cfg.snap_interval_secs.map(|interval_secs| ScheduledRun {
            vault: vault.clone(),
            interval_secs,
        })
    }));

    GetHealthResponse {
        stores: store_health,
        schedules,
    }
}

/// The vault a scheduled `[task.*]` automation targets (for the doctor/status
/// "scheduled backups" list). A `Copy` automation (e.g. `share … --live`)
/// maintains its destination vault.
fn task_scheduled_vault(spec: &TaskSpec) -> String {
    match spec {
        TaskSpec::Ingest { vault, .. }
        | TaskSpec::Publish { vault, .. }
        | TaskSpec::Backup { vault, .. }
        | TaskSpec::Restore { vault, .. } => vault.clone(),
        TaskSpec::Copy { dst_vault, .. } => dst_vault.clone(),
    }
}
