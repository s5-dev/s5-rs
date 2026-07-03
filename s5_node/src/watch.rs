//! Daemon-side automation engine — notify-driven and scheduled backups.
//!
//! The [`AutomationManager`] owns one supervised loop per *automation*: a
//! `[task.*]` entry whose `trigger` is `Watch` or `Every` (and that is not
//! `paused`). It [`reconcile`](AutomationManager::reconcile)s the live set of
//! loops against config on demand — spawning newly-desired automations,
//! cancelling ones that vanished or changed — so `automate add/pause/resume/rm`
//! (which are all `patch_config` ops) take effect live the moment the daemon
//! fires the automation-refresh signal.
//!
//! Two trigger kinds:
//! - **Watch**: a recursive `notify` watcher over the task's source paths + an
//!   event loop that coalesces changed paths and, every `FLUSH_INTERVAL`,
//!   submits an **incremental** Backup (`changed_paths = Some(burst)`) of just
//!   that burst to the local `TaskExecutor` — O(changed) rather than O(corpus).
//!   A periodic full Backup reconcile (`RECONCILE_INTERVAL`) is the safety net
//!   for events the watcher can't be trusted to deliver (in-place mmap
//!   modifies, inotify queue overflow).
//! - **Every**: a full backup every `interval_secs`, measured from the previous
//!   run's completion (a slow backup never stacks a second one).
//!
//! Backups are serialized — each `dispatch` awaits the task's terminal state —
//! so two never race the same vault root. Each loop owns a `CancellationToken`
//! that the daemon cancels at shutdown so no orphaned watcher threads are left
//! behind, and a [`WatchHealth`] the supervisor stamps so `GetStatus` can
//! report per-automation liveness. On a loop failure the supervisor
//! records the error, bumps a restart counter, backs off, and respawns until
//! cancelled.
//!
//! Legacy shim: any vault carrying the old `watch = true` /
//! `snap_interval_secs` knobs (with no equivalent `[task.*]` automation) is
//! synthesized into an in-memory automation, so pre-`automate` configs keep
//! working (a one-time warning nudges migration).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, anyhow, bail};
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use s5_node_api::config::{NodeConfigTask, TaskSpec, TaskTrigger};
use s5_node_api::{AutomationStatus, TaskState};
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::S5NodeConfig;
use crate::tasks::TaskExecutor;

/// How often the watch loop flushes a coalesced burst as an incremental snapshot.
const FLUSH_INTERVAL: Duration = Duration::from_secs(2);
/// How often the watch loop runs a full backup reconcile — the HOURLY periodic
/// backstop. The watcher + drainer is the reliable primary: every FS event is
/// folded into the `changed` set (never dropped, even mid-backup), and the
/// incremental snapshot every `FLUSH_INTERVAL` publishes it. This full reconcile is
/// only an hourly belt-and-suspenders for the rare event the watcher genuinely
/// can't observe (in-place mmap modifies on sources that append via mmap). The
/// interval is measured from when a reconcile *completes* (see `run_loop`), so
/// a reconcile's own multi-second runtime never counts against it. Tombstones
/// for compacted-away packs that the incremental rename events somehow missed
/// lag at most this long, which the cold-GC grace window tolerates.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(3600);
/// Cap on the supervisor's exponential restart backoff.
const MAX_RESTART_BACKOFF: Duration = Duration::from_secs(60);

/// Per-automation health, stamped by the supervised loop and read by
/// `GetStatus`. Shared (`Arc`) between the manager registry and the
/// spawned loop.
pub struct WatchHealth {
    vault: String,
    trigger: TaskTrigger,
    last_error: Mutex<Option<String>>,
    last_ok: Mutex<Option<SystemTime>>,
    restarts: AtomicU64,
    alive: AtomicBool,
}

impl WatchHealth {
    fn new(vault: String, trigger: TaskTrigger) -> Arc<Self> {
        Arc::new(Self {
            vault,
            trigger,
            last_error: Mutex::new(None),
            last_ok: Mutex::new(None),
            restarts: AtomicU64::new(0),
            alive: AtomicBool::new(false),
        })
    }

    fn set_alive(&self, alive: bool) {
        self.alive.store(alive, Ordering::Relaxed);
    }

    /// Stamp a successful backup dispatch.
    fn stamp_ok(&self) {
        *self.last_ok.lock().expect("last_ok lock poisoned") = Some(SystemTime::now());
    }

    /// Record a loop failure (kept until overwritten by the next one).
    fn record_error(&self, err: String) {
        *self.last_error.lock().expect("last_error lock poisoned") = Some(err);
    }

    fn bump_restart(&self) {
        self.restarts.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self, name: &str) -> AutomationStatus {
        let last_ok_unix = self
            .last_ok
            .lock()
            .expect("last_ok lock poisoned")
            .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
            .map(|d| d.as_secs());
        AutomationStatus {
            name: name.to_string(),
            vault: self.vault.clone(),
            trigger: self.trigger,
            paused: false,
            alive: self.alive.load(Ordering::Relaxed),
            restarts: self.restarts.load(Ordering::Relaxed),
            last_ok_unix,
            last_error: self
                .last_error
                .lock()
                .expect("last_error lock poisoned")
                .clone(),
        }
    }
}

struct ActiveWatch {
    cancel: CancellationToken,
    join: JoinHandle<()>,
    /// The task this loop was spawned from — reconcile respawns when it
    /// differs (spec/trigger/interval changed).
    task: NodeConfigTask,
    health: Arc<WatchHealth>,
}

/// Daemon-owned registry of active automation loops, keyed by **task name**.
/// Built once during node startup alongside `MountManager`; driven by
/// [`reconcile`](AutomationManager::reconcile) on every automation-refresh
/// signal, and drained by the daemon shutdown path.
pub struct AutomationManager {
    executor: Arc<TaskExecutor>,
    watches: RwLock<HashMap<String, ActiveWatch>>,
    /// Fires the one-time legacy-shim migration warning at most once.
    legacy_warned: AtomicBool,
}

/// Legacy alias — the automation engine grew out of the old `WatchManager`.
/// Kept so existing call sites (and the `watch_schedule_e2e` harness) compile
/// unchanged through the beta.
pub type WatchManager = AutomationManager;

impl AutomationManager {
    pub fn new(executor: Arc<TaskExecutor>) -> Self {
        Self {
            executor,
            watches: RwLock::new(HashMap::new()),
            legacy_warned: AtomicBool::new(false),
        }
    }

    /// Legacy one-shot entry point: reconcile once from config. Retained so
    /// pre-`automate` callers (and `watch_schedule_e2e`) keep working; new code
    /// drives [`reconcile`](Self::reconcile) directly on each refresh.
    pub async fn start_from_config(self: &Arc<Self>, config: &S5NodeConfig) {
        self.reconcile(config).await;
    }

    /// Drive the live loop set toward the config's desired automations: cancel
    /// loops that vanished or whose task changed, spawn newly-desired ones.
    /// Idempotent — a reconcile with no deltas touches nothing.
    pub async fn reconcile(self: &Arc<Self>, config: &S5NodeConfig) {
        let desired = self.desired_automations(config);

        // Cancel running loops no longer desired (removed / paused) or whose
        // backing task changed (spec/trigger/interval).
        let to_cancel: Vec<String> = {
            let watches = self.watches.read().await;
            watches
                .iter()
                .filter_map(|(name, aw)| match desired.get(name) {
                    None => Some(name.clone()),
                    Some(task) if *task != aw.task => Some(name.clone()),
                    Some(_) => None,
                })
                .collect()
        };
        for name in to_cancel {
            self.cancel_one(&name).await;
        }

        // Spawn desired loops that aren't already running.
        for (name, task) in &desired {
            if self.watches.read().await.contains_key(name) {
                continue;
            }
            let (spec, paths) = match prepare_task(config, task) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(
                        automation = name.as_str(),
                        "automation spec preparation failed: {e:#}"
                    );
                    continue;
                }
            };
            let health = WatchHealth::new(spec_vault(&spec), task.trigger);
            match task.trigger {
                TaskTrigger::Watch => {
                    if paths.is_empty() {
                        tracing::warn!(
                            automation = name.as_str(),
                            "watch automation has no source paths — skipping"
                        );
                        continue;
                    }
                    self.spawn_watch(name.clone(), task.clone(), spec, paths, health)
                        .await;
                }
                TaskTrigger::Every => {
                    let secs = task.interval_secs.unwrap_or(1).max(1);
                    self.spawn_schedule(
                        name.clone(),
                        task.clone(),
                        spec,
                        Duration::from_secs(secs),
                        health,
                    )
                    .await;
                }
                TaskTrigger::Manual => {} // never in `desired`
            }
        }
    }

    /// The desired automation set: `name → task` for every non-paused
    /// `[task.*]` with a non-`Manual` trigger, plus in-memory automations
    /// synthesized from the legacy `vault.watch` / `snap_interval_secs` knobs
    /// (only when no `[task.*]` already backs up that vault).
    fn desired_automations(&self, config: &S5NodeConfig) -> BTreeMap<String, NodeConfigTask> {
        let mut desired: BTreeMap<String, NodeConfigTask> = BTreeMap::new();

        for (name, task) in &config.task {
            if task.trigger != TaskTrigger::Manual && !task.paused {
                desired.insert(name.clone(), task.clone());
            }
        }

        // Legacy shim: translate `watch`/`snap_interval_secs` into synthetic
        // automations for any vault not already covered by a `[task.*]`
        // automation.
        let mut synthesized_any = false;
        for (vault_name, vault_cfg) in &config.vault {
            let covered = config.task.values().any(|t| {
                t.trigger != TaskTrigger::Manual && spec_backup_vault(&t.spec) == Some(vault_name)
            });
            if covered {
                continue;
            }
            // `watch` takes precedence over `snap_interval_secs` (the old
            // behaviour).
            let legacy = if vault_cfg.watch {
                synth_legacy_task(config, vault_name, vault_cfg, None)
            } else if let Some(interval) = vault_cfg.snap_interval_secs {
                synth_legacy_task(config, vault_name, vault_cfg, Some(interval))
            } else {
                None
            };
            if let Some(task) = legacy {
                // Keyed by vault name — but a `[task.*]` may already occupy that
                // key: a task NAMED like this vault that backs up a DIFFERENT
                // vault (`covered` only skips synthesis when a task BACKS UP
                // this vault). Don't silently drop the legacy watch: fall back
                // to a non-colliding synthetic name so it still gets a loop.
                let key = if desired.contains_key(vault_name) {
                    let mut n = 2;
                    let mut k = format!("{vault_name}-legacy");
                    while desired.contains_key(&k) {
                        k = format!("{vault_name}-legacy-{n}");
                        n += 1;
                    }
                    k
                } else {
                    vault_name.clone()
                };
                desired.insert(key, task);
                synthesized_any = true;
            }
        }
        if synthesized_any && !self.legacy_warned.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                "synthesizing automations from legacy vault.watch / snap_interval_secs — \
                 migrate to `[task.*]` automations (see `vup automate`)"
            );
        }

        desired
    }

    /// Scheduled snaps: dispatch `spec` every `interval`, measured from the
    /// previous run's COMPLETION (a backup slower than the interval never stacks
    /// a second one). Shares the registry + shutdown path with the watch loops.
    async fn spawn_schedule(
        self: &Arc<Self>,
        name: String,
        task: NodeConfigTask,
        spec: TaskSpec,
        interval: Duration,
        health: Arc<WatchHealth>,
    ) {
        let cancel = CancellationToken::new();
        let cancel_inner = cancel.clone();
        let executor = self.executor.clone();
        let name_for_loop = name.clone();
        let health_loop = health.clone();

        let join = tokio::spawn(async move {
            health_loop.set_alive(true);
            loop {
                tokio::select! {
                    _ = cancel_inner.cancelled() => {
                        health_loop.set_alive(false);
                        break;
                    }
                    _ = tokio::time::sleep(interval) => {
                        match dispatch(&executor, &spec, &name_for_loop).await {
                            Ok(()) => health_loop.stamp_ok(),
                            Err(e) => {
                                health_loop.record_error(format!("{e:#}"));
                                tracing::warn!(
                                    automation = name_for_loop.as_str(),
                                    "scheduled snap failed (retried next interval): {e:#}"
                                );
                            }
                        }
                    }
                }
            }
        });

        self.watches.write().await.insert(
            name.clone(),
            ActiveWatch {
                cancel,
                join,
                task,
                health,
            },
        );
        tracing::info!(
            automation = name.as_str(),
            interval_secs = interval.as_secs(),
            "scheduled automation spawned"
        );
    }

    /// Watch loop with an auto-restart supervisor: (re)run `run_loop` until the
    /// token is cancelled, recording failures + backing off between attempts.
    async fn spawn_watch(
        self: &Arc<Self>,
        name: String,
        task: NodeConfigTask,
        spec: TaskSpec,
        paths: Vec<PathBuf>,
        health: Arc<WatchHealth>,
    ) {
        let cancel = CancellationToken::new();
        let cancel_inner = cancel.clone();
        let executor = self.executor.clone();
        let name_for_loop = name.clone();
        let health_loop = health.clone();

        let join = tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                if cancel_inner.is_cancelled() {
                    break;
                }
                let result = run_loop(
                    &name_for_loop,
                    spec.clone(),
                    paths.clone(),
                    executor.clone(),
                    cancel_inner.clone(),
                    health_loop.clone(),
                )
                .await;
                health_loop.set_alive(false);
                match result {
                    // Clean exit == cancelled; stop supervising.
                    Ok(()) => break,
                    Err(e) => {
                        health_loop.record_error(format!("{e:#}"));
                        health_loop.bump_restart();
                        tracing::error!(
                            automation = name_for_loop.as_str(),
                            "watch loop failed, restarting after {}s: {e:#}",
                            backoff.as_secs()
                        );
                        tokio::select! {
                            _ = cancel_inner.cancelled() => break,
                            _ = tokio::time::sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(MAX_RESTART_BACKOFF);
                    }
                }
            }
        });

        self.watches.write().await.insert(
            name.clone(),
            ActiveWatch {
                cancel,
                join,
                task,
                health,
            },
        );
        tracing::info!(automation = name.as_str(), "watch automation spawned");
    }

    /// Cancel a single loop by name and await its task. No-op if not running.
    async fn cancel_one(&self, name: &str) {
        let entry = self.watches.write().await.remove(name);
        if let Some(w) = entry {
            w.cancel.cancel();
            let _ = w.join.await;
            tracing::debug!(automation = name, "automation loop stopped");
        }
    }

    /// Cancel every loop and await its task. Idempotent.
    pub async fn shutdown(&self) {
        let entries: Vec<(String, ActiveWatch)> = {
            let mut watches = self.watches.write().await;
            watches.drain().collect()
        };
        for (name, w) in entries {
            w.cancel.cancel();
            let _ = w.join.await;
            tracing::debug!(automation = name.as_str(), "automation loop stopped");
        }
    }

    /// Per-automation liveness snapshot for `GetStatus`, name-sorted.
    pub async fn status(&self) -> Vec<AutomationStatus> {
        let watches = self.watches.read().await;
        let mut out: Vec<AutomationStatus> = watches
            .iter()
            .map(|(name, aw)| aw.health.snapshot(name))
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }
}

/// The vault an automation targets (for status / logging).
fn spec_vault(spec: &TaskSpec) -> String {
    match spec {
        TaskSpec::Ingest { vault, .. }
        | TaskSpec::Publish { vault, .. }
        | TaskSpec::Backup { vault, .. }
        | TaskSpec::Restore { vault, .. } => vault.clone(),
        TaskSpec::Copy { dst_vault, .. } => dst_vault.clone(),
    }
}

/// The backed-up vault iff `spec` is a `Backup` (used by the legacy-shim
/// "already covered?" check).
fn spec_backup_vault(spec: &TaskSpec) -> Option<&str> {
    match spec {
        TaskSpec::Backup { vault, .. } => Some(vault.as_str()),
        _ => None,
    }
}

/// Synthesize an in-memory legacy automation from a vault's `watch` /
/// `snap_interval_secs`. `interval` `Some` → an `Every` schedule; `None` → a
/// `Watch`. Returns `None` (skip + it stays unhandled) when the vault can't be
/// expressed as a single-source Backup — the same cases the old `prepare` bailed
/// on.
fn synth_legacy_task(
    config: &S5NodeConfig,
    vault_name: &str,
    vault_cfg: &crate::config::NodeConfigVault,
    interval: Option<u64>,
) -> Option<NodeConfigTask> {
    let source = match vault_cfg.sources.as_slice() {
        [s] => s.clone(),
        _ => return None,
    };
    if vault_cfg.recipients.is_empty() {
        return None;
    }
    let blob_store = config
        .vault_data_store(vault_name, vault_cfg)
        .ok()?
        .to_string();
    Some(NodeConfigTask {
        then: Vec::new(),
        trigger: if interval.is_some() {
            TaskTrigger::Every
        } else {
            TaskTrigger::Watch
        },
        interval_secs: interval,
        paused: false,
        spec: TaskSpec::Backup {
            vault: vault_name.to_string(),
            source,
            blob_store,
            keys: vault_cfg.recipients.clone(),
            target_path: None,
            changed_paths: None,
        },
    })
}

/// Resolve an automation task into the `Backup` spec to dispatch + the source
/// paths to watch. Paths are only meaningful for `Watch`; an `Every` schedule
/// dispatches the spec verbatim (paths are returned empty for non-Backup specs).
fn prepare_task(config: &S5NodeConfig, task: &NodeConfigTask) -> Result<(TaskSpec, Vec<PathBuf>)> {
    let paths = match &task.spec {
        TaskSpec::Backup { source, .. } => {
            let s = config
                .source
                .get(source)
                .ok_or_else(|| anyhow!("references missing source '{source}'"))?;
            s.paths.iter().map(PathBuf::from).collect()
        }
        _ => Vec::new(),
    };
    Ok((task.spec.clone(), paths))
}

async fn run_loop(
    name: &str,
    spec: TaskSpec,
    paths: Vec<PathBuf>,
    executor: Arc<TaskExecutor>,
    cancel: CancellationToken,
    health: Arc<WatchHealth>,
) -> Result<()> {
    // The base `spec` is a full Backup (changed_paths None) — the initial
    // baseline + periodic reconcile. Pull its fields so each burst can build a
    // `changed_paths = Some(..)` incremental Backup of the same vault.
    let TaskSpec::Backup {
        vault: spec_vault,
        source,
        blob_store,
        keys,
        ..
    } = &spec
    else {
        bail!("watch automation requires a Backup spec");
    };
    let (spec_vault, source, blob_store, keys) = (
        spec_vault.clone(),
        source.clone(),
        blob_store.clone(),
        keys.clone(),
    );

    // notify::Watcher is sync and runs on a background thread; the event
    // handler hands the changed *paths* off to tokio (the old loop discarded
    // them). Dropping the watcher when this fn exits ends that thread cleanly.
    let (tx, mut rx) = mpsc::channel::<Vec<PathBuf>>(8192);
    let watcher_tx = tx.clone();
    let mut watcher = RecommendedWatcher::new(
        move |res: notify::Result<notify::Event>| {
            if let Ok(event) = res
                && !event.paths.is_empty()
                // Drop pure ACCESS events (open / read / close-nowrite). They
                // never change content, but the in-process readers — above all
                // the EsegLookup tombstone-resolver, which opens sealed `.eseg`
                // files thousands of times per second — emit them in a flood.
                // Left in, they pour unchanged paths into the changed-set that
                // `is_changed` then re-stats and discards, turning each
                // "incremental" snapshot into a near-whole-tree walk. Real content
                // changes always surface as Create/Modify/Remove; the periodic
                // full reconcile is the backstop for anything subtle this drops.
                && !matches!(event.kind, EventKind::Access(_))
            {
                // try_send (not blocking_send): on a full channel we drop the
                // event rather than stall the watcher thread — the periodic
                // reconcile re-syncs whatever is lost, same as inotify overflow.
                let _ = watcher_tx.try_send(event.paths);
            }
        },
        notify::Config::default(),
    )
    .map_err(|e| anyhow!("creating notify watcher: {e}"))?;

    for path in &paths {
        watcher
            .watch(path, RecursiveMode::Recursive)
            .with_context(|| format!("watching {}", path.display()))?;
    }

    // Watchers are established — the loop is healthy. The supervisor clears
    // this on any exit (cancel or failure).
    health.set_alive(true);

    // A dedicated drainer task owns `rx` and folds every FS-event path into the
    // shared `changed` set. This runs CONCURRENTLY with the backups below, so
    // events are never lost while a backup is in flight — the old `select!`
    // parked on `dispatch().await` and stopped polling `rx`, overflowing the
    // bounded channel (and dropping events) during every multi-second backup.
    // A `HashSet` coalesces repeated writes to the same path between flushes.
    let changed: Arc<Mutex<HashSet<PathBuf>>> = Arc::new(Mutex::new(HashSet::new()));
    let drain_set = Arc::clone(&changed);
    let drainer = tokio::spawn(async move {
        while let Some(paths) = rx.recv().await {
            drain_set
                .lock()
                .expect("changed-set lock poisoned")
                .extend(paths);
        }
    });

    // Initial full backup: pristine baseline that also picks up drift since the
    // last shutdown. The drainer is already folding events into `changed`, so
    // anything that changes during the baseline is handled on the first
    // incremental tick (content-addressed → a redundant re-upload is cheap).
    match dispatch(&executor, &spec, name).await {
        Ok(()) => health.stamp_ok(),
        Err(e) => tracing::warn!(automation = name, "initial reconcile failed: {e:#}"),
    }

    let mut last_reconcile = Instant::now();
    let mut flush = tokio::time::interval(FLUSH_INTERVAL);
    flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let result = loop {
        tokio::select! {
            _ = cancel.cancelled() => break Ok(()),
            _ = flush.tick() => {
                if last_reconcile.elapsed() >= RECONCILE_INTERVAL {
                    // Rare periodic full reconcile (the backstop). It supersedes
                    // the pending burst, so clear what's accumulated.
                    changed.lock().expect("changed-set lock poisoned").clear();
                    match dispatch(&executor, &spec, name).await {
                        Ok(()) => health.stamp_ok(),
                        Err(e) => tracing::warn!(automation = name, "reconcile failed: {e:#}"),
                    }
                    // Stamp AFTER completion so the reconcile's own runtime is
                    // not counted against the interval — otherwise a reconcile
                    // that overruns RECONCILE_INTERVAL retriggers immediately
                    // and starves the incremental path forever.
                    last_reconcile = Instant::now();
                    continue;
                }
                // Incremental snapshot of everything accumulated since the last
                // tick. Drain under the lock, then RELEASE it before the await.
                let batch: Vec<PathBuf> = {
                    let mut g = changed.lock().expect("changed-set lock poisoned");
                    if g.is_empty() {
                        continue;
                    }
                    g.drain().collect()
                };
                let n = batch.len();
                let inc = TaskSpec::Backup {
                    vault: spec_vault.clone(),
                    source: source.clone(),
                    blob_store: blob_store.clone(),
                    keys: keys.clone(),
                    target_path: None,
                    changed_paths: Some(batch),
                };
                match dispatch(&executor, &inc, name).await {
                    Ok(()) => health.stamp_ok(),
                    Err(e) => tracing::warn!(automation = name, paths = n, "incremental snap failed: {e:#}"),
                }
            }
        }
    };

    // Stop the drainer (drops its `rx`); the watcher thread ends when `watcher`
    // drops at function exit.
    drainer.abort();
    result
}

/// Spawn a backup task and AWAIT its terminal state, so the automation loop
/// never runs two backups for the same vault concurrently — concurrent
/// load→merge→save of the vault root would lose updates. `spawn` is
/// fire-and-forget, so we poll the task's status watch channel to completion.
async fn dispatch(executor: &TaskExecutor, spec: &TaskSpec, name: &str) -> Result<()> {
    let (task_id, _) = executor.spawn(spec.clone()).await?;
    tracing::debug!(
        automation = name,
        task_id = task_id,
        "automation snap dispatched"
    );
    if let Some(mut rx) = executor.watch_status(task_id).await {
        loop {
            let terminal = match &rx.borrow().state {
                TaskState::Completed => Ok(true),
                TaskState::Cancelled => Ok(true),
                TaskState::Failed { error } => Err(anyhow!("{error}")),
                _ => Ok(false),
            };
            match terminal {
                Ok(true) => break,
                Err(e) => return Err(e),
                Ok(false) => {}
            }
            if rx.changed().await.is_err() {
                break;
            }
        }
    }
    Ok(())
}
