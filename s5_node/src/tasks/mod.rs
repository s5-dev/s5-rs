//! Task executor for s5_node.
//!
//! Tasks are ephemeral units of work (ingest, restore, publish, backup)
//! submitted at runtime via RPC. The executor spawns each task as a tokio
//! task, tracks its state and progress, and supports cancellation.

pub mod ingest;
pub mod peer_load;
pub mod publish;
pub mod restore;
pub mod vault_persist;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, anyhow};
use s5_core::RegistryApi;
use s5_core::blob::BlobStore;
use s5_node_api::config::{NodeConfigKey, NodeConfigSource, NodeConfigVault, TaskSpec};
use s5_node_api::{TaskProgressMap, TaskState, TaskStatusResponse};
use tokio::sync::{RwLock, watch};
use tokio_util::sync::CancellationToken;

use crate::config::S5NodeConfig;

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

/// Shared context available to all running tasks.
///
/// Bundles the node's resolved stores, config, and other resources that
/// tasks need to do their work. Created once at node startup and shared
/// via `Arc`.
pub struct TaskExecutorContext {
    /// Full node config (for resolving vault/source/store/key names).
    /// Shared with the RPC server so `patch_config` updates are visible to tasks.
    pub config: Arc<RwLock<S5NodeConfig>>,
    /// Resolved blob stores (from `[store.*]` config).
    pub stores: HashMap<String, BlobStore>,
    /// Node secret (32 bytes) — used for key derivation (vault passphrase, signing keys).
    /// Derived from the iroh secret key via blake3.
    pub node_secret: [u8; 32],
    /// Registry for snapshot publishing (None if not configured).
    pub registry: Option<Arc<dyn RegistryApi + Send + Sync>>,
}

// ---------------------------------------------------------------------------
// Task reporter
// ---------------------------------------------------------------------------

/// A clonable handle that tasks use to report progress and state changes.
///
/// Wraps a `watch::Sender<TaskStatusResponse>` so that every mutation
/// immediately notifies all watchers (both the executor's `TaskHandle`
/// and any streaming RPC subscribers).
#[derive(Clone)]
pub struct TaskReporter {
    tx: watch::Sender<TaskStatusResponse>,
}

impl TaskReporter {
    /// Create a new reporter for a task. The initial status is `Running`
    /// with no progress.
    fn new(task_id: u64) -> (Self, watch::Receiver<TaskStatusResponse>) {
        let initial = TaskStatusResponse {
            task_id,
            state: TaskState::Running,
            progress: None,
        };
        let (tx, rx) = watch::channel(initial);
        (Self { tx }, rx)
    }

    /// Replace the progress map wholesale (used for initial setup).
    pub fn init_progress(&self, progress: TaskProgressMap) {
        self.tx.send_modify(|s| {
            s.progress = Some(progress);
        });
    }

    /// Mutate the progress map in-place. The closure receives
    /// `&mut TaskProgressMap`; if progress hasn't been initialised yet
    /// the call is a no-op.
    pub fn update_progress(&self, f: impl FnOnce(&mut TaskProgressMap)) {
        self.tx.send_modify(|s| {
            if let Some(ref mut p) = s.progress {
                f(p);
            }
        });
    }

    /// Set the task state (e.g. Completed, Failed, Cancelled).
    fn set_state(&self, state: TaskState) {
        self.tx.send_modify(|s| {
            s.state = state;
        });
    }
}

// ---------------------------------------------------------------------------
// Task handle
// ---------------------------------------------------------------------------

/// A handle to a running or completed task.
#[allow(dead_code)]
struct TaskHandle {
    /// Monotonically increasing task ID.
    id: u64,
    /// The resolved spec this task is executing.
    spec: TaskSpec,
    /// Watch receiver for the task's current status.
    /// Read the latest value with `borrow().clone()`.
    status: watch::Receiver<TaskStatusResponse>,
    /// Cancellation token — dropping or cancelling stops the task.
    cancel: CancellationToken,
    /// JoinHandle for the spawned tokio task.
    join: tokio::task::JoinHandle<()>,
}

// ---------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------

/// The task executor — owns all running and recently completed tasks.
pub struct TaskExecutor {
    ctx: Arc<TaskExecutorContext>,
    next_id: AtomicU64,
    tasks: RwLock<HashMap<u64, TaskHandle>>,
}

impl TaskExecutor {
    /// Create a new executor with the given context.
    pub fn new(ctx: Arc<TaskExecutorContext>) -> Self {
        Self {
            ctx,
            next_id: AtomicU64::new(1),
            tasks: RwLock::new(HashMap::new()),
        }
    }

    /// Access the shared task executor context.
    pub fn ctx(&self) -> &Arc<TaskExecutorContext> {
        &self.ctx
    }

    /// Spawn a task from a resolved [`TaskSpec`].
    ///
    /// Returns the assigned task ID and the spec (echoed back for confirmation).
    pub async fn spawn(&self, spec: TaskSpec) -> anyhow::Result<(u64, TaskSpec)> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let cancel = CancellationToken::new();
        let (reporter, status_rx) = TaskReporter::new(id);

        // Clone what the spawned future needs.
        let ctx = self.ctx.clone();
        let spec_clone = spec.clone();
        let cancel_clone = cancel.clone();
        let reporter_clone = reporter.clone();

        let join = tokio::spawn(async move {
            let result = run_task(ctx, &spec_clone, reporter_clone.clone(), cancel_clone).await;
            match result {
                Ok(true) => reporter_clone.set_state(TaskState::Cancelled),
                Ok(false) => reporter_clone.set_state(TaskState::Completed),
                Err(e) => {
                    let msg = format!("{e:#}");
                    tracing::error!(task_id = id, error = %e, "task failed");
                    reporter_clone.set_state(TaskState::Failed { error: msg });
                }
            }
        });

        let handle = TaskHandle {
            id,
            spec: spec.clone(),
            status: status_rx,
            cancel,
            join,
        };

        self.tasks.write().await.insert(id, handle);
        Ok((id, spec))
    }

    /// Get the status of a task by ID.
    pub async fn get_status(&self, task_id: u64) -> Option<TaskStatusResponse> {
        let tasks = self.tasks.read().await;
        let handle = tasks.get(&task_id)?;
        Some((*handle.status.borrow()).clone())
    }

    /// Get a cloned watch receiver for a task's status stream.
    /// Returns `None` if the task doesn't exist.
    pub async fn watch_status(&self, task_id: u64) -> Option<watch::Receiver<TaskStatusResponse>> {
        let tasks = self.tasks.read().await;
        let handle = tasks.get(&task_id)?;
        Some(handle.status.clone())
    }

    /// Cancel a running task. Returns true if the task was found.
    pub async fn cancel(&self, task_id: u64) -> bool {
        let tasks = self.tasks.read().await;
        if let Some(handle) = tasks.get(&task_id) {
            handle.cancel.cancel();
            true
        } else {
            false
        }
    }

    /// List all tasks (running and completed).
    pub async fn list(&self) -> Vec<TaskStatusResponse> {
        let tasks = self.tasks.read().await;
        let mut out = Vec::with_capacity(tasks.len());
        for handle in tasks.values() {
            out.push((*handle.status.borrow()).clone());
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Task dispatch
// ---------------------------------------------------------------------------

/// Dispatch a task spec to the appropriate handler.
async fn run_task(
    ctx: Arc<TaskExecutorContext>,
    spec: &TaskSpec,
    reporter: TaskReporter,
    cancel: CancellationToken,
) -> anyhow::Result<bool> {
    match spec {
        TaskSpec::Ingest {
            vault,
            source,
            blob_store,
            target_path,
        } => {
            let was_cancelled = ingest::run_ingest(
                &ctx,
                vault,
                source,
                blob_store,
                target_path.as_deref(),
                reporter,
                cancel,
            )
            .await?;
            Ok(was_cancelled)
        }
        TaskSpec::Restore {
            vault,
            target_path,
            blob_store,
        } => {
            restore::run_restore(
                &ctx,
                vault,
                target_path,
                blob_store.as_deref(),
                reporter,
                cancel,
            )
            .await?;
            Ok(false)
        }
        TaskSpec::Backup {
            vault,
            source,
            blob_store,
            keys,
            target_path,
        } => {
            // Backup = Ingest + Publish
            let was_cancelled = ingest::run_ingest(
                &ctx,
                vault,
                source,
                blob_store,
                target_path.as_deref(),
                reporter.clone(),
                cancel.clone(),
            )
            .await?;

            // Skip publishing if cancelled — we saved partial state to inprogress for resume
            if was_cancelled {
                tracing::info!("backup was cancelled, skipping publish");
                return Ok(true);
            }

            // Publish the snapshot to registry (if registry is available)
            publish::run_publish(&ctx, vault, keys).await?;

            Ok(false)
        }
        TaskSpec::Publish { vault, keys } => {
            publish::run_publish(&ctx, vault, keys).await?;
            Ok(false)
        }
        TaskSpec::RemoteRestore {
            vault,
            age_secret_key,
            blob_store,
            target_path,
        } => {
            restore::run_remote_restore(
                &ctx,
                age_secret_key,
                vault,
                blob_store,
                target_path,
                reporter,
                cancel,
            )
            .await?;
            Ok(false)
        }
    }
}

// ---------------------------------------------------------------------------
// Config resolution helpers
// ---------------------------------------------------------------------------

/// Resolve a vault name to its config.
pub(crate) fn resolve_vault<'a>(
    config: &'a S5NodeConfig,
    name: &str,
) -> anyhow::Result<&'a NodeConfigVault> {
    config
        .vault
        .get(name)
        .ok_or_else(|| anyhow!("vault '{}' not found in config", name))
}

/// Resolve a source name to its config.
pub(crate) fn resolve_source<'a>(
    config: &'a S5NodeConfig,
    name: &str,
) -> anyhow::Result<&'a NodeConfigSource> {
    config
        .source
        .get(name)
        .ok_or_else(|| anyhow!("source '{}' not found in config", name))
}

/// Resolve a store name to a `BlobStore`.
pub(crate) fn resolve_store<'a>(
    stores: &'a HashMap<String, BlobStore>,
    name: &str,
) -> anyhow::Result<&'a BlobStore> {
    stores.get(name).ok_or_else(|| {
        anyhow!(
            "store '{}' not found (not configured or is a local_links store)",
            name
        )
    })
}

/// Resolve a key name to its config.
#[allow(dead_code)]
pub(crate) fn resolve_key<'a>(
    config: &'a S5NodeConfig,
    name: &str,
) -> anyhow::Result<&'a NodeConfigKey> {
    config
        .key
        .get(name)
        .ok_or_else(|| anyhow!("key '{}' not found in config", name))
}

/// Resolve a vault's key to its recipient public keys and identity files.
///
/// Looks up the vault's `key` field in config, then resolves the key config
/// to get the `public_key` (recipient) and optional `identity_file`.
/// Returns `(recipients, identity_files)` for use with age encrypt/decrypt.
pub(crate) fn resolve_vault_key_info(
    config: &S5NodeConfig,
    vault_name: &str,
) -> anyhow::Result<(Vec<String>, Vec<String>)> {
    let vault = resolve_vault(config, vault_name)?;
    let key_config = resolve_key(config, &vault.key)
        .with_context(|| format!("resolving key for vault '{vault_name}'"))?;

    let recipients = vec![key_config.public_key.clone()];
    let identity_files = key_config.identity_file.iter().cloned().collect::<Vec<_>>();

    Ok((recipients, identity_files))
}

/// Build a meta store path from a vault's root_path.
///
/// The meta store lives at `{vault.root_path}/meta/` and holds prolly tree
/// nodes. This is always a local store.
pub(crate) fn vault_meta_store_path(vault: &NodeConfigVault) -> PathBuf {
    PathBuf::from(&vault.root_path).join("meta")
}
