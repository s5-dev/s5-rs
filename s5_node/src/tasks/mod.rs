//! Task executor for s5_node.
//!
//! Tasks are ephemeral units of work (ingest, restore, publish, backup)
//! submitted at runtime via RPC. The executor spawns each task as a tokio
//! task, tracks its state and progress, and supports cancellation.

pub mod ingest;
pub mod publish;
pub mod restore;
pub mod vault_persist;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::anyhow;
use s5_core::RegistryApi;
use s5_core::blob::BlobStore;
use s5_node_api::config::{NodeConfigKey, NodeConfigSource, NodeConfigVault, TaskSpec};
use s5_node_api::{TaskProgressMap, TaskState, TaskStatusResponse};
use tokio::sync::RwLock;
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
// Task handle
// ---------------------------------------------------------------------------

/// A handle to a running or completed task.
#[allow(dead_code)]
struct TaskHandle {
    /// Monotonically increasing task ID.
    id: u64,
    /// The resolved spec this task is executing.
    spec: TaskSpec,
    /// Current task state.
    state: Arc<RwLock<TaskState>>,
    /// Optional progress counters (task-type-specific).
    progress: Arc<RwLock<Option<TaskProgressMap>>>,
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
        let state = Arc::new(RwLock::new(TaskState::Running));
        let progress: Arc<RwLock<Option<TaskProgressMap>>> = Arc::new(RwLock::new(None));

        // Clone what the spawned future needs.
        let ctx = self.ctx.clone();
        let spec_clone = spec.clone();
        let state_clone = state.clone();
        let progress_clone = progress.clone();
        let cancel_clone = cancel.clone();

        let join = tokio::spawn(async move {
            let result = run_task(ctx, &spec_clone, progress_clone.clone(), cancel_clone).await;
            let mut s = state_clone.write().await;
            match result {
                Ok(()) => *s = TaskState::Completed,
                Err(e) => {
                    // Check if it was a cancellation
                    let msg = format!("{e:#}");
                    if msg.contains("cancelled") {
                        *s = TaskState::Cancelled;
                    } else {
                        tracing::error!(task_id = id, error = %e, "task failed");
                        *s = TaskState::Failed { error: msg };
                    }
                }
            }
        });

        let handle = TaskHandle {
            id,
            spec: spec.clone(),
            state,
            progress,
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
        let state = handle.state.read().await.clone();
        let progress = handle.progress.read().await.clone();
        Some(TaskStatusResponse {
            task_id,
            state,
            progress,
        })
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
            let state = handle.state.read().await.clone();
            let progress = handle.progress.read().await.clone();
            out.push(TaskStatusResponse {
                task_id: handle.id,
                state,
                progress,
            });
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
    progress: Arc<RwLock<Option<TaskProgressMap>>>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    match spec {
        TaskSpec::Ingest {
            vault,
            source,
            blob_store,
            target_path,
        } => {
            let _was_cancelled = ingest::run_ingest(
                &ctx,
                vault,
                source,
                blob_store,
                target_path.as_deref(),
                progress,
                cancel,
            )
            .await?;
            Ok(())
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
                progress,
                cancel,
            )
            .await
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
                progress.clone(),
                cancel.clone(),
            )
            .await?;

            // Skip publishing if cancelled — we saved partial state to inprogress for resume
            if was_cancelled {
                tracing::info!("backup was cancelled, skipping publish");
                return Ok(());
            }

            // Publish the snapshot to registry (if registry is available)
            publish::run_publish(&ctx, vault, keys).await?;

            Ok(())
        }
        TaskSpec::Publish { vault, keys } => publish::run_publish(&ctx, vault, keys).await,
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
                progress,
                cancel,
            )
            .await
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

/// Build a meta store path from a vault's root_path.
///
/// The meta store lives at `{vault.root_path}/meta/` and holds prolly tree
/// nodes. This is always a local store.
pub(crate) fn vault_meta_store_path(vault: &NodeConfigVault) -> PathBuf {
    PathBuf::from(&vault.root_path).join("meta")
}
