//! Task executor for s5_node.
//!
//! Tasks are ephemeral units of work (ingest, restore, publish, backup)
//! submitted at runtime via RPC. The executor spawns each task as a tokio
//! task, tracks its state and progress, and supports cancellation.

pub mod cold_gc;
pub mod copy;
pub mod ingest;
pub mod list;
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
use s5_core::blob::{BlobStore, Blobs};
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
    /// Resolved blob stores (from `[store.*]` config). The vault-facing
    /// `dyn Blobs` interface (read + write + delete by hash), so a content-
    /// addressed backend like the Sia `PackingStore` plugs in directly
    /// alongside `BlobStore`-wrapped path stores (local/S3) with no wrapper.
    pub stores: HashMap<String, Arc<dyn Blobs>>,
    /// Node secret (32 bytes) — used for key derivation (vault passphrase, signing keys).
    /// Derived from the iroh secret key via blake3.
    pub node_secret: [u8; 32],
    /// Registry for snapshot publishing (None if not configured).
    pub registry: Option<Arc<dyn RegistryApi + Send + Sync>>,
    /// Resolved membership state — read by `publish` to derive vault
    /// recipients from each member's `DidDocument.keyAgreement`.
    /// Optional for test harnesses that build the context without
    /// resolving an identity layer.
    pub membership: Option<Arc<RwLock<crate::membership::MembershipState>>>,
    /// Notify the daemon's membership coordinator wakes on. Fired by
    /// `publish` after registering a new `vault_id` so the per-peer
    /// subscriber bounces and picks up shared-vault data keys.
    /// `None` in test harnesses.
    pub membership_refresh: Option<Arc<tokio::sync::Notify>>,
    /// The identity-wide discovery `seed` from the `config` vault, populated
    /// once the bootstrap publish has read it. `publish` uses it to derive each
    /// vault's non-authoritative discovery keypair
    /// ([`crate::tasks::publish::discovery_signing_key`]) and mirror the current
    /// HEAD at `(discovery_pubkey, vault_id)` for paper/add-device recovery.
    /// Empty until then (and in test harnesses) — `publish` simply skips the
    /// mirror while unset.
    pub discovery_seed: Arc<std::sync::OnceLock<[u8; 32]>>,
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
                None,
            )
            .await?;
            Ok(was_cancelled)
        }
        TaskSpec::Restore {
            vault,
            target_path,
            blob_store,
            snapshot,
            subtree,
        } => {
            restore::run_restore(
                &ctx,
                vault,
                target_path,
                blob_store.as_deref(),
                snapshot.as_deref(),
                subtree.as_deref(),
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
            changed_paths,
        } => {
            // Backup = Ingest + Publish. `changed_paths = Some(..)` (set only
            // by the watch loop) runs the incremental path; `None` is a full
            // walk + deletion-detection reconcile.
            let was_cancelled = ingest::run_ingest(
                &ctx,
                vault,
                source,
                blob_store,
                target_path.as_deref(),
                reporter.clone(),
                cancel.clone(),
                changed_paths.as_deref(),
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
        TaskSpec::Copy {
            src_vault,
            src_path,
            src_snap,
            dst_vault,
            dst_path,
            blob_store,
            keys,
            deep,
            confirm_widen,
        } => {
            copy::run_copy(
                &ctx,
                src_vault,
                src_path.as_deref(),
                src_snap.as_deref(),
                dst_vault,
                dst_path.as_deref(),
                blob_store,
                keys,
                *deep,
                *confirm_widen,
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

/// Resolve a store name to its vault-facing `dyn Blobs` handle.
pub(crate) fn resolve_store<'a>(
    stores: &'a HashMap<String, Arc<dyn Blobs>>,
    name: &str,
) -> anyhow::Result<&'a Arc<dyn Blobs>> {
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

/// Path to the meta store directory for this vault.
///
/// The meta store lives at `{vault.root_path}/meta/` and holds prolly tree
/// nodes. Always local — verified streaming over iroh fetches *content*
/// blobs, not vault metadata.
pub(crate) fn vault_meta_store_path(vault: &NodeConfigVault) -> PathBuf {
    PathBuf::from(&vault.root_path).join("meta")
}

/// Open (or create) the local meta store for a vault.
///
/// Auto-detects the on-disk layout:
/// - If `{root_path}/meta/blob3/` exists → open as a `LocalStore`
///   (filesystem CAS layout — backwards-compat with vaults written by
///   older s5 versions before the fjall default landed).
/// - Otherwise → open as a `FjallStore` (LSM-tree with KV-separated
///   blob files; chosen because prolly tree nodes are 4–16 KiB and the
///   filesystem-CAS layout creates an inode per node, which dominates
///   `du`/walk cost on large vaults).
///
/// The returned `BlobStore` always has outboard writes disabled — meta
/// is local, no verified-streaming use case applies.
pub(crate) fn vault_meta_store_open(vault: &NodeConfigVault) -> anyhow::Result<BlobStore> {
    use s5_store_fjall::FjallStore;
    use s5_store_local::{LocalStore, LocalStoreConfig};

    let meta_path = vault_meta_store_path(vault);
    std::fs::create_dir_all(&meta_path)
        .with_context(|| format!("creating meta store at {}", meta_path.display()))?;

    let legacy_marker = meta_path.join("blob3");
    if legacy_marker.is_dir() {
        // Existing pre-fjall vault: keep the original layout so writes
        // and reads see the same blobs.
        let store = LocalStore::create(LocalStoreConfig {
            base_path: meta_path.to_string_lossy().into_owned(),
        });
        Ok(BlobStore::without_outboard(store))
    } else {
        // New (or already-fjall) vault: 256 MiB cache by default — meta
        // reads are repeatedly hot during change detection. The fjall
        // store creates its own files (`journals/`, `partitions/`, …)
        // directly under `meta_path`.
        let store = FjallStore::open(&meta_path)
            .with_context(|| format!("opening fjall meta store at {}", meta_path.display()))?;
        Ok(BlobStore::without_outboard(store))
    }
}

#[cfg(test)]
mod meta_store_tests {
    use super::*;
    use s5_node_api::config::NodeConfigVault;

    fn vault_at(root: &std::path::Path) -> NodeConfigVault {
        NodeConfigVault {
            root_path: root.to_string_lossy().into_owned(),
            key: "k".into(),
            data_store: None,
            preset: None,
            recipients: vec![],
            sources: vec![],
            meta_store: None,
            plaintext_tree: false,
            plaintext_published_tn: false,
            watch: false,
            members: vec![],
            pipelines: vec![],
            vault_id: None,
            ..Default::default()
        }
    }

    /// Empty vault root → fjall path. Verified via the directory layout
    /// fjall creates (no `blob3/` subdir, but a fjall manifest does land).
    #[test]
    fn meta_store_open_picks_fjall_for_fresh_vault() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let vault = vault_at(tmp.path());

        // First call creates the meta dir + fjall db.
        let _store = vault_meta_store_open(&vault).expect("open meta");

        let meta_dir = vault_meta_store_path(&vault);
        assert!(meta_dir.exists(), "meta dir should be created");
        assert!(
            !meta_dir.join("blob3").exists(),
            "fjall layout must not create blob3/"
        );
    }

    /// Pre-existing `meta/blob3/` (legacy LocalStore vault) → opens via
    /// LocalStore. We don't introspect the BlobStore (its inner store
    /// type is erased), but we verify the helper doesn't error and
    /// doesn't add fjall files alongside the LocalStore layout.
    #[test]
    fn meta_store_open_keeps_local_for_legacy_vault() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let vault = vault_at(tmp.path());

        // Pre-create the LocalStore CAS marker.
        let meta_dir = vault_meta_store_path(&vault);
        std::fs::create_dir_all(meta_dir.join("blob3")).expect("mkdir blob3");

        let _store = vault_meta_store_open(&vault).expect("open meta");

        // FjallStore::open() leaves a few well-known files; assert none of
        // them showed up — i.e. we picked the LocalStore branch.
        let entries: std::collections::HashSet<String> = std::fs::read_dir(&meta_dir)
            .expect("readdir")
            .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().into_owned()))
            .collect();
        assert!(entries.contains("blob3"), "blob3 marker preserved");
        assert!(
            !entries.contains("partitions") && !entries.contains("journals"),
            "fjall must not have created its files alongside an existing LocalStore vault"
        );
    }
}
