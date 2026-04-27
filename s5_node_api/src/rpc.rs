//! RPC protocol types for communicating with an S5 node.
//!
//! Follows the irpc `#[rpc_requests]` pattern. The macro generates
//! `S5NodeMessage` (the enum with attached channels) and implements
//! `irpc::Service` on `S5NodeProto`.

use irpc::channel::{mpsc, oneshot};
use irpc::rpc_requests;
use serde::{Deserialize, Serialize};

/// RPC protocol definition for an S5 node.
#[derive(Debug, Serialize, Deserialize)]
#[rpc_requests(message = S5NodeMessage)]
pub enum S5NodeProto {
    /// Run a named task from the node's config.
    #[rpc(tx = oneshot::Sender<RunTaskResponse>)]
    RunTask(RunTask),

    /// Get status of a running or completed task.
    #[rpc(tx = oneshot::Sender<TaskStatusResponse>)]
    GetTaskStatus(GetTaskStatus),

    /// Stream status updates for a task until it reaches a terminal state.
    #[rpc(tx = mpsc::Sender<TaskStatusResponse>)]
    WatchTaskStatus(WatchTaskStatus),

    /// Cancel a running task.
    #[rpc(tx = oneshot::Sender<CancelTaskResponse>)]
    CancelTask(CancelTask),

    /// List all tasks (running + recently completed).
    #[rpc(tx = oneshot::Sender<ListTasksResponse>)]
    ListTasks(ListTasks),

    /// Get the node's current configuration as JSON.
    #[rpc(tx = oneshot::Sender<GetConfigResponse>)]
    GetConfig(GetConfig),

    /// Apply a JSON Patch (RFC 6902) to the node's configuration.
    #[rpc(tx = oneshot::Sender<PatchConfigResponse>)]
    PatchConfig(PatchConfig),

    /// Get a high-level node status summary.
    #[rpc(tx = oneshot::Sender<GetStatusResponse>)]
    GetStatus(GetStatus),

    /// List vault snapshots.
    #[rpc(tx = oneshot::Sender<ListSnapshotsResponse>)]
    ListSnapshots(ListSnapshots),

    /// Graceful shutdown.
    #[rpc(tx = oneshot::Sender<()>)]
    Shutdown(Shutdown),
}

// ── Request types ──────────────────────────────────────────────────

/// Run a task. Either by name (looked up in config) or with an inline spec.
#[derive(Debug, Serialize, Deserialize)]
pub struct RunTask {
    /// Task name from `[task.*]` in config. Mutually exclusive with `spec_json`.
    pub name: Option<String>,
    /// Inline task spec as JSON string (postcard can't handle internally-tagged enums).
    /// Mutually exclusive with `name`.
    pub spec_json: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetTaskStatus {
    pub task_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WatchTaskStatus {
    pub task_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CancelTask {
    pub task_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListTasks;

#[derive(Debug, Serialize, Deserialize)]
pub struct Shutdown;

// ── Response types ─────────────────────────────────────────────────

/// Outcome of a `RunTask` RPC. Tagged so the daemon can communicate
/// dispatch failures (vault not in config, invalid spec, executor
/// refusal, …) without the previous `task_id == 0` sentinel that
/// callers had to know to special-case. The CLI client flattens this
/// into `Result<SpawnedTask>` at the seam (see `S5NodeClient::run_task`).
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum RunTaskResponse {
    /// Task spawned successfully.
    Spawned {
        /// Unique task ID for status/cancel.
        task_id: u64,
        /// The resolved task spec as JSON string.
        spec_json: String,
    },
    /// The daemon refused to spawn the task. `error` carries the
    /// human-readable reason (already includes the originating
    /// `vault`/`task` name where relevant).
    Refused { error: String },
}

/// Flattened success payload of a `RunTask` RPC. Returned by
/// `S5NodeClient::run_task` / `run_task_by_name` so callers don't have
/// to match on the `RunTaskResponse` enum themselves; refusals come
/// back as a real `Err` instead.
#[derive(Debug, Clone)]
pub struct SpawnedTask {
    pub task_id: u64,
    pub spec_json: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskStatusResponse {
    pub task_id: u64,
    pub state: TaskState,
    /// Task progress states. Keys are state names (e.g. "bytes", "files_added").
    pub progress: Option<TaskProgressMap>,
}

/// Current state of a task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskState {
    /// Task is queued but not yet started.
    Pending,
    /// Task is actively running.
    Running,
    /// Task completed successfully.
    Completed,
    /// Task failed with an error message.
    Failed { error: String },
    /// Task was cancelled.
    Cancelled,
}

/// Type of a progress metric.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressType {
    Count,
    Bytes,
}

/// A single progress metric for a task.
///
/// Each metric has a label (used as map key), a type (count or bytes),
/// an optional total (None for counters like "skipped" that have no natural bound),
/// a current progress value, and a complete flag.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ProgressState {
    /// Machine-readable key (e.g., "bytes", "files_added", "files_skipped").
    pub label: String,
    /// Human-readable display label (e.g., "bytes uploaded", "files added").
    /// Falls back to `label` if not set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_label: Option<String>,
    /// Whether this metric is in bytes or a plain count.
    #[serde(rename = "type")]
    pub progress_type: ProgressType,
    /// Total expected value, or None if no natural bound.
    pub total: Option<u64>,
    /// Current value.
    pub progress: u64,
    /// Whether this metric has reached a terminal state.
    pub complete: bool,
}

impl ProgressState {
    pub fn new(label: String, progress_type: ProgressType, total: Option<u64>) -> Self {
        Self {
            label,
            display_label: None,
            progress_type,
            total,
            progress: 0,
            complete: false,
        }
    }

    pub fn add(&mut self, n: u64) {
        self.progress += n;
        if let Some(total) = self.total {
            self.complete = self.progress >= total;
        }
    }

    /// Set a human-readable display label.
    pub fn set_display_label(&mut self, display_label: &str) {
        self.display_label = Some(display_label.into());
    }

    /// Return the display label, falling back to the key label.
    pub fn display_label(&self) -> &str {
        self.display_label.as_deref().unwrap_or(&self.label)
    }
}

/// An ordered list of progress states (insertion order = display order).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskProgressMap(pub Vec<ProgressState>);

impl TaskProgressMap {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Add a bytes progress state.
    pub fn bytes(&mut self, label: &str, progress: u64, total: Option<u64>) -> &mut ProgressState {
        self.0.push(ProgressState {
            label: label.into(),
            display_label: None,
            progress_type: ProgressType::Bytes,
            total,
            progress,
            complete: total.is_some_and(|t| progress >= t),
        });
        self.0.last_mut().unwrap()
    }

    /// Add a count progress state.
    pub fn count(&mut self, label: &str, progress: u64, total: Option<u64>) -> &mut ProgressState {
        self.0.push(ProgressState {
            label: label.into(),
            display_label: None,
            progress_type: ProgressType::Count,
            total,
            progress,
            complete: total.is_some_and(|t| progress >= t),
        });
        self.0.last_mut().unwrap()
    }

    /// Get a state by label.
    pub fn get(&self, label: &str) -> Option<&ProgressState> {
        self.0.iter().find(|s| s.label == label)
    }

    /// Get a mutable state by label.
    pub fn get_mut(&mut self, label: &str) -> Option<&mut ProgressState> {
        self.0.iter_mut().find(|s| s.label == label)
    }

    /// Iterate over states.
    pub fn iter(&self) -> impl Iterator<Item = &ProgressState> {
        self.0.iter()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CancelTaskResponse {
    pub ok: bool,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListTasksResponse {
    pub tasks: Vec<TaskStatusResponse>,
}

// ── Config management ─────────────────────────────────────────────

/// Request the node's current configuration as a JSON value.
#[derive(Debug, Serialize, Deserialize)]
pub struct GetConfig;

#[derive(Debug, Serialize, Deserialize)]
pub struct GetConfigResponse {
    /// The full config as a JSON string (serialized from S5NodeConfig).
    /// Postcard cannot serialize serde_json::Value, so we use a String.
    pub config_json: String,
}

/// Apply an RFC 6902 JSON Patch to the node's running configuration.
///
/// The node will:
/// 1. Serialize current config → JSON
/// 2. Apply the patch operations
/// 3. Deserialize back to S5NodeConfig
/// 4. Validate cross-references
/// 5. Persist to TOML on disk
/// 6. Hot-reload the in-memory config
#[derive(Debug, Serialize, Deserialize)]
pub struct PatchConfig {
    /// RFC 6902 JSON Patch operations as a JSON string.
    /// Postcard cannot serialize serde_json::Value, so we use a String.
    pub patch_json: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PatchConfigResponse {
    /// Whether the patch was applied successfully.
    pub ok: bool,
    /// Human-readable message (error details on failure).
    pub message: String,
    /// The config after patching as a JSON string (only present on success).
    pub config_json: Option<String>,
}

// ── Node status ───────────────────────────────────────────────────

/// Request a high-level node status summary.
#[derive(Debug, Serialize, Deserialize)]
pub struct GetStatus;

#[derive(Debug, Serialize, Deserialize)]
pub struct GetStatusResponse {
    /// Number of configured stores.
    pub store_count: usize,
    /// Number of configured vaults.
    pub vault_count: usize,
    /// Number of configured sources.
    pub source_count: usize,
    /// Currently running tasks.
    pub running_tasks: usize,
    /// Endpoint ID of the node.
    pub endpoint_id: String,
}

// ── Snapshots ─────────────────────────────────────────────────────

/// List snapshots for a vault.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListSnapshots {
    /// Vault name. If None, list snapshots from all vaults.
    pub vault: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListSnapshotsResponse {
    pub snapshots: Vec<SnapshotInfo>,
}

/// Summary information about a single vault snapshot.
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapshotInfo {
    /// Vault this snapshot belongs to.
    pub vault: String,
    /// Snapshot hash/ID (hex-encoded).
    pub hash: String,
    /// ISO 8601 UTC timestamp (e.g. `2025-06-15T12:34:56Z`), or `"current"` for
    /// the latest published snapshot.
    pub timestamp: String,
    /// Total number of files in the snapshot (if known).
    pub file_count: Option<u64>,
    /// Total size in bytes (if known).
    pub total_bytes: Option<u64>,
}
