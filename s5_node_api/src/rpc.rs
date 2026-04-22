//! RPC protocol types for communicating with an S5 node.
//!
//! Follows the irpc `#[rpc_requests]` pattern. The macro generates
//! `S5NodeMessage` (the enum with attached channels) and implements
//! `irpc::Service` on `S5NodeProto`.

use irpc::channel::oneshot;
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
pub struct CancelTask {
    pub task_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListTasks;

#[derive(Debug, Serialize, Deserialize)]
pub struct Shutdown;

// ── Response types ─────────────────────────────────────────────────

#[derive(Debug, Serialize, Deserialize)]
pub struct RunTaskResponse {
    /// Unique task ID for status/cancel.
    pub task_id: u64,
    /// The resolved task spec as JSON string.
    pub spec_json: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TaskStatusResponse {
    pub task_id: u64,
    pub state: TaskState,
    /// Task-type-specific progress counters as JSON string.
    pub progress_json: Option<String>,
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

/// Task-type-specific progress counters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TaskProgress {
    Ingest {
        files_scanned: u64,
        files_changed: u64,
        files_skipped: u64,
        files_errored: u64,
        bytes_uploaded: u64,
    },
    Restore {
        files_restored: u64,
        bytes_restored: u64,
    },
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
