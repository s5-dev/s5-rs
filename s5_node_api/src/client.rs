//! Thin client wrapper for the S5 node RPC protocol.

use anyhow::{Context, Result, anyhow};

use crate::config::TaskSpec;
use crate::rpc::*;

/// Client for communicating with an S5 node.
#[derive(Debug, Clone)]
pub struct S5NodeClient {
    inner: irpc::Client<S5NodeProto>,
    /// Kept alive so we can close it gracefully on drop.
    endpoint: Option<iroh::Endpoint>,
}

impl S5NodeClient {
    /// Create a client from a raw irpc client (no endpoint to manage).
    pub fn new(inner: irpc::Client<S5NodeProto>, endpoint: iroh::Endpoint) -> Self {
        Self {
            inner,
            endpoint: Some(endpoint),
        }
    }

    /// Access the underlying irpc client.
    pub fn inner(&self) -> &irpc::Client<S5NodeProto> {
        &self.inner
    }

    /// Run a task by name (looked up in node config).
    /// Daemon-side refusals (task missing, executor full, …) come back
    /// as `Err`, not as a sentinel response — callers don't have to
    /// special-case `task_id == 0` any more.
    pub async fn run_task_by_name(&self, name: impl Into<String>) -> Result<SpawnedTask> {
        let resp: RunTaskResponse = self
            .inner
            .rpc(RunTask {
                name: Some(name.into()),
                spec_json: None,
            })
            .await
            .context("run_task RPC failed")?;
        spawned_or_err(resp)
    }

    /// Run a task with an inline spec. See `run_task_by_name` for
    /// error-handling semantics.
    pub async fn run_task(&self, spec: TaskSpec) -> Result<SpawnedTask> {
        let spec_json = serde_json::to_string(&spec).context("failed to serialize task spec")?;
        let resp: RunTaskResponse = self
            .inner
            .rpc(RunTask {
                name: None,
                spec_json: Some(spec_json),
            })
            .await
            .context("run_task RPC failed")?;
        spawned_or_err(resp)
    }

    /// Get status of a task.
    pub async fn get_task_status(&self, task_id: u64) -> Result<TaskStatusResponse> {
        self.inner
            .rpc(GetTaskStatus { task_id })
            .await
            .context("get_task_status RPC failed")
    }

    /// Stream status updates for a task until it reaches a terminal state.
    ///
    /// Returns a receiver that yields `TaskStatusResponse` messages as the task
    /// progresses. The stream ends when the task completes, fails, or is cancelled.
    pub async fn watch_task_status(
        &self,
        task_id: u64,
    ) -> Result<irpc::channel::mpsc::Receiver<TaskStatusResponse>> {
        self.inner
            .server_streaming(WatchTaskStatus { task_id }, 4)
            .await
            .context("watch_task_status RPC failed")
    }

    /// Cancel a running task.
    pub async fn cancel_task(&self, task_id: u64) -> Result<CancelTaskResponse> {
        self.inner
            .rpc(CancelTask { task_id })
            .await
            .context("cancel_task RPC failed")
    }

    /// List all tasks.
    pub async fn list_tasks(&self) -> Result<ListTasksResponse> {
        self.inner
            .rpc(ListTasks)
            .await
            .context("list_tasks RPC failed")
    }

    /// Shut down the node.
    pub async fn shutdown(&self) -> Result<()> {
        self.inner
            .rpc(Shutdown)
            .await
            .context("shutdown RPC failed")
    }

    /// Get the node's current configuration as JSON.
    pub async fn get_config(&self) -> Result<GetConfigResponse> {
        self.inner
            .rpc(GetConfig)
            .await
            .context("get_config RPC failed")
    }

    /// Apply an RFC 6902 JSON Patch to the node's configuration.
    pub async fn patch_config(&self, patch: serde_json::Value) -> Result<PatchConfigResponse> {
        let patch_json =
            serde_json::to_string(&patch).context("failed to serialize patch to JSON string")?;
        self.inner
            .rpc(PatchConfig { patch_json })
            .await
            .context("patch_config RPC failed")
    }

    /// Get a high-level node status summary.
    pub async fn get_status(&self) -> Result<GetStatusResponse> {
        self.inner
            .rpc(GetStatus)
            .await
            .context("get_status RPC failed")
    }

    /// List vault snapshots.
    pub async fn list_snapshots(&self, vault: Option<String>) -> Result<ListSnapshotsResponse> {
        self.inner
            .rpc(ListSnapshots { vault })
            .await
            .context("list_snapshots RPC failed")
    }

    /// Mount a vault on the daemon as a FUSE filesystem. Returns a
    /// `MountedVault` carrying the handle the daemon expects on
    /// `unmount_vault`. Daemon-side preflight failures (vault missing,
    /// `/dev/fuse` absent, mount point doesn't exist, etc.) come back
    /// as `Err`, not as a sentinel response.
    pub async fn mount_vault(
        &self,
        vault: impl Into<String>,
        mountpoint: std::path::PathBuf,
        rw: bool,
        debounce_ms: u64,
    ) -> Result<MountedVault> {
        let resp: MountVaultResponse = self
            .inner
            .rpc(MountVault {
                vault: vault.into(),
                mountpoint,
                rw,
                debounce_ms,
            })
            .await
            .context("mount_vault RPC failed")?;
        match resp {
            MountVaultResponse::Mounted { mount_id } => Ok(MountedVault { mount_id }),
            MountVaultResponse::Refused { error } => Err(anyhow!(error)),
        }
    }

    /// Unmount a vault previously mounted via `mount_vault`. Drops the
    /// daemon-side `MountHandle` (which performs the actual FUSE
    /// unmount) and tears down any attached rw debounce loop.
    pub async fn unmount_vault(&self, mount_id: u64) -> Result<()> {
        let resp: UnmountVaultResponse = self
            .inner
            .rpc(UnmountVault { mount_id })
            .await
            .context("unmount_vault RPC failed")?;
        match resp {
            UnmountVaultResponse::Ok => Ok(()),
            UnmountVaultResponse::Err { error } => Err(anyhow!(error)),
        }
    }

    /// Build a frozen-anonymous share URL for a vault snapshot.
    /// Daemon-side errors come back as `Err`, not as a sentinel response.
    pub async fn export_vault(
        &self,
        vault: impl Into<String>,
        path: Option<String>,
    ) -> Result<ExportedShare> {
        let resp: ExportVaultResponse = self
            .inner
            .rpc(ExportVault {
                vault: vault.into(),
                path,
            })
            .await
            .context("export_vault RPC failed")?;
        match resp {
            ExportVaultResponse::Ok { url, blob_hash_hex } => {
                Ok(ExportedShare { url, blob_hash_hex })
            }
            ExportVaultResponse::Err { error } => Err(anyhow!(error)),
        }
    }

    /// Gracefully close the underlying iroh endpoint.
    ///
    /// Call this before dropping the client to avoid the
    /// "Endpoint dropped without calling close" warning.
    pub async fn close(&self) {
        if let Some(ref endpoint) = self.endpoint {
            endpoint.close().await;
        }
    }
}

/// Convert a `RunTaskResponse` into a flattened `Result<SpawnedTask>`,
/// used by both `run_task` and `run_task_by_name` so the matching
/// logic lives in exactly one place.
fn spawned_or_err(resp: RunTaskResponse) -> Result<SpawnedTask> {
    match resp {
        RunTaskResponse::Spawned { task_id, spec_json } => Ok(SpawnedTask { task_id, spec_json }),
        RunTaskResponse::Refused { error } => Err(anyhow!(error)),
    }
}

impl Drop for S5NodeClient {
    fn drop(&mut self) {
        // Best-effort: if the runtime is still alive, spawn a close task.
        // For a clean shutdown, call `client.close().await` before dropping.
        if let Some(endpoint) = self.endpoint.take()
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            handle.spawn(async move {
                endpoint.close().await;
            });
        }
    }
}
