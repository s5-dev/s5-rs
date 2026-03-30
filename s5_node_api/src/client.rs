//! Thin client wrapper for the S5 node RPC protocol.

use anyhow::{Context, Result};

use crate::config::TaskSpec;
use crate::rpc::*;

/// Client for communicating with an S5 node.
#[derive(Debug, Clone)]
pub struct S5NodeClient {
    inner: irpc::Client<S5NodeProto>,
}

impl S5NodeClient {
    /// Create a client from a raw irpc client.
    pub fn new(inner: irpc::Client<S5NodeProto>) -> Self {
        Self { inner }
    }

    /// Access the underlying irpc client.
    pub fn inner(&self) -> &irpc::Client<S5NodeProto> {
        &self.inner
    }

    /// Run a task by name (looked up in node config).
    pub async fn run_task_by_name(&self, name: impl Into<String>) -> Result<RunTaskResponse> {
        self.inner
            .rpc(RunTask {
                name: Some(name.into()),
                spec: None,
            })
            .await
            .context("run_task RPC failed")
    }

    /// Run a task with an inline spec.
    pub async fn run_task(&self, spec: TaskSpec) -> Result<RunTaskResponse> {
        self.inner
            .rpc(RunTask {
                name: None,
                spec: Some(spec),
            })
            .await
            .context("run_task RPC failed")
    }

    /// Get status of a task.
    pub async fn get_task_status(&self, task_id: u64) -> Result<TaskStatusResponse> {
        self.inner
            .rpc(GetTaskStatus { task_id })
            .await
            .context("get_task_status RPC failed")
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
        self.inner
            .rpc(PatchConfig { patch })
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
}
