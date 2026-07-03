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

    /// Create a client over a local (in-process) irpc transport — no
    /// iroh endpoint involved. Pair with `S5NodeServer::serve_local`
    /// (in the s5_node crate) on the server side to drive the same
    /// RPC surface as remote callers without an iroh round-trip.
    ///
    /// Used by an embedding host's `bg_persist` hook to fire the s5
    /// ingest task immediately after each persist cycle.
    pub fn local(inner: irpc::Client<S5NodeProto>) -> Self {
        Self {
            inner,
            endpoint: None,
        }
    }

    /// Access the underlying irpc client.
    pub fn inner(&self) -> &irpc::Client<S5NodeProto> {
        &self.inner
    }

    /// Run a task by name (looked up in node config).
    /// Daemon-side refusals (task missing, executor full, …) come back
    /// as `Err`.
    pub async fn run_task_by_name(&self, name: impl Into<String>) -> Result<SpawnedTask> {
        flatten_string_err(
            self.inner
                .rpc(RunTask {
                    name: Some(name.into()),
                    spec_json: None,
                })
                .await
                .context("run_task RPC failed")?,
        )
    }

    /// Run a task with an inline spec. See `run_task_by_name` for
    /// error-handling semantics.
    pub async fn run_task(&self, spec: TaskSpec) -> Result<SpawnedTask> {
        let spec_json = serde_json::to_string(&spec).context("failed to serialize task spec")?;
        flatten_string_err(
            self.inner
                .rpc(RunTask {
                    name: None,
                    spec_json: Some(spec_json),
                })
                .await
                .context("run_task RPC failed")?,
        )
    }

    /// Snapshot of one task's current status. Implemented as the
    /// first message of `watch_task_status` since `WatchTaskStatus`
    /// already replays the current state — no separate RPC needed.
    pub async fn get_task_status(&self, task_id: u64) -> Result<TaskStatusResponse> {
        let mut rx = self.watch_task_status(task_id).await?;
        match rx.recv().await {
            Ok(Some(status)) => Ok(status),
            Ok(None) => Err(anyhow!("task {task_id} not found")),
            Err(e) => Err(anyhow!("watch_task_status stream error: {e}")),
        }
    }

    /// Stream status updates for a task until it reaches a terminal state.
    ///
    /// First message = current state (so a single `recv` gives a
    /// snapshot equivalent to the old `get_task_status`); subsequent
    /// messages follow state changes. The stream ends when the task
    /// completes, fails, or is cancelled.
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
    pub async fn cancel_task(&self, task_id: u64) -> Result<()> {
        flatten_string_err(
            self.inner
                .rpc(CancelTask { task_id })
                .await
                .context("cancel_task RPC failed")?,
        )
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
    /// Returns the patched config serialised as a JSON string on
    /// success.
    pub async fn patch_config(&self, patch: serde_json::Value) -> Result<String> {
        let patch_json =
            serde_json::to_string(&patch).context("failed to serialize patch to JSON string")?;
        flatten_string_err(
            self.inner
                .rpc(PatchConfig { patch_json })
                .await
                .context("patch_config RPC failed")?,
        )
    }

    /// Get a high-level node status summary.
    pub async fn get_status(&self) -> Result<GetStatusResponse> {
        self.inner
            .rpc(GetStatus)
            .await
            .context("get_status RPC failed")
    }

    /// Walk the node's health signals: per-store reachability + staging
    /// gauges + configured schedules (`vup doctor` / `vup status`).
    pub async fn get_health(&self) -> Result<GetHealthResponse> {
        self.inner
            .rpc(GetHealth)
            .await
            .context("get_health RPC failed")
    }

    /// List vault snapshots.
    pub async fn list_snapshots(&self, vault: Option<String>) -> Result<ListSnapshotsResponse> {
        self.inner
            .rpc(ListSnapshots { vault })
            .await
            .context("list_snapshots RPC failed")
    }

    /// List a vault's tree contents, optionally at a past `#snap` and/or
    /// scoped to a `subtree`, depth-bounded by `max_depth`. Daemon-side
    /// failures (unknown vault, bad snapshot selector, missing subtree)
    /// come through as `Err`.
    pub async fn list_tree(
        &self,
        vault: impl Into<String>,
        snapshot: Option<String>,
        subtree: Option<String>,
        max_depth: Option<u32>,
    ) -> Result<ListTreeResponse> {
        flatten_string_err(
            self.inner
                .rpc(ListTree {
                    vault: vault.into(),
                    snapshot,
                    subtree,
                    max_depth,
                })
                .await
                .context("list_tree RPC failed")?,
        )
    }

    /// Mount a vault on the daemon as a FUSE filesystem. Returns the
    /// mount handle the daemon expects back on `unmount_vault`.
    /// Daemon-side preflight failures come through as `Err`.
    pub async fn mount_vault(
        &self,
        vault: impl Into<String>,
        mountpoint: std::path::PathBuf,
        rw: bool,
        debounce_ms: u64,
    ) -> Result<MountedVault> {
        flatten_string_err(
            self.inner
                .rpc(MountVault {
                    vault: vault.into(),
                    mountpoint,
                    rw,
                    debounce_ms,
                })
                .await
                .context("mount_vault RPC failed")?,
        )
    }

    /// Unmount a vault previously mounted via `mount_vault`. Drops the
    /// daemon-side `MountHandle` (which performs the actual FUSE
    /// unmount) and tears down any attached rw debounce loop.
    pub async fn unmount_vault(&self, mount_id: u64) -> Result<()> {
        flatten_string_err(
            self.inner
                .rpc(UnmountVault { mount_id })
                .await
                .context("unmount_vault RPC failed")?,
        )
    }

    /// Build a frozen-anonymous share URL for a vault snapshot.
    pub async fn export_vault(
        &self,
        vault: impl Into<String>,
        path: Option<String>,
    ) -> Result<ExportedShare> {
        flatten_string_err(
            self.inner
                .rpc(ExportVault {
                    vault: vault.into(),
                    path,
                })
                .await
                .context("export_vault RPC failed")?,
        )
    }

    /// Sender-side pair handshake. Returns a stream that yields
    /// exactly one `Minted { token }` event followed by either a
    /// `Redeemed { peer_did }` or `Failed { error }` event, then
    /// closes. The CLI prints the token and prompts for a petname
    /// once `Redeemed` arrives.
    pub async fn pair(&self) -> Result<irpc::channel::mpsc::Receiver<PairEvent>> {
        self.inner
            .server_streaming(Pair, 2)
            .await
            .context("pair RPC failed")
    }

    /// Receiver-side: parse `token`, dial the sender's iroh
    /// endpoint over `s5/pair/0`, present the secret, and return
    /// the sender's DID on success.
    pub async fn redeem_pair(&self, token: impl Into<String>) -> Result<String> {
        flatten_string_err(
            self.inner
                .rpc(RedeemPair {
                    token: token.into(),
                })
                .await
                .context("redeem_pair RPC failed")?,
        )
    }

    /// Inviter-side device enrollment (D10). Returns a stream that
    /// yields one `Minted { token }` event followed by either
    /// `Admitted { label, .. }` or `Failed { error }`, then closes.
    pub async fn device_invite(
        &self,
        label: Option<String>,
    ) -> Result<irpc::channel::mpsc::Receiver<DeviceInviteEvent>> {
        self.inner
            .server_streaming(DeviceInvite { label }, 2)
            .await
            .context("device_invite RPC failed")
    }

    /// Read the device catalogue (label → pubkeys; UI only).
    pub async fn list_devices(&self) -> Result<ListDevicesResponse> {
        flatten_string_err(
            self.inner
                .rpc(ListDevices)
                .await
                .context("list_devices RPC failed")?,
        )
    }

    /// Revoke a device by catalogue label (D18 routine removal): its
    /// four keys leave the identity bundle, its catalogue entry is
    /// removed, and the special vaults are re-wrapped to the survivors
    /// (+ paper). Refusals (unknown label, self-revoke) come back as
    /// `Err`.
    pub async fn revoke_device(&self, label: impl Into<String>) -> Result<RevokeDeviceResponse> {
        flatten_string_err(
            self.inner
                .rpc(RevokeDevice {
                    label: label.into(),
                })
                .await
                .context("revoke_device RPC failed")?,
        )
    }

    /// Persist a `[friend.<petname>]` entry with the supplied DID.
    /// Idempotent on identical pairings; refuses petname collisions
    /// with a different DID.
    pub async fn add_friend(
        &self,
        petname: impl Into<String>,
        did: impl Into<String>,
    ) -> Result<()> {
        flatten_string_err(
            self.inner
                .rpc(AddFriend {
                    petname: petname.into(),
                    did: did.into(),
                })
                .await
                .context("add_friend RPC failed")?,
        )
    }

    /// Append `@petname` to `vault.<vault>.members`, persist, and
    /// fire a membership refresh.
    /// Consume an `s5://export/…` share URL; returns the joined vault label.
    pub async fn join_export(&self, url: impl Into<String>) -> Result<String> {
        flatten_string_err(
            self.inner
                .rpc(JoinExport { url: url.into() })
                .await
                .context("join_export RPC failed")?,
        )
    }

    pub async fn grant_vault(
        &self,
        vault: impl Into<String>,
        petname: impl Into<String>,
        write: bool,
    ) -> Result<()> {
        flatten_string_err(
            self.inner
                .rpc(GrantVault {
                    vault: vault.into(),
                    petname: petname.into(),
                    write,
                })
                .await
                .context("grant_vault RPC failed")?,
        )
    }

    /// Snapshot of per-peer connection observation — every iroh
    /// pubkey this daemon has handshaked with, with per-ALPN
    /// counts and timestamps. Powers `vup debug peers`.
    pub async fn debug_peers(&self) -> Result<DebugPeersResponse> {
        self.inner
            .rpc(DebugPeers)
            .await
            .context("debug_peers RPC failed")
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

/// Lift a daemon-returned `Result<T, String>` into the client's
/// `anyhow::Result<T>` so callers don't have to match on a String
/// error variant themselves. Used by every RPC that returns
/// `Result<_, String>` on the wire.
fn flatten_string_err<T>(resp: std::result::Result<T, String>) -> Result<T> {
    resp.map_err(|e| anyhow!(e))
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
