//! S5 Node RPC server implementation.
//!
//! Accepts connections using the `s5/node/0` ALPN and dispatches incoming
//! messages to the [`TaskExecutor`] for task orchestration.

use std::path::PathBuf;
use std::sync::Arc;

use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use irpc::channel::oneshot;
use irpc_iroh::read_request;
use tokio::sync::RwLock;
use tokio::sync::oneshot as tokio_oneshot;
use tracing::info;

use s5_node_api::{
    CancelTask, CancelTaskResponse, GetConfig, GetConfigResponse, GetStatus, GetStatusResponse,
    GetTaskStatus, ListSnapshots, ListSnapshotsResponse, ListTasksResponse, PatchConfig,
    PatchConfigResponse, RunTask, RunTaskResponse, S5NodeMessage, S5NodeProto, SnapshotInfo,
    TaskState, TaskStatusResponse,
};

use crate::config::S5NodeConfig;
use crate::tasks::TaskExecutor;

/// The S5 Node RPC server.
///
/// Accepts connections using the `s5/node/0` ALPN, dispatching incoming
/// messages to the task executor, status queries, and shutdown handler.
#[derive(Clone)]
pub struct S5NodeServer {
    config: Arc<RwLock<S5NodeConfig>>,
    config_path: PathBuf,
    executor: Arc<TaskExecutor>,
    endpoint_id: String,
    /// Channel to signal shutdown to the node's run loop.
    shutdown_tx: Arc<RwLock<Option<tokio_oneshot::Sender<()>>>>,
}

impl std::fmt::Debug for S5NodeServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S5NodeServer").finish()
    }
}

impl S5NodeServer {
    /// Creates a new S5NodeServer with a task executor and shutdown channel.
    pub fn new(
        config: Arc<RwLock<S5NodeConfig>>,
        config_path: PathBuf,
        executor: Arc<TaskExecutor>,
        endpoint_id: String,
        shutdown_tx: tokio_oneshot::Sender<()>,
    ) -> Self {
        Self {
            config,
            config_path,
            executor,
            endpoint_id,
            shutdown_tx: Arc::new(RwLock::new(Some(shutdown_tx))),
        }
    }

    async fn handle_run_task(&self, req: RunTask) -> RunTaskResponse {
        let config = self.config.read().await;
        // Resolve task spec: either from config (by name) or inline.
        let spec = match (req.name, req.spec) {
            (Some(name), None) => {
                match config.task.get(&name) {
                    Some(tc) => tc.spec.clone(),
                    None => {
                        tracing::warn!(name = %name, "RunTask: task not found in config");
                        return RunTaskResponse {
                            task_id: 0,
                            spec: s5_node_api::config::TaskSpec::Ingest {
                                vault: String::new(),
                                source: String::new(),
                                blob_store: String::new(),
                                target_path: None,
                            },
                        };
                    }
                }
            }
            (None, Some(spec)) => spec,
            _ => {
                tracing::warn!("RunTask: must specify exactly one of `name` or `spec`");
                return RunTaskResponse {
                    task_id: 0,
                    spec: s5_node_api::config::TaskSpec::Ingest {
                        vault: String::new(),
                        source: String::new(),
                        blob_store: String::new(),
                        target_path: None,
                    },
                };
            }
        };
        drop(config);

        // Spawn the task on the executor.
        match self.executor.spawn(spec.clone()).await {
            Ok((task_id, resolved_spec)) => {
                tracing::info!(task_id, "task spawned");
                RunTaskResponse {
                    task_id,
                    spec: resolved_spec,
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to spawn task");
                RunTaskResponse { task_id: 0, spec }
            }
        }
    }

    async fn handle_get_task_status(&self, req: GetTaskStatus) -> TaskStatusResponse {
        match self.executor.get_status(req.task_id).await {
            Some(status) => status,
            None => TaskStatusResponse {
                task_id: req.task_id,
                state: TaskState::Failed {
                    error: format!("task {} not found", req.task_id),
                },
                progress: None,
            },
        }
    }

    async fn handle_cancel_task(&self, req: CancelTask) -> CancelTaskResponse {
        let ok = self.executor.cancel(req.task_id).await;
        CancelTaskResponse {
            ok,
            message: if ok {
                format!("task {} cancellation requested", req.task_id)
            } else {
                format!("task {} not found", req.task_id)
            },
        }
    }

    async fn handle_list_tasks(&self) -> ListTasksResponse {
        ListTasksResponse {
            tasks: self.executor.list().await,
        }
    }

    async fn handle_get_config(&self, _req: GetConfig) -> GetConfigResponse {
        let config = self.config.read().await;
        let value = match serde_json::to_value(&*config) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(error = %e, "failed to serialize config to JSON");
                serde_json::Value::Null
            }
        };
        GetConfigResponse { config: value }
    }

    async fn handle_patch_config(&self, req: PatchConfig) -> PatchConfigResponse {
        // Parse the patch operations.
        let patch: json_patch::Patch = match serde_json::from_value(req.patch) {
            Ok(p) => p,
            Err(e) => {
                return PatchConfigResponse {
                    ok: false,
                    message: format!("invalid JSON Patch: {e}"),
                    config: None,
                };
            }
        };

        let mut config = self.config.write().await;

        // 1. Serialize current config to JSON
        let mut value = match serde_json::to_value(&*config) {
            Ok(v) => v,
            Err(e) => {
                return PatchConfigResponse {
                    ok: false,
                    message: format!("failed to serialize current config: {e}"),
                    config: None,
                };
            }
        };

        // 2. Apply the patch
        if let Err(e) = json_patch::patch(&mut value, &patch) {
            return PatchConfigResponse {
                ok: false,
                message: format!("patch failed: {e}"),
                config: None,
            };
        }

        // 3. Deserialize back to S5NodeConfig
        let new_config: S5NodeConfig = match serde_json::from_value(value.clone()) {
            Ok(c) => c,
            Err(e) => {
                return PatchConfigResponse {
                    ok: false,
                    message: format!("patched config is invalid: {e}"),
                    config: None,
                };
            }
        };

        // 4. Validate cross-references
        let errors = new_config.validate();
        if !errors.is_empty() {
            return PatchConfigResponse {
                ok: false,
                message: format!("validation failed: {}", errors.join("; ")),
                config: None,
            };
        }

        // 5. Persist to TOML on disk
        let toml_str = match toml::to_string_pretty(&new_config) {
            Ok(s) => s,
            Err(e) => {
                return PatchConfigResponse {
                    ok: false,
                    message: format!("failed to serialize to TOML: {e}"),
                    config: None,
                };
            }
        };
        if let Err(e) = tokio::fs::write(&self.config_path, &toml_str).await {
            return PatchConfigResponse {
                ok: false,
                message: format!("failed to write config file: {e}"),
                config: None,
            };
        }

        // 6. Hot-reload in-memory config
        *config = new_config;

        tracing::info!(path = %self.config_path.display(), "config patched and persisted");

        PatchConfigResponse {
            ok: true,
            message: "config updated".into(),
            config: Some(value),
        }
    }

    async fn handle_get_status(&self, _req: GetStatus) -> GetStatusResponse {
        let config = self.config.read().await;
        let running_tasks = self
            .executor
            .list()
            .await
            .iter()
            .filter(|t| t.state == TaskState::Running || t.state == TaskState::Pending)
            .count();
        GetStatusResponse {
            store_count: config.store.len(),
            vault_count: config.vault.len(),
            source_count: config.source.len(),
            running_tasks,
            endpoint_id: self.endpoint_id.clone(),
        }
    }

    async fn handle_list_snapshots(&self, req: ListSnapshots) -> ListSnapshotsResponse {
        let ctx = self.executor.ctx();
        let config = self.config.read().await;
        let mut snapshots = Vec::new();

        let vault_names: Vec<String> = match req.vault {
            Some(ref name) => {
                if config.vault.contains_key(name) {
                    vec![name.clone()]
                } else {
                    return ListSnapshotsResponse { snapshots };
                }
            }
            None => config.vault.keys().cloned().collect(),
        };

        for vault_name in &vault_names {
            let vault = match config.vault.get(vault_name) {
                Some(v) => v,
                None => continue,
            };

            // Try to read the published TN from registry (has full history).
            if let Some(registry) = ctx.registry.as_ref() {
                if let Ok(published) = self
                    .list_published_snapshots(ctx, &config, vault_name, vault, registry.as_ref())
                    .await
                {
                    snapshots.extend(published);
                    continue;
                }
            }

            // Fallback: read local vault root (current snapshot only).
            let root_path =
                crate::tasks::vault_persist::vault_root_path(&vault.root_path);
            if let Ok(Some(node)) = crate::tasks::vault_persist::load_node(
                &root_path,
                &ctx.node_secret,
                vault_name,
            ) {
                if let Some(entry) = node.transparent_entry() {
                    if let Some(ref content) = entry.content {
                        snapshots.push(SnapshotInfo {
                            vault: vault_name.clone(),
                            hash: hex::encode(content.hash),
                            timestamp: String::from("current"),
                            file_count: None,
                            total_bytes: None,
                        });
                    }
                }
            }
        }

        ListSnapshotsResponse { snapshots }
    }

    /// Read published TN from registry and extract snapshot history.
    async fn list_published_snapshots(
        &self,
        ctx: &crate::tasks::TaskExecutorContext,
        config: &S5NodeConfig,
        vault_name: &str,
        vault: &s5_node_api::config::NodeConfigVault,
        registry: &dyn s5_core::RegistryApi,
    ) -> anyhow::Result<Vec<SnapshotInfo>> {
        use ed25519_dalek::VerifyingKey;
        use s5_core::StreamKey;

        // Derive the signing key → verifying key → stream key for this vault
        let signing_key =
            crate::tasks::publish::vault_signing_key(&ctx.node_secret, vault_name);
        let verifying_key: VerifyingKey = (&signing_key).into();
        let stream_key = StreamKey::PublicKeyEd25519(verifying_key.to_bytes());

        // Resolve identity files from vault's key config
        let mut identity_files = Vec::new();
        if let Some(key_config) = config.key.get(&vault.key) {
            if let Some(ref id_file) = key_config.identity_file {
                identity_files.push(id_file.clone());
            }
        }

        if identity_files.is_empty() {
            return Err(anyhow::anyhow!("no identity files for vault '{vault_name}'"));
        }

        // Resolve a blob store to download from
        let blob_store_name = vault
            .blob_stores
            .first()
            .ok_or_else(|| anyhow::anyhow!("no blob store for vault '{vault_name}'"))?;
        let blob_store = crate::tasks::resolve_store(&ctx.stores, blob_store_name)?;

        // Fetch the published TN
        let (node, _hash, _revision) =
            crate::tasks::publish::fetch_previous_published_node(
                registry,
                blob_store,
                &stream_key,
                &identity_files,
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("no published entry for vault '{vault_name}'"))?;

        // Extract snapshots from the Node entries
        let mut result = Vec::new();
        for (key, entry) in &node.entries {
            if let Some(ref content) = entry.content {
                let timestamp = if key.is_empty() {
                    String::from("current")
                } else {
                    key.clone()
                };
                result.push(SnapshotInfo {
                    vault: vault_name.to_string(),
                    hash: hex::encode(content.hash),
                    timestamp,
                    file_count: None,
                    total_bytes: None,
                });
            }
        }

        // Sort by timestamp descending ("current" sorts after ISO dates)
        result.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        Ok(result)
    }

    async fn handle_shutdown(&self) {
        info!("shutdown requested via S5 RPC");
        let mut guard = self.shutdown_tx.write().await;
        if let Some(tx) = guard.take() {
            tx.send(()).ok();
        }
    }
}

impl ProtocolHandler for S5NodeServer {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let remote_id = conn.remote_id();
        info!(peer = %remote_id.fmt_short(), "s5_node: accepted connection");

        while let Some(msg) = read_request::<S5NodeProto>(&conn).await? {
            match msg {
                S5NodeMessage::RunTask(irpc::WithChannels { inner, tx, .. }) => {
                    let resp = self.handle_run_task(inner).await;
                    let _ = oneshot::Sender::send(tx, resp).await;
                }
                S5NodeMessage::GetTaskStatus(irpc::WithChannels { inner, tx, .. }) => {
                    let resp = self.handle_get_task_status(inner).await;
                    let _ = oneshot::Sender::send(tx, resp).await;
                }
                S5NodeMessage::CancelTask(irpc::WithChannels { inner, tx, .. }) => {
                    let resp = self.handle_cancel_task(inner).await;
                    let _ = oneshot::Sender::send(tx, resp).await;
                }
                S5NodeMessage::ListTasks(irpc::WithChannels { inner: _, tx, .. }) => {
                    let resp = self.handle_list_tasks().await;
                    let _ = oneshot::Sender::send(tx, resp).await;
                }
                S5NodeMessage::GetConfig(irpc::WithChannels { inner, tx, .. }) => {
                    let resp = self.handle_get_config(inner).await;
                    let _ = oneshot::Sender::send(tx, resp).await;
                }
                S5NodeMessage::PatchConfig(irpc::WithChannels { inner, tx, .. }) => {
                    let resp = self.handle_patch_config(inner).await;
                    let _ = oneshot::Sender::send(tx, resp).await;
                }
                S5NodeMessage::GetStatus(irpc::WithChannels { inner, tx, .. }) => {
                    let resp = self.handle_get_status(inner).await;
                    let _ = oneshot::Sender::send(tx, resp).await;
                }
                S5NodeMessage::ListSnapshots(irpc::WithChannels { inner, tx, .. }) => {
                    let resp = self.handle_list_snapshots(inner).await;
                    let _ = oneshot::Sender::send(tx, resp).await;
                }
                S5NodeMessage::Shutdown(irpc::WithChannels { inner: _, tx, .. }) => {
                    self.handle_shutdown().await;
                    let _ = oneshot::Sender::send(tx, ()).await;
                }
            }
        }

        info!(peer = %remote_id.fmt_short(), "s5_node: connection closed");
        conn.closed().await;
        Ok(())
    }
}
