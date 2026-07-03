//! S5 Node RPC server implementation.
//!
//! Accepts connections using the `s5/node/0` ALPN and dispatches incoming
//! messages to the [`TaskExecutor`] for task orchestration.

use std::path::PathBuf;
use std::sync::Arc;

use iroh::endpoint::Connection;
use iroh::protocol::{AcceptError, ProtocolHandler};
use irpc::channel::{mpsc, oneshot};
use irpc_iroh::read_request;
use tokio::sync::RwLock;
use tokio::sync::oneshot as tokio_oneshot;
use tracing::info;

use s5_node_api::{
    AddFriend, CancelTask, DebugPeer, DebugPeerAlpn, DebugPeers, DebugPeersResponse, DeviceEntry,
    DeviceInvite, DeviceInviteEvent, ExportVault, ExportedShare, GetConfig, GetConfigResponse,
    GetHealth, GetHealthResponse, GetStatus, GetStatusResponse, GrantVault, JoinExport,
    ListDevices, ListDevicesResponse, ListSnapshots, ListSnapshotsResponse, ListTasksResponse,
    ListTree, ListTreeResponse, MountVault, MountedVault, Pair, PairEvent, PatchConfig, RedeemPair,
    RevokeDevice, RevokeDeviceResponse, RunTask, S5NodeMessage, S5NodeProto, SnapshotInfo,
    SpawnedTask, TaskState, TaskStatusResponse, UnmountVault, WatchTaskStatus,
};

use crate::config::S5NodeConfig;
use crate::fuse::MountManager;
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
    /// Daemon-side FUSE mount manager. Mount/unmount RPCs delegate
    /// here; the manager owns the live `MountHandle`s.
    mount_manager: Arc<MountManager>,
    endpoint_id: String,
    /// Channel to signal shutdown to the node's run loop.
    shutdown_tx: Arc<RwLock<Option<tokio_oneshot::Sender<()>>>>,
    /// Notify fired after any handler that mutates membership-relevant
    /// config (`PatchConfig` touching `friend`/`vault.<>.members`,
    /// `AddFriend`, `GrantVault`). The daemon's membership coordinator
    /// wakes on it to rebuild `MembershipState` and respawn per-peer
    /// subscribers. `None` in test harnesses that don't run a
    /// coordinator.
    membership_refresh: Option<Arc<tokio::sync::Notify>>,
    /// Notify fired after any handler that mutates the persisted `[task.*]`
    /// automation set (`PatchConfig`). The daemon's automation coordinator
    /// wakes on it and reconciles the live loop set. `None` in test harnesses
    /// that don't run a coordinator. Mirrors `membership_refresh` exactly.
    automation_refresh: Option<Arc<tokio::sync::Notify>>,
    /// The daemon's automation engine — read by `GetStatus` for per-automation
    /// liveness. `None` in harnesses without one.
    automation_manager: Option<Arc<crate::watch::AutomationManager>>,
    /// The `(vault, source)` of the most recent inline (manual) `Backup` this
    /// server dispatched — surfaced by `GetStatus.last_backup` so the
    /// `automate` wizard can offer "keep doing that?".
    last_backup: Arc<std::sync::Mutex<Option<s5_node_api::LastBackup>>>,
    /// In-memory pending-pair table shared with the `s5/pair/0`
    /// listener. The `Pair` RPC handler mints into it; the listener
    /// fires a oneshot back when a peer redeems. `None` until wired
    /// in `run_node`.
    pending_pairs: Option<crate::pair::PendingPairs>,
    /// In-memory pending-enroll table shared with the `s5/enroll/0`
    /// listener (D10). `DeviceInvite` mints into it; the listener
    /// fires a oneshot back once the joiner is fully enrolled. `None`
    /// when the daemon has no enroll listener (no registry / no
    /// durable bootstrap store).
    pending_enrolls: Option<crate::enroll::PendingEnrolls>,
    /// Iroh endpoint — needed by `RedeemPair` to dial the sender's
    /// address and present the secret, and by `Pair` to read this
    /// node's endpoint id when minting tokens.
    endpoint: Option<iroh::Endpoint>,
    /// Per-peer connection observer (iroh `EndpointHooks` sink).
    /// `None` until wired in `run_node`. `DebugPeers` snapshots it.
    peer_observer: Option<crate::peer_observer::PeerObserver>,
    /// This daemon's WARM master signing key — used by the pair handlers to
    /// sign (mint side) and prove (redeem side) the warm PoP (D17: the cold
    /// key never touches the daemon). `None` until `with_pair_support` wires
    /// it.
    master: Option<ed25519_dalek::SigningKey>,
    /// This daemon's DID pubkey (cold anchor key) + its signed cold-pointer
    /// entry — embedded in minted tokens and shipped in the pairing
    /// handshake for in-band self-certification (D17). `None` until
    /// `with_pair_support` wires them.
    pair_identity: Option<([u8; 32], s5_core::StreamMessage)>,
}

impl std::fmt::Debug for S5NodeServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S5NodeServer").finish()
    }
}

impl S5NodeServer {
    /// Creates a new S5NodeServer with a task executor, mount manager,
    /// and shutdown channel.
    pub fn new(
        config: Arc<RwLock<S5NodeConfig>>,
        config_path: PathBuf,
        executor: Arc<TaskExecutor>,
        mount_manager: Arc<MountManager>,
        endpoint_id: String,
        shutdown_tx: tokio_oneshot::Sender<()>,
    ) -> Self {
        Self {
            config,
            config_path,
            executor,
            mount_manager,
            endpoint_id,
            shutdown_tx: Arc::new(RwLock::new(Some(shutdown_tx))),
            membership_refresh: None,
            automation_refresh: None,
            automation_manager: None,
            last_backup: Arc::new(std::sync::Mutex::new(None)),
            pending_pairs: None,
            pending_enrolls: None,
            endpoint: None,
            peer_observer: None,
            master: None,
            pair_identity: None,
        }
    }

    /// Attach the daemon-wide peer observer for `DebugPeers`.
    pub fn with_peer_observer(mut self, observer: crate::peer_observer::PeerObserver) -> Self {
        self.peer_observer = Some(observer);
        self
    }

    /// Attach a `Notify` the server will fire after any handler that
    /// mutates membership-relevant config. Builder-style; intended to
    /// be called once in `run_node` after the membership coordinator
    /// is constructed.
    pub fn with_membership_refresh(mut self, notify: Arc<tokio::sync::Notify>) -> Self {
        self.membership_refresh = Some(notify);
        self
    }

    /// Attach a `Notify` the server fires after any handler that mutates the
    /// persisted `[task.*]` automation set. Builder-style; called once in
    /// `run_node` alongside the automation coordinator.
    pub fn with_automation_refresh(mut self, notify: Arc<tokio::sync::Notify>) -> Self {
        self.automation_refresh = Some(notify);
        self
    }

    /// Attach the daemon's automation engine so `GetStatus` can report
    /// per-automation liveness. Builder-style; called once in `run_node`.
    pub fn with_automation_manager(
        mut self,
        manager: Arc<crate::watch::AutomationManager>,
    ) -> Self {
        self.automation_manager = Some(manager);
        self
    }

    /// Attach the pair-flow plumbing: the pending-pair table shared with the
    /// `s5/pair/0` listener, the iroh endpoint used for outbound `RedeemPair`
    /// dials, the WARM master signing key (both pair handlers sign the warm
    /// PoP, D8/D17), and this daemon's DID pubkey + anchor entry (shipped
    /// in-band for self-certification). Builder-style; called once in
    /// `run_node`.
    pub fn with_pair_support(
        mut self,
        pending: crate::pair::PendingPairs,
        endpoint: iroh::Endpoint,
        warm: ed25519_dalek::SigningKey,
        did_pubkey: [u8; 32],
        anchor_entry: s5_core::StreamMessage,
    ) -> Self {
        self.pending_pairs = Some(pending);
        self.endpoint = Some(endpoint);
        self.master = Some(warm);
        self.pair_identity = Some((did_pubkey, anchor_entry));
        self
    }

    /// Attach the device-enrollment plumbing (D10): the pending-enroll
    /// table shared with the `s5/enroll/0` listener, or `None` when the
    /// daemon can't enroll (no registry / no durable bootstrap store) —
    /// `DeviceInvite` then reports the missing prerequisite.
    /// Builder-style; called once in `run_node`.
    pub fn with_enroll_support(mut self, pending: Option<crate::enroll::PendingEnrolls>) -> Self {
        self.pending_enrolls = pending;
        self
    }

    /// Seed a pairing peer's verified cold-pointer entry into the local
    /// registry (best-effort): the peer's DID then resolves locally even
    /// before its own registry is reachable — this is what makes a
    /// freshly-paired peer immediately resolvable (D17).
    async fn seed_peer_anchor(&self, pair: &crate::pair::VerifiedPair) {
        let Some(reg) = self.executor.ctx().registry.clone() else {
            return;
        };
        if let Err(e) = reg.set(pair.anchor_entry.clone()).await {
            tracing::warn!("pair: seeding peer anchor into local registry failed: {e:#}");
        }
    }

    fn notify_membership_refresh(&self) {
        if let Some(n) = self.membership_refresh.as_ref() {
            n.notify_one();
        }
    }

    fn notify_automation_refresh(&self) {
        if let Some(n) = self.automation_refresh.as_ref() {
            n.notify_one();
        }
    }

    async fn handle_mount_vault(&self, req: MountVault) -> Result<MountedVault, String> {
        self.mount_manager
            .mount(&req.vault, req.mountpoint, req.rw, req.debounce_ms)
            .await
            .map(|mount_id| MountedVault { mount_id })
            .map_err(|e| format!("{e:#}"))
    }

    async fn handle_unmount_vault(&self, req: UnmountVault) -> Result<(), String> {
        self.mount_manager
            .unmount(req.mount_id)
            .await
            .map_err(|e| format!("{e:#}"))
    }

    async fn handle_export_vault(&self, req: ExportVault) -> Result<ExportedShare, String> {
        let ctx = self.executor.ctx();
        let config = ctx.config.read().await;
        crate::export::run_export(&config, &ctx.stores, &req.vault, req.path.as_deref())
            .await
            .map(|result| ExportedShare {
                url: result.url,
                blob_hash_hex: hex::encode(result.blob_hash.as_bytes()),
            })
            .map_err(|e| format!("{e:#}"))
    }

    async fn handle_join_export(&self, req: JoinExport) -> Result<String, String> {
        // Peek at the label so we can pick a non-colliding vault name and a
        // root dir before doing the (heavier) fetch/decrypt.
        let parsed = crate::share::ExportUrl::parse(&req.url).map_err(|e| format!("{e:#}"))?;

        let mut config = self.config.write().await;

        // Pick a free vault name: the suggested label, else label-2, label-3…
        let mut label = parsed.label.clone();
        let mut n = 2;
        while config.vault.contains_key(&label) {
            label = format!("{}-{n}", parsed.label);
            n += 1;
        }

        // The consumer's own recipients (all `[key.*]` with a public key) so
        // the materialised root is readable by this daemon; and a key name for
        // the vault entry (prefer `main`).
        let recipients: Vec<String> = config.key.values().map(|k| k.public_key.clone()).collect();
        if recipients.is_empty() {
            return Err("no [key.*] configured — run `vup onboard` first".to_string());
        }
        let key_name = if config.key.contains_key("main") {
            "main".to_string()
        } else {
            config
                .key
                .keys()
                .next()
                .cloned()
                .expect("non-empty checked")
        };

        // Root dir: alongside existing vaults (their common `…/vaults/`
        // parent), else next to the config file. Keeps joined vaults where
        // the rest live without depending on the platform-dirs crate here.
        let vaults_dir = config
            .vault
            .values()
            .find_map(|v| {
                std::path::Path::new(&v.root_path)
                    .parent()
                    .map(|p| p.to_path_buf())
            })
            .or_else(|| self.config_path.parent().map(|p| p.join("vaults")))
            .ok_or_else(|| "could not determine a vaults directory".to_string())?;
        let root_dir = vaults_dir.join(&label);
        std::fs::create_dir_all(&root_dir).map_err(|e| format!("creating vault dir: {e}"))?;

        // Fetch + decrypt + materialise the read-only root.
        let ctx = self.executor.ctx();
        crate::share::join_export(&req.url, &ctx.stores, &recipients, &root_dir)
            .await
            .map_err(|e| format!("{e:#}"))?;

        // Add the `[vault.<label>]` entry (read-only: no members/writers; the
        // owner isn't a peer we track — this is a frozen snapshot).
        let vault_recipients: Vec<String> = config.key.keys().cloned().collect();
        let vault_data_store = config.default_store.clone();
        config.vault.insert(
            label.clone(),
            s5_node_api::config::NodeConfigVault {
                root_path: root_dir.to_string_lossy().into_owned(),
                key: key_name,
                recipients: vault_recipients,
                data_store: vault_data_store,
                ..Default::default()
            },
        );

        let errors = config.validate();
        if !errors.is_empty() {
            // Roll back the insert so a bad state isn't persisted.
            config.vault.remove(&label);
            return Err(format!(
                "post-join validation failed: {}",
                errors.join("; ")
            ));
        }
        let toml_str = toml::to_string_pretty(&*config)
            .map_err(|e| format!("failed to serialize config: {e}"))?;
        tokio::fs::write(&self.config_path, &toml_str)
            .await
            .map_err(|e| format!("failed to write config file: {e}"))?;
        drop(config);

        tracing::info!(vault = %label, url = %req.url, "joined frozen export");
        Ok(label)
    }

    async fn handle_run_task(&self, req: RunTask) -> Result<SpawnedTask, String> {
        use s5_node_api::config::TaskSpec;

        let config = self.config.read().await;
        let spec = match (req.name, req.spec_json) {
            (Some(name), None) => match config.task.get(&name) {
                Some(tc) => tc.spec.clone(),
                None => {
                    let error = format!("task '{name}' not found in config");
                    tracing::warn!(name = %name, "RunTask: {error}");
                    return Err(error);
                }
            },
            (None, Some(json)) => match serde_json::from_str::<TaskSpec>(&json) {
                Ok(spec) => spec,
                Err(e) => {
                    let error = format!("invalid spec_json: {e}");
                    tracing::warn!(error = %e, "RunTask: {error}");
                    return Err(error);
                }
            },
            _ => {
                let error =
                    "RunTask: must specify exactly one of `name` or `spec_json`".to_string();
                tracing::warn!("{error}");
                return Err(error);
            }
        };
        drop(config);

        match self.executor.spawn(spec).await {
            Ok((task_id, resolved_spec)) => {
                tracing::info!(task_id, "task spawned");
                // Remember the most recent inline Backup so the `automate`
                // wizard can offer to promote it into a live automation.
                if let TaskSpec::Backup { vault, source, .. } = &resolved_spec {
                    *self.last_backup.lock().expect("last_backup lock poisoned") =
                        Some(s5_node_api::LastBackup {
                            vault: vault.clone(),
                            source: source.clone(),
                        });
                }
                Ok(SpawnedTask {
                    task_id,
                    spec_json: serde_json::to_string(&resolved_spec).unwrap_or_default(),
                })
            }
            Err(e) => {
                let error = format!("failed to spawn task: {e}");
                tracing::error!(error = %e, "{error}");
                Err(error)
            }
        }
    }

    async fn handle_cancel_task(&self, req: CancelTask) -> Result<(), String> {
        if self.executor.cancel(req.task_id).await {
            Ok(())
        } else {
            Err(format!("task {} not found", req.task_id))
        }
    }

    async fn handle_list_tasks(&self) -> ListTasksResponse {
        ListTasksResponse {
            tasks: self.executor.list().await,
        }
    }

    async fn handle_get_config(&self, _req: GetConfig) -> GetConfigResponse {
        let config = self.config.read().await;
        let json_str = match serde_json::to_string_pretty(&*config) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to serialize config to JSON");
                String::from("null")
            }
        };
        GetConfigResponse {
            config_json: json_str,
        }
    }

    async fn handle_patch_config(&self, req: PatchConfig) -> Result<String, String> {
        let patch: json_patch::Patch = serde_json::from_str(&req.patch_json)
            .map_err(|e| format!("invalid JSON Patch: {e}"))?;

        let mut config = self.config.write().await;

        let mut value = serde_json::to_value(&*config)
            .map_err(|e| format!("failed to serialize current config: {e}"))?;

        json_patch::patch(&mut value, &patch).map_err(|e| format!("patch failed: {e}"))?;

        let new_config: S5NodeConfig = serde_json::from_value(value.clone())
            .map_err(|e| format!("patched config is invalid: {e}"))?;

        let errors = new_config.validate();
        if !errors.is_empty() {
            return Err(format!("validation failed: {}", errors.join("; ")));
        }

        let toml_str = toml::to_string_pretty(&new_config)
            .map_err(|e| format!("failed to serialize to TOML: {e}"))?;
        tokio::fs::write(&self.config_path, &toml_str)
            .await
            .map_err(|e| format!("failed to write config file: {e}"))?;

        *config = new_config;
        drop(config);

        tracing::info!(path = %self.config_path.display(), "config patched and persisted");

        // A JSON patch may have touched membership-relevant fields
        // (`friend`, `vault.<>.members`, `vault.<>.data_store`, …).
        // Cheap to over-fire: the coordinator's rebuild is a no-op
        // when the resolved state hasn't changed.
        self.notify_membership_refresh();
        // …or the `[task.*]` automation set (`automate add/pause/resume/rm`).
        // The automation coordinator reconciles the live loops; a no-op when
        // no automation changed.
        self.notify_automation_refresh();

        Ok(value.to_string())
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
        let automations = match self.automation_manager.as_ref() {
            Some(m) => m.status().await,
            None => Vec::new(),
        };
        let last_backup = self
            .last_backup
            .lock()
            .expect("last_backup lock poisoned")
            .clone();
        GetStatusResponse {
            store_count: config.store.len(),
            vault_count: config.vault.len(),
            source_count: config.source.len(),
            running_tasks,
            endpoint_id: self.endpoint_id.clone(),
            automations,
            last_backup,
        }
    }

    async fn handle_get_health(&self, _req: GetHealth) -> GetHealthResponse {
        let config = self.config.read().await;
        let ctx = self.executor.ctx();
        crate::health::gather_health(&config, &ctx.stores).await
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
            if let Some(registry) = ctx.registry.as_ref()
                && let Ok(published) = self
                    .list_published_snapshots(ctx, &config, vault_name, vault, registry.as_ref())
                    .await
            {
                snapshots.extend(published);
                continue;
            }

            // Fallback: read local vault root (current snapshot only).
            let root_path = crate::tasks::vault_persist::vault_root_path(&vault.root_path);
            let id_files: Vec<String> = config
                .key
                .get(&vault.key)
                .and_then(|k| k.identity_file.clone())
                .into_iter()
                .collect();
            if let Ok(Some(node)) = crate::tasks::vault_persist::load_node(&root_path, &id_files)
                && let Some(entry) = node.transparent_entry()
                && let Some(ref content) = entry.content
            {
                snapshots.push(SnapshotInfo {
                    vault: vault_name.clone(),
                    hash: hex::encode(content.hash),
                    timestamp: String::from("current"),
                    file_count: None,
                    total_bytes: None,
                });
            }
        }

        ListSnapshotsResponse { snapshots }
    }

    async fn handle_list_tree(&self, req: ListTree) -> Result<ListTreeResponse, String> {
        // Cheap up-front existence check for a clean error before we open
        // stores / the registry.
        {
            let config = self.config.read().await;
            if !config.vault.contains_key(&req.vault) {
                return Err(format!("no such vault '{}'", req.vault));
            }
        }
        let ctx = self.executor.ctx();
        crate::tasks::list::list_tree(
            ctx,
            &req.vault,
            req.snapshot.as_deref(),
            req.subtree.as_deref(),
            req.max_depth,
        )
        .await
        .map(|entries| ListTreeResponse { entries })
        .map_err(|e| format!("{e:#}"))
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

        // Resolve identity files from vault's key config
        let mut identity_files = Vec::new();
        if let Some(key_config) = config.key.get(&vault.key)
            && let Some(ref id_file) = key_config.identity_file
        {
            identity_files.push(id_file.clone());
        }

        if identity_files.is_empty() {
            return Err(anyhow::anyhow!(
                "no identity files for vault '{vault_name}'"
            ));
        }

        // Derive the vault's `vault_id` from its local root's KEY_SLOT_RECOVERY
        // slot — the same value the publish path signs registry entries under,
        // so the hashes listed here match what `restore vault:#snap` accepts. A
        // cold device with no local root yet can't derive it; fall back to the
        // zero placeholder (finds nothing → the caller's local fallback runs).
        let root_path = crate::tasks::vault_persist::vault_root_path(&vault.root_path);
        let vault_id = crate::tasks::vault_persist::load_vault_root(&root_path, &identity_files)
            .ok()
            .flatten()
            .and_then(|(_, _, root_ctx)| {
                crate::tasks::publish::vault_id_from_context(&root_ctx).ok()
            })
            .unwrap_or([0u8; 16]);

        // Derive the device's signing key → verifying key → stream key.
        let signing_key = crate::tasks::publish::device_signing_key(&ctx.node_secret);
        let verifying_key: VerifyingKey = (&signing_key).into();
        let stream_key = StreamKey::Vault {
            pubkey: verifying_key.to_bytes(),
            vault_id,
        };

        // Resolve a blob store to download from — the published TN lives
        // on the vault's meta primary (D1).
        let blob_store_name = config.vault_meta_store(vault_name, vault)?;
        let blob_store = crate::tasks::resolve_store(&ctx.stores, blob_store_name)?;

        // Fetch the published TN
        let (node, _hash, _revision) = crate::tasks::publish::fetch_previous_published_node(
            registry,
            blob_store.as_ref(),
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

    async fn handle_pair(&self, _req: Pair, tx: mpsc::Sender<PairEvent>) {
        let Some(pending) = self.pending_pairs.as_ref() else {
            let _ = tx
                .send(PairEvent::Failed {
                    error: "daemon has no pair listener configured".into(),
                })
                .await;
            return;
        };
        let Some(endpoint) = self.endpoint.as_ref() else {
            let _ = tx
                .send(PairEvent::Failed {
                    error: "daemon has no iroh endpoint configured".into(),
                })
                .await;
            return;
        };
        let Some((did_pubkey, _)) = self.pair_identity.as_ref() else {
            let _ = tx
                .send(PairEvent::Failed {
                    error: "daemon has no identity configured for pairing".into(),
                })
                .await;
            return;
        };
        let endpoint_id = *endpoint.id().as_bytes();
        let (token, rx) = pending.mint(endpoint_id, *did_pubkey).await;
        tracing::info!("pair: token minted, awaiting redemption");
        if tx
            .send(PairEvent::Minted {
                token: token.encode(),
            })
            .await
            .is_err()
        {
            // Client gave up before reading the token; nothing to do.
            return;
        }

        let event = match rx.await {
            Ok(pair) => {
                // `pair.peer.did_pubkey` is VERIFIED (the receiver proved
                // possession of the warm key its in-band anchor names) — the
                // DID is the cold anchor key, never the transport key (D8/D17).
                self.seed_peer_anchor(&pair).await;
                let did = s5_core::identity::Did::from_pubkey(
                    s5_core::identity::DidMasterPubkey::new(pair.peer.did_pubkey),
                );
                tracing::info!(peer_did = %did, "pair: redeemed");
                PairEvent::Redeemed {
                    peer_did: did.to_string(),
                }
            }
            Err(_) => PairEvent::Failed {
                error: "pending-pair channel closed without redemption (token expired?)".into(),
            },
        };
        let _ = tx.send(event).await;
    }

    async fn handle_device_invite(&self, req: DeviceInvite, tx: mpsc::Sender<DeviceInviteEvent>) {
        let Some(pending) = self.pending_enrolls.as_ref() else {
            let _ = tx
                .send(DeviceInviteEvent::Failed {
                    error: "this daemon cannot enroll devices — it needs a registry and a \
                            durable [identity].bootstrap_store"
                        .into(),
                })
                .await;
            return;
        };
        let Some(endpoint) = self.endpoint.as_ref() else {
            let _ = tx
                .send(DeviceInviteEvent::Failed {
                    error: "daemon has no iroh endpoint configured".into(),
                })
                .await;
            return;
        };
        let Some((did_pubkey, _)) = self.pair_identity.as_ref() else {
            let _ = tx
                .send(DeviceInviteEvent::Failed {
                    error: "daemon has no identity configured for enrollment".into(),
                })
                .await;
            return;
        };
        let endpoint_id = *endpoint.id().as_bytes();
        let (token, rx) = pending.mint(endpoint_id, *did_pubkey, req.label).await;
        tracing::info!("enroll: invite token minted, awaiting redemption");
        if tx
            .send(DeviceInviteEvent::Minted {
                token: token.encode(),
            })
            .await
            .is_err()
        {
            return; // client gave up before reading the token
        }

        let event = match rx.await {
            Ok(enrolled) => {
                tracing::info!(label = enrolled.label.as_str(), "enroll: device admitted");
                // Freshly-admitted sibling: rebuild membership so its
                // iroh/acl/signing keys take effect without a restart.
                self.notify_membership_refresh();
                DeviceInviteEvent::Admitted {
                    label: enrolled.label,
                    signing_hex: hex::encode(enrolled.keys.signing),
                }
            }
            Err(_) => DeviceInviteEvent::Failed {
                error: "enrollment did not complete (token expired, or the daemon-side \
                        device-add failed — check the daemon log); mint a new invite"
                    .into(),
            },
        };
        let _ = tx.send(event).await;
    }

    /// Shared preflight for the device-catalogue RPCs: the durable
    /// bootstrap store hosting `identity_secrets`, the local age
    /// identity files that open it, and this config's `[key.*]` age
    /// recipients (device key(s) + paper).
    async fn escrow_vault_parts(
        &self,
    ) -> Result<(Arc<dyn s5_core::blob::Blobs>, Vec<String>, Vec<String>), String> {
        let ctx = self.executor.ctx();
        let config = self.config.read().await;
        let bootstrap = config
            .identity
            .bootstrap_store
            .as_deref()
            .ok_or_else(|| "[identity].bootstrap_store not configured".to_string())?;
        let store =
            ctx.stores.get(bootstrap).cloned().ok_or_else(|| {
                format!("bootstrap store '{bootstrap}' not found among built stores")
            })?;
        let identity_files: Vec<String> = config
            .key
            .values()
            .filter_map(|k| k.identity_file.clone())
            .collect();
        let recipients: Vec<String> = config.key.values().map(|k| k.public_key.clone()).collect();
        Ok((store, identity_files, recipients))
    }

    async fn handle_list_devices(&self, _req: ListDevices) -> Result<ListDevicesResponse, String> {
        let warm = self
            .master
            .as_ref()
            .ok_or_else(|| "daemon has no identity configured".to_string())?;
        let registry = self
            .executor
            .ctx()
            .registry
            .clone()
            .ok_or_else(|| "no registry configured".to_string())?;
        let (store, identity_files, _recipients) = self.escrow_vault_parts().await?;
        let vault = crate::identity_secrets_vault::IdentitySecretsVault::new(
            warm.clone(),
            store,
            registry,
            Vec::new(),
            identity_files,
        );
        let catalogue = vault.read_devices().await.map_err(|e| format!("{e:#}"))?;
        Ok(ListDevicesResponse {
            devices: catalogue
                .into_iter()
                .map(|(label, keys)| DeviceEntry {
                    label,
                    signing_hex: hex::encode(keys.signing),
                    acl_hex: hex::encode(keys.acl),
                    iroh_hex: hex::encode(keys.iroh),
                    age_recipient: keys.age_recipient,
                })
                .collect(),
        })
    }

    /// D18 device revoke — the routine `identity-rotation.md` §6.1
    /// removal, by catalogue label:
    ///
    /// 1. resolve label → the device's key 4-tuple (labels never
    ///    authorize; they only *name* the keys to drop),
    /// 2. drop the four keys from the identity bundle
    ///    ([`crate::admission::remove_device_keys`]),
    /// 3. remove the catalogue entry,
    /// 4. re-wrap `identity_secrets` + `config` to the SURVIVING
    ///    recipient set (+ paper),
    /// 5. fire a membership refresh so the revoked signer stops being
    ///    accepted without a restart.
    ///
    /// Refuses to revoke the device whose keys match this daemon's own
    /// keyset: removing the keys the daemon itself runs on would wedge
    /// it (no key left to sign the bundle repair with) — run the revoke
    /// from a surviving device instead.
    async fn handle_revoke_device(
        &self,
        req: RevokeDevice,
    ) -> Result<RevokeDeviceResponse, String> {
        let warm = self
            .master
            .as_ref()
            .ok_or_else(|| "daemon has no identity configured".to_string())?;
        let ctx = self.executor.ctx();
        let registry = ctx
            .registry
            .clone()
            .ok_or_else(|| "no registry configured".to_string())?;
        let (escrow_store, identity_files, own_recipients) = self.escrow_vault_parts().await?;

        // 1. Label → keys, from the catalogue.
        let vault = crate::identity_secrets_vault::IdentitySecretsVault::new(
            warm.clone(),
            escrow_store.clone(),
            registry.clone(),
            Vec::new(),
            identity_files.clone(),
        );
        let catalogue = vault.read_devices().await.map_err(|e| format!("{e:#}"))?;
        let Some(keys) = catalogue.get(&req.label).cloned() else {
            return Err(format!(
                "no device '{}' in the catalogue (see `vup device ls`)",
                req.label
            ));
        };

        // Self-revoke refusal: the device being revoked must not be the
        // one this daemon runs as.
        let own_signing = crate::tasks::publish::device_signing_key(&ctx.node_secret)
            .verifying_key()
            .to_bytes();
        let own_iroh = self.endpoint.as_ref().map(|e| *e.id().as_bytes());
        if keys.signing == own_signing || Some(keys.iroh) == own_iroh {
            return Err(format!(
                "'{}' is THIS device — revoking a device from itself would drop the \
                 keys this daemon runs on and wedge it. Run the revoke from another \
                 enrolled device.",
                req.label
            ));
        }

        // 2. Drop the four keys from the bundle (the authorization step).
        let bundle_revision =
            crate::admission::remove_device_keys(warm, registry.as_ref(), &ctx.stores, &keys)
                .await
                .map_err(|e| format!("removing device keys from the identity bundle: {e:#}"))?;

        // Surviving recipient set: what the post-removal bundle names
        // (sibling devices + paper), unioned with this config's own
        // `[key.*]` recipients (writers ⊆ readers), minus the revoked
        // device's key.
        let warm_pub = warm.verifying_key().to_bytes();
        let mut survivors =
            match crate::admission::read_current_bundle(warm_pub, registry.as_ref(), &ctx.stores)
                .await
            {
                Ok(Some((bundle, _))) => bundle.age_recipients,
                Ok(None) => Vec::new(),
                Err(e) => return Err(format!("reading the post-removal bundle: {e:#}")),
            };
        for r in own_recipients {
            if !survivors.contains(&r) {
                survivors.push(r);
            }
        }
        survivors.retain(|r| r != &keys.age_recipient);
        if survivors.is_empty() {
            return Err(
                "no surviving age recipients after the removal — refusing to seal the \
                 special vaults shut"
                    .to_string(),
            );
        }

        // 3. Catalogue entry out, sealed to the survivors.
        let vault = crate::identity_secrets_vault::IdentitySecretsVault::new(
            warm.clone(),
            escrow_store.clone(),
            registry.clone(),
            survivors.clone(),
            identity_files.clone(),
        );
        vault
            .remove_device(&req.label)
            .await
            .map_err(|e| format!("removing the catalogue entry: {e:#}"))?;

        // 4. Re-wrap both special vaults to the survivors (+ paper).
        crate::admission::rewrap_special_vaults(
            warm,
            registry.as_ref(),
            escrow_store,
            &identity_files,
            &survivors,
        )
        .await
        .map_err(|e| format!("re-wrapping the special vaults: {e:#}"))?;

        tracing::info!(
            label = req.label.as_str(),
            bundle_revision,
            signing = %hex::encode(&keys.signing[..4]),
            "revoke: device removed + special vaults re-wrapped to the survivors"
        );
        // 5. The revoked signer must stop being accepted without a restart.
        self.notify_membership_refresh();

        Ok(RevokeDeviceResponse {
            signing_hex: hex::encode(keys.signing),
            bundle_revision,
        })
    }

    async fn handle_redeem_pair(&self, req: RedeemPair) -> Result<String, String> {
        let token = crate::pair::PairToken::decode(&req.token).map_err(|e| format!("{e:#}"))?;
        let endpoint = self.endpoint.as_ref().ok_or_else(|| {
            "daemon has no iroh endpoint configured for outbound dials".to_string()
        })?;
        let warm = self
            .master
            .as_ref()
            .ok_or_else(|| "daemon has no warm key configured for pairing".to_string())?;
        let (did_pubkey, anchor_entry) = self
            .pair_identity
            .as_ref()
            .ok_or_else(|| "daemon has no identity configured for pairing".to_string())?;
        let pair =
            crate::pair::redeem_pair_token(endpoint, &token, warm, *did_pubkey, anchor_entry)
                .await
                .map_err(|e| format!("{e:#}"))?;
        // `pair.peer.did_pubkey` is VERIFIED against the token's DID (the
        // sender proved possession of the warm key its anchor names) —
        // anchored in the cold key, never iroh (D8/D17).
        self.seed_peer_anchor(&pair).await;
        let did = s5_core::identity::Did::from_pubkey(s5_core::identity::DidMasterPubkey::new(
            pair.peer.did_pubkey,
        ));
        tracing::info!(peer_did = %did, "pair: redeemed remote token");
        Ok(did.to_string())
    }

    async fn handle_add_friend(&self, req: AddFriend) -> Result<(), String> {
        use s5_core::identity::Did;
        use s5_node_api::config::NodeConfigFriend;

        Did::parse(&req.did).map_err(|e| format!("invalid did:s5:b... value: {e}"))?;
        if req.petname.is_empty() {
            return Err("petname must not be empty".into());
        }

        let mut config = self.config.write().await;
        match config.friend.get(&req.petname) {
            Some(existing) if existing.id == req.did => return Ok(()),
            Some(existing) => {
                return Err(format!(
                    "friend petname '{}' already maps to a different DID ({}) — \
                     choose a different name or unpair the existing one first",
                    req.petname, existing.id
                ));
            }
            None => {
                config.friend.insert(
                    req.petname.clone(),
                    NodeConfigFriend {
                        id: req.did.clone(),
                        // Pair flow (TODO): the pair handshake carries
                        // the inviter's iroh pubkey alongside the DID.
                        // Surface it here so the post-pair daemon can
                        // dial without needing a second config patch.
                        iroh_pubkey_hex: None,
                    },
                );
            }
        }

        let errors = config.validate();
        if !errors.is_empty() {
            return Err(format!(
                "post-pair validation failed: {}",
                errors.join("; ")
            ));
        }
        let toml_str = toml::to_string_pretty(&*config)
            .map_err(|e| format!("failed to serialize config to TOML: {e}"))?;
        tokio::fs::write(&self.config_path, &toml_str)
            .await
            .map_err(|e| format!("failed to write config file: {e}"))?;
        drop(config);

        tracing::info!(petname = %req.petname, did = %req.did, "pair: friend added");
        self.notify_membership_refresh();
        Ok(())
    }

    async fn handle_grant_vault(&self, req: GrantVault) -> Result<(), String> {
        let mut config = self.config.write().await;
        if !config.vault.contains_key(&req.vault) {
            return Err(format!("vault '{}' not found in [vault.*]", req.vault));
        }
        if !config.friend.contains_key(&req.petname) {
            return Err(format!(
                "no [friend.{}] in config — run `vup friend pair` first to record the \
                 friend's DID",
                req.petname
            ));
        }
        let vault_cfg = config.vault.get_mut(&req.vault).expect("checked above");
        if !vault_cfg.members.iter().any(|m| m == &req.petname) {
            vault_cfg.members.push(req.petname.clone());
        }
        // Capability = keyset membership (D11): --write also joins `writers`.
        // Idempotent; a later `grant --write` promotes an existing read-only
        // member. (Demotion is a `revoke` + re-grant — explicit.)
        if req.write && !vault_cfg.writers.iter().any(|w| w == &req.petname) {
            vault_cfg.writers.push(req.petname.clone());
        }

        let errors = config.validate();
        if !errors.is_empty() {
            return Err(format!(
                "post-grant validation failed: {}",
                errors.join("; ")
            ));
        }
        let toml_str = toml::to_string_pretty(&*config)
            .map_err(|e| format!("failed to serialize config to TOML: {e}"))?;
        tokio::fs::write(&self.config_path, &toml_str)
            .await
            .map_err(|e| format!("failed to write config file: {e}"))?;
        drop(config);

        tracing::info!(
            vault = %req.vault,
            petname = %req.petname,
            "pair: granted friend access to vault"
        );
        self.notify_membership_refresh();
        Ok(())
    }

    async fn handle_debug_peers(&self, _req: DebugPeers) -> DebugPeersResponse {
        let Some(observer) = self.peer_observer.as_ref() else {
            return DebugPeersResponse { peers: Vec::new() };
        };
        let mut peers: Vec<DebugPeer> = observer
            .snapshot()
            .into_iter()
            .map(|(pubkey, stats)| {
                let mut alpns: Vec<DebugPeerAlpn> = stats
                    .by_alpn
                    .into_iter()
                    .map(|(alpn_bytes, s)| DebugPeerAlpn {
                        alpn: String::from_utf8_lossy(&alpn_bytes).into_owned(),
                        handshakes: s.handshakes,
                        first_seen_unix: s.first_seen_unix,
                        last_seen_unix: s.last_seen_unix,
                        last_was_incoming: s.last_was_incoming,
                    })
                    .collect();
                alpns.sort_by(|a, b| a.alpn.cmp(&b.alpn));
                DebugPeer {
                    pubkey_hex: hex::encode(pubkey),
                    alpns,
                }
            })
            .collect();
        peers.sort_by(|a, b| a.pubkey_hex.cmp(&b.pubkey_hex));
        DebugPeersResponse { peers }
    }

    async fn handle_shutdown(&self) {
        info!("shutdown requested via S5 RPC");
        let mut guard = self.shutdown_tx.write().await;
        if let Some(tx) = guard.take() {
            tx.send(()).ok();
        }
    }

    /// Stream task status updates.
    ///
    /// Sends the initial status immediately, then waits on the watch channel
    /// for change notifications. The watch channel's latest-value semantics
    /// coalesce rapid updates — multiple producer send_modify() calls between
    /// two receiver poll points collapse into a single notification with the
    /// newest value. The stream ends when the task reaches a terminal state
    /// or the client disconnects.
    async fn handle_watch_task_status(
        &self,
        req: WatchTaskStatus,
        tx: mpsc::Sender<TaskStatusResponse>,
    ) {
        let Some(mut rx) = self.executor.watch_status(req.task_id).await else {
            // Task not found — send a single error status and close.
            let _ = tx
                .send(TaskStatusResponse {
                    task_id: req.task_id,
                    state: TaskState::Failed {
                        error: format!("task {} not found", req.task_id),
                    },
                    progress: None,
                })
                .await;
            return;
        };

        // Send the current state immediately.
        let initial = (*rx.borrow_and_update()).clone();
        let is_terminal = matches!(
            initial.state,
            TaskState::Completed | TaskState::Failed { .. } | TaskState::Cancelled
        );
        if tx.send(initial).await.is_err() {
            return; // client disconnected
        }
        if is_terminal {
            return;
        }

        // Stream updates. The watch channel's "latest value" semantics
        // naturally coalesce rapid updates — if multiple send_modify()
        // calls happen before we poll changed(), we only see the latest.
        loop {
            // Wait for a change notification.
            if rx.changed().await.is_err() {
                break; // sender dropped (task handle removed)
            }

            // Take the latest value (skipping any intermediate states).
            let status = (*rx.borrow_and_update()).clone();
            let is_terminal = matches!(
                status.state,
                TaskState::Completed | TaskState::Failed { .. } | TaskState::Cancelled
            );

            if tx.send(status).await.is_err() {
                break; // client disconnected
            }
            if is_terminal {
                break;
            }
        }
    }
}

impl S5NodeServer {
    /// Dispatch a single decoded RPC message to the appropriate
    /// handler. Extracted out of `accept()` so the same dispatch can
    /// drive in-process callers via `serve_local()` — the iroh
    /// transport (per-connection `accept`) and the local transport
    /// (single mpsc) share this one match.
    pub async fn dispatch_message(&self, msg: S5NodeMessage) {
        match msg {
            S5NodeMessage::RunTask(irpc::WithChannels { inner, tx, .. }) => {
                let resp = self.handle_run_task(inner).await;
                let _ = oneshot::Sender::send(tx, resp).await;
            }
            S5NodeMessage::WatchTaskStatus(irpc::WithChannels { inner, tx, .. }) => {
                self.handle_watch_task_status(inner, tx).await;
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
            S5NodeMessage::GetHealth(irpc::WithChannels { inner, tx, .. }) => {
                let resp = self.handle_get_health(inner).await;
                let _ = oneshot::Sender::send(tx, resp).await;
            }
            S5NodeMessage::ListSnapshots(irpc::WithChannels { inner, tx, .. }) => {
                let resp = self.handle_list_snapshots(inner).await;
                let _ = oneshot::Sender::send(tx, resp).await;
            }
            S5NodeMessage::ListTree(irpc::WithChannels { inner, tx, .. }) => {
                let resp = self.handle_list_tree(inner).await;
                let _ = oneshot::Sender::send(tx, resp).await;
            }
            S5NodeMessage::MountVault(irpc::WithChannels { inner, tx, .. }) => {
                let resp = self.handle_mount_vault(inner).await;
                let _ = oneshot::Sender::send(tx, resp).await;
            }
            S5NodeMessage::UnmountVault(irpc::WithChannels { inner, tx, .. }) => {
                let resp = self.handle_unmount_vault(inner).await;
                let _ = oneshot::Sender::send(tx, resp).await;
            }
            S5NodeMessage::ExportVault(irpc::WithChannels { inner, tx, .. }) => {
                let resp = self.handle_export_vault(inner).await;
                let _ = oneshot::Sender::send(tx, resp).await;
            }
            S5NodeMessage::Pair(irpc::WithChannels { inner, tx, .. }) => {
                self.handle_pair(inner, tx).await;
            }
            S5NodeMessage::RedeemPair(irpc::WithChannels { inner, tx, .. }) => {
                let resp = self.handle_redeem_pair(inner).await;
                let _ = oneshot::Sender::send(tx, resp).await;
            }
            S5NodeMessage::DeviceInvite(irpc::WithChannels { inner, tx, .. }) => {
                self.handle_device_invite(inner, tx).await;
            }
            S5NodeMessage::ListDevices(irpc::WithChannels { inner, tx, .. }) => {
                let resp = self.handle_list_devices(inner).await;
                let _ = oneshot::Sender::send(tx, resp).await;
            }
            S5NodeMessage::RevokeDevice(irpc::WithChannels { inner, tx, .. }) => {
                let resp = self.handle_revoke_device(inner).await;
                let _ = oneshot::Sender::send(tx, resp).await;
            }
            S5NodeMessage::AddFriend(irpc::WithChannels { inner, tx, .. }) => {
                let resp = self.handle_add_friend(inner).await;
                let _ = oneshot::Sender::send(tx, resp).await;
            }
            S5NodeMessage::GrantVault(irpc::WithChannels { inner, tx, .. }) => {
                let resp = self.handle_grant_vault(inner).await;
                let _ = oneshot::Sender::send(tx, resp).await;
            }
            S5NodeMessage::JoinExport(irpc::WithChannels { inner, tx, .. }) => {
                let resp = self.handle_join_export(inner).await;
                let _ = oneshot::Sender::send(tx, resp).await;
            }
            S5NodeMessage::DebugPeers(irpc::WithChannels { inner, tx, .. }) => {
                let resp = self.handle_debug_peers(inner).await;
                let _ = oneshot::Sender::send(tx, resp).await;
            }
            S5NodeMessage::Shutdown(irpc::WithChannels { inner: _, tx, .. }) => {
                self.handle_shutdown().await;
                let _ = oneshot::Sender::send(tx, ()).await;
            }
        }
    }

    /// Spawn an in-process dispatcher and return a sender that can be
    /// converted into an `irpc::Client::<S5NodeProto>::local()` for
    /// callers in the same process — the same RPC surface as the iroh
    /// transport, but without the network round-trip.
    ///
    /// Each message is dispatched in its own task so concurrent RPCs
    /// don't head-of-line block each other; matches the per-stream
    /// concurrency the iroh accept loop offers via separate
    /// connections.
    ///
    /// The returned sender's lifetime is tied to this `Arc<Self>` —
    /// when all senders drop, the dispatcher loop exits.
    pub fn serve_local(self: std::sync::Arc<Self>) -> tokio::sync::mpsc::Sender<S5NodeMessage> {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<S5NodeMessage>(64);
        tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                let server = self.clone();
                tokio::spawn(async move {
                    server.dispatch_message(msg).await;
                });
            }
        });
        tx
    }
}

impl ProtocolHandler for S5NodeServer {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let remote_id = conn.remote_id();
        info!(peer = %remote_id.fmt_short(), "s5_node: accepted connection");

        while let Some(msg) = read_request::<S5NodeProto>(&conn).await? {
            self.dispatch_message(msg).await;
        }

        info!(peer = %remote_id.fmt_short(), "s5_node: connection closed");
        conn.closed().await;
        Ok(())
    }
}

/// Cookie-auth gate in front of the control-RPC handler (the F03 fix).
///
/// The control ALPN is served ONLY on a dedicated loopback-bound endpoint
/// (see `ControlPlane` in `lib.rs`) and, on top of that, every connection
/// must open one bi-stream and present `CONTROL_AUTH_MAGIC ‖ token` before
/// the inner handler sees it. The token is the per-run random secret from
/// the 0600 service lock file, so passing the gate proves the caller can
/// read the daemon owner's files. Remote daemon control is NOT a feature:
/// if it is ever wanted it must be an explicit, separately-authenticated
/// opt-in — never a bind-anywhere default.
///
/// Generic over the inner handler so the gate is testable without a full
/// daemon.
#[derive(Debug, Clone)]
pub struct ControlAuthGate<H> {
    inner: H,
    token: [u8; s5_node_api::CONTROL_TOKEN_LEN],
}

impl<H> ControlAuthGate<H> {
    pub fn new(inner: H, token: [u8; s5_node_api::CONTROL_TOKEN_LEN]) -> Self {
        Self { inner, token }
    }
}

impl<H: ProtocolHandler + Clone> ProtocolHandler for ControlAuthGate<H> {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        const PREAMBLE_LEN: usize =
            s5_node_api::CONTROL_AUTH_MAGIC.len() + s5_node_api::CONTROL_TOKEN_LEN;
        let mut expected = [0u8; PREAMBLE_LEN];
        expected[..s5_node_api::CONTROL_AUTH_MAGIC.len()]
            .copy_from_slice(s5_node_api::CONTROL_AUTH_MAGIC);
        expected[s5_node_api::CONTROL_AUTH_MAGIC.len()..].copy_from_slice(&self.token);

        // The whole preamble is deadlined so an idle pre-auth connection
        // cannot hold the accept task open indefinitely.
        let handshake = async {
            let (mut send, mut recv) = conn.accept_bi().await.ok()?;
            let mut presented = [0u8; PREAMBLE_LEN];
            recv.read_exact(&mut presented).await.ok()?;
            if !constant_time_eq(&presented, &expected) {
                return None;
            }
            send.write_all(&[0x01]).await.ok()?;
            let _ = send.finish();
            Some(())
        };
        let authed = tokio::time::timeout(std::time::Duration::from_secs(10), handshake).await;
        if !matches!(authed, Ok(Some(()))) {
            tracing::warn!(
                peer = %conn.remote_id().fmt_short(),
                "control: rejecting connection (bad or missing auth preamble)"
            );
            conn.close(1u32.into(), b"control auth failed");
            return Ok(());
        }
        self.inner.accept(conn).await
    }
}

/// Timing-independent byte comparison for the auth preamble: XOR-fold the
/// whole buffers instead of short-circuiting on the first mismatch.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod control_auth_tests {
    use super::constant_time_eq;

    #[test]
    fn constant_time_eq_semantics() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"ab"));
        assert!(constant_time_eq(b"", b""));
    }
}
