//! Daemon-side FUSE mount manager.
//!
//! `MountManager` owns the live `MountHandle`s for every active vault
//! mount on this node. The CLI (or any other RPC client) drives the
//! lifecycle through `MountVault` / `UnmountVault`; the daemon does
//! all the actual store opening, snapshot decryption, FUSE plumbing,
//! and (for rw mounts) the debounce + publish loop. The CLI never
//! has to depend on `s5_fuse` / `s5_fs_v2` / `s5_store_local`.
//!
//! ## Lifecycle
//!
//! Mount: `MountManager::mount` runs preflight, resolves the vault
//! config, opens the meta store + each `[store.<name>]` as a
//! `LocalStore`, decrypts the vault root, builds a `Snapshot`,
//! spawns a tokio task that calls `s5_fuse::mount` (or `mount_rw`)
//! with a oneshot-driven cancellation future, and stores the cancel
//! sender + join handle under a fresh `mount_id`.
//!
//! Unmount: `MountManager::unmount` removes the entry, sends on the
//! cancel oneshot (which makes the mount task return, which drops
//! the `MountHandle`, which performs the actual FUSE unmount), and
//! awaits the join handle.
//!
//! ## rw mounts
//!
//! For `rw = true`, the mount task additionally spawns a child
//! debounce task driven by the `WritableFs` clone the entry point
//! hands back. On every debounce: fold the overlay into a fresh
//! snapshot (`flush_overlay`), persist the new vault root, and
//! submit a `Publish` task to the daemon's executor. The debounce
//! task also listens on the same cancel oneshot so it tears down
//! cleanly on unmount.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow, bail};
use s5_core::blob::BlobStore;
use s5_core::{BlobsRead, FallbackBlobsRead};
use s5_fs_v2::snapshot::Snapshot;
use s5_node_api::config::TaskSpec;
use tokio::sync::{RwLock, oneshot};
use tokio::task::JoinHandle;

use crate::config::NodeConfigStore;
use crate::tasks::TaskExecutor;
use crate::tasks::vault_persist::{load_vault_root, save_vault_root, vault_root_path};

/// Per-mount state the manager keeps. `cancel` triggers shutdown of
/// both the FUSE session and (for rw) its attached debounce loop;
/// `join` is the mount task itself, awaited on unmount so the actual
/// `umount(2)` has finished by the time the RPC returns.
struct ActiveMount {
    cancel: oneshot::Sender<()>,
    join: JoinHandle<Result<()>>,
    #[allow(dead_code)] // kept for diagnostics + future ListMounts RPC
    mountpoint: PathBuf,
}

/// Daemon-side FUSE mount manager. Built once during node startup
/// alongside the `TaskExecutor`, shared with the RPC server.
pub struct MountManager {
    executor: Arc<TaskExecutor>,
    next_id: AtomicU64,
    mounts: RwLock<HashMap<u64, ActiveMount>>,
}

impl std::fmt::Debug for MountManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MountManager").finish()
    }
}

impl MountManager {
    pub fn new(executor: Arc<TaskExecutor>) -> Self {
        Self {
            executor,
            next_id: AtomicU64::new(1),
            mounts: RwLock::new(HashMap::new()),
        }
    }

    /// Mount `vault` at `mountpoint`. See module docs for the full
    /// lifecycle. Returns the assigned `mount_id`; preflight or
    /// resolution failures come back as an `Err` before any kernel
    /// state is touched.
    pub async fn mount(
        &self,
        vault: &str,
        mountpoint: PathBuf,
        rw: bool,
        debounce_ms: u64,
    ) -> Result<u64> {
        // Preflight first so the RPC error appears before any of the
        // slower setup work below — and before the kernel sees us.
        s5_fuse::preflight(&mountpoint)?;

        let mut resolved = self.resolve_vault(vault).await?;

        // Move the context out before passing the rest of `resolved`
        // into the rw spawn helper (TraversalContext isn't Copy).
        let context = std::mem::take(&mut resolved.context);
        let snapshot = Snapshot::new(
            resolved.root,
            resolved.read_store.clone(),
            context,
            resolved.root_plaintext_hash,
        );

        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let cancel_fut = async move {
            let _ = cancel_rx.await;
        };

        let mountpoint_for_task = mountpoint.clone();
        let join: JoinHandle<Result<()>> = if rw {
            self.spawn_rw_mount(
                vault.to_string(),
                resolved,
                snapshot,
                mountpoint_for_task,
                debounce_ms,
                cancel_fut,
            )
        } else {
            tokio::spawn(async move {
                s5_fuse::mount(&mountpoint_for_task, snapshot, false, true, cancel_fut)
                    .await
                    .with_context(|| format!("FUSE mount at {}", mountpoint_for_task.display()))
            })
        };

        let mount_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut mounts = self.mounts.write().await;
        mounts.insert(
            mount_id,
            ActiveMount {
                cancel: cancel_tx,
                join,
                mountpoint,
            },
        );
        Ok(mount_id)
    }

    /// Unmount the mount with the given `mount_id`. Returns `Err` if
    /// the id is unknown (idempotent at the wire level — the CLI
    /// surfaces the error string, but other mounts are unaffected).
    pub async fn unmount(&self, mount_id: u64) -> Result<()> {
        let active = {
            let mut mounts = self.mounts.write().await;
            mounts
                .remove(&mount_id)
                .ok_or_else(|| anyhow!("unknown mount_id {mount_id}"))?
        };
        // Best effort: sending the cancel triggers the mount task to
        // return; if it already exited (kernel-side eject) the receiver
        // is gone and `send` errors — that's fine, we still join below.
        let _ = active.cancel.send(());
        match active.join.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(e.context(format!("mount task for id {mount_id}"))),
            Err(e) => Err(anyhow!("mount task for id {mount_id} panicked: {e}")),
        }
    }

    fn spawn_rw_mount(
        &self,
        vault: String,
        resolved: ResolvedMount,
        snapshot: Snapshot,
        mountpoint: PathBuf,
        debounce_ms: u64,
        cancel_fut: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> JoinHandle<Result<()>> {
        let executor = self.executor.clone();
        let primary_store = resolved.primary_store.clone();
        let recipient_key_names = resolved.recipient_key_names;
        let recipient_pubkeys = resolved.recipient_pubkeys;
        let vault_root_file = resolved.vault_root_file;

        tokio::spawn(async move {
            // The on_mount callback fires once the FS is built but
            // before the kernel sees it; we use it to spawn the
            // debounce task with a clone of the WritableFs.
            let executor_for_cb = executor.clone();
            let vault_for_cb = vault.clone();
            let recipient_key_names_for_cb = recipient_key_names.clone();
            let recipient_pubkeys_for_cb = recipient_pubkeys.clone();
            let vault_root_file_for_cb = vault_root_file.clone();
            let on_mount = move |fs: s5_fuse::WritableFs| {
                tokio::spawn(async move {
                    s5_fuse::debounce::run(
                        fs,
                        std::time::Duration::from_millis(debounce_ms),
                        move |snapshot| {
                            let executor = executor_for_cb.clone();
                            let vault = vault_for_cb.clone();
                            let recipient_key_names = recipient_key_names_for_cb.clone();
                            let recipient_pubkeys = recipient_pubkeys_for_cb.clone();
                            let vault_root_file = vault_root_file_for_cb.clone();
                            async move {
                                publish_after_flush(
                                    executor,
                                    vault,
                                    snapshot,
                                    recipient_key_names,
                                    recipient_pubkeys,
                                    vault_root_file,
                                )
                                .await
                            }
                        },
                        std::future::pending::<()>(),
                    )
                    .await;
                });
            };

            s5_fuse::mount_rw(
                &mountpoint,
                snapshot,
                primary_store,
                false,
                true,
                on_mount,
                cancel_fut,
            )
            .await
            .with_context(|| format!("FUSE rw mount at {}", mountpoint.display()))
        })
    }

    async fn resolve_vault(&self, vault: &str) -> Result<ResolvedMount> {
        let ctx = self.executor.ctx();
        let config = ctx.config.read().await;

        let vault_cfg = config
            .vault
            .get(vault)
            .ok_or_else(|| anyhow!("vault '{vault}' not found in config"))?;

        let root_path = &vault_cfg.root_path;
        let vault_key_name = &vault_cfg.key;

        let identity_file = config
            .key
            .get(vault_key_name)
            .and_then(|k| k.identity_file.as_deref())
            .ok_or_else(|| {
                anyhow!(
                    "vault '{vault}' key '{vault_key_name}' has no identity_file — cannot decrypt local TN"
                )
            })?;

        if vault_cfg.blob_stores.is_empty() {
            bail!("vault '{vault}' has no blob_stores configured — nothing to mount from");
        }

        // Re-open vault stores in-process for the mount. The daemon
        // already has these stores in `ctx.stores`, but mount needs
        // them as raw `BlobsRead` (not `BlobStore`) so we can stack
        // them through `FallbackBlobsRead`. Reaching back into
        // `ctx.stores` directly would skip the meta-prepended chain.
        let meta_path = std::path::Path::new(root_path).join("meta");
        if !meta_path.exists() {
            bail!(
                "vault '{vault}' meta store not found at {}",
                meta_path.display()
            );
        }
        let meta_store: Arc<dyn BlobsRead> = Arc::new(BlobStore::new(
            s5_store_local::LocalStore::create(s5_store_local::LocalStoreConfig {
                base_path: meta_path.to_string_lossy().into_owned(),
            }),
        ));

        let mut concrete_stores: Vec<BlobStore> = Vec::new();
        for name in &vault_cfg.blob_stores {
            let store_cfg = config
                .store
                .get(name)
                .ok_or_else(|| anyhow!("blob_store '{name}' not declared in [store.*]"))?;
            // Until the mount path moves through a daemon-internal
            // store handle (rather than re-opening from config), we
            // only support local stores in-process.
            let local_cfg = match store_cfg {
                NodeConfigStore::Local(cfg) => cfg,
                _ => bail!(
                    "blob_store '{name}' is not a local store — daemon-side mount currently \
                     supports only local stores (remote backends will be added when \
                     the mount path uses the daemon's open store handles directly)"
                ),
            };
            concrete_stores.push(BlobStore::new(s5_store_local::LocalStore::create(
                local_cfg.clone(),
            )));
        }

        let mut blob_layers: Vec<Arc<dyn BlobsRead>> = concrete_stores
            .iter()
            .map(|s| Arc::new(s.clone()) as Arc<dyn BlobsRead>)
            .collect();
        let mut combined: Arc<dyn BlobsRead> = blob_layers.pop().unwrap();
        while let Some(primary) = blob_layers.pop() {
            combined = Arc::new(FallbackBlobsRead::new(primary, combined));
        }
        let read_store: Arc<dyn BlobsRead> = Arc::new(FallbackBlobsRead::new(meta_store, combined));

        let current_path = vault_root_path(root_path);
        let (root, root_plaintext_hash, context) =
            load_vault_root(&current_path, &[identity_file.to_string()])
                .with_context(|| format!("reading vault root for '{vault}'"))?
                .ok_or_else(|| {
                    anyhow!(
                        "vault '{vault}' has no snapshot to mount — run `vup +{vault} snap` first"
                    )
                })?;

        // Recipients (key names + age pubkeys) are only consumed by
        // the rw flush; resolving them up front keeps the rw branch
        // off the hot path and gives us one error site for missing
        // [key.*] entries.
        let recipient_key_names = vault_cfg.recipients.clone();
        let mut recipient_pubkeys = Vec::with_capacity(recipient_key_names.len());
        for name in &recipient_key_names {
            let pubkey = config
                .key
                .get(name)
                .map(|k| k.public_key.clone())
                .ok_or_else(|| {
                    anyhow!("vault '{vault}' recipient '{name}' has no public_key in [key.{name}]")
                })?;
            recipient_pubkeys.push(pubkey);
        }

        let primary_store = concrete_stores
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("vault '{vault}' has no blob_stores configured"))?;
        let vault_root_file = vault_root_path(root_path);

        Ok(ResolvedMount {
            root,
            root_plaintext_hash,
            context,
            read_store,
            primary_store,
            recipient_key_names,
            recipient_pubkeys,
            vault_root_file,
        })
    }
}

/// Everything `MountManager::mount` needs after vault resolution.
struct ResolvedMount {
    root: s5_core::Hash,
    root_plaintext_hash: Option<[u8; 32]>,
    context: s5_fs_v2::node::TraversalContext,
    read_store: Arc<dyn BlobsRead>,
    primary_store: BlobStore,
    recipient_key_names: Vec<String>,
    recipient_pubkeys: Vec<String>,
    vault_root_file: PathBuf,
}

/// On every debounced flush: persist the new snapshot's vault root to
/// disk (so the publish task can pick it up) and submit a
/// `TaskSpec::Publish` to the daemon's executor. We don't poll to
/// completion — that would block the next debounce cycle. The task
/// id is logged so daemon logs and the resulting registry entry stay
/// correlatable.
async fn publish_after_flush(
    executor: Arc<TaskExecutor>,
    vault: String,
    snapshot: Snapshot,
    recipient_key_names: Vec<String>,
    recipient_pubkeys: Vec<String>,
    vault_root_file: PathBuf,
) -> Result<()> {
    save_vault_root(&vault_root_file, &snapshot, &recipient_pubkeys)
        .with_context(|| format!("rw flush: saving vault root for +{vault}"))?;

    let (task_id, _) = executor
        .spawn(TaskSpec::Publish {
            vault: vault.clone(),
            keys: recipient_key_names,
        })
        .await
        .with_context(|| format!("rw flush: dispatching publish task for +{vault}"))?;

    tracing::info!(
        vault = %vault,
        task_id,
        snapshot_root = %snapshot.root().fmt_short(),
        "rw flush: published"
    );
    Ok(())
}

/// Compatibility stub kept for the existing call site in `S5Node`
/// startup. Mounts are now driven via the `MountVault` RPC, not at
/// node-start time, so this is a no-op.
pub async fn spawn_fuse_mounts(_node: &crate::S5Node) -> Result<()> {
    tracing::debug!("spawn_fuse_mounts: no-op (mounts now driven via MountVault RPC)");
    Ok(())
}
