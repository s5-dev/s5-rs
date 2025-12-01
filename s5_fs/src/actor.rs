//! The core actor implementation for managing directory state.

use crate::{
    FSResult,
    context::{DirContext, DirContextParentLink, DirHandlePath},
    dir::{DirV1, FileRef},
};
use anyhow::{Context, anyhow};
use s5_core::{Hash, StreamKey};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

mod listing;
mod merge;
mod persistence;
pub(crate) mod sharding;
mod snapshots;

pub(crate) type ListResult = FSResult<(Vec<(String, crate::api::CursorKind)>, Option<String>)>;

type Value = Option<FileRef>;

/// A trait for tasks that can be executed on a `FileRef` value.
pub(crate) trait Task: std::fmt::Debug {
    fn execute(self: Box<Self>, value: &mut Value);
}

/// A concrete `Task` that executes a function and sends the result back.
struct FunctionTask<R> {
    func: Box<dyn FnOnce(&mut Value) -> R + Send>,
    responder: oneshot::Sender<R>,
}

impl<R: Send> std::fmt::Debug for FunctionTask<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FunctionTask")
    }
}

impl<R: Send> Task for FunctionTask<R> {
    fn execute(self: Box<Self>, value: &mut Value) {
        let result = (self.func)(value);
        // The receiver might have been dropped if the caller doesn't care
        // about the result, so we ignore the potential error.
        let _ = self.responder.send(result);
    }
}

/// Messages sent from a `DirActorHandle` to a `DirActor`.
#[derive(Debug)]
pub(crate) enum ActorMessage {
    /// An operation to be performed on a path within the directory.
    PathOp {
        path: String,
        op: ActorMessageOp,
    },
    /// A message from a child actor to update its hash in the parent's directory listing.
    UpdateDirRefHash {
        path: DirHandlePath,
        hash: Hash,
    },
    SaveIfDirty {
        responder: oneshot::Sender<FSResult<Option<Hash>>>,
    },
    /// Opens (or creates) a subdirectory at the given logical path
    /// and returns a handle to its directory actor.
    OpenSubdir {
        path: String,
        responder: oneshot::Sender<FSResult<DirActorHandle>>,
    },
    /// Computes and persists a snapshot blob for the current directory
    /// state, returning its BLAKE3 hash. Works for any context.
    ExportSnapshotHash {
        responder: oneshot::Sender<FSResult<Hash>>,
    },
    /// Creates a named snapshot of the current root directory state.
    /// Only meaningful for the local FS5 root (`DirContextParentLink::LocalFile`).
    /// This message is only available on native platforms.
    #[cfg(not(target_arch = "wasm32"))]
    CreateSnapshot {
        responder: oneshot::Sender<FSResult<(String, Hash)>>,
    },
    /// Deletes a named snapshot from `snapshots.fs5.cbor` and unpins its
    /// `PinContext::LocalFsSnapshot` entry, if present.
    /// This message is only available on native platforms.
    #[cfg(not(target_arch = "wasm32"))]
    DeleteSnapshot {
        name: String,
        responder: oneshot::Sender<FSResult<()>>,
    },
    /// Lists entries with a cursor and limit.
    List {
        cursor: Option<String>,
        limit: usize,
        responder: oneshot::Sender<ListResult>,
    },
    /// Lists entries at a nested path.
    ListAt {
        path: String,
        cursor: Option<String>,
        limit: usize,
        responder: oneshot::Sender<ListResult>,
    },
    ExportSnapshot {
        responder: oneshot::Sender<FSResult<DirV1>>,
    },
    ExportMergedSnapshot {
        responder: oneshot::Sender<FSResult<DirV1>>,
    },
    /// Exports snapshot at a nested path.
    ExportSnapshotAt {
        path: String,
        responder: oneshot::Sender<FSResult<DirV1>>,
    },
    /// Exports merged snapshot at a nested path.
    ExportMergedSnapshotAt {
        path: String,
        responder: oneshot::Sender<FSResult<DirV1>>,
    },
    MergeSnapshot {
        snapshot: DirV1,
        responder: oneshot::Sender<FSResult<()>>,
    },
    SetAutosave {
        debounce_ms: u64,
    },
    AutosaveTick,
    MarkAsDirty,
    Shutdown {
        responder: oneshot::Sender<()>,
    },
}

/// The specific operations that can be performed via `ActorMessage::PathOp`.
#[derive(Debug)]
pub(crate) enum ActorMessageOp {
    /// An operation on a file, encapsulated as a `Task`.
    FileOp { task: Box<dyn Task + Send> },
    /// Creates a new subdirectory.
    CreateDir {
        enable_encryption: bool,
        responder: oneshot::Sender<FSResult<()>>,
    },
}

/// The actor that manages the state of a single directory.
struct DirActor {
    pub(super) context: DirContext,
    pub(super) receiver: mpsc::Receiver<ActorMessage>,
    pub(super) handle: Option<WeakDirActorHandle>,
    pub(super) state: DirV1,
    pub(super) autosave_debounce_ms: Option<u64>,
    pub(super) autosave_timer_active: bool,
    pub(super) dirty: bool,
    pub(super) initial_state: Option<DirV1>,
    pub(super) dir_handles: HashMap<String, DirActorHandle>,
    pub(super) dir_shard_handles: HashMap<u8, DirActorHandle>,

    /// Current hash of this directory's persisted snapshot, if known.
    ///
    /// For the local FS5 root (`DirContextParentLink::LocalFile`), this
    /// is used together with `DirContext.pins` to keep the live head
    /// (`PinContext::LocalFsHead`) up to date.
    pub(super) current_hash: Option<Hash>,

    /// Cached length of the last serialized directory state (unencrypted).
    /// Used as a cheap lower bound when deciding whether to shard.
    pub(super) last_serialized_len: usize,

    /// Approximate number of state mutations since the last sharding
    /// size check. Incremented when the directory is marked dirty.
    pub(super) shard_size_check_ops: u64,
}

impl Drop for DirActor {
    fn drop(&mut self) {
        // Actor teardown is silent; rely on tracing inside the
        // actor loop for diagnostics rather than printing here.
    }
}

impl DirActor {
    /// Creates a new actor.
    fn new(
        receiver: mpsc::Receiver<ActorMessage>,
        context: DirContext,
        initial_state: Option<DirV1>,
        autosave_debounce_ms: Option<u64>,
    ) -> Self {
        Self {
            receiver,
            handle: None,
            state: DirV1::new(),
            dirty: false,
            context,
            dir_handles: HashMap::new(),
            dir_shard_handles: HashMap::new(),
            initial_state,
            autosave_debounce_ms,
            autosave_timer_active: false,
            current_hash: None,
            last_serialized_len: 0,
            shard_size_check_ops: 0,
        }
    }

    /// The main loop for the actor, processing incoming messages.
    async fn run(&mut self) {
        if let Some(initial_state) = self.initial_state.take() {
            self.state = initial_state;
            // The first save will publish the hash to the parent/registry
            if let Err(e) = self.save(true).await {
                tracing::error!("Initial save failed: {}", e);
            }
        } else if let Err(e) = self.load().await {
            tracing::error!("Failed to load directory state: {}", e);
            // Abort this actor so callers observe a hard error
            // instead of an implicitly empty directory state.
            return;
        }

        while let Some(msg) = self.receiver.recv().await {
            if let ActorMessage::Shutdown { responder } = msg {
                if self.dirty {
                    self.shard_if_needed().await.ok();
                    if let Err(e) = self.save(false).await {
                        tracing::error!("shutdown save failed: {e}");
                    }
                }

                for handle in self.dir_handles.values() {
                    let _ = handle.shutdown().await;
                }
                for handle in self.dir_shard_handles.values() {
                    let _ = handle.shutdown().await;
                }
                let _ = responder.send(());
                break;
            }
            if let Err(e) = self.process_msg(msg).await {
                tracing::error!("Failed to process message: {}", e);
            }
        }

        // Safety net: if the actor loop exits (e.g. channel closed), ensure we save if dirty.
        if self.dirty {
            self.shard_if_needed().await.ok();
            if let Err(e) = self.save(false).await {
                tracing::error!("shutdown save failed: {e}");
            }
        }
    }

    /// Routes a path to a child actor (either a direct subdirectory or a shard).
    /// Returns `Some((handle, remaining_path))` if a child is found, or `None` if
    /// the path refers to a local entry (or doesn't exist).
    async fn route_to_child(&mut self, path: &str) -> FSResult<Option<(DirActorHandle, String)>> {
        let (dir_name, rest) = match path.split_once('/') {
            Some((d, r)) => (d, r.to_string()),
            None => (path, String::new()),
        };

        // Check sharding first (if enabled, dirs are in shards)
        if let Some(shard_level) = self.state.header.shard_level {
            let index = crate::actor::sharding::shard_bucket_for(dir_name, shard_level);
            if let Some(shards) = &self.state.header.shards
                && shards.contains_key(&index)
            {
                let handle = self.open_dir_shard(index, None).await?;
                // When routing to a shard, we pass the FULL path, because the shard
                // acts as a container for the entry.
                return Ok(Some((handle, path.to_string())));
            }
        }

        // Check direct child
        if self.state.dirs.contains_key(dir_name) {
            let handle = self.open_dir(dir_name, None).await?;
            // When routing to a direct child directory, we pass the REST of the path,
            // because we have already traversed 'dir_name'.
            return Ok(Some((handle, rest)));
        }

        Ok(None)
    }

    /// Processes a single message.
    async fn process_msg(&mut self, msg: ActorMessage) -> FSResult<()> {
        match msg {
            ActorMessage::PathOp { path, op } => {
                if let Some((handle, next_path)) = self.route_to_child(&path).await? {
                    let _ = handle
                        .send_msg(ActorMessage::PathOp {
                            path: next_path,
                            op,
                        })
                        .await;
                    return Ok(());
                }

                match op {
                    // TODO: Add support for read-only file operations.
                    ActorMessageOp::FileOp { task } => {
                        let mut value = self.state.files.remove(&path);
                        task.execute(&mut value);
                        if let Some(file_ref) = value {
                            self.state.files.insert(path.clone(), file_ref);
                            self.check_auto_promote(&path).await?;
                        }
                        self.mark_as_dirty().await;
                    }
                    ActorMessageOp::CreateDir {
                        enable_encryption,
                        responder,
                    } => {
                        let result = self.create_dir_at(&path, enable_encryption).await;
                        let _ = responder.send(result);
                    }
                }
            }
            ActorMessage::OpenSubdir { path, responder } => {
                let result: FSResult<DirActorHandle> = async {
                    if let Some((handle, next_path)) = self.route_to_child(&path).await? {
                        if next_path.is_empty() {
                            return Ok(handle);
                        }
                        let (tx, rx) = oneshot::channel();
                        handle
                            .send_msg(ActorMessage::OpenSubdir {
                                path: next_path,
                                responder: tx,
                            })
                            .await?;
                        return rx.await?;
                    }

                    // If we are here, the first component of the path does not exist.
                    // We must create it and recurse.
                    let (first, rest) = match path.split_once('/') {
                        Some((f, r)) => (f, r),
                        None => (path.as_str(), ""),
                    };

                    if !self.state.dirs.contains_key(first) {
                        let inherit_encryption = self.context.encryption_type.is_some();
                        self.create_dir_at(first, inherit_encryption).await?;
                    }
                    let handle = self.open_dir(first, None).await?;

                    if rest.is_empty() {
                        Ok(handle)
                    } else {
                        let (tx, rx) = oneshot::channel();
                        handle
                            .send_msg(ActorMessage::OpenSubdir {
                                path: rest.to_string(),
                                responder: tx,
                            })
                            .await?;
                        rx.await?
                    }
                }
                .await;
                let _ = responder.send(result);
            }
            ActorMessage::UpdateDirRefHash { path, hash } => {
                let dir_ref = match &path {
                    DirHandlePath::Path(path) => self
                        .state
                        .dirs
                        .get_mut(path)
                        .context("dir does not exist")?,
                    DirHandlePath::Shard(index) => {
                        let shards = self
                            .state
                            .header
                            .shards
                            .as_mut()
                            .context("dir shard not exist")?;
                        shards.get_mut(index).context("dir shard not exist")?
                    }
                };

                if dir_ref.hash == *hash.as_bytes() {
                    return Ok(());
                }

                dir_ref.hash = hash.into();

                self.mark_as_dirty().await;
            }
            ActorMessage::SaveIfDirty { responder } => {
                let result = self.save_if_dirty().await;
                let _ = responder.send(result);
            }
            ActorMessage::List {
                cursor,
                limit,
                responder,
            } => {
                let result = self.list_entries(cursor.as_deref(), limit).await;
                let _ = responder.send(result);
            }
            ActorMessage::ListAt {
                path,
                cursor,
                limit,
                responder,
            } => {
                let result = self.list_at_path(path, cursor, limit).await;
                let _ = responder.send(result);
            }
            ActorMessage::ExportSnapshot { responder } => {
                let _ = responder.send(Ok(self.state.clone()));
            }
            ActorMessage::ExportMergedSnapshot { responder } => {
                let result = self.export_merged_snapshot().await;
                let _ = responder.send(result);
            }
            ActorMessage::ExportSnapshotAt { path, responder } => {
                let result = self.export_snapshot_at(path).await;
                let _ = responder.send(result);
            }
            ActorMessage::ExportMergedSnapshotAt { path, responder } => {
                let result = self.export_merged_snapshot_at(path).await;
                let _ = responder.send(result);
            }
            ActorMessage::MergeSnapshot {
                snapshot,
                responder,
            } => {
                let result = self.merge_snapshot(snapshot).await;
                let _ = responder.send(result);
            }
            ActorMessage::SetAutosave { debounce_ms } => {
                self.autosave_debounce_ms = Some(debounce_ms);
                // Propagate to children
                for handle in self.dir_handles.values() {
                    let _ = handle
                        .send_msg(ActorMessage::SetAutosave { debounce_ms })
                        .await;
                }
                for handle in self.dir_shard_handles.values() {
                    let _ = handle
                        .send_msg(ActorMessage::SetAutosave { debounce_ms })
                        .await;
                }
            }
            ActorMessage::AutosaveTick => {
                self.autosave_timer_active = false;
                if self.dirty {
                    self.shard_if_needed().await?;
                    match self.save(true).await {
                        Ok(_) => {
                            self.dirty = false;
                        }
                        Err(e) => {
                            tracing::error!("autosave failed: {e}");
                        }
                    }
                }
            }
            /*   ActorMessage::GetShardLevel { responder } => {
                responder.send(self.state.header.shard_level).unwrap();
            } */
            ActorMessage::MarkAsDirty => {
                self.mark_as_dirty().await;
            }
            ActorMessage::ExportSnapshotHash { responder } => {
                let result = self.export_snapshot_hash().await;
                let _ = responder.send(result);
            }
            #[cfg(not(target_arch = "wasm32"))]
            ActorMessage::CreateSnapshot { responder } => {
                let result = self.create_snapshot().await;
                let _ = responder.send(result);
            }
            #[cfg(not(target_arch = "wasm32"))]
            ActorMessage::DeleteSnapshot { name, responder } => {
                let result = self.delete_snapshot(name).await;
                let _ = responder.send(result);
            }
            ActorMessage::Shutdown { .. } => {
                // Handled in run loop
            }
        }
        if let Some(ms) = self.autosave_debounce_ms
            && self.dirty
            && !self.autosave_timer_active
        {
            self.autosave_timer_active = true;
            if let Some(weak) = &self.handle {
                let weak_handle = weak.clone();
                crate::spawn::spawn_delayed(ms, async move {
                    if let Some(handle) = weak_handle.upgrade() {
                        let _ = handle.sender.send(ActorMessage::AutosaveTick).await;
                    }
                });
            }
        }
        Ok(())
    }

    async fn mark_as_dirty(&mut self) {
        self.shard_size_check_ops = self.shard_size_check_ops.saturating_add(1);
        if !self.dirty {
            self.dirty = true;
            if let DirContextParentLink::DirHandle { handle, .. } = &self.context.link
                && let Some(handle) = handle.upgrade()
            {
                let _ = handle.send_msg(ActorMessage::MarkAsDirty).await;
            }
        }
    }

    /// Helper to create a logical subdirectory at `path` under this actor.
    ///
    /// - Idempotent: if the directory already exists, this is a no-op.
    /// - Partitions files with the `path/` prefix into a new DirV1 for the
    ///   subdirectory and wires up a `DirRef` in this directory.
    async fn create_dir_at(&mut self, path: &str, enable_encryption: bool) -> FSResult<()> {
        if self.state.dirs.contains_key(path) {
            return Ok(());
        }

        // Move `path/` files into a new child directory snapshot.

        let new_dir_state = self.extract_child_dir_state(path);

        let dir_ref = self.build_child_dir_ref(enable_encryption);
        self.state.dirs.insert(path.to_owned(), dir_ref);

        self.open_dir(path, Some(new_dir_state)).await?;
        self.mark_as_dirty().await;
        Ok(())
    }

    /// Gets a handle to a subdirectory actor, creating it if necessary.
    async fn open_dir(
        &mut self,
        sub_path: &str,
        initial_state: Option<DirV1>,
    ) -> anyhow::Result<DirActorHandle> {
        if let Some(handle) = self.dir_handles.get(sub_path) {
            return Ok(handle.clone());
        }

        tracing::debug!("open_dir: opening {}", sub_path);
        let dir_ref = self.state.dirs.get(sub_path).context("dir not found")?;

        let link = match dir_ref.ref_type() {
            crate::dir::DirRefType::Blake3Hash => DirContextParentLink::DirHandle {
                shard_level: 0,
                path: DirHandlePath::Path(sub_path.to_owned()),
                handle: self.handle.clone().context("actor has no handle")?,
                initial_hash: dir_ref.hash,
            },
            crate::dir::DirRefType::RegistryKey => {
                let key = StreamKey::PublicKeyEd25519(dir_ref.hash);
                if let Some(handle) = self.context.registry_dir_handles.get(&key) {
                    return Ok(handle.clone());
                }
                DirContextParentLink::RegistryKey {
                    public_key: key,
                    signing_key: self.context.signing_key.clone(),
                }
            }
        };

        let context = self.context.with_new_ref(dir_ref, link);
        // TODO: Propagate autosave and ensure recursive save/dirty semantics are correct
        let handle = DirActorHandle::spawn(context, initial_state, self.autosave_debounce_ms);

        match dir_ref.ref_type() {
            crate::dir::DirRefType::Blake3Hash => {
                self.dir_handles.insert(sub_path.to_owned(), handle.clone());
            }
            crate::dir::DirRefType::RegistryKey => {
                let key = StreamKey::PublicKeyEd25519(dir_ref.hash);
                self.context
                    .registry_dir_handles
                    .insert(key, handle.clone());
                // Track registry-backed dirs as children so saves cascade.
                self.dir_handles.insert(sub_path.to_owned(), handle.clone());
            }
        }
        Ok(handle)
    }

    async fn open_dir_shard(
        &mut self,
        shard_index: u8,
        initial_state: Option<DirV1>,
    ) -> anyhow::Result<DirActorHandle> {
        self.open_dir_shard_impl(shard_index, initial_state).await
    }
}

/// A handle for communicating with a `DirActor`. It can be cloned and sent across threads.
#[derive(Clone, Debug)]
pub struct DirActorHandle {
    sender: mpsc::Sender<ActorMessage>,
}

impl DirActorHandle {
    /// Spawns a new `DirActor` task and returns a handle to it.
    pub(crate) fn spawn(
        context: DirContext,
        initial_state: Option<DirV1>,
        autosave_debounce_ms: Option<u64>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(1024);
        let mut actor = DirActor::new(receiver, context, initial_state, autosave_debounce_ms);
        let handle = Self { sender };
        actor.handle = Some(handle.downgrade());

        crate::spawn::spawn_task(async move {
            actor.run().await;
        });

        handle
    }

    /// Sends a message to the actor.
    pub(crate) async fn send_msg(&self, msg: ActorMessage) -> FSResult<()> {
        self.sender
            .send(msg)
            .await
            .map_err(|_| anyhow!("Actor task has been closed."))?;
        Ok(())
    }

    pub(crate) async fn save_if_dirty(&self) -> FSResult<Option<Hash>> {
        let (responder, receiver) = oneshot::channel();
        let msg = ActorMessage::SaveIfDirty { responder };

        if self.sender.send(msg).await.is_err() {
            return Err(anyhow!("Actor task has been closed."));
        }

        receiver.await?
    }

    /// Submits a function to be executed by the actor on a `FileRef` at the given path.
    pub(crate) async fn execute<F, R>(&self, path: String, f: F) -> FSResult<R>
    where
        F: FnOnce(&mut Value) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (responder, receiver) = oneshot::channel();
        let task = Box::new(FunctionTask {
            func: Box::new(f),
            responder,
        });
        let msg = ActorMessage::PathOp {
            path,
            op: ActorMessageOp::FileOp { task },
        };

        if self.sender.send(msg).await.is_err() {
            return Err(anyhow!("Actor task has been closed."));
        }

        Ok(receiver.await?)
    }

    /// Submits a function to be executed by the actor on a `FileRef` at the given path.
    pub(crate) async fn execute_and_forget<F, R>(&self, path: String, f: F) -> FSResult<()>
    where
        F: FnOnce(&mut Value) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (responder, _) = oneshot::channel();
        let task = Box::new(FunctionTask {
            func: Box::new(f),
            responder,
        });
        let msg = ActorMessage::PathOp {
            path,
            op: ActorMessageOp::FileOp { task },
        };

        if self.sender.send(msg).await.is_err() {
            return Err(anyhow!("Actor task has been closed."));
        }
        Ok(())
    }

    pub(crate) async fn shutdown(&self) -> FSResult<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .sender
            .send(ActorMessage::Shutdown { responder: tx })
            .await;
        let _ = rx.await;
        Ok(())
    }

    pub fn downgrade(&self) -> WeakDirActorHandle {
        WeakDirActorHandle {
            sender: self.sender.downgrade(),
        }
    }
}

#[derive(Clone)]
pub struct WeakDirActorHandle {
    sender: mpsc::WeakSender<ActorMessage>,
}

impl WeakDirActorHandle {
    pub fn upgrade(&self) -> Option<DirActorHandle> {
        self.sender
            .upgrade()
            .map(|sender| DirActorHandle { sender })
    }
}
