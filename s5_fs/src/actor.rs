//! The core actor implementation for managing directory state.

use crate::{
    FSResult,
    context::{DirContext, DirContextParentLink, DirHandlePath},
    dir::{DirRef, DirV1, ENCRYPTION_TYPE_XCHACHA20_POLY1305, FileRef},
};
use anyhow::{Context, anyhow};
use bytes::{BufMut, Bytes, BytesMut};
use chacha20poly1305::{
    AeadCore, KeyInit, XChaCha20Poly1305,
    aead::{Aead, OsRng, rand_core::RngCore},
};
use chrono::Utc;
use futures::future::join_all;
use minicbor::bytes::ByteVec;
use s5_core::{Hash, StreamKey, StreamMessage, api::streams::RegistryApi, stream::MessageType};
use std::{
    collections::{BTreeMap, HashMap},
    io::{self, Read, Write},
};
use tempfile::NamedTempFile;
use tokio::sync::{mpsc, oneshot};

type Value = Option<FileRef>;

const MAX_DIR_BYTES_BEFORE_SHARD: usize = 65_536;

/// A trait for tasks that can be executed on a `FileRef` value.
pub trait Task: std::fmt::Debug {
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
pub enum ActorMessage {
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
        responder: oneshot::Sender<()>,
    },
    MarkAsDirty,
}

/// The specific operations that can be performed via `ActorMessage::PathOp`.
#[derive(Debug)]
pub enum ActorMessageOp {
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
    context: DirContext,
    receiver: mpsc::Receiver<ActorMessage>,
    handle: Option<DirActorHandle>,
    state: DirV1,
    autosave: bool,
    dirty: bool,
    initial_state: Option<DirV1>,
    dir_handles: HashMap<String, DirActorHandle>,
    dir_shard_handles: HashMap<u8, DirActorHandle>,

    hashes_to_delete: Vec<Hash>,
}

impl DirActor {
    /// Creates a new actor.
    fn new(
        receiver: mpsc::Receiver<ActorMessage>,
        context: DirContext,
        initial_state: Option<DirV1>,
        autosave: bool,
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
            autosave,
            hashes_to_delete: vec![],
        }
    }

    /// The main loop for the actor, processing incoming messages.
    async fn run(&mut self) {
        if let Some(initial_state) = self.initial_state.take() {
            self.state = initial_state;
            // The first save will publish the hash to the parent/registry
            if let Err(e) = self.save().await {
                log::error!("Initial save failed: {}", e);
            }
        } else if let Err(e) = self.load().await {
            log::error!("Failed to load directory state: {}", e);
        }

        while let Some(msg) = self.receiver.recv().await {
            if let Err(e) = self.process_msg(msg).await {
                log::error!("Failed to process message: {}", e);
            }
        }
    }

    /// Processes a single message.
    async fn process_msg(&mut self, msg: ActorMessage) -> FSResult<()> {
        /*    match &self.context.link {
            DirContextParentLink::DirHandle { path, .. } => {
                // Debug-only: log path and message
                // println!("{:?} {msg:?}", path);
            }
            _ => {}
        } */

        match msg {
            ActorMessage::PathOp { path, op } => {
                if let Some((dir_name, rest_of_path)) = path.split_once('/') {
                    if let Some(shard_level) = self.state.header.shard_level {
                        let name_hash = blake3::hash(dir_name.as_bytes());
                        let index = name_hash.as_bytes()[shard_level as usize];
                        if let Some(_) = &self.state.shards.get(&index) {
                            let handle = self.open_dir_shard(index, None).await?;
                            let _ = handle
                                .send_msg(ActorMessage::PathOp {
                                    path: path.clone(),
                                    op,
                                })
                                .await;
                            return Ok(());
                        }
                    }
                    if self.state.dirs.contains_key(dir_name) {
                        let handle = self.open_dir(dir_name, None).await?;
                        let _ = handle
                            .send_msg(ActorMessage::PathOp {
                                path: rest_of_path.to_string(),
                                op,
                            })
                            .await;
                        return Ok(());
                    }
                }

                match op {
                    // TODO: Add support for read-only file operations.
                    ActorMessageOp::FileOp { task } => {
                        let mut value = self.state.files.remove(&path);
                        task.execute(&mut value);
                        if let Some(file_ref) = value {
                            self.state.files.insert(path, file_ref);
                        }
                        self.mark_as_dirty().await;
                    }
                    ActorMessageOp::CreateDir {
                        enable_encryption,
                        responder,
                    } => {
                        let result = async {
                            if self.state.dirs.contains_key(&path) {
                                return Ok(()); // Idempotent
                            }
                            let prefix = format!("{}/", path);
                            let (matching, other): (
                                BTreeMap<String, FileRef>,
                                BTreeMap<String, FileRef>,
                            ) = self
                                .state
                                .files
                                .clone()
                                .into_iter()
                                .partition(|(k, _v)| k.starts_with(&prefix));

                            let mut new_dir_state = DirV1::new();
                            for (file_path, file_ref) in matching {
                                let sub_path = file_path
                                    .strip_prefix(&prefix)
                                    .expect("prefix already verified by partition")
                                    .to_string();
                                new_dir_state.files.insert(sub_path, file_ref);
                            }

                            // let signing_key = SigningKey::generate(&mut OsRng);
                            // let public_key: VerifyingKey = (&signing_key).into();

                            let mut keys = BTreeMap::new();
                            if enable_encryption {
                                let key: [u8; 32] =
                                    XChaCha20Poly1305::generate_key(&mut OsRng).into();
                                keys.insert(0x0e, ByteVec::from(key.to_vec()));
                            }
                            let mut registry_pointer = [0u8; 32];
                            OsRng.fill_bytes(&mut registry_pointer);
                            // keys.insert(0x0c, ByteVec::from(signing_key.as_bytes().to_vec()));

                            let now = Utc::now();
                            let dir_ref = DirRef {
                                encryption_type: if enable_encryption {
                                    Some(ENCRYPTION_TYPE_XCHACHA20_POLY1305)
                                } else {
                                    None
                                },
                                extra: None,
                                hash: registry_pointer,
                                ref_type: crate::dir::DirRefType::RegistryKey,
                                keys: Some(keys),
                                ts_seconds: Some(now.timestamp() as u32),
                                ts_nanos: Some(now.timestamp_subsec_nanos() as u32),
                            };
                            self.state.dirs.insert(path.to_owned(), dir_ref);
                            self.state.files = other;

                            self.open_dir(&path, Some(new_dir_state)).await?;
                            Ok(())
                        }
                        .await;
                        let _ = responder.send(result);
                        self.mark_as_dirty().await;
                    }
                }
            }
            ActorMessage::UpdateDirRefHash { path, hash } => {
                let mut dir_ref = match &path {
                    DirHandlePath::Path(path) => {
                        self.state.dirs.remove(path).context("dir does not exist")?
                    }
                    DirHandlePath::Shard(index) => self
                        .state
                        .shards
                        .remove(&index)
                        .context("dir shard not exist")?,
                };

                if dir_ref.hash != [0; 32] {
                    self.hashes_to_delete.push(dir_ref.hash.into());
                }

                dir_ref.hash = hash.into();

                match path {
                    DirHandlePath::Path(path) => self.state.dirs.insert(path, dir_ref),
                    DirHandlePath::Shard(index) => self.state.shards.insert(index, dir_ref),
                };
                self.mark_as_dirty().await;
            }
            ActorMessage::SaveIfDirty { responder } => {
                if self.dirty {
                    // println!("[fs] saving dir");
                    self.shard_if_needed().await?;

                    join_all(self.dir_shard_handles.values().map(|h| h.save_if_dirty())).await;

                    join_all(self.dir_handles.values().map(|h| h.save_if_dirty())).await;

                    self.save().await?;

                    for hash in &self.hashes_to_delete {
                        self.context.meta_blob_store.delete(*hash).await?;
                    }
                    self.hashes_to_delete.clear();

                    self.dirty = false;
                }
                let _ = responder.send(());
            }
            /*   ActorMessage::GetShardLevel { responder } => {
                responder.send(self.state.header.shard_level).unwrap();
            } */
            ActorMessage::MarkAsDirty => {
                self.mark_as_dirty().await;
            }
        }
        if self.autosave {
            self.shard_if_needed().await?;
            self.save().await?;
        }
        Ok(())
    }

    async fn mark_as_dirty(&mut self) {
        if !self.dirty {
            self.dirty = true;
            match &self.context.link {
                DirContextParentLink::DirHandle { handle, .. } => {
                    let _ = handle.send_msg(ActorMessage::MarkAsDirty).await;
                }
                _ => {}
            }
        }
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

        let dir_ref = self.state.dirs.get(sub_path).context("dir not found")?;

        let link = match dir_ref.ref_type {
            crate::dir::DirRefType::Blake3Hash => DirContextParentLink::DirHandle {
                shard_level: 0,
                path: DirHandlePath::Path(sub_path.to_owned()),
                handle: self.handle.clone().context("actor has no handle")?,
                initial_hash: dir_ref.hash,
            },
            crate::dir::DirRefType::RegistryKey => {
                let key = StreamKey::Local(dir_ref.hash);
                if let Some(handle) = self.context.registry_dir_handles.get(&key) {
                    return Ok(handle.clone());
                }
                DirContextParentLink::RegistryKey {
                    public_key: key,
                    signing_key: Some(crate::context::SigningKey([0u8; 32])),
                }
            }
        };

        let context = self.context.with_new_ref(dir_ref, link);
        // TODO: Propagate autosave and ensure recursive save/dirty semantics are correct
        let handle = DirActorHandle::spawn(context, initial_state, self.autosave);

        match dir_ref.ref_type {
            crate::dir::DirRefType::Blake3Hash => {
                self.dir_handles.insert(sub_path.to_owned(), handle.clone());
            }
            crate::dir::DirRefType::RegistryKey => {
                let key = StreamKey::Local(dir_ref.hash);
                self.context
                    .registry_dir_handles
                    .insert(key, handle.clone());
            }
        }
        Ok(handle)
    }

    async fn open_dir_shard(
        &mut self,
        shard_index: u8,
        initial_state: Option<DirV1>,
    ) -> anyhow::Result<DirActorHandle> {
        if let Some(handle) = self.dir_shard_handles.get(&shard_index) {
            return Ok(handle.clone());
        }

        let dir_ref = self
            .state
            .shards
            .get(&shard_index)
            .context("shard not found")?;

        let link = match dir_ref.ref_type {
            crate::dir::DirRefType::Blake3Hash => DirContextParentLink::DirHandle {
                shard_level: self.state.header.shard_level
                    .ok_or_else(|| anyhow!("missing shard level in parent when opening shard"))? + 1,
                path: DirHandlePath::Shard(shard_index),
                handle: self.handle.clone().context("actor has no handle")?,
                initial_hash: dir_ref.hash,
            },
            _ => return Err(anyhow!("dir shards can only be blake3 hash dir refs")),
        };

        let context = self.context.with_new_ref(dir_ref, link);
        // TODO: Propagate autosave and ensure recursive save/dirty semantics are correct
        let handle = DirActorHandle::spawn(context, initial_state, self.autosave);

        self.dir_shard_handles.insert(shard_index, handle.clone());
        Ok(handle)
    }

    /// Loads the directory state from storage.
    async fn load(&mut self) -> FSResult<()> {
        self.state = match &mut self.context.link {
            DirContextParentLink::LocalFile { file, .. } => {
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer)?;
                DirV1::from_bytes(&buffer)?
            }
            DirContextParentLink::DirHandle { initial_hash, .. } => {
                let bytes = self
                    .context
                    .meta_blob_store
                    .read_as_bytes((*initial_hash).into(), 0, None)
                    .await
                    .context("while reading from blob store")?;
                DirV1::from_bytes(&Self::decrypt_if_needed(bytes, &self.context)?)?
            }
            DirContextParentLink::RegistryKey { public_key, .. } => {
                if let Some(entry) = self.context.registry.get(public_key).await? {
                    let bytes = self
                        .context
                        .meta_blob_store
                        .read_as_bytes(entry.hash.into(), 0, None)
                        .await?;
                    DirV1::from_bytes(&Self::decrypt_if_needed(bytes, &self.context)?)?
                } else {
                    DirV1::new()
                }
            }
        };
        Ok(())
    }

    /// Decrypts directory bytes if encryption is enabled.
    fn decrypt_if_needed(bytes: Bytes, context: &DirContext) -> FSResult<Bytes> {
        if let Some(enc_type) = context.encryption_type {
            if enc_type == ENCRYPTION_TYPE_XCHACHA20_POLY1305 {
                let encryption_key = context
                    .keys
                    .get(&0x0e)
                    .ok_or_else(|| anyhow!("missing encryption key 0x0e for XChaCha20-Poly1305"))?;
                let cipher = XChaCha20Poly1305::new(encryption_key.as_ref().into());
                let nonce = &bytes[0..24];
                let plaintext = cipher
                    .decrypt(nonce.into(), &bytes[24..])
                    .map_err(|e| anyhow!("Failed to decrypt directory: {}", e))?;
                Ok(plaintext.into())
            } else {
                Err(anyhow!("encryption type {} not supported", enc_type))
            }
        } else {
            Ok(bytes)
        }
    }

    async fn shard_if_needed(&mut self) -> FSResult<()> {
        // TODO: Account for encryption overhead in size threshold
        if self.state.to_bytes()?.len() >= MAX_DIR_BYTES_BEFORE_SHARD {
            self.shard().await?;
        }

        Ok(())
    }

    async fn shard(&mut self) -> FSResult<()> {
        tracing::debug!("shard");
        // let mut shards = self.state.shards;

        if self.state.header.shard_level.is_none() {
            let shard_level = match &self.context.link {
                DirContextParentLink::DirHandle { shard_level, .. } => {
                    *shard_level
                    /*  let (responder, receiver) = oneshot::channel();
                    let msg = ActorMessage::GetShardLevel { responder };
                    handle.send_msg(msg).await;

                    if let Some(parent_shard_level) = receiver.await? {
                        parent_shard_level + 1
                    } else {
                        0
                    } */
                }
                _ => 0,
            };
            self.state.header.shard_level = Some(shard_level);
            tracing::debug!("shard_level {shard_level}");

            /* for i in 0..255 {
                self.state.shards.insert(i, DirRef::new_empty());
            } */

            let mut dirs = vec![DirV1::new(); 256];

            for dir in &self.state.dirs {
                let name_hash = blake3::hash(dir.0.as_bytes());
                let index = name_hash.as_bytes()[shard_level as usize];
                // TODO(perf): Avoid cloning here; consider borrowing or moving
                dirs[index as usize]
                    .dirs
                    .insert(dir.0.to_owned(), dir.1.to_owned());
            }
            for file in &self.state.files {
                let name_hash = blake3::hash(file.0.as_bytes());
                let index = name_hash.as_bytes()[shard_level as usize];
                dirs[index as usize]
                    .files
                    .insert(file.0.to_owned(), file.1.to_owned());
            }
            tracing::debug!("created new dirs");

            for (i, shard_state) in dirs.into_iter().enumerate() {
                self.state.shards.insert(i as u8, DirRef::new_empty());
                let shard = self.open_dir_shard(i as u8, Some(shard_state)).await?;
                shard.save_if_dirty().await?;
            }

            tracing::debug!("creating shards..");
            while let Some(msg) = self.receiver.recv().await {
                let future = self.process_msg(msg);
                if let Err(e) = Box::pin(future).await {
                    log::error!("Failed to process message: {}", e);
                }

                let mut all_set = true;
                for shard in &self.state.shards {
                    if shard.1.hash == [0; 32] {
                        all_set = false;
                    }
                }
                if all_set {
                    break;
                };

                /* if self.state.shards.len() == 256 {
                    break;
                } */
            }
            tracing::debug!("created shards!");

            self.state.dirs.clear();
            self.state.files.clear();
        } else {
            return Err(anyhow!("already sharded; cannot shard again"));
        }

        Ok(())
    }

    /// Saves the current directory state to storage.
    async fn save(&mut self) -> FSResult<()> {
        let bytes: Bytes = if let Some(enc_type) = self.context.encryption_type {
            if enc_type == ENCRYPTION_TYPE_XCHACHA20_POLY1305 {
                let encryption_key = self
                    .context
                    .keys
                    .get(&0x0e)
                    .ok_or_else(|| anyhow!("missing encryption key 0x0e for XChaCha20-Poly1305"))?;
                let cipher = XChaCha20Poly1305::new(encryption_key.as_ref().into());
                let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
                let ciphertext = cipher
                    .encrypt(&nonce, &self.state.to_bytes()?[..])
                    .map_err(|e| anyhow!("Failed to encrypt directory: {}", e))?;
                let mut bytes = BytesMut::new();
                bytes.put_slice(&nonce);
                bytes.put_slice(&ciphertext);
                bytes.into()
            } else {
                return Err(anyhow!("encryption type {} not supported", enc_type));
            }
        } else {
            let mut random_id = [0u8; 16];
            OsRng.fill_bytes(&mut random_id);

            self.state.header.random_id = Some(random_id);

            self.state.to_bytes()?
        };

        match &mut self.context.link {
            DirContextParentLink::LocalFile { path, .. } => {
                let parent_dir = path.parent().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "Could not find parent directory")
                })?;
                let mut temp_file = NamedTempFile::new_in(parent_dir)?;
                temp_file.write_all(&bytes)?;
                temp_file.as_file().sync_all()?;
                temp_file.persist(path)?;
            }
            DirContextParentLink::DirHandle {
                path,
                handle,
                shard_level: _,
                initial_hash,
            } => {
                let hash = self.context.meta_blob_store.import_bytes(bytes).await?;
                handle
                    .send_msg(ActorMessage::UpdateDirRefHash {
                        path: path.clone(),
                        hash: hash.hash,
                    })
                    .await?;
                 // TODO: Ensure parent save ordering is correct

                initial_hash.copy_from_slice(hash.hash.as_bytes());
            }
            DirContextParentLink::RegistryKey {
                public_key,
                signing_key,
            } => {
                let hash = self.context.meta_blob_store.import_bytes(bytes).await?;
                if let Some(_) = signing_key.as_ref() {
                    let current = self.context.registry.get(public_key).await?;
                    let entry = StreamMessage::new(
                        MessageType::Registry,
                        *public_key,
                        current.map_or_else(|| 0, |v| v.revision + 1),
                        hash.hash,
                        Box::new([]),
                        None,
                    )?;
                    self.context.registry.set(entry).await?;
                }
            }
        }
        Ok(())
    }
}

/// A handle for communicating with a `DirActor`. It can be cloned and sent across threads.
#[derive(Clone)]
pub struct DirActorHandle {
    sender: mpsc::Sender<ActorMessage>,
}

impl DirActorHandle {
    /// Spawns a new `DirActor` task and returns a handle to it.
    pub fn spawn(context: DirContext, initial_state: Option<DirV1>, autosave: bool) -> Self {
        let (sender, receiver) = mpsc::channel(1024);
        let mut actor = DirActor::new(receiver, context, initial_state, autosave);
        let handle = Self { sender };
        actor.handle = Some(handle.clone());

        tokio::spawn(async move {
            actor.run().await;
        });

        handle
    }

    /// Sends a message to the actor.
    pub async fn send_msg(&self, msg: ActorMessage) -> FSResult<()> {
        self
            .sender
            .send(msg)
            .await
            .map_err(|_| anyhow!("Actor task has been closed."))?;
        Ok(())
    }

    pub async fn save_if_dirty(&self) -> FSResult<()> {
        let (responder, receiver) = oneshot::channel();
        let msg = ActorMessage::SaveIfDirty { responder };

        // tracing::info!("save_if_dirty1");
        if self.sender.send(msg).await.is_err() {
            return Err(anyhow!("Actor task has been closed."));
        }
        // tracing::info!("save_if_dirty2");

        Ok(receiver.await?)
    }

    /// Submits a function to be executed by the actor on a `FileRef` at the given path.
    pub async fn execute<F, R>(
        &self,
        path: String,
        f: F,
    ) -> FSResult<R>
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
    pub async fn execute_and_forget<F, R>(&self, path: String, f: F) -> FSResult<()>
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
}
