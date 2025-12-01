use anyhow::{Context, anyhow};
use bytes::Bytes;
use chacha20poly1305::KeyInit;
use chacha20poly1305::XChaCha20Poly1305;
use chacha20poly1305::aead::OsRng;
use ed25519::signature::Signer;
#[cfg(not(target_arch = "wasm32"))]
use tempfile::NamedTempFile;

use crate::{
    FSResult,
    context::DirContextParentLink,
    dir::{DirV1, ENCRYPTION_TYPE_XCHACHA20_POLY1305},
};
#[cfg(not(target_arch = "wasm32"))]
use s5_core::PinContext;
use s5_core::{Hash, MessageType, StreamMessage};

use super::{DirActor, DirActorHandle};
use futures::future::join_all;

type EncodedDir = (Bytes, Option<std::collections::BTreeMap<u8, [u8; 32]>>);

impl DirActor {
    /// Loads the directory state from storage.
    pub(super) async fn load(&mut self) -> FSResult<()> {
        tracing::debug!("load: starting load");
        self.state = match &mut self.context.link {
            #[cfg(not(target_arch = "wasm32"))]
            DirContextParentLink::LocalFile { file, .. } => {
                use std::io::Read;
                let mut buffer = Vec::new();
                file.read_to_end(&mut buffer)?;
                // Track the current hash of the root snapshot so that we can
                // later update `PinContext::LocalFsHead` when saving.
                self.current_hash = Some(Hash::new(&buffer));
                self.last_serialized_len = buffer.len();
                DirV1::from_bytes(&buffer)?
            }
            DirContextParentLink::DirHandle { initial_hash, .. } => {
                tracing::debug!("load: reading blob {}", Hash::from(*initial_hash));
                let bytes = self
                    .context
                    .meta_blob_store
                    .read_as_bytes((*initial_hash).into(), 0, None)
                    .await
                    .context("while reading from blob store")?;
                let decrypted = Self::decrypt_if_needed(bytes, &self.context)?;
                self.last_serialized_len = decrypted.len();
                DirV1::from_bytes(&decrypted)?
            }

            DirContextParentLink::RegistryKey { public_key, .. } => {
                if let Some(entry) = self.context.registry.get(public_key).await? {
                    let bytes = self
                        .context
                        .meta_blob_store
                        .read_as_bytes(entry.hash, 0, None)
                        .await?;
                    let decrypted = Self::decrypt_if_needed(bytes, &self.context)?;
                    self.last_serialized_len = decrypted.len();
                    DirV1::from_bytes(&decrypted)?
                } else {
                    self.last_serialized_len = 0;
                    DirV1::new()
                }
            }
        };
        Ok(())
    }

    /// Decrypts directory bytes if encryption is enabled.
    fn decrypt_if_needed(bytes: Bytes, context: &crate::context::DirContext) -> FSResult<Bytes> {
        if let Some(enc_type) = context.encryption_type {
            if enc_type == ENCRYPTION_TYPE_XCHACHA20_POLY1305 {
                let encryption_key = context
                    .keys
                    .get(&0x0e)
                    .ok_or_else(|| anyhow!("missing encryption key 0x0e for XChaCha20-Poly1305"))?;
                crate::dir::decrypt_dir_bytes(bytes, Some(encryption_key))
            } else {
                Err(anyhow!("encryption type {} not supported", enc_type))
            }
        } else {
            Ok(bytes)
        }
    }

    /// Encodes a child directory snapshot using this context's
    /// encryption settings, returning the serialized bytes and any
    /// derived per-directory encryption keys.
    ///
    /// TODO(perf): consider reusing a shared CBOR buffer here (and
    /// in `encode_state_bytes`) if profiles show the per-call
    /// allocation cost is significant for metadata-heavy workloads.
    pub(super) fn encode_child_dir_bytes_for_child(&self, state: &DirV1) -> FSResult<EncodedDir> {
        Self::encode_child_dir_bytes(state, &self.context)
    }

    fn encode_child_dir_bytes(
        state: &DirV1,
        context: &crate::context::DirContext,
    ) -> FSResult<EncodedDir> {
        let enable_encryption = context.encryption_type.is_some();
        let mut keys = std::collections::BTreeMap::new();

        let plain = state.to_bytes()?;

        let bytes = if enable_encryption {
            let key: [u8; 32] = XChaCha20Poly1305::generate_key(&mut OsRng).into();
            keys.insert(0x0e, key);

            crate::dir::encrypt_dir_bytes(&key, &plain)?
        } else {
            plain
        };

        Ok((bytes, if enable_encryption { Some(keys) } else { None }))
    }

    /// Encodes the current directory state to bytes, applying encryption
    /// if configured on the context.
    ///
    /// TODO(perf): reuse a small scratch buffer for CBOR encoding to
    /// reduce allocator pressure in write-heavy import or sync flows.
    fn encode_state_bytes(&self) -> FSResult<Bytes> {
        let plain = self.state.to_bytes()?;

        if let Some(enc_type) = self.context.encryption_type {
            if enc_type == ENCRYPTION_TYPE_XCHACHA20_POLY1305 {
                let encryption_key =
                    self.context.keys.get(&0x0e).ok_or_else(|| {
                        anyhow!("missing encryption key 0x0e for XChaCha20-Poly1305")
                    })?;
                crate::dir::encrypt_dir_bytes(encryption_key, &plain)
            } else {
                Err(anyhow!("encryption type {} not supported", enc_type))
            }
        } else {
            Ok(plain)
        }
    }

    /// Computes and persists a snapshot blob for the current directory state
    /// and returns its BLAKE3 hash. Works for any DirContext.
    pub(super) async fn export_snapshot_hash(&mut self) -> FSResult<Hash> {
        let bytes = self.encode_state_bytes()?;
        let blob_id = self.context.meta_blob_store.import_bytes(bytes).await?;
        Ok(blob_id.hash)
    }

    /// Saves the current directory state to storage.
    pub(super) async fn save(&mut self, notify_parent: bool) -> FSResult<Option<Hash>> {
        let bytes = self.encode_state_bytes()?;

        match &mut self.context.link {
            #[cfg(not(target_arch = "wasm32"))]
            DirContextParentLink::LocalFile { path, .. } => {
                use std::io::Write;
                log::debug!(
                    "saving local root snapshot: files={} dirty={}",
                    self.state.files.len(),
                    self.dirty
                );
                // Import the root snapshot bytes into the meta blob store so
                // that it has a content-addressed hash, and update local
                // pins to reflect the live head.
                let blob_id = self
                    .context
                    .meta_blob_store
                    .import_bytes(bytes.clone())
                    .await?;

                if let Some(pins) = &self.context.pins {
                    // Best-effort cleanup of the previous head pin.
                    if let Some(prev) = self.current_hash.take() {
                        let _ = pins.unpin_hash(prev, PinContext::LocalFsHead).await;
                    }
                    // Pin the new head hash for this local FS5 root.
                    pins.pin_hash(blob_id.hash, PinContext::LocalFsHead).await?;
                    self.current_hash = Some(blob_id.hash);
                }

                let parent_dir = path.parent().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Could not find parent directory",
                    )
                })?;
                let mut temp_file = NamedTempFile::new_in(parent_dir)?;
                temp_file.write_all(&bytes)?;
                temp_file.as_file().sync_all()?;
                temp_file.persist(path)?;
                Ok(None)
            }
            DirContextParentLink::DirHandle {
                path,
                handle,
                shard_level: _,
                initial_hash,
            } => {
                let hash = self.context.meta_blob_store.import_bytes(bytes).await?;

                if notify_parent && let Some(handle) = handle.upgrade() {
                    handle
                        .send_msg(super::ActorMessage::UpdateDirRefHash {
                            path: path.clone(),
                            hash: hash.hash,
                        })
                        .await?;
                }

                initial_hash.copy_from_slice(hash.hash.as_bytes());
                Ok(Some(hash.hash))
            }
            DirContextParentLink::RegistryKey {
                public_key,
                signing_key,
            } => {
                let hash = self.context.meta_blob_store.import_bytes(bytes).await?;
                if let Some(signing_key) = signing_key.as_ref() {
                    let current = self.context.registry.get(public_key).await?;
                    let revision = current.as_ref().map_or(0, |entry| entry.revision + 1);
                    let dalek_key = ed25519_dalek::SigningKey::from_bytes(signing_key.as_bytes());
                    let (key_type, key_bytes) = public_key.to_bytes();
                    let mut sign_bytes = Vec::with_capacity(1 + 1 + key_bytes.len() + 8 + 1 + 32);
                    sign_bytes.push(MessageType::Registry as u8);
                    sign_bytes.push(key_type);
                    sign_bytes.extend_from_slice(key_bytes);
                    sign_bytes.extend_from_slice(&revision.to_be_bytes());
                    sign_bytes.push(0x21);
                    sign_bytes.extend_from_slice(hash.hash.as_bytes());
                    let signature = dalek_key.sign(&sign_bytes);
                    let entry = StreamMessage::new(
                        MessageType::Registry,
                        *public_key,
                        revision,
                        hash.hash,
                        signature.to_bytes().to_vec().into_boxed_slice(),
                        None,
                    )?;
                    self.context.registry.set(entry).await?;
                }
                Ok(None)
            }
        }
    }

    /// Flushes this directory and children when dirty.
    ///
    /// The root directory (backed by a local file) always walks
    /// its children, even if the root itself is not marked dirty,
    /// so that pending changes in subtrees are not lost.
    pub(super) async fn save_if_dirty(&mut self) -> FSResult<Option<Hash>> {
        #[cfg(not(target_arch = "wasm32"))]
        let is_root = matches!(self.context.link, DirContextParentLink::LocalFile { .. });
        #[cfg(target_arch = "wasm32")]
        let is_root = false;

        if self.dirty || is_root {
            self.shard_if_needed().await?;

            // Collect handles to iterate over results later
            let shard_handles: Vec<(u8, DirActorHandle)> = self
                .dir_shard_handles
                .iter()
                .map(|(k, v)| (*k, v.clone()))
                .collect();
            let dir_handles: Vec<(String, DirActorHandle)> = self
                .dir_handles
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            let shard_futures = shard_handles.iter().map(|(_, h)| h.save_if_dirty());
            let dir_futures = dir_handles.iter().map(|(_, h)| h.save_if_dirty());

            let shard_results = join_all(shard_futures).await;
            let dir_results = join_all(dir_futures).await;

            // Process shard updates
            for ((index, _), result) in shard_handles.into_iter().zip(shard_results) {
                match result {
                    Ok(Some(hash)) => {
                        if let Some(shards) = self.state.header.shards.as_mut()
                            && let Some(dir_ref) = shards.get_mut(&index)
                            && dir_ref.hash != *hash.as_bytes()
                        {
                            dir_ref.hash = hash.into();
                            self.dirty = true;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::error!("failed to save shard {index}: {e}");
                    }
                }
            }

            // Process dir updates
            for ((name, _), result) in dir_handles.into_iter().zip(dir_results) {
                match result {
                    Ok(Some(hash)) => {
                        if let Some(dir_ref) = self.state.dirs.get_mut(&name)
                            && dir_ref.hash != *hash.as_bytes()
                        {
                            dir_ref.hash = hash.into();
                            self.dirty = true;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        tracing::error!("failed to save child dir {name}: {e}");
                    }
                }
            }

            if self.dirty {
                let res = self.save(false).await?;

                self.dirty = false;
                return Ok(res);
            }
        }

        Ok(None)
    }
}
