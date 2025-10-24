//! Defines the context for a directory actor, including its storage and parent link.

use crate::{
    FSResult,
    actor::DirActorHandle,
    dir::{DirRef, DirV1},
};
use anyhow::Context;
use dashmap::DashMap;
use fs4::fs_std::FileExt;
use s5_core::{BlobStore, RedbRegistry, StreamKey};
use s5_store_local::{LocalStore, LocalStoreConfig};
use std::{collections::BTreeMap, fs::OpenOptions, path::Path, sync::Arc};
use zeroize::Zeroize;

/// Placeholder signing key type for registry updates.
/// TODO implement
#[derive(Clone, Debug)]
pub struct SigningKey(pub [u8; 32]);

/// The context required for a `DirActor` to operate.
///
/// It contains storage backends, encryption keys, and a link to its parent.
pub struct DirContext {
    pub link: DirContextParentLink,
    pub encryption_type: Option<u8>,
    pub keys: BTreeMap<u8, [u8; 32]>,
    pub meta_blob_store: BlobStore,
    pub registry: RedbRegistry,
    pub registry_dir_handles: Arc<DashMap<StreamKey, DirActorHandle>>,
}

/// Defines how a directory is linked to its parent.
pub enum DirContextParentLink {
    /// The directory is a child of another directory, identified by a registry key.
    RegistryKey {
        public_key: StreamKey,
        signing_key: Option<SigningKey>,
    },
    /// The directory is the root of a local file system, backed by a file.
    LocalFile {
        file: std::fs::File,
        path: std::path::PathBuf,
    },
    /// The directory is a child of another directory, accessed via an actor handle.
    DirHandle {
        path: DirHandlePath,
        handle: DirActorHandle,
        initial_hash: [u8; 32],
        shard_level: u8,
    },
}

// TODO(perf): Avoid cloning; consider using an interner or lightweight ID index
#[derive(Clone, Debug)]
pub enum DirHandlePath {
    Path(String),
    Shard(u8),
}

impl DirContext {
    /// Opens a local file system root under `path`.
    ///
    /// - Creates `root.fs5.cbor` if missing and locks it for exclusive access.
    /// - Initializes a local blob store and registry co-located with `path`.
    pub fn open_local_root<P: AsRef<Path>>(path: P) -> FSResult<Self> {
        let path = path.as_ref().to_path_buf();
        let root_file = path.join("root.fs5.cbor");

        if !root_file.exists() {
            std::fs::create_dir_all(
                root_file
                    .parent()
                    .context("path cannot be the root directory")?,
            )?;
            std::fs::write(&root_file, DirV1::new().to_bytes()?)?;
        }

        let file = OpenOptions::new().read(true).write(true).open(&root_file)?;
        file.lock_exclusive()?;

        let meta_store = LocalStore::create(LocalStoreConfig {
            base_path: path.to_string_lossy().into(),
        });
        let registry = RedbRegistry::open(&path)?;

        Ok(Self::new(
            DirContextParentLink::LocalFile {
                file,
                path: root_file,
            },
            BlobStore::new(meta_store),
            registry,
        ))
    }

    /// Creates a new `DirContext` with provided parent link, meta store, and registry.
    pub fn new(
        link: DirContextParentLink,
        meta_blob_store: BlobStore,
        registry: RedbRegistry,
    ) -> Self {
        Self {
            encryption_type: None,
            keys: BTreeMap::new(),
            meta_blob_store,
            link,
            registry,
            registry_dir_handles: Arc::new(DashMap::new()),
        }
    }

    /// Derives a child directory context from this context and a `dir_ref`.
    ///
    /// - Inherits encryption type and keys, merging any keys in `dir_ref`.
    /// - Shares the blob store and registry handles.
    pub fn with_new_ref(&self, dir_ref: &DirRef, link: DirContextParentLink) -> Self {
        let mut new_context = Self {
            encryption_type: dir_ref.encryption_type.or(self.encryption_type),
            keys: self.keys.clone(),
            meta_blob_store: self.meta_blob_store.clone(),
            registry: self.registry.clone(),
            registry_dir_handles: self.registry_dir_handles.clone(),
            link,
        };
        if let Some(dir_keys) = &dir_ref.keys {
            for (key_type, key_bytes) in dir_keys {
                if let Ok(key_array) = key_bytes.to_vec().try_into() {
                    new_context.keys.insert(*key_type, key_array);
                }
            }
        }
        new_context
    }
}

impl Drop for DirContext {
    fn drop(&mut self) {
        for v in self.keys.values_mut() {
            v.zeroize();
        }
    }
}
