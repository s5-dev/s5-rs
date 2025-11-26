//! Defines the context for a directory actor, including its storage and parent link.

use crate::{
    FSResult,
    actor::{DirActorHandle, WeakDirActorHandle},
    dir::{DirRef, DirV1},
};
use anyhow::Context;
use dashmap::DashMap;
use fs4::fs_std::FileExt;
use s5_core::{BlobStore, Pins, RedbRegistry, RegistryApi, RegistryPinner, StreamKey};
use s5_store_local::{LocalStore, LocalStoreConfig};
use std::{collections::BTreeMap, fs::OpenOptions, path::Path, sync::Arc};
use zeroize::Zeroize;

/// Signing key type for registry updates (Ed25519 private key seed).
#[derive(Clone, Debug)]
pub struct SigningKey([u8; 32]);

impl SigningKey {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8; 32] {
        &mut self.0
    }
}

/// The context required for a `DirActor` to operate.
///
/// It contains storage backends, encryption keys, and a link to its parent.
pub struct DirContext {
    pub link: DirContextParentLink,
    pub encryption_type: Option<u8>,
    pub keys: BTreeMap<u8, [u8; 32]>,
    pub meta_blob_store: BlobStore,
    pub registry: Arc<dyn RegistryApi + Send + Sync>,
    /// Optional pinning interface associated with this context's registry.
    ///
    /// For local FS5 roots opened via `open_local_root`, this is backed by a
    /// `RegistryPinner<RedbRegistry>` and is used to track local pins such as
    /// `PinContext::LocalFsHead` and `PinContext::LocalFsSnapshot`.
    pub pins: Option<Arc<dyn Pins + Send + Sync>>,
    pub signing_key: Option<SigningKey>,
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
        handle: WeakDirActorHandle,
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
        let snapshots_file = path.join("snapshots.fs5.cbor");

        if !root_file.exists() {
            std::fs::create_dir_all(
                root_file
                    .parent()
                    .context("path cannot be the root directory")?,
            )?;
            std::fs::write(&root_file, DirV1::new().to_bytes()?)?;
        }

        // Ensure a snapshots index root exists alongside `root.fs5.cbor`.
        if !snapshots_file.exists() {
            std::fs::write(&snapshots_file, DirV1::new().to_bytes()?)?;
        }

        let file = OpenOptions::new().read(true).write(true).open(&root_file)?;
        file.lock_exclusive()?;

        let meta_store = LocalStore::create(LocalStoreConfig {
            base_path: path.to_string_lossy().into(),
        });

        // Use a RegistryPinner over the local RedbRegistry so that the
        // same registry DB is shared for both pin metadata and other
        // registry usage.
        let registry_db = RedbRegistry::open(&path)?;
        let pinner = RegistryPinner::new(registry_db);
        let registry: Arc<dyn RegistryApi + Send + Sync> = pinner.registry_arc();
        let pins: Arc<dyn Pins + Send + Sync> = Arc::new(pinner);

        let mut ctx = Self::new(
            DirContextParentLink::LocalFile {
                file,
                path: root_file,
            },
            BlobStore::new(meta_store),
            registry,
        );
        ctx.pins = Some(pins);
        Ok(ctx)
    }

    /// Creates a new `DirContext` with provided parent link, meta store, and registry.
    pub fn new(
        link: DirContextParentLink,
        meta_blob_store: BlobStore,
        registry: Arc<dyn RegistryApi + Send + Sync>,
    ) -> Self {
        Self {
            encryption_type: None,
            keys: BTreeMap::new(),
            meta_blob_store,
            link,
            registry,
            pins: None,
            signing_key: None,
            registry_dir_handles: Arc::new(DashMap::new()),
        }
    }

    /// Derives a child directory context from this context and a `dir_ref`.
    ///
    /// - Inherits encryption type and keys, merging any keys in `dir_ref`.
    /// - Shares the blob store and registry handles.
    pub fn with_new_ref(&self, dir_ref: &DirRef, link: DirContextParentLink) -> Self {
        let inherited_signing_key = match &link {
            DirContextParentLink::RegistryKey { signing_key, .. } => signing_key.clone(),
            _ => self.signing_key.clone(),
        };
        let mut new_context = Self {
            encryption_type: dir_ref.encryption_type.or(self.encryption_type),
            keys: self.keys.clone(),
            meta_blob_store: self.meta_blob_store.clone(),
            registry: self.registry.clone(),
            pins: self.pins.clone(),
            signing_key: inherited_signing_key,
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
        // Best-effort key scrubbing on drop.
        for v in self.keys.values_mut() {
            v.zeroize();
        }
        if let Some(key) = self.signing_key.as_mut() {
            key.as_bytes_mut().zeroize();
        }
    }
}
