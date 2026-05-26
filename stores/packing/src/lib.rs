//! `PackingStore` — bundle many small entries into larger packs
//! before they hit a slow or expensive backend.
//!
//! ## Path-agnostic key-value packing
//!
//! `PackingStore` is a pure `Store` decorator. It does **not** assume
//! its paths encode a content hash, and does **not** verify that the
//! bytes stored under a path hash to anything in particular. The
//! in-pack index key for any record is `BLAKE3(path_bytes)[..12]`.
//!
//! Callers who want content-addressed semantics on top should
//! compose with the standard `BlobStore` adapter:
//! `BlobStore::without_outboard(packing_store)` — that's where hash
//! derivation and verification belong. Layering keeps each layer's
//! contract honest.
//!
//! ## Wire format
//!
//! See `manifest.rs`. Custom little-endian binary (not CBOR).
//! Manifests sit at `manifests/<hex(pack_hash)>`; per-member 16-byte
//! records `(hash_prefix[12], length: u32)` sorted ascending by full
//! BLAKE3-of-path.
//!
//! ## Deletion / GC
//!
//! Not implemented in this revision (spec §10.7 deferred). The
//! `Store::delete` impl returns `NotSupported`. The in-memory index
//! is build-then-grow; there is no reclamation path yet.

mod binpack;
mod manifest;

pub use manifest::{HASH_PREFIX_LEN, PackManifest, PackMember};

use bytes::Bytes;
use futures::stream::{self, Stream, StreamExt};
use s5_core::Hash;
use s5_core::blob::BlobsReadWrite;
use s5_core::blob::location::BlobLocation;
use s5_core::store::{Store, StoreFeatures, StoreResult};

use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, RwLock};

use crate::binpack::{PackGroup, PendingBlob, first_fit};

/// Configuration knobs.
#[derive(Clone, Debug)]
pub struct PackingConfig {
    /// Don't flush automatically until at least this many bytes are
    /// staged. Default 40 MiB.
    pub min_group_size: u64,
    /// Hard cap on a single pack's size. Default 256 MiB.
    pub max_group_size: u64,
    /// Slab unit for the waste-tolerance heuristic in `tryAdd`.
    /// Default 4 MiB (typical Sia slab size).
    pub slab_size: u64,
    /// Once a group's waste fraction drops below this, only entries
    /// that reduce waste or fit the tail are accepted. Default 0.10.
    pub waste_pct: f64,
    /// Background packing loop tick interval. Default 60 s.
    pub interval: Duration,
    /// `true` → `put_bytes` force-flushes inline (high latency,
    /// immediately durable). `false` → returns as soon as the bytes
    /// are durably staged.
    pub flush_on_put: bool,
    /// Manifest store path prefix. Default `manifests/`.
    pub manifests_prefix: String,
}

impl Default for PackingConfig {
    fn default() -> Self {
        Self {
            min_group_size: 40 * 1024 * 1024,
            max_group_size: 256 * 1024 * 1024,
            slab_size: 4 * 1024 * 1024,
            waste_pct: 0.10,
            interval: Duration::from_secs(60),
            flush_on_put: false,
            manifests_prefix: "manifests/".to_string(),
        }
    }
}

#[derive(Default, Debug)]
struct Index {
    /// Parsed manifests, in the order they were loaded/flushed.
    /// `by_prefix` indexes into this vec.
    packs: Vec<PackManifest>,
    /// `BLAKE3(path)[..12]` → index into `packs`.
    by_prefix: HashMap<[u8; HASH_PREFIX_LEN], u32>,
    /// Entries still in staging (not yet packed).
    pending: HashMap<[u8; HASH_PREFIX_LEN], PendingBlob>,
}

impl Index {
    /// Locate a packed entry by its 12-byte path-hash prefix.
    /// Returns `(pack_hash, offset_in_pack, length)`.
    ///
    /// Offset is computed by cumulative sum over the preceding
    /// members. For a pack with M members this is O(log M + i) where
    /// i is the member index — fine at typical pack sizes
    /// (hundreds–thousands of entries), and saves storing a per-entry
    /// `BlobLoc` in memory.
    fn locate(&self, prefix: &[u8; HASH_PREFIX_LEN]) -> Option<(Hash, u32, u32)> {
        let pack_idx = *self.by_prefix.get(prefix)? as usize;
        let manifest = &self.packs[pack_idx];
        let i = manifest
            .members
            .binary_search_by_key(prefix, |m| m.hash_prefix)
            .ok()?;
        let offset: u32 = manifest.members[..i].iter().map(|m| m.length).sum();
        Some((manifest.pack_hash, offset, manifest.members[i].length))
    }
}

/// Three-store pack decorator.
#[derive(Debug)]
pub struct PackingStore<B, M, S> {
    blobs: B,
    manifests: M,
    staging: S,
    config: PackingConfig,
    state: RwLock<Index>,
    flush_notify: Notify,
}

impl<B, M, S> PackingStore<B, M, S>
where
    B: BlobsReadWrite + std::fmt::Debug + Send + Sync + 'static,
    M: Store,
    S: Store,
{
    pub async fn open(
        blobs: B,
        manifests: M,
        staging: S,
        config: PackingConfig,
    ) -> StoreResult<Arc<Self>> {
        let store = Arc::new(Self {
            blobs,
            manifests,
            staging,
            config,
            state: RwLock::new(Index::default()),
            flush_notify: Notify::new(),
        });
        store.load_index().await?;
        store.reconcile_staging().await?;
        Ok(store)
    }

    fn manifest_path(&self, pack_hash: Hash) -> String {
        format!(
            "{}{}",
            self.config.manifests_prefix,
            s5_core::blob::paths::path_for_hash(pack_hash, &self.manifests.features())
        )
    }

    /// Load all manifests at startup and populate the in-memory index.
    async fn load_index(&self) -> StoreResult<()> {
        let mut stream = self.manifests.list().await?;
        let mut paths = Vec::new();
        while let Some(path) = stream.next().await {
            let path = path?;
            if path.starts_with(&self.config.manifests_prefix) {
                paths.push(path);
            }
        }
        drop(stream);

        let mut index = self.state.write().await;
        for path in paths {
            let bytes = self.manifests.open_read_bytes(&path, 0, None).await?;
            let manifest = PackManifest::decode(&bytes)
                .map_err(|e| anyhow::anyhow!("decoding manifest at {path}: {e}"))?;
            let pack_idx = index.packs.len() as u32;
            for member in &manifest.members {
                index.by_prefix.insert(member.hash_prefix, pack_idx);
            }
            index.packs.push(manifest);
        }
        Ok(())
    }

    /// Reconcile staging on startup: drop any staged entry whose key
    /// is already in the live index; re-enqueue the rest.
    async fn reconcile_staging(&self) -> StoreResult<()> {
        let paths = self.list_staging_paths().await?;
        let mut index = self.state.write().await;
        for path in paths {
            let key = key_of(&path);
            if index.by_prefix.contains_key(&key) {
                drop(index);
                self.staging.delete(&path).await?;
                index = self.state.write().await;
                continue;
            }
            let length = self.staging.size(&path).await? as u32;
            index.pending.insert(
                key,
                PendingBlob {
                    key,
                    staging_path: path,
                    length,
                },
            );
        }
        Ok(())
    }

    async fn list_staging_paths(&self) -> StoreResult<Vec<String>> {
        let mut stream = self.staging.list().await?;
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.push(item?);
        }
        Ok(out)
    }

    /// Background loop. Run with `tokio::spawn(store.clone().run_upload_loop())`.
    pub async fn run_upload_loop(self: Arc<Self>) {
        let interval = self.config.interval;
        loop {
            let tick = tokio::time::sleep(interval);
            tokio::pin!(tick);
            tokio::select! {
                _ = &mut tick => {}
                _ = self.flush_notify.notified() => {}
            }
            if let Err(err) = self.pack_once(false).await {
                tracing::warn!("packing-store upload tick failed: {err:?}");
            }
        }
    }

    /// Drive one pack-and-flush cycle. `force_all = true` flushes
    /// every pending group regardless of size.
    pub async fn pack_once(&self, force_all: bool) -> StoreResult<()> {
        let pending: Vec<PendingBlob> = {
            let state = self.state.read().await;
            let mut v: Vec<_> = state.pending.values().cloned().collect();
            v.sort_by(|a, b| a.staging_path.cmp(&b.staging_path));
            v
        };
        if pending.is_empty() {
            return Ok(());
        }

        let mut groups: Vec<PackGroup> = Vec::new();
        for blob in pending {
            first_fit(
                &mut groups,
                blob,
                self.config.slab_size,
                self.config.max_group_size,
                self.config.waste_pct,
            );
        }

        for group in groups {
            let ready = force_all || group.total_size >= self.config.min_group_size;
            if !ready {
                continue;
            }
            self.flush_group(group).await?;
        }
        Ok(())
    }

    async fn flush_group(&self, mut group: PackGroup) -> StoreResult<()> {
        // Members sorted ascending by full BLAKE3-of-path before pack
        // bytes are concatenated. Deterministic across devices.
        group.members.sort_by_key(|m| m.key);

        let mut pack_buf: Vec<u8> = Vec::with_capacity(group.total_size as usize);
        let mut manifest_members = Vec::with_capacity(group.members.len());

        for blob in &group.members {
            let bytes = self
                .staging
                .open_read_bytes(&blob.staging_path, 0, None)
                .await?;
            if bytes.len() as u32 != blob.length {
                return Err(anyhow::anyhow!(
                    "staging size mismatch for {}: expected {}, got {}",
                    blob.staging_path,
                    blob.length,
                    bytes.len()
                ));
            }
            pack_buf.extend_from_slice(&bytes);
            manifest_members.push(PackMember {
                hash_prefix: blob.key,
                length: blob.length,
            });
        }

        let pack_bytes = Bytes::from(pack_buf);
        let blob_id = self.blobs.blob_upload_bytes(pack_bytes).await?;
        let pack_hash = blob_id.hash;

        let manifest = PackManifest {
            pack_hash,
            members: manifest_members,
        };
        let manifest_bytes = manifest.encode();
        let manifest_path = self.manifest_path(pack_hash);
        self.manifests
            .put_bytes(&manifest_path, manifest_bytes)
            .await?;

        // Update in-memory index + drop staging entries.
        {
            let mut state = self.state.write().await;
            let pack_idx = state.packs.len() as u32;
            for member in &manifest.members {
                state.pending.remove(&member.hash_prefix);
                state.by_prefix.insert(member.hash_prefix, pack_idx);
            }
            state.packs.push(manifest);
        }
        for blob in &group.members {
            self.staging.delete(&blob.staging_path).await?;
        }
        Ok(())
    }

    /// Read an entry's bytes from whatever pack contains it, or from
    /// staging if not yet packed.
    async fn read_entry(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        let key = key_of(path);

        let staging_path = {
            let state = self.state.read().await;
            state.pending.get(&key).map(|p| p.staging_path.clone())
        };
        if let Some(p) = staging_path {
            return self.staging.open_read_bytes(&p, offset, max_len).await;
        }

        let (pack_hash, in_pack_offset, length) = {
            let state = self.state.read().await;
            state.locate(&key).ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("no such path: {path}"))
            })?
        };
        let pack_offset = in_pack_offset as u64 + offset;
        let effective_len = match max_len {
            Some(l) => l.min(length as u64 - offset),
            None => length as u64 - offset,
        };
        self.blobs
            .blob_download_slice(pack_hash, pack_offset, Some(effective_len))
            .await
    }
}

#[async_trait::async_trait]
impl<B, M, S> Store for PackingStore<B, M, S>
where
    B: BlobsReadWrite + std::fmt::Debug + Send + Sync + 'static,
    M: Store,
    S: Store,
{
    fn features(&self) -> StoreFeatures {
        StoreFeatures {
            supports_rename: false,
            case_sensitive: true,
            recommended_max_dir_size: u64::MAX,
            supports_reflink: false,
        }
    }

    async fn exists(&self, path: &str) -> StoreResult<bool> {
        let key = key_of(path);
        let state = self.state.read().await;
        Ok(state.by_prefix.contains_key(&key) || state.pending.contains_key(&key))
    }

    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<()> {
        let key = key_of(path);
        let length = bytes.len() as u32;

        {
            let state = self.state.read().await;
            if state.by_prefix.contains_key(&key) || state.pending.contains_key(&key) {
                return Ok(());
            }
        }

        self.staging.put_bytes(path, bytes).await?;
        {
            let mut state = self.state.write().await;
            state.pending.insert(
                key,
                PendingBlob {
                    key,
                    staging_path: path.to_string(),
                    length,
                },
            );
        }

        if self.config.flush_on_put {
            self.pack_once(true).await?;
        }
        Ok(())
    }

    async fn put_stream(
        &self,
        path: &str,
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<()> {
        let bytes = collect_stream(stream).await?;
        self.put_bytes(path, bytes).await
    }

    async fn open_read_bytes(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        self.read_entry(path, offset, max_len).await
    }

    async fn open_read_stream(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static>>
    {
        let bytes = self.read_entry(path, offset, max_len).await?;
        let s = stream::once(async move { Ok::<_, io::Error>(bytes) });
        Ok(Box::new(Box::pin(s)))
    }

    async fn size(&self, path: &str) -> StoreResult<u64> {
        let key = key_of(path);
        let state = self.state.read().await;
        if let Some(p) = state.pending.get(&key) {
            return Ok(p.length as u64);
        }
        if let Some((_, _, length)) = state.locate(&key) {
            return Ok(length as u64);
        }
        Err(io::Error::new(io::ErrorKind::NotFound, format!("no such path: {path}")).into())
    }

    async fn delete(&self, _path: &str) -> StoreResult<()> {
        Err(anyhow::anyhow!(
            "PackingStore: delete is not implemented in this revision (deletion/GC deferred)"
        ))
    }

    async fn list(
        &self,
    ) -> StoreResult<Box<dyn Stream<Item = Result<String, io::Error>> + Send + Unpin + 'static>>
    {
        // Manifests carry only 12-byte path-hash prefixes, not full
        // paths — packed entries can't be enumerated through this
        // surface. We yield only staging-pending entries (which still
        // know their full paths). Callers who need exhaustive
        // enumeration should keep their own external index.
        let state = self.state.read().await;
        let mut paths: Vec<Result<String, io::Error>> = Vec::new();
        for blob in state.pending.values() {
            paths.push(Ok(blob.staging_path.clone()));
        }
        drop(state);
        Ok(Box::new(Box::pin(stream::iter(paths))))
    }

    async fn rename(&self, _old: &str, _new: &str) -> StoreResult<()> {
        Err(anyhow::anyhow!(
            "PackingStore does not support rename (paths are immutable index keys)"
        ))
    }

    async fn provide(&self, _path: &str) -> StoreResult<Vec<BlobLocation>> {
        Ok(Vec::new())
    }

    async fn sync(&self) -> StoreResult<()> {
        self.pack_once(true).await?;
        self.staging.sync().await?;
        self.blobs.blob_sync().await?;
        self.manifests.sync().await?;
        Ok(())
    }
}

// ---- helpers ----

/// `BLAKE3(path)[..12]` — the 12-byte in-pack index key for `path`.
fn key_of(path: &str) -> [u8; HASH_PREFIX_LEN] {
    let mut key = [0u8; HASH_PREFIX_LEN];
    key.copy_from_slice(&blake3::hash(path.as_bytes()).as_bytes()[..HASH_PREFIX_LEN]);
    key
}

async fn collect_stream(
    mut stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static>,
) -> StoreResult<Bytes> {
    let mut out: Vec<u8> = Vec::new();
    while let Some(chunk) = futures::StreamExt::next(&mut stream).await {
        let chunk = chunk?;
        out.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests;
