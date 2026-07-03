//! Writable FUSE adapter: a `WritableOverlay` plus per-path in-flight
//! buffers, layered on top of a base [`ReadableLayer`].
//!
//! Reads stack: in-flight write buffer → overlay (committed) → base.
//! Writes accumulate in per-path in-memory buffers; on `release` the
//! buffer is hashed, uploaded, and the resulting `NodeEntry` is inserted
//! into the overlay. Deletions become tombstones in the overlay.
//!
//! ## Persistence boundary
//!
//! [`WritableFs::flush_overlay`] folds the overlay into a freshly
//! persisted root via [`Pipeline::merge_and_persist`] and returns
//! the resulting `Snapshot`. Wiring the result back through the daemon
//! (publish a new HEAD, then swap base + clear overlay) is the
//! caller's job — see the mount entry points and `crate::debounce`.
//!
//! ## Layered shape
//!
//! `WritableFs` is intentionally thin: it holds an
//! [`Arc<WritableOverlay>`] (which itself owns the read base + the
//! per-blob `Pipeline` + the entry buffer), a `BlobStore` for the
//! write side, and the FUSE-specific bits (in-flight buffers, fh
//! counter, write signal). Reads route through the overlay's
//! `ReadableLayer`; bytes are materialised via `overlay.pipeline()`;
//! flush goes through `overlay.flush(store)`. The base is always a
//! `MergedView` (length 1 in the single-snapshot case via
//! [`WritableFs::new`], length N via [`WritableFs::with_layers`]) so
//! a future hot-swap of the merged stack for live peer-tip composition
//! reuses the same slot.
//!
//! ## Limits (intentional, v0)
//! - Single-writer-per-file. Concurrent FDs writing the same path race;
//!   the last `release` wins. POSIX semantics arrive when we add an
//!   open-count per path.
//! - In-flight buffers live in RAM; whole-file writes pay the size in
//!   memory. Streaming chunked writes via `Pipeline::import_bytes`
//!   are a follow-up.

use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use bytes::Bytes;
use fuse3::path::prelude::*;
use fuse3::{Errno, Result as FuseResult};
use futures_util::stream;
use s5_core::BlobsRead;
use s5_core::blob::BlobStore;
use s5_fs_v2::layer::ReadableLayer;
use s5_fs_v2::merge::MergedView;
use s5_fs_v2::node::{ContentRef, NodeEntry, SemanticMeta, Structural};
use s5_fs_v2::overlay::WritableOverlay;
use s5_fs_v2::pipeline::Pipeline;
use s5_fs_v2::snapshot::Snapshot;
use tokio::sync::{Mutex, Notify};
use tracing::warn;

use crate::attr::{BLOCK_SIZE, ENTRY_TTL, dir_attr, file_attr};
use crate::path::{
    ResolvedEntry, join, list_children, list_children_with_entries, resolve, snapshot_key,
};

/// Build a `SemanticMeta` carrying the current wall-clock time as the
/// modification timestamp. Subsequent `stat` calls will see this as the
/// file's mtime rather than 1970.
fn now_semantic() -> SemanticMeta {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs().min(u32::MAX as u64) as u32;
    SemanticMeta {
        timestamp: Some(secs),
        timestamp_subsec_nanos: Some(now.subsec_nanos()),
        media_type: None,
        unix: None,
        warc: None,
    }
}

/// Merge two `SemanticMeta`s preferring `next`'s timestamp fields,
/// falling back to `prev`'s for everything else (media type, unix
/// metadata, …). Used so `import_bytes`-derived semantic info isn't
/// lost when we overwrite mtime on commit.
fn merged_semantic(prev: Option<SemanticMeta>, next: SemanticMeta) -> SemanticMeta {
    match prev {
        Some(prev) => SemanticMeta {
            timestamp: next.timestamp.or(prev.timestamp),
            timestamp_subsec_nanos: next.timestamp_subsec_nanos.or(prev.timestamp_subsec_nanos),
            media_type: prev.media_type.or(next.media_type),
            unix: prev.unix.or(next.unix),
            warc: prev.warc.or(next.warc),
        },
        None => next,
    }
}

/// Writable FUSE adapter: a [`WritableOverlay`] (which already carries
/// its read base + per-blob `Pipeline`) plus per-path in-flight write
/// buffers and a write-side blob store for `flush_overlay`.
///
/// All interior state is `Arc`-wrapped, so `Clone` is cheap. That lets
/// the mount entry point hand one clone to `fuse3::path::Session`
/// (which takes the FS by value) and keep another clone alive for the
/// caller's `flush_overlay` use.
#[derive(Clone)]
pub struct WritableFs {
    /// Owns base + pipeline + the entry buffer. Reads route through
    /// the overlay's `ReadableLayer` impl; bytes are materialised via
    /// `overlay.pipeline()`; flush goes through `overlay.flush(store)`.
    overlay: Arc<WritableOverlay>,
    /// Per-path in-flight write buffers. Allocated by `create`/`open`,
    /// extended by `write`, drained by `release` / `drain_in_flight`.
    in_flight: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    /// Monotonic file-handle counter. We don't use `fh` for dispatch
    /// (path is provided in every callback) — it just satisfies the
    /// kernel's expectation that each open gets a distinct handle.
    next_fh: Arc<AtomicU64>,
    /// Pulsed on every state-changing FUSE call (write, create,
    /// release, unlink, setattr-with-size). The debounce helper
    /// (see [`crate::debounce`]) listens on this to wake up its idle
    /// timer; the FS itself is unaware of debounce policy.
    write_signal: Arc<Notify>,
    /// Write-side handle for `import_bytes` on commit and
    /// `WritableOverlay::flush` on debounce. The overlay's `pipeline`
    /// holds the read side; this is the matching write capability.
    store: BlobStore,
}

impl WritableFs {
    /// Build a writable FS over a base snapshot. Wraps the snapshot in
    /// a length-1 [`MergedView`] so reads route through the same code
    /// path as multi-layer mounts constructed via [`Self::with_layers`],
    /// and bundles the snapshot's pipeline into the overlay so the
    /// "base + pipeline always travel together" invariant lives in one
    /// place.
    pub fn new(base: Snapshot, store: BlobStore) -> Self {
        let pipeline = Arc::new(base.as_pipeline());
        let layer: Arc<dyn ReadableLayer> = Arc::new(base);
        Self::with_layers(vec![layer], pipeline, store)
    }

    /// Build a writable FS from an explicit ordered stack of read-only
    /// layers (index 0 = highest priority). The overlay sits on top of
    /// the merged stack; `pipeline` materialises file bytes for entries
    /// served by any layer (pick one whose context covers the union —
    /// typically layer 0's pipeline). `store` is the write target for
    /// commits + flush.
    pub fn with_layers(
        layers: Vec<Arc<dyn ReadableLayer>>,
        pipeline: Arc<Pipeline>,
        store: BlobStore,
    ) -> Self {
        let base: Arc<dyn ReadableLayer> = Arc::new(MergedView::new(layers));
        let overlay = Arc::new(WritableOverlay::new(base, pipeline));
        Self {
            overlay,
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            next_fh: Arc::new(AtomicU64::new(1)),
            write_signal: Arc::new(Notify::new()),
            store,
        }
    }

    /// Synthesise a `NodeEntry` for an in-flight write. Hash is left as
    /// zeros — the entry is only consumed by attribute callbacks
    /// (which inspect `content.size` + `semantic.timestamp`); reads of
    /// in-flight paths come from the buffer directly, never via
    /// `export_bytes`. The timestamp is set to `now` so attribute
    /// callbacks against open-but-unflushed files report a sensible
    /// mtime (not the Unix epoch).
    fn in_flight_entry(size: usize) -> NodeEntry {
        NodeEntry {
            content: Some(ContentRef {
                structural: Structural::Leaf,
                hash: [0u8; 32],
                size: size as u64,
                plaintext_hash: None,
                stored_blocks: None,
            }),
            semantic: Some(now_semantic()),
            child_context: None,
            tombstone: None,
        }
    }

    /// Three-tier resolve: in-flight → overlay → base.
    async fn resolve_for_attr(&self, key: &str) -> FuseResult<ResolvedEntry> {
        if !key.is_empty() {
            let in_flight = self.in_flight.lock().await;
            if let Some(buf) = in_flight.get(key) {
                return Ok(ResolvedEntry::File(Box::new(Self::in_flight_entry(
                    buf.len(),
                ))));
            }
        }
        resolve(self.overlay.as_ref() as &dyn ReadableLayer, key).await
    }

    /// Flush all in-flight buffers into the overlay. Called by
    /// [`Self::flush_overlay`] before computing the new snapshot so
    /// not-yet-released writes also make it into the persisted state.
    async fn drain_in_flight(&self) -> FuseResult<()> {
        let drained: Vec<(String, Vec<u8>)> = {
            let mut map = self.in_flight.lock().await;
            map.drain().collect()
        };
        for (path, bytes) in drained {
            self.commit_buffer(&path, bytes).await?;
        }
        Ok(())
    }

    /// Hash + upload + insert a buffer into the overlay as a leaf entry.
    /// Stamps the entry's `semantic.timestamp` with the current time so
    /// post-flush stat calls report a real mtime instead of the epoch.
    /// `pub(crate)` so sibling-module tests (e.g. `debounce::tests`)
    /// can stage overlay state without going through full FUSE plumbing.
    pub(crate) async fn commit_buffer(&self, path: &str, bytes: Vec<u8>) -> FuseResult<()> {
        let mut entry = self
            .overlay
            .pipeline()
            .import_bytes(&bytes, &self.store, None)
            .await
            .map_err(|err| {
                warn!(path, error = %err, "import_bytes failed");
                Errno::from(libc::EIO)
            })?;
        entry.semantic = Some(merged_semantic(entry.semantic, now_semantic()));
        self.overlay.put(path.to_string(), entry);
        Ok(())
    }

    /// Persist the overlay (plus any in-flight buffers) into a fresh
    /// snapshot via [`WritableOverlay::flush`]. Returns the new
    /// snapshot, or `None` if there was nothing to persist. The caller
    /// decides what to do with the result — typically: publish the new
    /// HEAD via the daemon, then swap our base + clear the overlay
    /// (live-swap not done in v0).
    pub async fn flush_overlay(&self) -> anyhow::Result<Option<Snapshot>> {
        self.drain_in_flight()
            .await
            .map_err(|e| anyhow::anyhow!("drain in-flight: {e:?}"))?;
        let result = self.overlay.flush(&self.store).await?;
        Ok(result.map(|(root, plaintext_hash, _stats)| {
            let read_store: Arc<dyn BlobsRead> = Arc::new(self.store.clone());
            Snapshot::new(
                root,
                read_store,
                self.overlay.pipeline().context().clone(),
                Some(plaintext_hash),
            )
        }))
    }

    /// Returns a clone of the write-signal handle. The debounce
    /// helper waits on this to wake its idle timer.
    pub(crate) fn write_signal(&self) -> Arc<Notify> {
        Arc::clone(&self.write_signal)
    }

    /// Test/diagnostic accessor: read access to the pipeline. Hidden
    /// behind `cfg(test)` because the pipeline is meant to be an
    /// implementation detail (see module docs).
    #[cfg(test)]
    pub(crate) fn pipeline(&self) -> &Pipeline {
        self.overlay.pipeline()
    }

    /// Pulse the write signal — called by every state-changing FUSE
    /// callback. Cheap (a single atomic + waker wake).
    fn signal_write(&self) {
        self.write_signal.notify_one();
    }

    /// Read access to the in-flight buffer for the test/diagnostic path.
    #[cfg(test)]
    async fn in_flight_len(&self, key: &str) -> Option<usize> {
        self.in_flight.lock().await.get(key).map(|b| b.len())
    }
}

impl PathFilesystem for WritableFs {
    async fn init(&self, _req: Request) -> FuseResult<ReplyInit> {
        Ok(ReplyInit {
            max_write: std::num::NonZeroU32::new(1024 * 1024).unwrap(),
        })
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, parent: &OsStr, name: &OsStr) -> FuseResult<ReplyEntry> {
        let path = join(parent, name);
        let key = snapshot_key(&path);
        match self.resolve_for_attr(&key).await? {
            ResolvedEntry::File(entry) => Ok(ReplyEntry {
                ttl: ENTRY_TTL,
                attr: file_attr(&entry),
            }),
            ResolvedEntry::Directory => Ok(ReplyEntry {
                ttl: ENTRY_TTL,
                attr: dir_attr(),
            }),
            ResolvedEntry::Tombstone => Err(Errno::from(libc::ENOENT)),
        }
    }

    async fn getattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        _flags: u32,
    ) -> FuseResult<ReplyAttr> {
        let path = path.ok_or_else(|| Errno::from(libc::ENOENT))?;
        let key = snapshot_key(path);
        match self.resolve_for_attr(&key).await? {
            ResolvedEntry::File(entry) => Ok(ReplyAttr {
                ttl: ENTRY_TTL,
                attr: file_attr(&entry),
            }),
            ResolvedEntry::Directory => Ok(ReplyAttr {
                ttl: ENTRY_TTL,
                attr: dir_attr(),
            }),
            ResolvedEntry::Tombstone => Err(Errno::from(libc::ENOENT)),
        }
    }

    /// Truncate-only setattr (size). Other attributes (perms, times)
    /// silently succeed — the snapshot doesn't carry them yet.
    async fn setattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        set_attr: SetAttr,
    ) -> FuseResult<ReplyAttr> {
        let path = path.ok_or_else(|| Errno::from(libc::ENOENT))?;
        let key = snapshot_key(path);
        if let Some(new_size) = set_attr.size {
            let mut in_flight = self.in_flight.lock().await;
            let buf = in_flight.entry(key.clone()).or_default();
            buf.resize(new_size as usize, 0);
            drop(in_flight);
            self.signal_write();
        }
        // Re-read attrs (size now reflects the truncation).
        match self.resolve_for_attr(&key).await? {
            ResolvedEntry::File(entry) => Ok(ReplyAttr {
                ttl: ENTRY_TTL,
                attr: file_attr(&entry),
            }),
            ResolvedEntry::Directory => Ok(ReplyAttr {
                ttl: ENTRY_TTL,
                attr: dir_attr(),
            }),
            ResolvedEntry::Tombstone => Err(Errno::from(libc::ENOENT)),
        }
    }

    async fn create(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        _mode: u32,
        flags: u32,
    ) -> FuseResult<ReplyCreated> {
        let path = join(parent, name);
        let key = snapshot_key(&path);
        // Allocate (or reset) the in-flight buffer.
        {
            let mut in_flight = self.in_flight.lock().await;
            in_flight.insert(key.clone(), Vec::new());
        }
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.signal_write();
        Ok(ReplyCreated {
            ttl: ENTRY_TTL,
            attr: file_attr(&Self::in_flight_entry(0)),
            generation: 0,
            fh,
            flags,
        })
    }

    async fn open(&self, _req: Request, path: &OsStr, flags: u32) -> FuseResult<ReplyOpen> {
        let key = snapshot_key(path);
        // O_TRUNC: clear any existing in-flight buffer; the file's
        // committed bytes (in overlay/snapshot) get shadowed once the
        // first write lands.
        if (flags as i32) & libc::O_TRUNC != 0 {
            {
                let mut in_flight = self.in_flight.lock().await;
                in_flight.insert(key, Vec::new());
            }
            self.signal_write();
        }
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        Ok(ReplyOpen { fh, flags })
    }

    async fn write(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: u64,
        offset: u64,
        data: &[u8],
        _write_flags: u32,
        _flags: u32,
    ) -> FuseResult<ReplyWrite> {
        let path = path.ok_or_else(|| Errno::from(libc::ENOENT))?;
        let key = snapshot_key(path);
        let mut in_flight = self.in_flight.lock().await;
        let buf = in_flight.entry(key).or_default();
        let end = offset as usize + data.len();
        if buf.len() < end {
            buf.resize(end, 0);
        }
        buf[offset as usize..end].copy_from_slice(data);
        drop(in_flight);
        self.signal_write();
        Ok(ReplyWrite {
            written: data.len() as u32,
        })
    }

    async fn read(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: u64,
        offset: u64,
        size: u32,
    ) -> FuseResult<ReplyData> {
        let path = path.ok_or_else(|| Errno::from(libc::ENOENT))?;
        let key = snapshot_key(path);

        // Tier 1: in-flight buffer (write-in-progress).
        {
            let in_flight = self.in_flight.lock().await;
            if let Some(buf) = in_flight.get(&key) {
                let start = (offset as usize).min(buf.len());
                let end = start.saturating_add(size as usize).min(buf.len());
                return Ok(ReplyData {
                    data: Bytes::copy_from_slice(&buf[start..end]),
                });
            }
        }

        // Tier 2/3: overlay (with base fall-through inside).
        let entry = self
            .overlay
            .get(&key)
            .await
            .map_err(|err| {
                warn!(key, error = %err, "overlay.get failed");
                Errno::from(libc::EIO)
            })?
            .ok_or_else(|| Errno::from(libc::ENOENT))?;
        let bytes = self
            .overlay
            .pipeline()
            .export_bytes(&entry)
            .await
            .map_err(|err| {
                warn!(key, error = %err, "export_bytes failed");
                Errno::from(libc::EIO)
            })?;
        let start = (offset as usize).min(bytes.len());
        let end = start.saturating_add(size as usize).min(bytes.len());
        Ok(ReplyData {
            data: bytes.slice(start..end),
        })
    }

    /// On the last close of the FD, FUSE calls `release`. We commit
    /// the in-flight buffer to the overlay here. Multiple FDs on the
    /// same path will each trigger a release; v0 commits per release
    /// and the last writer wins.
    async fn release(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> FuseResult<()> {
        let Some(path) = path else { return Ok(()) };
        let key = snapshot_key(path);
        let buf = {
            let mut in_flight = self.in_flight.lock().await;
            in_flight.remove(&key)
        };
        if let Some(bytes) = buf {
            self.commit_buffer(&key, bytes).await?;
            self.signal_write();
        }
        Ok(())
    }

    /// `flush` (close-time, can fire multiple times): currently a
    /// no-op. We commit on `release`. Real fsync semantics will land
    /// when the daemon-side flush_overlay timer is wired.
    async fn flush(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        _fh: u64,
        _lock_owner: u64,
    ) -> FuseResult<()> {
        Ok(())
    }

    async fn unlink(&self, _req: Request, parent: &OsStr, name: &OsStr) -> FuseResult<()> {
        let path = join(parent, name);
        let key = snapshot_key(&path);
        // Drop any pending in-flight buffer for this path.
        {
            let mut in_flight = self.in_flight.lock().await;
            in_flight.remove(&key);
        }
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as u32)
            .unwrap_or(0);
        self.overlay.delete(key, NodeEntry::tombstone(timestamp));
        self.signal_write();
        Ok(())
    }

    async fn opendir(&self, _req: Request, path: &OsStr, _flags: u32) -> FuseResult<ReplyOpen> {
        let key = snapshot_key(path);
        match resolve(self.overlay.as_ref() as &dyn ReadableLayer, &key).await? {
            ResolvedEntry::Directory => Ok(ReplyOpen { fh: 0, flags: 0 }),
            _ => Err(Errno::from(libc::ENOTDIR)),
        }
    }

    async fn readdir<'a>(
        &'a self,
        _req: Request,
        path: &'a OsStr,
        _fh: u64,
        offset: i64,
    ) -> FuseResult<
        ReplyDirectory<impl futures_util::Stream<Item = FuseResult<DirectoryEntry>> + Send + 'a>,
    > {
        let key = snapshot_key(path);
        let mut children = list_children(self.overlay.as_ref() as &dyn ReadableLayer, &key).await?;
        // Splice in any in-flight files that land directly under this directory
        // and aren't yet in the overlay.
        let prefix = if key.is_empty() {
            String::new()
        } else {
            format!("{key}/")
        };
        {
            let in_flight = self.in_flight.lock().await;
            for in_flight_key in in_flight.keys() {
                let Some(suffix) = in_flight_key.strip_prefix(&prefix) else {
                    continue;
                };
                if suffix.contains('/') {
                    continue; // belongs to a deeper directory
                }
                if !children.iter().any(|(n, _)| n == suffix) {
                    children.push((suffix.to_string(), FileType::RegularFile));
                }
            }
        }

        let mut entries: Vec<DirectoryEntry> = Vec::with_capacity(children.len() + 2);
        entries.push(DirectoryEntry {
            kind: FileType::Directory,
            name: OsString::from("."),
            offset: 1,
        });
        entries.push(DirectoryEntry {
            kind: FileType::Directory,
            name: OsString::from(".."),
            offset: 2,
        });
        for (idx, (child_name, kind)) in children.into_iter().enumerate() {
            entries.push(DirectoryEntry {
                kind,
                name: OsString::from(child_name),
                offset: (idx as i64) + 3,
            });
        }
        let skip = (offset.max(0) as usize).min(entries.len());
        let yielded = entries.into_iter().skip(skip).map(Ok).collect::<Vec<_>>();
        Ok(ReplyDirectory {
            entries: stream::iter(yielded),
        })
    }

    /// Combined `readdir + lookup` — see `ReadOnlyFs::readdirplus` for
    /// the rationale (kernels >= 3.9 prefer this and some don't fall
    /// back from ENOSYS gracefully). In-flight buffer entries are
    /// spliced in just like `readdir` does, with synthetic attrs from
    /// `Self::in_flight_entry`.
    async fn readdirplus<'a>(
        &'a self,
        _req: Request,
        path: &'a OsStr,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> FuseResult<
        ReplyDirectoryPlus<
            impl futures_util::Stream<Item = FuseResult<DirectoryEntryPlus>> + Send + 'a,
        >,
    > {
        let key = snapshot_key(path);
        let mut children =
            list_children_with_entries(self.overlay.as_ref() as &dyn ReadableLayer, &key).await?;
        // Splice in any in-flight files that land directly under this
        // directory and aren't yet in the overlay (mirrors readdir).
        let prefix = if key.is_empty() {
            String::new()
        } else {
            format!("{key}/")
        };
        {
            let in_flight = self.in_flight.lock().await;
            for (in_flight_key, buf) in in_flight.iter() {
                let Some(suffix) = in_flight_key.strip_prefix(&prefix) else {
                    continue;
                };
                if suffix.contains('/') {
                    continue;
                }
                if !children.iter().any(|(n, _, _)| n == suffix) {
                    children.push((
                        suffix.to_string(),
                        FileType::RegularFile,
                        Some(Self::in_flight_entry(buf.len())),
                    ));
                }
            }
        }

        let self_attr = dir_attr();
        let parent_attr = dir_attr();
        let mut entries: Vec<DirectoryEntryPlus> = Vec::with_capacity(children.len() + 2);
        entries.push(DirectoryEntryPlus {
            kind: FileType::Directory,
            name: OsString::from("."),
            offset: 1,
            attr: self_attr,
            entry_ttl: ENTRY_TTL,
            attr_ttl: ENTRY_TTL,
        });
        entries.push(DirectoryEntryPlus {
            kind: FileType::Directory,
            name: OsString::from(".."),
            offset: 2,
            attr: parent_attr,
            entry_ttl: ENTRY_TTL,
            attr_ttl: ENTRY_TTL,
        });
        for (idx, (child_name, kind, entry)) in children.into_iter().enumerate() {
            let attr = match (&kind, entry.as_ref()) {
                (FileType::RegularFile, Some(e)) => file_attr(e),
                _ => dir_attr(),
            };
            entries.push(DirectoryEntryPlus {
                kind,
                name: OsString::from(child_name),
                offset: (idx as i64) + 3,
                attr,
                entry_ttl: ENTRY_TTL,
                attr_ttl: ENTRY_TTL,
            });
        }
        let skip = (offset as usize).min(entries.len());
        let yielded = entries.into_iter().skip(skip).map(Ok).collect::<Vec<_>>();
        Ok(ReplyDirectoryPlus {
            entries: stream::iter(yielded),
        })
    }

    async fn statfs(&self, _req: Request, _path: &OsStr) -> FuseResult<ReplyStatFs> {
        Ok(ReplyStatFs {
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files: 0,
            ffree: 0,
            bsize: BLOCK_SIZE,
            namelen: 4096,
            frsize: BLOCK_SIZE,
        })
    }

    async fn access(&self, _req: Request, _path: &OsStr, _mask: u32) -> FuseResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s5_core::blob::BlobStore;
    use s5_fs_v2::node::{
        BlobPipeline, CompressionStrategy, EncryptionStrategy, PaddingStrategy, TraversalContext,
    };
    use s5_fs_v2::snapshot::{KEY_SLOT_LEAF, KEY_SLOT_NODE};
    use s5_store_local::{LocalStore, LocalStoreConfig};
    use std::collections::BTreeMap;

    /// Build an empty encrypted Snapshot + matching BlobStore.
    fn empty_snapshot() -> (Snapshot, BlobStore) {
        let store_dir = tempfile::tempdir().unwrap();
        let store = BlobStore::new(LocalStore::create(LocalStoreConfig {
            base_path: store_dir.path().to_string_lossy().into_owned(),
        }));
        // Leak the tempdir so the store stays valid for the test's lifetime.
        std::mem::forget(store_dir);

        let mut keys = BTreeMap::new();
        keys.insert(KEY_SLOT_LEAF, [42u8; 32]);
        keys.insert(KEY_SLOT_NODE, [43u8; 32]);
        let pad = Some(PaddingStrategy { block_size: 1024 });
        let leaf_pipeline = BlobPipeline {
            compression: Some(CompressionStrategy::Zstd),
            padding: pad.clone(),
            encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_LEAF)),
            skip_when_unhelpful: None,
        };
        let node_pipeline = BlobPipeline {
            compression: Some(CompressionStrategy::Zstd),
            padding: pad,
            encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_NODE)),
            skip_when_unhelpful: None,
        };
        let ctx = TraversalContext {
            keys: Some(keys),
            leaf: Some(leaf_pipeline),
            node: Some(node_pipeline),
            chunking: None,
        };
        let read_store: Arc<dyn s5_core::BlobsRead> = Arc::new(store.clone());
        let snapshot = Snapshot::empty(read_store, ctx);
        (snapshot, store)
    }

    /// create → write → read (without release) → release → flush → re-read.
    /// Verifies the three-tier read path (in-flight, overlay, snapshot) and
    /// the `flush_overlay` round-trip.
    #[tokio::test]
    async fn write_then_read_roundtrip() -> anyhow::Result<()> {
        let (snapshot, store) = empty_snapshot();
        let fs = WritableFs::new(snapshot, store);

        let key = "hello.txt".to_string();
        let data = b"writable v0\n".to_vec();

        // Stage the buffer manually (this is what `create` + `write` do
        // — exercising the helpers without standing up fuse3).
        {
            let mut in_flight = fs.in_flight.lock().await;
            in_flight.insert(key.clone(), data.clone());
        }
        // Tier-1 read: serve from in-flight buffer.
        assert_eq!(fs.in_flight_len(&key).await, Some(data.len()));

        // Drain in-flight → overlay (what `release` does).
        fs.commit_buffer(&key, data.clone())
            .await
            .map_err(|e| anyhow::anyhow!("commit: {e:?}"))?;
        {
            let mut in_flight = fs.in_flight.lock().await;
            in_flight.remove(&key);
        }
        assert!(fs.in_flight_len(&key).await.is_none());

        // Tier-2 read: overlay has the entry, bytes via snapshot store.
        let entry = fs
            .overlay
            .get(&key)
            .await?
            .expect("entry committed to overlay");
        let bytes = fs.pipeline().export_bytes(&entry).await?;
        assert_eq!(&bytes[..], &data[..]);

        // Flush: overlay folds into a fresh snapshot.
        let new_snap = fs
            .flush_overlay()
            .await?
            .expect("non-empty overlay should produce a snapshot");
        let entry2 = new_snap
            .get(&key)
            .await?
            .expect("flushed snapshot has the entry");
        let bytes2 = new_snap.export_bytes(&entry2).await?;
        assert_eq!(&bytes2[..], &data[..]);

        Ok(())
    }

    /// A freshly committed file's `semantic.timestamp` reflects the
    /// commit time, not 1970. The earlier adapter defaulted everything
    /// to UNIX_EPOCH; this test pins the fix.
    #[tokio::test]
    async fn commit_buffer_stamps_real_mtime() -> anyhow::Result<()> {
        let (snapshot, store) = empty_snapshot();
        let fs = WritableFs::new(snapshot, store);

        let before = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_secs();

        fs.commit_buffer("hello.txt", b"hi".to_vec())
            .await
            .map_err(|e| anyhow::anyhow!("commit: {e:?}"))?;

        let entry = fs
            .overlay
            .get("hello.txt")
            .await?
            .expect("entry committed to overlay");
        let sem = entry.semantic.as_ref().expect("commit_buffer set semantic");
        let stamp = sem.timestamp.expect("commit_buffer set semantic.timestamp") as u64;

        assert!(
            stamp >= before && stamp <= before + 5,
            "expected mtime ≈ now ({before}..{}), got {stamp}",
            before + 5,
        );
        // file_attr should round-trip the timestamp into FileAttr.mtime.
        let attr = crate::attr::file_attr(&entry);
        let mtime_secs = attr
            .mtime
            .duration_since(std::time::SystemTime::UNIX_EPOCH)?
            .as_secs();
        assert_eq!(
            mtime_secs, stamp,
            "FileAttr.mtime mirrors semantic.timestamp"
        );
        Ok(())
    }

    /// Unlink stamps a tombstone in the overlay; subsequent reads see ENOENT.
    #[tokio::test]
    async fn unlink_tombstones_path() -> anyhow::Result<()> {
        let (snapshot, store) = empty_snapshot();
        let fs = WritableFs::new(snapshot, store);

        // Commit a file, then "unlink" it.
        fs.commit_buffer("doomed.txt", b"goodbye".to_vec())
            .await
            .map_err(|e| anyhow::anyhow!("commit: {e:?}"))?;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        fs.overlay
            .delete("doomed.txt".to_string(), NodeEntry::tombstone(timestamp));

        // Resolve via the writable resolver — sees Tombstone (which the
        // PathFilesystem methods translate to ENOENT).
        match fs
            .resolve_for_attr("doomed.txt")
            .await
            .map_err(|e| anyhow::anyhow!("resolve: {e:?}"))?
        {
            ResolvedEntry::Tombstone => {}
            _ => panic!("expected ResolvedEntry::Tombstone after unlink"),
        }

        Ok(())
    }
}
