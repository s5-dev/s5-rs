//! `CachingStore` — a read-through RAM cache decorator over two [`Store`]s.
//!
//! Lives in its own module so `store.rs` stays the `Store` trait plus the
//! cross-store migration primitives; this file is the one concrete decorator.

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;

use crate::blob::location::BlobLocation;
use crate::store::{Store, StoreFeatures, StoreResult};

/// A read-through cache decorator over any two [`Store`]s.
///
/// Reads are served from `cache` whenever the blob is resident — this covers
/// both **whole-blob** reads AND **sliced** reads. The peer-serve download
/// path reads each blob in fixed (e.g. 64 KiB) slices, so the cache must serve
/// those from the one cached copy or it would never help that path: a sliced
/// miss of a blob at or below [`Self::slice_materialize_max`] materializes the
/// WHOLE blob into `cache` (then returns the requested window), turning the
/// remaining N-1 slices of a sequential download into Arc-bump RAM hits
/// instead of per-slice open/seek/read/close syscalls on the file store.
/// Blobs above the cap fall through to a direct ranged `inner` read (we don't
/// pull, e.g., a 292 MB rindex shard into RAM to serve one 64 KiB slice).
/// Writes go straight to `inner` — the cache is a NON-DURABLE read
/// accelerator, never the source of truth, and is not populated on write (the
/// first read of a blob fills it). Deletes and renames invalidate the cache.
///
/// `cache` is meant to be a byte-budgeted RAM store (e.g.
/// `s5_store_memory::MemoryStore::with_budget`) so the resident set is bounded
/// and eviction is automatic; `inner` is the persistent backing store. S5
/// blobs are content-addressed (path = hash), so a cached entry can never go
/// stale — invalidation is only needed on delete/rename.
///
/// Intended for the publisher serve path: many peers (plus per-revision
/// manifest re-walks) re-fetch the same hot blobs, so a RAM cache above the
/// hot tier turns repeated serves into Arc-bump RAM hits instead of
/// open/read/close syscalls on the file store.
pub struct CachingStore {
    cache: std::sync::Arc<dyn Store>,
    inner: std::sync::Arc<dyn Store>,
    /// A sliced/ranged read of a blob at most this many bytes materializes the
    /// WHOLE blob into `cache` (then serves the requested window from it), so
    /// the remaining slices of a sequential download are RAM hits. Larger
    /// blobs fall through to a direct ranged `inner` read — we don't pull a
    /// large single-blob file (e.g. a ~292 MB rindex shard) into RAM to serve
    /// one small slice. Whole-blob reads are cached regardless of size.
    slice_materialize_max: u64,
}

/// Default [`CachingStore::slice_materialize_max`]. Comfortably covers feedy's
/// chunked blobs (`.seg`/`.eseg`/`interner`/`ledger`, ≤64 MiB) while excluding
/// the large single-blob `rindex` shards (~292 MB), which are fetched whole
/// once per consumer and gain nothing from publisher-side slice caching.
pub const DEFAULT_SLICE_MATERIALIZE_MAX: u64 = 128 * 1024 * 1024;

impl CachingStore {
    /// Wrap `inner` with a read-through `cache`. The cache should be bounded
    /// (it grows with the read working set); `inner` holds the durable data.
    /// Uses [`DEFAULT_SLICE_MATERIALIZE_MAX`] as the slice-materialize cap.
    pub fn new(cache: std::sync::Arc<dyn Store>, inner: std::sync::Arc<dyn Store>) -> Self {
        Self::with_slice_materialize_max(cache, inner, DEFAULT_SLICE_MATERIALIZE_MAX)
    }

    /// Like [`Self::new`] but with an explicit slice-materialize cap (see
    /// [`Self::slice_materialize_max`]).
    pub fn with_slice_materialize_max(
        cache: std::sync::Arc<dyn Store>,
        inner: std::sync::Arc<dyn Store>,
        slice_materialize_max: u64,
    ) -> Self {
        Self {
            cache,
            inner,
            slice_materialize_max,
        }
    }
}

/// Clamp + slice `bytes` to the `(offset, max_len)` window, matching the
/// [`Store::open_read_bytes`] contract: an `offset` past the end yields empty,
/// a `max_len` past the end is truncated. `(0, None)` returns `bytes` as-is
/// (no copy — `Bytes::slice` is a refcount bump either way).
fn slice_bytes(bytes: Bytes, offset: u64, max_len: Option<u64>) -> Bytes {
    if offset == 0 && max_len.is_none() {
        return bytes;
    }
    let start = (offset as usize).min(bytes.len());
    let end = match max_len {
        Some(len) => start.saturating_add(len as usize).min(bytes.len()),
        None => bytes.len(),
    };
    bytes.slice(start..end)
}

impl std::fmt::Debug for CachingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachingStore")
            .field("inner", &self.inner)
            .field("cache", &self.cache)
            .finish()
    }
}

#[async_trait]
impl Store for CachingStore {
    fn features(&self) -> StoreFeatures {
        // Capabilities (rename/reflink support, case sensitivity) are the
        // backing store's — the cache forwards those operations to it.
        self.inner.features()
    }

    async fn exists(&self, path: &str) -> StoreResult<bool> {
        // Authoritative from inner: a cached read does not prove the durable
        // store still holds the blob (it may have been GC'd).
        self.inner.exists(path).await
    }

    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<()> {
        // Write-passthrough: the cache is non-durable and not populated on
        // write (content-addressed → the first read populates it).
        self.inner.put_bytes(path, bytes).await
    }

    async fn put_stream(
        &self,
        path: &str,
        stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<()> {
        self.inner.put_stream(path, stream).await
    }

    async fn open_read_stream(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>>
    {
        // Streaming reads pass through — the cached serve path is whole-blob
        // `open_read_bytes`; caching a stream isn't worth the buffering.
        self.inner.open_read_stream(path, offset, max_len).await
    }

    async fn open_read_bytes(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        let whole = offset == 0 && max_len.is_none();
        // Serve from the cached WHOLE blob whenever it is resident — covers
        // both whole-blob reads and sliced reads (the peer-serve download path
        // reads each blob in fixed slices). A miss surfaces as an Err from an
        // in-RAM store → fall through.
        if let Ok(bytes) = self.cache.open_read_bytes(path, 0, None).await {
            return Ok(slice_bytes(bytes, offset, max_len));
        }
        // Whole-blob miss: read the full blob once, populate, return it.
        if whole {
            let bytes = self.inner.open_read_bytes(path, 0, None).await?;
            // Best-effort populate — a cache write failure must not fail the read.
            let _ = self.cache.put_bytes(path, bytes.clone()).await;
            return Ok(bytes);
        }
        // Sliced/ranged miss: if the blob is small enough to be worth holding,
        // materialize the WHOLE blob into the cache (so the rest of a
        // sequential download hits RAM) and serve the slice from it; otherwise
        // read just the requested window from inner.
        match self.inner.size(path).await {
            Ok(size) if size <= self.slice_materialize_max => {
                let bytes = self.inner.open_read_bytes(path, 0, None).await?;
                let _ = self.cache.put_bytes(path, bytes.clone()).await;
                Ok(slice_bytes(bytes, offset, max_len))
            }
            _ => self.inner.open_read_bytes(path, offset, max_len).await,
        }
    }

    async fn size(&self, path: &str) -> StoreResult<u64> {
        self.inner.size(path).await
    }

    async fn list(
        &self,
    ) -> StoreResult<Box<dyn Stream<Item = Result<String, std::io::Error>> + Send + Unpin + 'static>>
    {
        self.inner.list().await
    }

    async fn delete(&self, path: &str) -> StoreResult<()> {
        // Invalidate the cache, then delete durably.
        let _ = self.cache.delete(path).await;
        self.inner.delete(path).await
    }

    async fn rename(&self, old_path: &str, new_path: &str) -> StoreResult<()> {
        // Drop any stale cache entries; the new path repopulates on next read.
        let _ = self.cache.delete(old_path).await;
        let _ = self.cache.delete(new_path).await;
        self.inner.rename(old_path, new_path).await
    }

    async fn provide(&self, path: &str) -> StoreResult<Vec<BlobLocation>> {
        self.inner.provide(path).await
    }

    async fn sync(&self) -> StoreResult<()> {
        // Only the durable store needs syncing; the cache is non-durable.
        self.inner.sync().await
    }

    async fn modified(&self, path: &str) -> StoreResult<Option<std::time::SystemTime>> {
        self.inner.modified(path).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn reflink_file_to(&self, source: &std::path::Path, dest_path: &str) -> StoreResult<()> {
        self.inner.reflink_file_to(source, dest_path).await
    }
}

#[cfg(test)]
mod caching_store_tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Minimal in-RAM `Store` that counts reads/writes (s5_core cannot depend
    /// on `s5_store_memory` — circular — so we roll a tiny one here).
    #[derive(Debug, Default)]
    struct CountingStore {
        data: Mutex<HashMap<String, Bytes>>,
        reads: AtomicUsize,
        writes: AtomicUsize,
    }
    impl CountingStore {
        fn reads(&self) -> usize {
            self.reads.load(Ordering::Relaxed)
        }
        fn writes(&self) -> usize {
            self.writes.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl Store for CountingStore {
        fn features(&self) -> StoreFeatures {
            StoreFeatures::default()
        }
        async fn exists(&self, path: &str) -> StoreResult<bool> {
            Ok(self.data.lock().unwrap().contains_key(path))
        }
        async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<()> {
            self.writes.fetch_add(1, Ordering::Relaxed);
            self.data.lock().unwrap().insert(path.to_string(), bytes);
            Ok(())
        }
        async fn put_stream(
            &self,
            _: &str,
            _: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
        ) -> StoreResult<()> {
            unimplemented!()
        }
        async fn open_read_stream(
            &self,
            _: &str,
            _: u64,
            _: Option<u64>,
        ) -> StoreResult<
            Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
        > {
            unimplemented!()
        }
        async fn open_read_bytes(
            &self,
            path: &str,
            offset: u64,
            max_len: Option<u64>,
        ) -> StoreResult<Bytes> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            let g = self.data.lock().unwrap();
            let full = g
                .get(path)
                .ok_or_else(|| anyhow::anyhow!("not found: {path}"))?;
            // Test-only minimal slice so the ranged-passthrough test is honest.
            let start = (offset as usize).min(full.len());
            let end = max_len
                .map(|l| start.saturating_add(l as usize).min(full.len()))
                .unwrap_or(full.len());
            Ok(full.slice(start..end))
        }
        async fn size(&self, path: &str) -> StoreResult<u64> {
            Ok(self
                .data
                .lock()
                .unwrap()
                .get(path)
                .map(|b| b.len() as u64)
                .unwrap_or(0))
        }
        async fn list(
            &self,
        ) -> StoreResult<
            Box<dyn Stream<Item = Result<String, std::io::Error>> + Send + Unpin + 'static>,
        > {
            unimplemented!()
        }
        async fn delete(&self, path: &str) -> StoreResult<()> {
            self.data.lock().unwrap().remove(path);
            Ok(())
        }
        async fn rename(&self, _: &str, _: &str) -> StoreResult<()> {
            unimplemented!()
        }
        async fn provide(&self, _: &str) -> StoreResult<Vec<BlobLocation>> {
            Ok(vec![])
        }
    }

    #[tokio::test]
    async fn hit_serves_second_read_without_touching_inner() {
        let inner = Arc::new(CountingStore::default());
        let cache = Arc::new(CountingStore::default());
        inner
            .put_bytes("blob3/a", Bytes::from_static(b"hello world"))
            .await
            .unwrap();
        // The direct put above counts as a write; reset the lens by reading it.
        let caching = CachingStore::new(cache.clone(), inner.clone());

        // First read: cache miss → one inner read + cache populate.
        let r1 = caching.open_read_bytes("blob3/a", 0, None).await.unwrap();
        assert_eq!(&r1[..], b"hello world");
        assert_eq!(inner.reads(), 1, "first read must hit inner");
        assert_eq!(cache.writes(), 1, "miss must populate the cache");

        // Second read: cache hit → NO additional inner read.
        let r2 = caching.open_read_bytes("blob3/a", 0, None).await.unwrap();
        assert_eq!(&r2[..], b"hello world");
        assert_eq!(inner.reads(), 1, "cache hit must not touch inner");
    }

    #[tokio::test]
    async fn sliced_read_materializes_whole_blob_then_serves_from_cache() {
        let inner = Arc::new(CountingStore::default());
        let cache = Arc::new(CountingStore::default());
        inner
            .put_bytes("blob3/b", Bytes::from_static(b"0123456789"))
            .await
            .unwrap();
        let caching = CachingStore::new(cache.clone(), inner.clone());

        // Sliced miss (the 64 KiB peer-serve pattern): read the WHOLE blob from
        // inner ONCE, populate the cache, serve the requested window from it.
        let r = caching
            .open_read_bytes("blob3/b", 2, Some(3))
            .await
            .unwrap();
        assert_eq!(&r[..], b"234");
        assert_eq!(inner.reads(), 1, "sliced miss reads the whole blob once");
        assert!(
            cache.exists("blob3/b").await.unwrap(),
            "sliced miss must populate the cache with the whole blob"
        );

        // The next slices of the same blob (a sequential download) are served
        // from the cached copy — inner is NOT touched again. This is the win:
        // N slices → 1 inner read instead of N file open/seek/read/close.
        let r2 = caching
            .open_read_bytes("blob3/b", 0, Some(2))
            .await
            .unwrap();
        assert_eq!(&r2[..], b"01");
        let r3 = caching
            .open_read_bytes("blob3/b", 5, Some(64))
            .await
            .unwrap();
        assert_eq!(&r3[..], b"56789", "max_len past the end is truncated");
        // ...and a whole-blob read of it is a cache hit too.
        let whole = caching.open_read_bytes("blob3/b", 0, None).await.unwrap();
        assert_eq!(&whole[..], b"0123456789");
        assert_eq!(
            inner.reads(),
            1,
            "subsequent slices + whole read must hit the cache, not inner"
        );
    }

    #[tokio::test]
    async fn sliced_read_above_cap_does_not_materialize() {
        let inner = Arc::new(CountingStore::default());
        let cache = Arc::new(CountingStore::default());
        inner
            .put_bytes("blob3/big", Bytes::from_static(b"0123456789"))
            .await
            .unwrap();
        // Cap below the blob size → a sliced read passes straight through to
        // inner and never pulls the whole blob into RAM (the rindex-shard case).
        let caching = CachingStore::with_slice_materialize_max(cache.clone(), inner.clone(), 4);

        let r = caching
            .open_read_bytes("blob3/big", 2, Some(3))
            .await
            .unwrap();
        assert_eq!(&r[..], b"234");
        assert!(
            !cache.exists("blob3/big").await.unwrap(),
            "an above-cap sliced read must not populate the cache"
        );
        // A second slice still goes to inner (no caching) — passthrough preserved.
        caching
            .open_read_bytes("blob3/big", 0, Some(2))
            .await
            .unwrap();
        assert_eq!(inner.reads(), 2, "above-cap slices are not cached");
    }

    #[tokio::test]
    async fn writes_passthrough_and_delete_invalidates() {
        let inner = Arc::new(CountingStore::default());
        let cache = Arc::new(CountingStore::default());
        let caching = CachingStore::new(cache.clone(), inner.clone());

        // Write goes to inner only (cache not populated on write).
        caching
            .put_bytes("blob3/c", Bytes::from_static(b"xyz"))
            .await
            .unwrap();
        assert!(inner.exists("blob3/c").await.unwrap());
        assert!(!cache.exists("blob3/c").await.unwrap());

        // Read populates the cache.
        caching.open_read_bytes("blob3/c", 0, None).await.unwrap();
        assert!(cache.exists("blob3/c").await.unwrap());

        // Delete invalidates BOTH tiers.
        caching.delete("blob3/c").await.unwrap();
        assert!(!cache.exists("blob3/c").await.unwrap());
        assert!(!inner.exists("blob3/c").await.unwrap());
    }
}
