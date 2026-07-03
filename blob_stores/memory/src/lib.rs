//! In-memory `s5_core::store::Store`.
//!
//! Two backends behind one type:
//!
//! - [`MemoryStore::new`] — unbounded `DashMap`. No eviction, no
//!   counters. This is the original behavior; every existing caller
//!   keeps it unchanged.
//! - [`MemoryStore::with_budget`] — byte-weighted W-TinyLFU (`moka`)
//!   with a hard budget plus lifetime hit/miss counters. A drop-in
//!   bounded read-through tier in front of a slower store: hot blobs
//!   (by access frequency) live
//!   here with O(1) reads; cold blobs evict and fall through. Pure
//!   access-pattern eviction — no path prefixes, no pinning, no
//!   recency rules — so it adapts to whatever traffic each peer sees.
//!
//! The `Store` trait is path-based; inside a `BlobStore` wrapper paths
//! are deterministic hash-derived identifiers, so caching by path is
//! equivalent to caching by content hash.

use bytes::Bytes;
use dashmap::DashMap;
use futures::stream::{self, Stream, TryStreamExt};
use moka::sync::Cache as MokaCache;
use s5_core::{
    blob::location::BlobLocation,
    store::{StoreFeatures, StoreResult},
};

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};

/// Live metrics for a budgeted store. `None` for an unbounded one
/// (no budget, no counters — nothing to report).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryStoreStats {
    /// Approximate live byte weight (moka updates asynchronously).
    pub weighted_size: u64,
    /// Number of live entries.
    pub entry_count: u64,
    /// Lifetime cache hits observed at `open_read_bytes`.
    pub hits: u64,
    /// Lifetime cache misses observed at `open_read_bytes`.
    pub misses: u64,
}

#[derive(Debug)]
enum Backend {
    /// Unbounded — original behavior. No eviction, no counters.
    Unbounded(DashMap<String, Bytes>),
    /// Byte-weighted W-TinyLFU with a hard budget + hit/miss counters.
    /// moka 0.12 dropped public stats, so we count at the trait
    /// boundary to keep observability without a crate-internal flag.
    Budgeted {
        cache: MokaCache<String, Bytes>,
        hits: AtomicU64,
        misses: AtomicU64,
    },
}

#[derive(Debug)]
pub struct MemoryStore {
    backend: Backend,
}

impl MemoryStore {
    /// Unbounded in-memory store (original behavior; no eviction).
    pub fn new() -> Self {
        Self {
            backend: Backend::Unbounded(DashMap::new()),
        }
    }

    /// Byte-weighted W-TinyLFU store with a hard `budget_bytes` cap.
    /// Inserts past the budget evict the least-valuable entries (moka's
    /// frequency sketch). Weights are exact (`Bytes::len()`); the slack
    /// from `Bytes::clone()` is shared refcount, not extra weight.
    pub fn with_budget(budget_bytes: u64) -> Self {
        let cache = MokaCache::builder()
            .weigher(|_k: &String, v: &Bytes| u32::try_from(v.len()).unwrap_or(u32::MAX))
            .max_capacity(budget_bytes)
            .build();
        Self {
            backend: Backend::Budgeted {
                cache,
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
            },
        }
    }

    /// Live cache metrics — `Some` for a budgeted store, `None` for an
    /// unbounded one.
    pub fn stats(&self) -> Option<MemoryStoreStats> {
        match &self.backend {
            Backend::Unbounded(_) => None,
            Backend::Budgeted {
                cache,
                hits,
                misses,
            } => Some(MemoryStoreStats {
                weighted_size: cache.weighted_size(),
                entry_count: cache.entry_count(),
                hits: hits.load(Ordering::Relaxed),
                misses: misses.load(Ordering::Relaxed),
            }),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

fn not_found(path: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, format!("no such key: {path}"))
}

#[async_trait::async_trait]
impl s5_core::store::Store for MemoryStore {
    /// Consumes a stream of bytes and stores the concatenated result.
    async fn put_stream(
        &self,
        path: &str,
        stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<()> {
        let chunks: Vec<Bytes> = stream.try_collect().await?;
        let bytes = Bytes::from(chunks.concat());
        self.put_bytes(path, bytes).await
    }

    /// Returns the features supported by this store.
    fn features(&self) -> StoreFeatures {
        StoreFeatures {
            supports_rename: true,
            case_sensitive: true,
            recommended_max_dir_size: u64::MAX,
            supports_reflink: false,
        }
    }

    /// Checks if an object exists at the given path.
    async fn exists(&self, path: &str) -> StoreResult<bool> {
        Ok(match &self.backend {
            Backend::Unbounded(m) => m.contains_key(path),
            Backend::Budgeted { cache, .. } => cache.contains_key(path),
        })
    }

    /// Stores a `Bytes` object at the given path.
    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<()> {
        match &self.backend {
            Backend::Unbounded(m) => {
                m.insert(path.to_string(), bytes);
            }
            Backend::Budgeted { cache, .. } => cache.insert(path.to_string(), bytes),
        }
        Ok(())
    }

    /// Returns a stream that yields the bytes of the object.
    async fn open_read_stream(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>>
    {
        let bytes = self.open_read_bytes(path, offset, max_len).await?;
        let future = Box::pin(async { Ok(bytes) });
        let stream = stream::once(future);
        Ok(Box::new(stream))
    }

    /// Returns the bytes of the object at the given path. The budgeted
    /// backend counts hits/misses here; the unbounded one does not
    /// (preserving its original side-effect-free behavior).
    async fn open_read_bytes(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        let file: Bytes = match &self.backend {
            Backend::Unbounded(m) => m
                .get(path)
                .map(|r| r.value().clone())
                .ok_or_else(|| not_found(path))?,
            Backend::Budgeted {
                cache,
                hits,
                misses,
            } => match cache.get(path) {
                Some(b) => {
                    hits.fetch_add(1, Ordering::Relaxed);
                    b
                }
                None => {
                    misses.fetch_add(1, Ordering::Relaxed);
                    return Err(not_found(path).into());
                }
            },
        };

        let file_len = file.len();
        let start = offset as usize;
        if start >= file_len {
            return Ok(Bytes::new());
        }
        let remaining = file_len - start;
        let len = match max_len {
            Some(max) => remaining.min(max as usize),
            None => remaining,
        };
        Ok(file.slice(start..start + len))
    }

    /// Returns the total size of the object at the given path.
    async fn size(&self, path: &str) -> StoreResult<u64> {
        let len = match &self.backend {
            Backend::Unbounded(m) => m.get(path).map(|r| r.value().len()),
            Backend::Budgeted { cache, .. } => cache.get(path).map(|b| b.len()),
        };
        Ok(len.ok_or_else(|| not_found(path))? as u64)
    }

    /// Returns a stream of all object paths (best-effort snapshot).
    async fn list(
        &self,
    ) -> StoreResult<Box<dyn Stream<Item = Result<String, std::io::Error>> + Send + Unpin + 'static>>
    {
        let keys: Vec<Result<String, io::Error>> = match &self.backend {
            Backend::Unbounded(m) => m
                .iter()
                .map(|e| Ok::<_, io::Error>(e.key().clone()))
                .collect(),
            Backend::Budgeted { cache, .. } => cache
                .iter()
                .map(|(k, _v)| Ok::<_, io::Error>((*k).clone()))
                .collect(),
        };
        let stream = stream::iter(keys);
        Ok(Box::new(stream))
    }

    /// Deletes the object at the given path.
    async fn delete(&self, path: &str) -> StoreResult<()> {
        match &self.backend {
            Backend::Unbounded(m) => {
                m.remove(path);
            }
            Backend::Budgeted { cache, .. } => cache.invalidate(path),
        }
        Ok(())
    }

    /// Renames an object from an old path to a new path.
    async fn rename(&self, old_path: &str, new_path: &str) -> StoreResult<()> {
        if old_path == new_path {
            return Ok(());
        }
        match &self.backend {
            Backend::Unbounded(m) => {
                let (_k, value) = m.remove(old_path).ok_or_else(|| not_found(old_path))?;
                m.insert(new_path.to_string(), value);
            }
            Backend::Budgeted { cache, .. } => {
                let value = cache.get(old_path).ok_or_else(|| not_found(old_path))?;
                cache.insert(new_path.to_string(), value);
                cache.invalidate(old_path);
            }
        }
        Ok(())
    }

    /// Locations for a blob. For an in-memory store, always empty.
    async fn provide(&self, _path: &str) -> StoreResult<Vec<BlobLocation>> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s5_core::store::Store;
    use s5_core::testutil::StoreTests;

    /// Characterization: the unbounded backend (the original behavior)
    /// must still satisfy the full `Store` contract for every existing
    /// caller.
    #[tokio::test]
    async fn test_memory_store() {
        let store = MemoryStore::new();
        StoreTests::new(&store).run_all().await.unwrap();
    }

    /// The budgeted backend must satisfy the same contract.
    #[tokio::test]
    async fn budgeted_satisfies_store_contract() {
        let store = MemoryStore::with_budget(64 * 1024 * 1024);
        StoreTests::new(&store).run_all().await.unwrap();
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let store = MemoryStore::with_budget(1024 * 1024);
        let bytes = Bytes::from_static(b"hello world");
        store.put_bytes("foo", bytes.clone()).await.unwrap();
        assert!(store.exists("foo").await.unwrap());
        let got = store.open_read_bytes("foo", 0, None).await.unwrap();
        assert_eq!(got, bytes);
        assert_eq!(store.size("foo").await.unwrap(), 11);
    }

    #[tokio::test]
    async fn byte_weighted_eviction() {
        // 8 KB budget; inserting 16 KB must evict down to the budget.
        let store = MemoryStore::with_budget(8 * 1024);
        for i in 0..16 {
            let bytes = Bytes::from(vec![0u8; 1024]);
            store.put_bytes(&format!("k{i:02}"), bytes).await.unwrap();
        }
        // moka eviction is async; settle it deterministically.
        let stats = match &store.backend {
            Backend::Budgeted { cache, .. } => {
                cache.run_pending_tasks();
                store.stats().unwrap()
            }
            Backend::Unbounded(_) => unreachable!("with_budget is budgeted"),
        };
        assert!(
            stats.weighted_size <= 8 * 1024,
            "weight {} exceeds 8 KB budget — eviction not bounding",
            stats.weighted_size
        );
        assert!(
            stats.weighted_size >= 4 * 1024,
            "weight {} suspiciously low — over-evicted",
            stats.weighted_size
        );
    }

    #[tokio::test]
    async fn read_slice_offset_and_max_len() {
        let store = MemoryStore::with_budget(1024);
        store
            .put_bytes("x", Bytes::from_static(b"abcdefghij"))
            .await
            .unwrap();
        let mid = store.open_read_bytes("x", 3, Some(4)).await.unwrap();
        assert_eq!(mid, Bytes::from_static(b"defg"));
        let tail = store.open_read_bytes("x", 7, None).await.unwrap();
        assert_eq!(tail, Bytes::from_static(b"hij"));
    }

    #[tokio::test]
    async fn rename_moves_value() {
        let store = MemoryStore::with_budget(1024);
        store
            .put_bytes("old", Bytes::from_static(b"v"))
            .await
            .unwrap();
        store.rename("old", "new").await.unwrap();
        assert!(!store.exists("old").await.unwrap());
        assert!(store.exists("new").await.unwrap());
    }

    #[tokio::test]
    async fn rename_same_path_is_noop() {
        let store = MemoryStore::with_budget(1024);
        store
            .put_bytes("same", Bytes::from_static(b"v"))
            .await
            .unwrap();
        store.rename("same", "same").await.unwrap();
        assert!(store.exists("same").await.unwrap());
    }

    /// A downstream stats consumer depends on this exact contract:
    /// budgeted → `Some`, unbounded → `None`.
    #[tokio::test]
    async fn stats_some_for_budgeted_none_for_unbounded() {
        assert!(MemoryStore::new().stats().is_none());
        let b = MemoryStore::with_budget(1024);
        let s = b.stats().expect("budgeted store reports stats");
        assert_eq!(s.hits, 0);
        assert_eq!(s.misses, 0);
        // A miss then a hit must move the counters.
        let _ = b.open_read_bytes("absent", 0, None).await;
        b.put_bytes("k", Bytes::from_static(b"v")).await.unwrap();
        let _ = b.open_read_bytes("k", 0, None).await.unwrap();
        let s2 = b.stats().unwrap();
        assert_eq!(s2.misses, 1);
        assert_eq!(s2.hits, 1);
    }
}
