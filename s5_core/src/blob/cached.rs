//! Read-through cache for [`BlobsRead`].
//!
//! Wraps an inner store and caches `blob_download` results in memory.
//! Designed for metadata/prolly-tree nodes that are read many times
//! during a single operation (e.g. change detection across thousands
//! of files).
//!
//! The cache is unbounded — callers should create a fresh instance per
//! operation rather than keeping one alive indefinitely.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use bytes::Bytes;

use super::{BlobResult, BlobsRead};
use crate::Hash;

/// A [`BlobsRead`] wrapper that caches full blob downloads in memory.
///
/// Cache hits avoid disk I/O + decryption entirely. `Bytes` is
/// reference-counted, so clones are cheap.
///
/// Only `blob_download` is cached — sliced reads and streaming reads
/// pass through to the inner store.
pub struct CachedBlobsRead {
    inner: Arc<dyn BlobsRead>,
    cache: RwLock<HashMap<Hash, Bytes>>,
}

impl CachedBlobsRead {
    /// Wrap an existing [`BlobsRead`] with an in-memory cache.
    pub fn new(inner: Arc<dyn BlobsRead>) -> Self {
        Self {
            inner,
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Wrap with a pre-sized cache (hint for expected number of blobs).
    pub fn with_capacity(inner: Arc<dyn BlobsRead>, capacity: usize) -> Self {
        Self {
            inner,
            cache: RwLock::new(HashMap::with_capacity(capacity)),
        }
    }
}

impl std::fmt::Debug for CachedBlobsRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.cache.read().map(|c| c.len()).unwrap_or(0);
        f.debug_struct("CachedBlobsRead")
            .field("cached_blobs", &len)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl BlobsRead for CachedBlobsRead {
    async fn blob_contains(&self, hash: Hash) -> BlobResult<bool> {
        // Check cache first (avoids disk I/O for known blobs).
        {
            let cache = self.cache.read().unwrap();
            if cache.contains_key(&hash) {
                return Ok(true);
            }
        }
        self.inner.blob_contains(hash).await
    }

    async fn blob_get_size(&self, hash: Hash) -> BlobResult<u64> {
        // Check cache first — we know the size from cached bytes.
        {
            let cache = self.cache.read().unwrap();
            if let Some(bytes) = cache.get(&hash) {
                return Ok(bytes.len() as u64);
            }
        }
        self.inner.blob_get_size(hash).await
    }

    async fn blob_download(&self, hash: Hash) -> BlobResult<Bytes> {
        // Fast path: return cached copy (Bytes::clone is Arc-bump).
        {
            let cache = self.cache.read().unwrap();
            if let Some(bytes) = cache.get(&hash) {
                return Ok(bytes.clone());
            }
        }

        // Miss: fetch from inner store.
        let bytes = self.inner.blob_download(hash).await?;

        // Insert into cache.
        {
            let mut cache = self.cache.write().unwrap();
            cache.insert(hash, bytes.clone());
        }

        Ok(bytes)
    }

    async fn blob_download_slice(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> BlobResult<Bytes> {
        // Try to serve from cache.
        {
            let cache = self.cache.read().unwrap();
            if let Some(bytes) = cache.get(&hash) {
                let start = offset as usize;
                let end = match max_len {
                    Some(len) => (start + len as usize).min(bytes.len()),
                    None => bytes.len(),
                };
                if start <= bytes.len() {
                    return Ok(bytes.slice(start..end));
                }
            }
        }

        self.inner.blob_download_slice(hash, offset, max_len).await
    }

    async fn blob_read(
        &self,
        hash: Hash,
    ) -> BlobResult<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
        // Streaming reads pass through — not worth caching.
        self.inner.blob_read(hash).await
    }
}
