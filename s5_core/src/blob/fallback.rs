//! Fallback reader: tries a primary [`BlobsRead`], falls back to a secondary.
//!
//! Useful when blobs are spread across multiple stores (e.g. tree nodes
//! in a local meta store, file content in a remote blob store). Wrap
//! both in a `FallbackBlobsRead` so callers can transparently fetch
//! from either.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use super::{BlobResult, BlobsRead};
use crate::Hash;

/// A [`BlobsRead`] adapter that tries `primary` first, then `secondary`.
///
/// # Error handling
///
/// Any error from the primary triggers the fallback — the secondary's
/// result (success or error) is then returned directly.
pub struct FallbackBlobsRead {
    primary: Arc<dyn BlobsRead>,
    secondary: Arc<dyn BlobsRead>,
}

impl FallbackBlobsRead {
    /// Create a new fallback reader.
    ///
    /// `primary` is tried first. `secondary` is the fallback.
    pub fn new(primary: Arc<dyn BlobsRead>, secondary: Arc<dyn BlobsRead>) -> Self {
        Self { primary, secondary }
    }
}

impl std::fmt::Debug for FallbackBlobsRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FallbackBlobsRead").finish_non_exhaustive()
    }
}

#[async_trait]
impl BlobsRead for FallbackBlobsRead {
    async fn blob_contains(&self, hash: Hash) -> BlobResult<bool> {
        if self.primary.blob_contains(hash).await? {
            return Ok(true);
        }
        self.secondary.blob_contains(hash).await
    }

    async fn blob_get_size(&self, hash: Hash) -> BlobResult<u64> {
        match self.primary.blob_get_size(hash).await {
            Ok(size) => Ok(size),
            Err(_) => self.secondary.blob_get_size(hash).await,
        }
    }

    async fn blob_download(&self, hash: Hash) -> BlobResult<Bytes> {
        match self.primary.blob_download(hash).await {
            Ok(bytes) => Ok(bytes),
            Err(_) => self.secondary.blob_download(hash).await,
        }
    }

    async fn blob_download_slice(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> BlobResult<Bytes> {
        match self
            .primary
            .blob_download_slice(hash, offset, max_len)
            .await
        {
            Ok(bytes) => Ok(bytes),
            Err(_) => {
                self.secondary
                    .blob_download_slice(hash, offset, max_len)
                    .await
            }
        }
    }

    async fn blob_read(
        &self,
        hash: Hash,
    ) -> BlobResult<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
        match self.primary.blob_read(hash).await {
            Ok(reader) => Ok(reader),
            Err(_) => self.secondary.blob_read(hash).await,
        }
    }
}
