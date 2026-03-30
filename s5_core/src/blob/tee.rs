//! Tee writer: fans out [`BlobsWrite::blob_upload_bytes`] to two writers.
//!
//! Used when tree nodes must be persisted to both a local meta store
//! and a remote blob store (e.g. S3) so that disaster recovery is
//! possible from the remote alone.

use async_trait::async_trait;
use bytes::Bytes;

use super::{BlobId, BlobResult, BlobsWrite};
use crate::Hash;

/// A [`BlobsWrite`] adapter that writes to both `primary` and `secondary`.
///
/// `blob_upload_bytes` writes to primary first, then secondary.
/// The `BlobId` from primary is returned.
pub struct TeeBlobsWrite<'a> {
    primary: &'a dyn BlobsWrite,
    secondary: &'a dyn BlobsWrite,
}

impl<'a> TeeBlobsWrite<'a> {
    pub fn new(primary: &'a dyn BlobsWrite, secondary: &'a dyn BlobsWrite) -> Self {
        Self { primary, secondary }
    }
}

#[async_trait]
impl BlobsWrite for TeeBlobsWrite<'_> {
    async fn blob_upload_bytes(&self, bytes: Bytes) -> BlobResult<BlobId> {
        let id = self.primary.blob_upload_bytes(bytes.clone()).await?;
        self.secondary.blob_upload_bytes(bytes).await?;
        Ok(id)
    }

    async fn blob_upload_reader<R, F>(
        &self,
        _hash: Hash,
        _size: u64,
        _reader: R,
        _on_progress: F,
    ) -> BlobResult<BlobId>
    where
        Self: Sized,
        R: tokio::io::AsyncRead + Send + Unpin + 'static,
        F: Fn(u64) -> std::io::Result<()> + Send + Sync + 'static,
    {
        Err(anyhow::anyhow!("TeeBlobsWrite does not support blob_upload_reader"))
    }

    async fn blob_upload_stream<S>(&self, _stream: S) -> BlobResult<BlobId>
    where
        Self: Sized,
        S: futures::Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static,
    {
        Err(anyhow::anyhow!("TeeBlobsWrite does not support blob_upload_stream"))
    }

    #[cfg(not(target_arch = "wasm32"))]
    async fn blob_upload_file(&self, _path: std::path::PathBuf) -> BlobResult<BlobId> {
        Err(anyhow::anyhow!("TeeBlobsWrite does not support blob_upload_file"))
    }

    async fn blob_sync(&self) -> BlobResult<()> {
        self.primary.blob_sync().await?;
        self.secondary.blob_sync().await?;
        Ok(())
    }
}
