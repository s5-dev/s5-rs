pub mod identifier;
pub mod location;
pub mod store;

use crate::{BlobId, Hash};
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::{io, path::PathBuf};
use tokio::io::AsyncRead;

pub type BlobResult<T> = anyhow::Result<T>;

/// High-level async read interface for content-addressed blobs.
#[async_trait]
pub trait BlobsRead: Sync + Send {
    /// Returns true if the blob exists.
    async fn blob_contains(&self, hash: Hash) -> BlobResult<bool>;

    /// Returns the size of the blob in bytes.
    async fn blob_get_size(&self, hash: Hash) -> BlobResult<u64>;

    /// Downloads a full blob into memory.
    async fn blob_download(&self, hash: Hash) -> BlobResult<Bytes>;

    /// Downloads a slice of a blob to memory, starting from `offset`
    /// (inclusive) with optional maximum length `max_len`.
    async fn blob_download_slice(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> BlobResult<Bytes>;

    /// Returns an async reader for the blob contents.
    async fn blob_read(&self, hash: Hash) -> BlobResult<Box<dyn AsyncRead + Send + Unpin>>;
}

/// High-level async write interface for content-addressed blobs.
#[async_trait]
pub trait BlobsWrite: Sync + Send {
    /// Upload a small blob of bytes.
    async fn blob_upload_bytes(&self, bytes: Bytes) -> BlobResult<BlobId>;

    /// Upload a blob from a reader.
    async fn blob_upload_reader<R, F>(
        &self,
        hash: Hash,
        size: u64,
        reader: R,
        on_progress: F,
    ) -> BlobResult<BlobId>
    where
        R: AsyncRead + Send + Unpin + 'static,
        F: Fn(u64) -> io::Result<()> + Send + Sync + 'static;

    /// Upload a blob from a streaming source of bytes.
    async fn blob_upload_stream<S>(&self, stream: S) -> BlobResult<BlobId>
    where
        S: Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static;

    /// Upload a local file as a blob.
    async fn blob_upload_file(&self, path: PathBuf) -> BlobResult<BlobId>;
}
