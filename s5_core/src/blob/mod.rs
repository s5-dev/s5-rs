pub mod identifier;
pub mod import;
pub mod location;
pub mod paths;
pub mod read;
pub mod store;
pub mod fallback;
pub mod tee;

pub use identifier::BlobId;
pub use location::BlobLocation;
pub use store::BlobStore;

use crate::Hash;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::io;
use tokio::io::AsyncRead;

#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

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
    ///
    /// Not available through `dyn BlobsWrite` (requires `Self: Sized`).
    async fn blob_upload_reader<R, F>(
        &self,
        hash: Hash,
        size: u64,
        reader: R,
        on_progress: F,
    ) -> BlobResult<BlobId>
    where
        Self: Sized,
        R: AsyncRead + Send + Unpin + 'static,
        F: Fn(u64) -> io::Result<()> + Send + Sync + 'static;

    /// Upload a blob from a streaming source of bytes.
    ///
    /// Not available through `dyn BlobsWrite` (requires `Self: Sized`).
    async fn blob_upload_stream<S>(&self, stream: S) -> BlobResult<BlobId>
    where
        Self: Sized,
        S: Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static;

    /// Upload a local file as a blob.
    #[cfg(not(target_arch = "wasm32"))]
    async fn blob_upload_file(&self, path: PathBuf) -> BlobResult<BlobId>;

    /// Flush pending writes to durable storage.
    ///
    /// The default implementation is a no-op. Backends that buffer writes
    /// (e.g. local-disk stores) should override this.
    async fn blob_sync(&self) -> BlobResult<()> {
        Ok(())
    }
}

/// Combined read + write interface for content-addressed blobs.
///
/// This is the primary trait for code that needs to both read and write
/// blobs by hash without caring about storage layout (paths, directories,
/// etc.).  Any type that implements both `BlobsRead` and `BlobsWrite`
/// automatically implements `BlobsReadWrite`.
pub trait BlobsReadWrite: BlobsRead + BlobsWrite {}

impl<T: BlobsRead + BlobsWrite> BlobsReadWrite for T {}
