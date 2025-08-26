use crate::{BlobId, Hash};
use bytes::Bytes;
use std::path::PathBuf;

// TODO return results
// TODO move this to root

pub mod store;

pub trait BlobsRead: Sync + Send {
    /// Downloads a full file blob to memory
    // TODO this should be try
    fn blob_download(&self, hash: Hash) -> impl std::future::Future<Output = Bytes> + Send;

    // Downloads a slice of a blob to memory, starting from `offset` (inclusive) with length `length`
    fn blob_download_slice(
        &self,
        hash: Hash,
        offset: u64,
        length: u64,
    ) -> impl std::future::Future<Output = Bytes> + Send;

    fn blob_get_size(&self, hash: Hash) -> impl std::future::Future<Output = u64> + Send;
}

pub trait BlobsWrite: Sync + Send {
    /// Upload a small blob of bytes
    fn blob_upload_bytes(&self, bytes: Bytes) -> impl std::future::Future<Output = BlobId> + Send;

    /// Upload a blob from a reader
    fn blob_upload_reader(
        &self,
        hash: Hash,
        size: u64,
        // TODO add reader
        // TODO add on_progress callback
    ) -> impl std::future::Future<Output = BlobId> + Send;

    /// Upload a local file as a blob
    fn blob_upload_file(&self, path: PathBuf) -> impl std::future::Future<Output = BlobId> + Send;
}
