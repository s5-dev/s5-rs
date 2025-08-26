use std::{error::Error, fmt::Debug, path::PathBuf};

use bytes::Bytes;
use futures_core::Stream;

use crate::Hash;

pub trait OutboardStore {
    fn contains_hash(hash: Hash) -> impl Future<Output = bool> + Send;
}

pub trait BlobStore: std::fmt::Debug + Clone + Send + Sync + 'static {
    type Error: Sized + Debug + Send + Sync + 'static;
    // TODO contains_hash_sync() is tried first

    // fn init();
    // fn blobs(&self) -> impl Future<Output = io::Result<DbIter<Hash>>> + Send;

    fn contains_hash(&self, hash: Hash) -> impl Future<Output = bool> + Send;

    /// This trait method imports a file from a local path.
    ///
    /// `data` is the path to the file.
    /// `mode` is a hint how the file should be imported.
    /// `progress` is a sender that provides a way for the importer to send progress messages
    /// when importing large files. This also serves as a way to cancel the import. If the
    /// consumer of the progress messages is dropped, subsequent attempts to send progress
    /// will fail.
    ///
    /// Returns the hash of the imported file. The reason to have this method is that some database
    /// implementations might be able to import a file without copying it.
    fn import_file(
        &self,
        path: PathBuf,
        // TODO progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> impl Future<Output = Result<(Hash, u64), Self::Error>> + Send;

    /// Import data from memory.
    ///
    /// It is a special case of `import` that does not use the file system.
    fn import_bytes(
        &self,
        bytes: bytes::Bytes,
    ) -> impl Future<Output = Result<Hash, Self::Error>> + Send;
    // impl Future<Output = io::Result<iroh_blobs::TempTag>> + Send;

    // Import data from a stream of bytes.
    fn import_stream(
        &self,
        stream: impl Stream<Item = Bytes> + Send + 'static,
        // progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> impl Future<Output = Result<Hash, Self::Error>> + Send;

    // Import data from an async byte reader.
    /*     fn import_reader(
        &self,
        data: impl AsyncRead + Send + Unpin + 'static,
        progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> impl Future<Output = io::Result<(Hash, u64)>> + Send {
        let stream = tokio_util::io::ReaderStream::new(data);
        self.import_stream(stream, format, progress)
    } */
}
