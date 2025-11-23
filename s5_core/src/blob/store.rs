use base64::Engine;
use bytes::Bytes;
use futures_core::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{fmt::Debug, path::PathBuf, sync::Arc};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead};
use tokio_util::io::{ReaderStream, StreamReader, SyncIoBridge};

use crate::{
    BlobId, Hash,
    bao::outboard::compute_outboard,
    blob::location::BlobLocation,
    blob::{BlobsRead, BlobsWrite},
    store::{Store, StoreFeatures, StoreResult},
};

/// High-level blob API built on top of a generic `Store`.
///
/// `BlobStore` organizes content-addressed blobs under deterministic
/// paths derived from their `Hash` and can optionally store Bao
/// outboard data alongside the main blob data.
#[derive(Debug, Clone)]
pub struct BlobStore {
    store: Arc<dyn Store>,
    outboard_store: Option<Arc<dyn Store>>,
}

impl BlobStore {
    /// Create a `BlobStore` that uses the same backend for
    /// both blob data and (if enabled) Bao outboard data.
    pub fn new<S>(store: S) -> Self
    where
        S: Store + 'static,
    {
        let store: Arc<dyn Store> = Arc::new(store);
        Self {
            store: store.clone(),
            outboard_store: Some(store),
        }
    }

    /// Create a `BlobStore` from a boxed `Store`, using it for
    /// both blob data and outboard data.
    pub fn new_boxed(store: Box<dyn Store + 'static>) -> Self {
        let store: Arc<dyn Store> = Arc::from(store);
        Self {
            store: store.clone(),
            outboard_store: Some(store),
        }
    }

    /// Create a `BlobStore` that never stores Bao outboard data.
    /// Only the main blob content is persisted.
    pub fn without_outboard<S>(store: S) -> Self
    where
        S: Store + 'static,
    {
        let store: Arc<dyn Store> = Arc::new(store);
        Self {
            store,
            outboard_store: None,
        }
    }

    /// Create a `BlobStore` with separate backends for blob data
    /// and Bao outboard data. Pass `None` for `outboard_store` to
    /// disable outboard persistence entirely.
    pub fn with_outboard<S, O>(store: S, outboard_store: Option<O>) -> Self
    where
        S: Store + 'static,
        O: Store + 'static,
    {
        let store: Arc<dyn Store> = Arc::new(store);
        let outboard_store = outboard_store.map(|s| -> Arc<dyn Store> { Arc::new(s) });

        Self {
            store,
            outboard_store,
        }
    }
    fn path_for_hash(&self, hash: Hash, features: &StoreFeatures) -> String {
        let hash_str = if features.case_sensitive {
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash)
        } else {
            let mut output = Vec::with_capacity(base32_fs::encoded_len(hash.as_bytes().len()));
            base32_fs::encode(hash.as_bytes(), &mut output);
            String::from_utf8(output).unwrap()
        };

        if features.recommended_max_dir_size < 10000 {
            if features.case_sensitive {
                format!("{}/{}/{}", &hash_str[0..2], &hash_str[2..4], &hash_str[4..],)
            } else {
                format!(
                    "{}/{}/{}/{}",
                    &hash_str[0..2],
                    &hash_str[2..4],
                    &hash_str[4..6],
                    &hash_str[6..]
                )
            }
        } else {
            hash_str
        }
    }

    fn blob_path_for_hash(&self, hash: Hash) -> String {
        format!("blob3/{}", self.path_for_hash(hash, &self.store.features()))
    }

    /// returns path for storing the bao outboard metadata for a specific blob hash
    fn obao6_path_for_hash(&self, hash: Hash) -> String {
        format!(
            "obao6/{}",
            self.path_for_hash(hash, &self.outboard_store.as_ref().unwrap().features())
        )
    }

    /// Deletes a blob and its associated outboard data from the store.
    pub async fn delete(&self, hash: Hash) -> StoreResult<()> {
        // Delete the main blob data.
        self.store.delete(&self.blob_path_for_hash(hash)).await?;

        // If an outboard store is configured, delete the outboard data as well.
        if let Some(obao_store) = &self.outboard_store {
            // It's okay if the outboard data doesn't exist, so we can ignore a NotFound error.
            match obao_store.delete(&self.obao6_path_for_hash(hash)).await {
                Ok(_) => {}
                Err(_) => {
                    // A proper error type would allow matching on "NotFound".
                    // For now, we assume any error here is likely due to the file not existing,
                    // which is acceptable. A more robust solution would be to have typed errors.
                }
            }
        }
        Ok(())
    }

    pub async fn size(&self, hash: Hash) -> StoreResult<u64> {
        self.store.size(&self.blob_path_for_hash(hash)).await
    }

    pub async fn contains(&self, hash: Hash) -> StoreResult<bool> {
        self.store.exists(&self.blob_path_for_hash(hash)).await
    }

    pub async fn contains_obao6(&self, hash: Hash) -> StoreResult<bool> {
        if let Some(obao_store) = &self.outboard_store {
            obao_store.exists(&self.obao6_path_for_hash(hash)).await
        } else {
            Ok(false)
        }
    }

    pub async fn provide(&self, hash: Hash) -> StoreResult<Vec<BlobLocation>> {
        self.store.provide(&self.blob_path_for_hash(hash)).await
    }

    pub async fn provide_obao6(&self, hash: Hash) -> StoreResult<Vec<BlobLocation>> {
        if let Some(obao_store) = &self.outboard_store {
            obao_store.provide(&self.obao6_path_for_hash(hash)).await
        } else {
            Ok(vec![])
        }
    }

    pub async fn read_as_bytes(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        self.store
            .open_read_bytes(&self.blob_path_for_hash(hash), offset, max_len)
            .await
    }

    /// Insert an in-memory blob of bytes to the blob store
    pub async fn import_bytes(&self, bytes: bytes::Bytes) -> StoreResult<BlobId> {
        let size = bytes.len() as u64;
        let compute_outboard_flag = self.outboard_store.is_some();
        let bytes_clone = bytes.clone();

        let (hash, obao) =
            tokio::task::spawn_blocking(move || -> std::io::Result<(Hash, Option<Vec<u8>>)> {
                if compute_outboard_flag {
                    let (hash, obao) = compute_outboard(bytes_clone.as_ref(), size, |_| Ok(()))?;
                    Ok((hash, obao))
                } else {
                    Ok((blake3::hash(&bytes_clone).into(), None))
                }
            })
            .await??;

        if let Some(outboard) = obao
            && let Some(outboard_store) = &self.outboard_store
        {
            outboard_store
                .put_bytes(&self.obao6_path_for_hash(hash), outboard.into())
                .await?;
        }

        if self.store.exists(&self.blob_path_for_hash(hash)).await? {
            return Ok(BlobId { hash, size });
        }

        self.store
            .put_bytes(&self.blob_path_for_hash(hash), bytes)
            .await?;

        Ok(BlobId { hash, size })
    }

    /// Import a blob from a stream of bytes.
    ///
    /// This method consumes the stream exactly once. It calculates the
    /// hash (and outboard data, if configured) while streaming the content to a
    /// temporary location in the underlying store.
    ///
    /// Upon successful completion of the stream, the temporary file is atomically
    /// renamed to its final content-addressed path. If any part of the process fails,
    /// the temporary file is cleaned up.
    pub async fn import_stream(
        &self,
        stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<BlobId> {
        let hasher = Arc::new(std::sync::Mutex::new(blake3::Hasher::new()));
        let writer = HasherWriter {
            hasher: hasher.clone(),
        };
        let tee_stream = TeeStream::new(stream, writer);

        let temp_path = self.store.put_temp(Box::new(tee_stream)).await?;
        let size = self.store.size(&temp_path).await?;

        let hash: Hash = hasher.lock().unwrap().finalize().into();

        let outboard = if self.outboard_store.is_some() {
            let (h2, ob) = self
                .compute_from_store(&temp_path, size, |_| Ok(()))
                .await?;
            if h2 != hash {
                return Err(anyhow::anyhow!("Hash mismatch during import"));
            }
            ob
        } else {
            None
        };

        let (hash, size) = self
            .finalize_import(temp_path, hash, size, outboard)
            .await?;

        Ok(BlobId { hash, size })
    }

    pub async fn read_stream(
        &self,
        hash: Hash,
    ) -> StoreResult<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
        let stream = self
            .store
            .open_read_stream(&self.blob_path_for_hash(hash), 0, None)
            .await?;
        Ok(Box::new(tokio_util::io::StreamReader::new(stream)))
    }

    /// This trait method imports a file from a local path.
    ///
    /// `data` is the path to the file.
    /// `mode` is a hint how the file should be imported.
    /// `progress` is a sender that provides a way for the importer to send progress messages
    /// when importing large files. This also serves as a way to cancel the import. If the
    /// consumer of the progress messages is dropped, subsequent attempts to send progress
    /// will fail.
    ///
    /// Returns the blob identifier of the imported file. The reason to have this method is that some database
    /// implementations might be able to import a file without copying it.
    pub async fn import_file(
        &self,
        path: PathBuf,
        on_progress: impl Fn(u64) -> std::io::Result<()> + Send + Sync + 'static,
    ) -> StoreResult<BlobId> {
        let meta = tokio::fs::metadata(&path).await?;
        let size = meta.len();

        let (client, server) = tokio::io::duplex(64 * 1024);
        let compute_outboard_flag = self.outboard_store.is_some();

        let compute_task = tokio::task::spawn_blocking(move || {
            let reader = SyncIoBridge::new(server);
            if compute_outboard_flag {
                compute_outboard(reader, size, on_progress)
            } else {
                // Only compute the BLAKE3 hash when no outboard store is configured.
                // We still report progress via `on_progress` but avoid allocating
                // or persisting Bao outboard data.
                use std::io::Read;

                let mut hasher = blake3::Hasher::new();
                let mut reader = std::io::BufReader::new(reader);
                let mut buf = [0u8; 8192];
                let mut processed: u64 = 0;

                loop {
                    let n = reader.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                    processed += n as u64;
                    on_progress(processed)?;
                }

                let hash: Hash = hasher.finalize().into();
                Ok((hash, None))
            }
        });

        let file = tokio::fs::File::open(&path).await?;
        let stream =
            FramedRead::new(file, BytesCodec::new()).map(|result| result.map(|b| b.into()));

        let tee_stream = TeeStream::new(stream, client);

        let temp_path = self.store.put_temp(Box::new(tee_stream)).await?;

        let (hash, outboard) = compute_task.await??;

        let (hash, size) = self
            .finalize_import(temp_path, hash, size, outboard)
            .await?;

        Ok(BlobId { hash, size })
    }

    async fn compute_from_store(
        &self,
        path: &str,
        size: u64,
        progress: impl Fn(u64) -> std::io::Result<()> + Send + Sync + 'static,
    ) -> StoreResult<(Hash, Option<Vec<u8>>)> {
        let stream = self.store.open_read_stream(path, 0, None).await?;
        let reader = StreamReader::new(stream);
        let reader = SyncIoBridge::new(reader);

        let compute_outboard_flag = self.outboard_store.is_some();

        let (hash, outboard) = tokio::task::spawn_blocking(move || {
            if compute_outboard_flag {
                compute_outboard(reader, size, progress)
            } else {
                // Fallback if no outboard needed, though currently we just use compute_outboard
                // as it handles hashing too.
                compute_outboard(reader, size, progress)
            }
        })
        .await??;

        Ok((hash, outboard))
    }

    async fn finalize_import(
        &self,
        temp_path: String,
        hash: Hash,
        size: u64,
        outboard: Option<Vec<u8>>,
    ) -> StoreResult<(Hash, u64)> {
        if let Some(outboard_data) = outboard
            && let Some(obao_store) = &self.outboard_store
        {
            obao_store
                .put_bytes(&self.obao6_path_for_hash(hash), outboard_data.into())
                .await?;
        }

        let final_path = self.blob_path_for_hash(hash);

        if self.store.exists(&final_path).await? {
            self.store.delete(&temp_path).await?;
        } else if self.store.features().supports_rename {
            self.store.rename(&temp_path, &final_path).await?;
        } else {
            let stream = self.store.open_read_stream(&temp_path, 0, None).await?;
            self.store.put_stream(&final_path, stream).await?;
            self.store.delete(&temp_path).await?;
        }

        Ok((hash, size))
    }
}

#[async_trait::async_trait]
impl BlobsRead for BlobStore {
    async fn blob_contains(&self, hash: Hash) -> StoreResult<bool> {
        self.contains(hash).await
    }

    async fn blob_get_size(&self, hash: Hash) -> StoreResult<u64> {
        self.size(hash).await
    }

    async fn blob_download(&self, hash: Hash) -> StoreResult<Bytes> {
        self.read_as_bytes(hash, 0, None).await
    }

    async fn blob_download_slice(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        self.read_as_bytes(hash, offset, max_len).await
    }

    async fn blob_read(&self, hash: Hash) -> StoreResult<Box<dyn AsyncRead + Send + Unpin>> {
        let stream = self
            .store
            .open_read_stream(&self.blob_path_for_hash(hash), 0, None)
            .await?;
        Ok(Box::new(StreamReader::new(stream)))
    }
}

#[async_trait::async_trait]
impl BlobsWrite for BlobStore {
    async fn blob_upload_bytes(&self, bytes: Bytes) -> StoreResult<BlobId> {
        self.import_bytes(bytes).await
    }

    async fn blob_upload_reader<R, F>(
        &self,
        hash: Hash,
        _size: u64,
        reader: R,
        _on_progress: F,
    ) -> StoreResult<BlobId>
    where
        R: AsyncRead + Send + Unpin + 'static,
        F: Fn(u64) -> std::io::Result<()> + Send + Sync + 'static,
    {
        let stream = ReaderStream::new(reader);
        let blob_id = self.import_stream(Box::new(stream)).await?;
        if blob_id.hash != hash {
            return Err(anyhow::anyhow!(
                "Hash mismatch: expected {}, got {}",
                hash,
                blob_id.hash
            ));
        }
        Ok(blob_id)
    }

    async fn blob_upload_stream<S>(&self, stream: S) -> StoreResult<BlobId>
    where
        S: Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static,
    {
        self.import_stream(Box::new(stream)).await
    }

    async fn blob_upload_file(&self, path: PathBuf) -> StoreResult<BlobId> {
        self.import_file(path, |_| Ok(())).await
    }
}

struct HasherWriter {
    hasher: Arc<std::sync::Mutex<blake3::Hasher>>,
}

impl AsyncWrite for HasherWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        self.hasher.lock().unwrap().update(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Poll::Ready(Ok(()))
    }
}

struct TeeStream<S, W> {
    stream: S,
    writer: W,
    write_buf: Option<Bytes>,
    stream_done: bool,
}

impl<S, W> TeeStream<S, W> {
    fn new(stream: S, writer: W) -> Self {
        Self {
            stream,
            writer,
            write_buf: None,
            stream_done: false,
        }
    }
}

impl<S, W> Stream for TeeStream<S, W>
where
    S: Stream<Item = Result<Bytes, std::io::Error>> + Unpin,
    W: AsyncWrite + Unpin,
{
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(buf) = &this.write_buf {
                match Pin::new(&mut this.writer).poll_write(cx, buf) {
                    Poll::Ready(Ok(n)) => {
                        if n == buf.len() {
                            this.write_buf = None;
                        } else {
                            let new_buf = buf.slice(n..);
                            this.write_buf = Some(new_buf);
                            continue;
                        }
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                    Poll::Pending => return Poll::Pending,
                }
            }

            if this.stream_done {
                match Pin::new(&mut this.writer).poll_shutdown(cx) {
                    Poll::Ready(_) => return Poll::Ready(None),
                    Poll::Pending => return Poll::Pending,
                }
            }

            match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    this.write_buf = Some(bytes.clone());
                    return Poll::Ready(Some(Ok(bytes)));
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    this.stream_done = true;
                    continue;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_tee_stream() {
        let data = b"hello world";
        let stream = tokio_stream::iter(vec![
            Ok(Bytes::from(&data[..5])),
            Ok(Bytes::from(&data[5..])),
        ]);

        let (client, mut server) = tokio::io::duplex(1024);
        let tee = TeeStream::new(stream, client);

        let mut collected = Vec::new();
        let mut tee = Box::pin(tee);
        while let Some(chunk) = tee.next().await {
            collected.extend_from_slice(&chunk.unwrap());
        }

        assert_eq!(collected, data);

        let mut server_data = Vec::new();
        server.read_to_end(&mut server_data).await.unwrap();
        assert_eq!(server_data, data);
    }
}
