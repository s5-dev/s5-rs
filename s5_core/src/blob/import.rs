use crate::{
    BlobId, Hash,
    bao::outboard::compute_outboard,
    store::{Store, StoreResult},
};
use bytes::Bytes;
use futures_core::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;
use tokio_util::io::{StreamReader, SyncIoBridge};

#[cfg(not(target_arch = "wasm32"))]
use tokio_stream::StreamExt;

#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;
#[cfg(not(target_arch = "wasm32"))]
use tokio_util::codec::{BytesCodec, FramedRead};

use super::paths::{blob_path_for_hash, obao6_path_for_hash};

/// TODO(perf): expose the Bao/outboard threshold and hashing
/// strategy as a tunable policy so different deployments can
/// trade CPU vs metadata size explicitly.
pub async fn import_bytes(
    store: &Arc<dyn Store>,
    outboard_store: &Option<Arc<dyn Store>>,
    bytes: bytes::Bytes,
) -> StoreResult<BlobId> {
    let size = bytes.len() as u64;
    // Only compute Bao outboard data for blobs >= 2^16 bytes.
    let compute_outboard_flag = outboard_store.is_some() && size >= (1u64 << 16);
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

    if let Some(ref outboard) = obao
        && let Some(outboard_store) = outboard_store
    {
        outboard_store
            .put_bytes(
                &obao6_path_for_hash(hash, &outboard_store.features()),
                outboard.clone().into(),
            )
            .await?;
    }

    let final_path = blob_path_for_hash(hash, &store.features());
    if store.exists(&final_path).await? {
        return Ok(BlobId { hash, size });
    }

    // TODO(remote-blobs): RemoteBlobStore currently relies on this
    // helper to compute BLAKE3 and optional outboard data. In the
    // future, the remote backend should perform its own hashing,
    // enforce hash equality for uploaded data, and then expose a
    // simpler content-addressed API to BlobStore.
    if store.features().supports_rename {
        let temp_path = store
            .put_temp(Box::new(tokio_stream::once(Ok(bytes))))
            .await?;

        let (hash, size) =
            finalize_import(store, outboard_store, temp_path, hash, size, obao).await?;

        Ok(BlobId { hash, size })
    } else {
        store.put_bytes(&final_path, bytes).await?;
        Ok(BlobId { hash, size })
    }
}

/// TODO(perf): consider computing Bao outboard incrementally during
/// the initial stream write instead of re-reading via
/// `compute_from_store`, especially for remote or slow stores.
pub async fn import_stream(
    store: &Arc<dyn Store>,
    outboard_store: &Option<Arc<dyn Store>>,
    stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
) -> StoreResult<BlobId> {
    let hasher = Arc::new(std::sync::Mutex::new(blake3::Hasher::new()));
    let writer = HasherWriter {
        hasher: hasher.clone(),
    };
    let tee_stream = TeeStream::new(stream, writer);

    let temp_path = store.put_temp(Box::new(tee_stream)).await?;
    let size = store.size(&temp_path).await?;

    let hash: Hash = hasher.lock().unwrap().finalize().into();

    let outboard = if outboard_store.is_some() {
        let (h2, ob) =
            compute_from_store(store, outboard_store, &temp_path, size, |_| Ok(())).await?;
        if h2 != hash {
            return Err(anyhow::anyhow!("Hash mismatch during import"));
        }
        ob
    } else {
        None
    };

    let (hash, size) =
        finalize_import(store, outboard_store, temp_path, hash, size, outboard).await?;

    Ok(BlobId { hash, size })
}

#[cfg(not(target_arch = "wasm32"))]
pub async fn import_file(
    store: &Arc<dyn Store>,
    outboard_store: &Option<Arc<dyn Store>>,
    path: PathBuf,
    on_progress: impl Fn(u64) -> std::io::Result<()> + Send + Sync + 'static,
) -> StoreResult<BlobId> {
    let meta = tokio::fs::metadata(&path).await?;
    let size = meta.len();

    let (client, server) = tokio::io::duplex(64 * 1024);
    let compute_outboard_flag = outboard_store.is_some();

    let compute_task = tokio::task::spawn_blocking(move || {
        let reader = SyncIoBridge::new(server);
        if compute_outboard_flag {
            compute_outboard(reader, size, on_progress)
        } else {
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
    let stream = FramedRead::new(file, BytesCodec::new()).map(|result| result.map(|b| b.into()));

    let tee_stream = TeeStream::new(stream, client);

    let temp_path = store.put_temp(Box::new(tee_stream)).await?;

    let (hash, outboard) = compute_task.await??;

    let (hash, size) =
        finalize_import(store, outboard_store, temp_path, hash, size, outboard).await?;

    Ok(BlobId { hash, size })
}

async fn compute_from_store(
    store: &Arc<dyn Store>,
    outboard_store: &Option<Arc<dyn Store>>,
    path: &str,
    size: u64,
    progress: impl Fn(u64) -> std::io::Result<()> + Send + Sync + 'static,
) -> StoreResult<(Hash, Option<Vec<u8>>)> {
    let stream = store.open_read_stream(path, 0, None).await?;
    let reader = StreamReader::new(stream);
    let reader = SyncIoBridge::new(reader);

    let compute_outboard_flag = outboard_store.is_some();

    let (hash, outboard) = tokio::task::spawn_blocking(move || {
        if compute_outboard_flag {
            compute_outboard(reader, size, progress)
        } else {
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
                progress(processed)?;
            }

            let hash: Hash = hasher.finalize().into();
            Ok((hash, None))
        }
    })
    .await??;

    Ok((hash, outboard))
}

async fn finalize_import(
    store: &Arc<dyn Store>,
    outboard_store: &Option<Arc<dyn Store>>,
    temp_path: String,
    hash: Hash,
    size: u64,
    outboard: Option<Vec<u8>>,
) -> StoreResult<(Hash, u64)> {
    if let Some(outboard_data) = outboard
        && let Some(obao_store) = outboard_store
    {
        obao_store
            .put_bytes(
                &obao6_path_for_hash(hash, &obao_store.features()),
                outboard_data.into(),
            )
            .await?;
    }

    let final_path = blob_path_for_hash(hash, &store.features());

    if store.exists(&final_path).await? {
        store.delete(&temp_path).await?;
    } else if store.features().supports_rename {
        store.rename(&temp_path, &final_path).await?;
    } else {
        let stream = store.open_read_stream(&temp_path, 0, None).await?;
        store.put_stream(&final_path, stream).await?;
        store.delete(&temp_path).await?;
    }

    Ok((hash, size))
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

pub struct TeeStream<S, W> {
    stream: S,
    writer: W,
    write_buf: Option<Bytes>,
    stream_done: bool,
}

impl<S, W> TeeStream<S, W> {
    pub fn new(stream: S, writer: W) -> Self {
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
