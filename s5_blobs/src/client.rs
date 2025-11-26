use std::collections::BTreeSet;
use std::path::PathBuf;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use futures_util::StreamExt;
use iroh::Endpoint;
use irpc::Client as IrpcClient;
use irpc_iroh::IrohRemoteConnection;
use s5_core::blob::{BlobResult, BlobsRead, BlobsWrite};
use s5_core::{BlobId, Hash};
use std::io::Cursor;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::rpc::{DeleteBlob, DownloadBlob, Query, QueryResponse, RpcProto, UploadBlob};

#[derive(Clone)]
// TODO: Support multi-peer connections (pool of remote peers) with per-peer trust/health scores and reuse connections.
pub struct Client {
    inner: IrpcClient<RpcProto>,
}

impl Client {
    // TODO: Add high-level "fetch" that queries a set of peers, aggregates QueryResponse.locations, and chooses best source.
    pub const ALPN: &'static [u8] = crate::rpc::ALPN;

    pub fn connect(endpoint: Endpoint, addr: impl Into<iroh::EndpointAddr>) -> Self {
        let conn = IrohRemoteConnection::new(endpoint, addr.into(), Self::ALPN.to_vec());
        Client {
            inner: IrpcClient::boxed(conn),
        }
    }

    /// Requests that the remote peer unpin this client's reference to
    /// the given blob hash and, if no pins remain, delete it from the
    /// underlying blob store.
    ///
    /// The returned `Result` is nested: the outer `Result` reflects RPC
    /// transport errors, the inner one is the server's `Result<bool, String>`
    /// where `Ok(true)` means the blob became orphaned and was deleted,
    /// `Ok(false)` means other pins remain, and `Err(String)` carries a
    /// permission or other server-side error message.
    pub async fn delete_blob(&self, hash: Hash) -> Result<Result<bool, String>, irpc::Error> {
        self.inner
            .rpc(DeleteBlob {
                hash: *hash.as_bytes(),
            })
            .await
    }

    // TODO: Maintain per-hash location cache with TTL; merge results from multiple peers; rate-limit repeated queries.

    // TODO: Track per-peer blob availability state to avoid repeatedly querying non-holders.
    // TODO: Consider exchanging/maintaining chunk availability as RoaringBitmap to inform download planning.
    pub async fn query(
        &self,
        hash: Hash,
        location_types: BTreeSet<u8>,
    ) -> Result<QueryResponse, irpc::Error> {
        self.inner
            .rpc(Query {
                hash: *hash.as_bytes(),
                location_types,
            })
            .await
    }

    // TODO: Fallback to non-RPC locations (URL, Sia, Iroh pointers) when peer lacks content; support partial availability/multi-source download.
    // TODO: Add per-blob chunk cache and readahead window for ranged reads.
    // TODO: Use RoaringBitmap to represent per-peer chunk availability and schedule requests accordingly.
    pub async fn download(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> Result<irpc::channel::mpsc::Receiver<Bytes>, irpc::Error> {
        self.inner
            .server_streaming(
                DownloadBlob {
                    hash: *hash.as_bytes(),
                    offset,
                    max_len,
                },
                8,
            )
            .await
    }

    pub async fn upload_begin(
        &self,
        expected_hash: Hash,
        size: u64,
        capacity: usize,
    ) -> Result<
        (
            irpc::channel::mpsc::Sender<Bytes>,
            irpc::channel::oneshot::Receiver<Result<(), String>>,
        ),
        irpc::Error,
    > {
        self.inner
            .client_streaming(
                UploadBlob {
                    expected_hash: *expected_hash.as_bytes(),
                    size,
                },
                capacity,
            )
            .await
    }
}

#[async_trait]
impl BlobsRead for Client {
    async fn blob_contains(&self, hash: Hash) -> BlobResult<bool> {
        let resp = self
            .query(hash, BTreeSet::new())
            .await
            .map_err(|e| anyhow!(e))?;
        Ok(resp.exists)
    }

    async fn blob_get_size(&self, hash: Hash) -> BlobResult<u64> {
        let resp = self
            .query(hash, BTreeSet::new())
            .await
            .map_err(|e| anyhow!(e))?;
        resp.size
            .ok_or_else(|| anyhow!("size unavailable for blob {}", hash))
    }

    async fn blob_download(&self, hash: Hash) -> BlobResult<Bytes> {
        self.blob_download_slice(hash, 0, None).await
    }

    async fn blob_download_slice(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> BlobResult<Bytes> {
        let mut receiver = self
            .download(hash, offset, max_len)
            .await
            .map_err(|e| anyhow!(e))?;
        let mut buffer = Vec::new();
        loop {
            match receiver.recv().await {
                Ok(Some(chunk)) => buffer.extend_from_slice(&chunk),
                Ok(None) => break,
                Err(err) => return Err(anyhow!("download failed: {err}")),
            }
        }
        Ok(Bytes::from(buffer))
    }

    async fn blob_read(&self, hash: Hash) -> BlobResult<Box<dyn AsyncRead + Send + Unpin>> {
        let bytes = self.blob_download(hash).await?;
        Ok(Box::new(Cursor::new(bytes)))
    }
}

#[async_trait]
impl BlobsWrite for Client {
    async fn blob_upload_bytes(&self, bytes: Bytes) -> BlobResult<BlobId> {
        let size = bytes.len() as u64;
        let hash: Hash = blake3::hash(&bytes).into();
        let (tx, rx) = self
            .upload_begin(hash, size, 8)
            .await
            .map_err(|e| anyhow!(e))?;

        tx.send(bytes)
            .await
            .map_err(|e| anyhow!("failed to send upload chunk: {e}"))?;
        drop(tx);

        match rx.await.map_err(|e| anyhow!(e))? {
            Ok(()) => Ok(BlobId { hash, size }),
            Err(err) => Err(anyhow!(err)),
        }
    }

    async fn blob_upload_reader<R, F>(
        &self,
        hash: Hash,
        size: u64,
        mut reader: R,
        on_progress: F,
    ) -> BlobResult<BlobId>
    where
        R: AsyncRead + Send + Unpin + 'static,
        F: Fn(u64) -> std::io::Result<()> + Send + Sync + 'static,
    {
        const CHUNK: usize = 64 * 1024;
        let (tx, rx) = self
            .upload_begin(hash, size, 8)
            .await
            .map_err(|e| anyhow!(e))?;

        let mut sent: u64 = 0;
        let mut buf = vec![0u8; CHUNK];

        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            sent += n as u64;
            on_progress(sent)?;
            tx.send(Bytes::copy_from_slice(&buf[..n]))
                .await
                .map_err(|e| anyhow!("failed to send upload chunk: {e}"))?;
        }

        if sent != size {
            return Err(anyhow!(
                "size mismatch while uploading blob: expected {size}, sent {sent}"
            ));
        }

        drop(tx);
        match rx.await.map_err(|e| anyhow!(e))? {
            Ok(()) => Ok(BlobId { hash, size }),
            Err(err) => Err(anyhow!(err)),
        }
    }

    async fn blob_upload_stream<S>(&self, mut stream: S) -> BlobResult<BlobId>
    where
        S: Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static,
    {
        const CHUNK_CAP: usize = 8;
        let mut hasher = blake3::Hasher::new();
        let mut total: u64 = 0;
        let mut chunks: Vec<Bytes> = Vec::new();

        while let Some(item) = stream.next().await {
            let chunk = item?;
            total += chunk.len() as u64;
            hasher.update(&chunk);
            chunks.push(chunk);
        }

        let hash: Hash = hasher.finalize().into();
        let (tx, rx) = self
            .upload_begin(hash, total, CHUNK_CAP)
            .await
            .map_err(|e| anyhow!(e))?;

        for chunk in chunks {
            tx.send(chunk)
                .await
                .map_err(|e| anyhow!("failed to send upload chunk: {e}"))?;
        }
        drop(tx);

        match rx.await.map_err(|e| anyhow!(e))? {
            Ok(()) => Ok(BlobId { hash, size: total }),
            Err(err) => Err(anyhow!(err)),
        }
    }

    async fn blob_upload_file(&self, path: PathBuf) -> BlobResult<BlobId> {
        let data = tokio::fs::read(&path).await?;
        self.blob_upload_bytes(Bytes::from(data)).await
    }
}
