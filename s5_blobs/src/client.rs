use std::collections::BTreeSet;

use bytes::Bytes;
use ed25519_dalek::Signer;
use iroh::Endpoint;
use irpc::Client as IrpcClient;
use irpc_iroh::IrohLazyRemoteConnection;
use s5_core::Hash;

use crate::rpc::{DeleteBlob, DownloadBlob, PinBlob, Query, QueryResponse, RpcProto, UploadBlob};

use {
    anyhow::anyhow,
    async_trait::async_trait,
    s5_core::BlobId,
    s5_core::blob::{BlobResult, BlobsRead, BlobsWrite},
    std::io::Cursor,
    std::path::PathBuf,
    tokio::io::AsyncRead,
    tokio::io::AsyncReadExt,
};

#[cfg(feature = "server")]
use {futures::Stream, futures_util::StreamExt};

#[derive(Clone)]
// TODO: Support multi-peer connections (pool of remote peers) with per-peer trust/health scores and reuse connections.
pub struct Client {
    inner: IrpcClient<RpcProto>,
}

impl Client {
    // TODO: Add high-level "fetch" that queries a set of peers, aggregates QueryResponse.locations, and chooses best source.
    pub const ALPN_PUBLIC: &'static [u8] = crate::rpc::ALPN_PUBLIC;
    pub const ALPN_ACL: &'static [u8] = crate::rpc::ALPN_ACL;

    /// Open a connection on the given ALPN without any handshake.
    /// Exposed `pub` so test code can drive the F02 challenge by hand
    /// (verifying replay-rejection etc.). Production code uses
    /// [`Self::connect_to_peer_public`] or
    /// [`Self::connect_to_peer_acl`].
    pub fn connect_with_alpn(
        endpoint: Endpoint,
        peer_pubkey: [u8; 32],
        alpn: &[u8],
    ) -> anyhow::Result<Self> {
        let id = iroh::EndpointId::from_bytes(&peer_pubkey)
            .map_err(|e| anyhow::anyhow!("invalid peer pubkey: {e}"))?;
        Ok(Self::connect_with_addr(
            endpoint,
            iroh::EndpointAddr::from(id),
            alpn,
        ))
    }

    /// Like [`Self::connect_with_alpn`] but accepts a full
    /// `EndpointAddr` (including direct socket addresses and relay
    /// info). Useful in tests where the discovery system isn't running
    /// and the caller has the server's `endpoint.addr()` directly.
    pub fn connect_with_addr(
        endpoint: Endpoint,
        addr: impl Into<iroh::EndpointAddr>,
        alpn: &[u8],
    ) -> Self {
        let conn = IrohLazyRemoteConnection::new(endpoint, addr.into(), alpn.to_vec());
        Client {
            inner: IrpcClient::boxed(conn),
        }
    }

    /// **F02 step 1.** Issue an `AuthChallenge` RPC and return the
    /// server's nonce. Public so tests can pair this with
    /// [`Self::auth_prove_raw`] to construct adversarial scenarios.
    pub async fn auth_challenge(&self) -> anyhow::Result<[u8; 32]> {
        let resp = self
            .inner
            .rpc(crate::rpc::AuthChallenge::default())
            .await
            .map_err(|e| anyhow::anyhow!("AuthChallenge RPC failed: {e}"))?;
        Ok(resp.nonce)
    }

    /// **F02 step 2.** Submit an `AuthProve` message with the given
    /// `acl_pubkey` + signature halves. Production callers use
    /// [`Self::connect_to_peer_acl`] which computes a correctly-bound
    /// signature internally. Public for test replay scenarios.
    pub async fn auth_prove_raw(
        &self,
        acl_pubkey: [u8; 32],
        sig_r: [u8; 32],
        sig_s: [u8; 32],
    ) -> anyhow::Result<Result<(), String>> {
        self.inner
            .rpc(crate::rpc::AuthProve {
                acl_pubkey,
                sig_r,
                sig_s,
            })
            .await
            .map_err(|e| anyhow::anyhow!("AuthProve RPC failed: {e}"))
    }

    /// Compute the channel-bound F02 binding for `(server, client,
    /// nonce)` — both sides do this independently. Returns the 32-byte
    /// blake3-derived binding that goes under the signature.
    pub fn f02_binding(
        nonce: &[u8; 32],
        client_iroh_pubkey: &[u8; 32],
        server_iroh_pubkey: &[u8; 32],
    ) -> [u8; 32] {
        let mut h = blake3::Hasher::new_derive_key(crate::net_protocol::F02_BINDING_DOMAIN);
        h.update(nonce);
        h.update(client_iroh_pubkey);
        h.update(server_iroh_pubkey);
        *h.finalize().as_bytes()
    }

    /// Compute the bytes the client signs in `AuthProve`:
    /// `F02_SIG_PREFIX || binding`.
    pub fn f02_signed_message(binding: &[u8; 32]) -> Vec<u8> {
        let mut out = Vec::with_capacity(crate::net_protocol::F02_SIG_PREFIX.len() + 32);
        out.extend_from_slice(crate::net_protocol::F02_SIG_PREFIX);
        out.extend_from_slice(binding);
        out
    }

    /// Connect to a peer on the **public** ALPN (no F02 challenge).
    /// The peer serves only blobs in their `public_blob_hashes` set —
    /// today that's identity bundles, advertised public-vault content.
    pub fn connect_to_peer_public(
        endpoint: Endpoint,
        peer_pubkey: [u8; 32],
    ) -> anyhow::Result<Self> {
        Self::connect_with_alpn(endpoint, peer_pubkey, Self::ALPN_PUBLIC)
    }

    /// Connect to a peer on the **ACL** ALPN and complete the F02
    /// challenge handshake. On success the returned `Client` is bound
    /// to `acl_signing_key.verifying_key()` for its lifetime — all
    /// subsequent requests are gated against
    /// `BlobAcl::allow_acl_read(acl_pubkey, hash)` server-side.
    ///
    /// Two RPC round-trips:
    /// 1. `AuthChallenge` → server returns fresh 32-byte nonce.
    /// 2. `AuthProve` → client signs the channel-bound binding;
    ///    server verifies sig + checks the principal is recognised.
    pub async fn connect_to_peer_acl(
        endpoint: Endpoint,
        peer_pubkey: [u8; 32],
        acl_signing_key: &ed25519_dalek::SigningKey,
    ) -> anyhow::Result<Self> {
        let client = Self::connect_with_alpn(endpoint.clone(), peer_pubkey, Self::ALPN_ACL)?;
        let client_iroh = *endpoint.id().as_bytes();

        let nonce = client.auth_challenge().await?;
        let binding = Self::f02_binding(&nonce, &client_iroh, &peer_pubkey);
        let signed = Self::f02_signed_message(&binding);
        let sig = acl_signing_key.sign(&signed);
        let sig_bytes = sig.to_bytes();
        let acl_pubkey = acl_signing_key.verifying_key().to_bytes();
        let mut sig_r = [0u8; 32];
        let mut sig_s = [0u8; 32];
        sig_r.copy_from_slice(&sig_bytes[..32]);
        sig_s.copy_from_slice(&sig_bytes[32..]);

        let result = client.auth_prove_raw(acl_pubkey, sig_r, sig_s).await?;
        result.map_err(|e| anyhow::anyhow!("F02 challenge rejected: {e}"))?;

        Ok(client)
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

    /// Requests that the remote peer pin the given blob hash.
    ///
    /// Returns `Ok(true)` if the blob was found and pinned, `Ok(false)` if not found.
    pub async fn pin_blob(&self, hash: Hash) -> Result<Result<bool, String>, irpc::Error> {
        self.inner
            .rpc(PinBlob {
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
                blinded: false,
            })
            .await
    }

    /// Query using a blinded hash for privacy.
    ///
    /// The server only learns the real hash if it has the blob.
    /// If the blob exists, `QueryResponse.actual_hash` will contain the real hash.
    pub async fn query_blinded(
        &self,
        blinded_hash: [u8; 32],
        location_types: BTreeSet<u8>,
    ) -> Result<QueryResponse, irpc::Error> {
        self.inner
            .rpc(Query {
                hash: blinded_hash,
                location_types,
                blinded: true,
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

    /// Upload bytes directly (simpler API for WASM).
    ///
    /// Computes the BLAKE3 hash, streams the data, and returns the BlobId on success.
    pub async fn upload_bytes(&self, bytes: Bytes) -> Result<(Hash, u64), String> {
        let size = bytes.len() as u64;
        let hash: Hash = blake3::hash(&bytes).into();
        let (tx, rx) = self
            .upload_begin(hash, size, 8)
            .await
            .map_err(|e| format!("upload_begin failed: {e}"))?;

        tx.send(bytes)
            .await
            .map_err(|e| format!("failed to send upload chunk: {e}"))?;
        drop(tx);

        match rx
            .await
            .map_err(|e| format!("upload response failed: {e}"))?
        {
            Ok(()) => Ok((hash, size)),
            Err(err) => Err(err),
        }
    }

    /// Download a blob to bytes (simpler API for WASM).
    pub async fn download_bytes(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> Result<Bytes, String> {
        let mut receiver = self
            .download(hash, offset, max_len)
            .await
            .map_err(|e| format!("download failed: {e}"))?;
        let mut buffer = Vec::new();
        loop {
            match receiver.recv().await {
                Ok(Some(chunk)) => buffer.extend_from_slice(&chunk),
                Ok(None) => break,
                Err(err) => return Err(format!("download stream failed: {err}")),
            }
        }
        Ok(Bytes::from(buffer))
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
        // Full reads verify the content address (BlobsRead contract) —
        // a remote peer is exactly where wrong bytes come from.
        let bytes = self.blob_download_slice(hash, 0, None).await?;
        s5_core::blob::verify_bytes(hash, bytes)
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

#[cfg(feature = "server")]
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
