use std::collections::BTreeSet;
use std::fmt;

use anyhow::{Result, anyhow};
use base64::Engine;
use bytes::Bytes;
use futures::Stream;
use futures_util::StreamExt;
use s5_core::{
    Hash,
    blob::location::BlobLocation,
    store::{Store, StoreFeatures, StoreResult},
};

use crate::Client as BlobsClient;

const UPLOAD_CHANNEL_CAPACITY: usize = 8;

/// Remote blob-backed implementation of the low-level `Store` trait.
///
/// This type wraps an iroh-based `s5_blobs::Client` and interprets
/// store paths as content hashes (e.g. `blob3/aa/bb/cccc...`).
///
/// TODO(remote-blobs): in the long run this should
/// only accept BLAKE3 blobs and be responsible for
/// computing/verifying hashes and outboard data for
/// uploaded content, rather than trusting the caller.
#[derive(Clone)]
pub struct RemoteBlobStore {
    client: BlobsClient,
}

impl RemoteBlobStore {
    pub fn new(client: BlobsClient) -> Self {
        Self { client }
    }

    fn hash_from_path(path: &str) -> Result<Hash> {
        // Expect paths like "blob3/aa/bb/cccc..." or "blob3/<flatbase64>".
        // Reconstruct the base64 URL-safe string by removing slashes after the prefix.
        let relevant = match path.split_once('/') {
            Some((prefix, rest)) if prefix.ends_with("blob3") || prefix.ends_with("obao6") => rest,
            _ => path,
        };
        let b64: String = relevant.chars().filter(|c| *c != '/').collect();
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(b64.as_bytes())
            .map_err(|err| anyhow!("invalid blob path '{path}': {err}"))?;
        if bytes.len() != 32 {
            return Err(anyhow!(
                "invalid blob path '{path}': expected 32-byte hash, got {}",
                bytes.len()
            ));
        }
        let mut array = [0u8; 32];
        array.copy_from_slice(&bytes);
        Ok(Hash::from(array))
    }

    async fn upload_chunks(&self, hash: Hash, total_size: u64, chunks: Vec<Bytes>) -> Result<()> {
        let (tx, rx) = self
            .client
            .upload_begin(hash, total_size, UPLOAD_CHANNEL_CAPACITY)
            .await
            .map_err(|err| anyhow!(err))?;
        for chunk in chunks {
            tx.send(chunk)
                .await
                .map_err(|err| anyhow!("failed to send upload chunk: {err}"))?;
        }
        drop(tx);
        match rx.await.map_err(|err| anyhow!(err))? {
            Ok(()) => Ok(()),
            Err(err) => Err(anyhow!(err)),
        }
    }
}

impl fmt::Debug for RemoteBlobStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RemoteBlobStore").finish()
    }
}

#[async_trait::async_trait]
impl Store for RemoteBlobStore {
    async fn put_temp(
        &self,
        stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<String> {
        // For remote stores we don't rely on temp paths for
        // import_bytes (BlobStore bypasses this when rename is
        // unsupported), so we can forward to `put_stream` with a
        // synthetic but parseable path. The actual content hash is
        // determined by the final `put_bytes` call.
        let path = "blob3/temp".to_string();
        self.put_stream(&path, stream).await?;
        Ok(path)
    }

    async fn put_stream(
        &self,
        path: &str,
        mut stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<()> {
        let expected_hash = Self::hash_from_path(path)?;
        let mut total = 0u64;
        let mut chunks = Vec::new();
        let mut hasher = blake3::Hasher::new();

        while let Some(item) = stream.next().await {
            let chunk = item.map_err(|err| anyhow!(err))?;
            total += chunk.len() as u64;
            hasher.update(&chunk);
            chunks.push(chunk);
        }

        let actual_hash: Hash = hasher.finalize().into();
        if actual_hash != expected_hash {
            return Err(anyhow!(
                "hash mismatch: expected {}, got {}",
                expected_hash,
                actual_hash
            ));
        }

        self.upload_chunks(expected_hash, total, chunks).await?;
        Ok(())
    }

    fn features(&self) -> StoreFeatures {
        StoreFeatures {
            supports_rename: false,
            case_sensitive: true,
            recommended_max_dir_size: u64::MAX,
        }
    }

    async fn exists(&self, path: &str) -> StoreResult<bool> {
        let hash = Self::hash_from_path(path)?;
        let response = self
            .client
            .query(hash, BTreeSet::new())
            .await
            .map_err(|err| anyhow!(err))?;
        Ok(response.exists)
    }

    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<()> {
        let hash = Self::hash_from_path(path)?;
        let total = bytes.len() as u64;
        self.upload_chunks(hash, total, vec![bytes]).await?;
        Ok(())
    }

    async fn open_read_stream(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>>
    {
        let hash = Self::hash_from_path(path)?;
        let receiver = self
            .client
            .download(hash, offset, max_len)
            .await
            .map_err(|err| anyhow!(err))?;

        let stream = futures::stream::unfold(receiver, |mut rx| async move {
            match rx.recv().await {
                Ok(Some(chunk)) => Some((Ok(chunk), rx)),
                Ok(None) => None,
                Err(err) => Some((Err(std::io::Error::other(err.to_string())), rx)),
            }
        });

        Ok(Box::new(Box::pin(stream)))
    }

    async fn open_read_bytes(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        let hash = Self::hash_from_path(path)?;
        let mut receiver = self
            .client
            .download(hash, offset, max_len)
            .await
            .map_err(|err| anyhow!(err))?;

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

    async fn size(&self, path: &str) -> StoreResult<u64> {
        let hash = Self::hash_from_path(path)?;
        let response = self
            .client
            .query(hash, BTreeSet::new())
            .await
            .map_err(|err| anyhow!(err))?;
        response
            .size
            .ok_or_else(|| anyhow!("size unavailable for blob {path}"))
    }

    async fn list(
        &self,
    ) -> StoreResult<Box<dyn Stream<Item = Result<String, std::io::Error>> + Send + Unpin + 'static>>
    {
        Err(anyhow!("list not supported for RemoteBlobStore"))
    }

    /// Deletes a blob by path by issuing a `DeleteBlob` RPC to the
    /// remote peer. The server will unpin the calling node's reference
    /// to the blob and, if no pins remain, remove it from its stores.
    async fn delete(&self, path: &str) -> StoreResult<()> {
        let hash = Self::hash_from_path(path)?;
        // `delete_blob` returns Result<bool, String> inside the RPC
        // response; we flatten that into `StoreResult<()>`.
        let res = self
            .client
            .delete_blob(hash)
            .await
            .map_err(|err| anyhow!(err))?;
        match res {
            Ok(_orphaned) => Ok(()),
            Err(msg) => Err(anyhow!(msg)),
        }
    }

    async fn rename(&self, _old_path: &str, _new_path: &str) -> StoreResult<()> {
        Err(anyhow!("rename not supported for RemoteBlobStore"))
    }

    async fn provide(&self, _path: &str) -> StoreResult<Vec<BlobLocation>> {
        Ok(Vec::new())
    }
}
