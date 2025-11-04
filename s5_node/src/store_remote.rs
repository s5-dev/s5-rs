use std::collections::BTreeSet;
use std::fmt;
use std::io;

use anyhow::{anyhow, Result};
use base64::Engine;
use bytes::Bytes;
use futures_util::StreamExt;
use s5_blobs::Client as BlobsClient;
use s5_core::{
    blob::location::BlobLocation,
    store::{PutResponse, Store, StoreFeatures, StoreResult},
    Hash,
};

const UPLOAD_CHANNEL_CAPACITY: usize = 8;

#[derive(Clone)]
pub struct RemoteBlobStore {
    client: BlobsClient,
}

impl RemoteBlobStore {
    pub fn new(client: BlobsClient) -> Self {
        Self { client }
    }

    fn hash_from_path(path: &str) -> Result<Hash> {
        // Expect paths like "blob3/aa/bb/cccc..." or "blob3/<flatbase64>"
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
        let (mut tx, rx) = self
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
    async fn put_stream(
        &self,
        path: &str,
        mut stream: Box<
            dyn futures_core::Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static,
        >,
    ) -> StoreResult<PutResponse> {
        let hash = Self::hash_from_path(path)?;
        let mut total = 0u64;
        let mut chunks = Vec::new();

        while let Some(item) = stream.next().await {
            let chunk = item.map_err(|err| anyhow!(err))?;
            total += chunk.len() as u64;
            chunks.push(chunk);
        }

        self.upload_chunks(hash, total, chunks).await?;
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

    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<PutResponse> {
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
    ) -> StoreResult<
        Box<dyn futures_core::Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    > {
        let hash = Self::hash_from_path(path)?;
        let mut receiver = self
            .client
            .download(hash, offset, max_len)
            .await
            .map_err(|err| anyhow!(err))?;

        // Collect into memory to produce an Unpin stream compatible with the Store trait.
        let mut chunks: Vec<Bytes> = Vec::new();
        loop {
            match receiver.recv().await {
                Ok(Some(chunk)) => chunks.push(chunk),
                Ok(None) => break,
                Err(err) => return Err(anyhow!("download failed: {err}")),
            }
        }
        let iter = tokio_stream::iter(chunks.into_iter().map(|b| Ok::<Bytes, io::Error>(b)));
        Ok(Box::new(iter))
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
        response.size.ok_or_else(|| anyhow!("size unavailable for blob {path}"))
    }

    async fn list(
        &self,
    ) -> StoreResult<
        Box<dyn futures_core::Stream<Item = Result<String, std::io::Error>> + Send + Unpin + 'static>,
    > {
        Err(anyhow!("list not supported for RemoteBlobStore"))
    }

    async fn delete(&self, _path: &str) -> StoreResult<()> {
        Ok(())
    }

    async fn rename(&self, _old_path: &str, _new_path: &str) -> StoreResult<()> {
        Err(anyhow!("rename not supported for RemoteBlobStore"))
    }

    async fn provide(&self, _path: &str) -> StoreResult<Vec<BlobLocation>> {
        Ok(Vec::new())
    }
}
