//! `IndexdStore` — a `Store` implementation backed by Sia hosts
//! coordinated through an indexd service via `sia_storage::Sdk`.
//!
//! Two-tier storage per blob:
//!
//! - **`pointers: P: Store`** — small fixed-size pointer records
//!   (72 bytes each), one per caller path. Each record holds the
//!   `object_id` (indexd lookup key) and the `metadata_hash`
//!   (BLAKE3 of the SealedObject bytes — content-addressed key for
//!   the metadata cache). See [`pointer`].
//!
//! - **`metadata: M: BlobsReadWrite`** — content-addressed cache of
//!   SealedObject bytes. Reads consult this cache first; on miss the
//!   adapter falls back to `Sdk::object(&object_id)` and
//!   write-throughs into the cache.
//!
//! The caller supplies both — no defaults that drag in other store
//! crates.

pub mod auth;
pub mod encode;
pub mod pointer;

use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::stream::Stream;
use s5_core::Hash;
use s5_core::blob::BlobsReadWrite;
use s5_core::blob::location::BlobLocation;
use s5_core::store::{Store, StoreFeatures, StoreResult};
use sia_storage::{AppKey, DownloadOptions, Object, Sdk, SealedObject, UploadOptions};

use crate::encode::{decode_sealed, encode_sealed_to_vec};
use crate::pointer::Pointer;

/// Pre-image used to derive the canonical S5-indexd AppID.
///
/// **Once any non-mock IndexdStore writes data, the byte string below
/// must never change** — it salts the AppKey HKDF, so a different
/// pre-image yields a different AppKey and existing data becomes
/// unreachable (§10.13 of the spec).
pub const S5_INDEXD_APP_ID_PREIMAGE: &[u8] = b"s5.indexd.app.v1";

/// Canonical AppID for the S5 indexd integration: 32 bytes of
/// `blake3(S5_INDEXD_APP_ID_PREIMAGE)`.
pub fn app_id_bytes() -> [u8; 32] {
    *blake3::hash(S5_INDEXD_APP_ID_PREIMAGE).as_bytes()
}

/// Lowercase hex form of [`app_id_bytes`].
pub fn app_id_hex() -> String {
    hex::encode(app_id_bytes())
}

/// Default indexer URL — the public Sia Foundation indexer. Callers
/// who want a self-hosted or alternative indexer should pass that URL
/// explicitly.
pub const DEFAULT_INDEXER_URL: &str = "https://sia.storage";

/// Wraps a `sia_storage::Sdk` to expose it as an S5 `Store`.
///
/// `P` is where structural pointers live; `M` is the SealedObject
/// metadata cache (with on-miss fallback to indexd via `Sdk::object`).
pub struct IndexdStore<P: Store, M: BlobsReadWrite> {
    sdk: Sdk,
    app_key: Arc<AppKey>,
    pointers: P,
    metadata: M,
    config: IndexdConfig,
}

impl<P: Store, M: BlobsReadWrite> std::fmt::Debug for IndexdStore<P, M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexdStore")
            .field("pointers", &self.pointers)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct IndexdConfig {
    /// Validity window for `Store::provide(path)` signed share URLs.
    /// Default 24 h.
    pub share_validity: Duration,
    /// `UploadOptions` for new uploads. Defaults to sia_storage's own
    /// default (10-of-30 EC).
    pub upload_options: Option<UploadOptionsBuilder>,
    /// Indexer URL the SDK was built against. Stored for introspection
    /// (e.g. logging) — not load-bearing for read/write.
    pub indexer_url: String,
}

/// Builder-style copy of `UploadOptions` so `IndexdConfig` is Clone
/// (the real `UploadOptions` carries a non-clone callback).
#[derive(Debug, Clone)]
pub struct UploadOptionsBuilder {
    pub data_shards: u8,
    pub parity_shards: u8,
    pub max_inflight: usize,
}

impl UploadOptionsBuilder {
    pub fn build(&self) -> UploadOptions {
        UploadOptions {
            data_shards: self.data_shards,
            parity_shards: self.parity_shards,
            max_inflight: self.max_inflight,
            ..UploadOptions::default()
        }
    }
}

impl Default for IndexdConfig {
    fn default() -> Self {
        Self {
            share_validity: Duration::from_secs(24 * 3600),
            upload_options: None,
            indexer_url: String::new(),
        }
    }
}

impl<P: Store, M: BlobsReadWrite> IndexdStore<P, M> {
    pub fn new(sdk: Sdk, pointers: P, metadata: M) -> Self {
        let app_key = Arc::new(sdk.app_key().clone());
        Self {
            sdk,
            app_key,
            pointers,
            metadata,
            config: IndexdConfig::default(),
        }
    }

    pub fn with_config(sdk: Sdk, pointers: P, metadata: M, config: IndexdConfig) -> Self {
        let app_key = Arc::new(sdk.app_key().clone());
        Self {
            sdk,
            app_key,
            pointers,
            metadata,
            config,
        }
    }

    /// The AppKey backing this store. Cheap-clone (it's behind an Arc).
    pub fn app_key(&self) -> Arc<AppKey> {
        self.app_key.clone()
    }

    fn upload_options(&self) -> UploadOptions {
        match &self.config.upload_options {
            Some(b) => b.build(),
            None => UploadOptions::default(),
        }
    }

    async fn load_pointer(&self, path: &str) -> StoreResult<Pointer> {
        let bytes = self.pointers.open_read_bytes(path, 0, None).await?;
        Pointer::decode(&bytes)
            .map_err(|e| anyhow!("decoding indexd pointer at {path} failed: {e}"))
    }

    async fn store_pointer(&self, path: &str, ptr: &Pointer) -> StoreResult<()> {
        let bytes = Bytes::copy_from_slice(&ptr.encode());
        self.pointers.put_bytes(path, bytes).await
    }

    /// Fetch a SealedObject by its content-addressed
    /// `metadata_hash`, falling back to indexd via `Sdk::object` and
    /// write-throughing into the metadata cache on miss.
    async fn fetch_sealed(&self, ptr: &Pointer) -> StoreResult<SealedObject> {
        let cache_hash = Hash::from_bytes(ptr.metadata_hash);
        if let Ok(bytes) = self.metadata.blob_download(cache_hash).await {
            return decode_sealed(&bytes)
                .map_err(|e| anyhow!("decoding cached SealedObject failed: {e}"));
        }
        // Cache miss: ask indexd.
        let object =
            self.sdk.object(&ptr.object_id()).await.map_err(|e| {
                anyhow!("Sdk::object({}) failed: {e:?}", hex::encode(ptr.object_id))
            })?;
        let sealed = object.seal(&self.app_key);
        // Write-through. We don't fail the read if the cache write
        // fails — the read can still succeed from the in-memory
        // SealedObject we just got back.
        let bytes = Bytes::from(encode_sealed_to_vec(&sealed));
        if let Err(err) = self.metadata.blob_upload_bytes(bytes).await {
            tracing::warn!("metadata cache write-through failed: {err:?}");
        }
        Ok(sealed)
    }
}

#[async_trait]
impl<P, M> Store for IndexdStore<P, M>
where
    P: Store,
    M: BlobsReadWrite + std::fmt::Debug + Send + Sync + 'static,
{
    fn features(&self) -> StoreFeatures {
        // Inherit from pointers — IndexdStore adds no FS semantics.
        self.pointers.features()
    }

    async fn exists(&self, path: &str) -> StoreResult<bool> {
        self.pointers.exists(path).await
    }

    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<()> {
        // Overwrite semantics: match LocalStore / MemoryStore. If a
        // pointer already exists at this path we'll replace it, and
        // make a best-effort attempt to reclaim the old Sia object
        // after the new pointer is durable. There's still a
        // check-then-act race with concurrent writers to the same
        // path; the consequence is an orphaned Sia object, not data
        // loss.
        let old_pointer = if self.pointers.exists(path).await? {
            Some(self.load_pointer(path).await?)
        } else {
            None
        };

        // Zero-copy upload: `stream::iter([Ok(bytes)])` is Unpin and
        // turns into `AsyncRead` via `StreamReader` without copying.
        let stream = futures::stream::iter([Ok::<_, std::io::Error>(bytes)]);
        let reader = tokio_util::io::StreamReader::new(stream);
        let object = self
            .sdk
            .upload(Object::default(), reader, self.upload_options())
            .await
            .map_err(|e| anyhow!("Sdk::upload failed for {path}: {e:?}"))?;
        self.sdk
            .pin_object(&object)
            .await
            .map_err(|e| anyhow!("Sdk::pin_object failed for {path}: {e:?}"))?;

        let sealed = object.seal(&self.app_key);
        let sealed_bytes = encode_sealed_to_vec(&sealed);
        let metadata_hash = *blake3::hash(&sealed_bytes).as_bytes();

        // Write the metadata blob first, then the pointer. Crashing
        // between the two leaves orphan metadata, which is harmless
        // (content-addressed; re-upload of the same bytes is a
        // no-op). The reverse order would leave a dangling pointer.
        self.metadata
            .blob_upload_bytes(Bytes::from(sealed_bytes))
            .await
            .map_err(|e| anyhow!("metadata cache write failed for {path}: {e:?}"))?;
        let ptr = Pointer::new(object.id(), metadata_hash);
        self.store_pointer(path, &ptr).await?;

        // Reclaim the previous Sia object now that the new pointer is
        // durable. Failure here only leaks storage; the new value is
        // still reachable through the new pointer.
        if let Some(old) = old_pointer
            && let Err(err) = self.sdk.delete_object(&old.object_id()).await
        {
            tracing::warn!(
                "put_bytes overwrote {path}; old Sia object {} not cleaned: {err:?}",
                hex::encode(old.object_id)
            );
        }
        Ok(())
    }

    async fn put_stream(
        &self,
        path: &str,
        mut stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<()> {
        use futures::StreamExt;
        let mut buf: Vec<u8> = Vec::new();
        while let Some(chunk) = stream.next().await {
            buf.extend_from_slice(&chunk?);
        }
        self.put_bytes(path, Bytes::from(buf)).await
    }

    async fn open_read_bytes(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        let ptr = self.load_pointer(path).await?;
        let sealed = self.fetch_sealed(&ptr).await?;
        let object = sealed
            .open(&self.app_key)
            .map_err(|e| anyhow!("SealedObject::open failed for {path}: {e:?}"))?;

        let opts = DownloadOptions {
            offset,
            length: max_len,
            ..DownloadOptions::default()
        };
        let mut reader = self
            .sdk
            .download(&object, opts)
            .map_err(|e| anyhow!("Sdk::download failed for {path}: {e:?}"))?;
        let mut buf: Vec<u8> = Vec::new();
        tokio::io::copy(&mut reader, &mut buf)
            .await
            .map_err(|e| anyhow!("download copy failed for {path}: {e}"))?;
        Ok(Bytes::from(buf))
    }

    async fn open_read_stream(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>>
    {
        // Materialize then re-stream. Sia downloads are inherently
        // chunked but a streaming bridge isn't needed yet.
        let bytes = self.open_read_bytes(path, offset, max_len).await?;
        let s = futures::stream::once(async move { Ok::<_, std::io::Error>(bytes) });
        Ok(Box::new(Box::pin(s)))
    }

    async fn delete(&self, path: &str) -> StoreResult<()> {
        let ptr = match self.load_pointer(path).await {
            Ok(p) => p,
            Err(_) => return Ok(()), // already gone — idempotent
        };
        if let Err(err) = self.sdk.delete_object(&ptr.object_id()).await {
            tracing::warn!(
                "Sdk::delete_object failed for {path}; dropping local pointer anyway: {err:?}"
            );
        }
        self.pointers.delete(path).await?;
        // We deliberately leave the metadata cache entry alone: it's
        // content-addressed and may still be referenced by another
        // pointer. Cache eviction is the metadata Store's concern.
        Ok(())
    }

    async fn size(&self, path: &str) -> StoreResult<u64> {
        let ptr = self.load_pointer(path).await?;
        let sealed = self.fetch_sealed(&ptr).await?;
        Ok(sealed.slabs.iter().map(|s| s.length as u64).sum())
    }

    async fn list(
        &self,
    ) -> StoreResult<Box<dyn Stream<Item = Result<String, std::io::Error>> + Send + Unpin + 'static>>
    {
        // We enumerate the pointer Store — its paths are the caller's
        // namespace.
        self.pointers.list().await
    }

    async fn rename(&self, old_path: &str, new_path: &str) -> StoreResult<()> {
        // Metadata-only — bytes on Sia don't move; only the pointer
        // moves to a new key.
        self.pointers.rename(old_path, new_path).await
    }

    async fn sync(&self) -> StoreResult<()> {
        self.pointers.sync().await?;
        // metadata is a BlobsReadWrite; flush its own sync if any.
        self.metadata.blob_sync().await?;
        Ok(())
    }

    async fn provide(&self, path: &str) -> StoreResult<Vec<BlobLocation>> {
        let ptr = self.load_pointer(path).await?;
        let sealed = self.fetch_sealed(&ptr).await?;
        let object = sealed
            .open(&self.app_key)
            .map_err(|e| anyhow!("SealedObject::open failed for {path}: {e:?}"))?;
        let valid_until = Utc::now()
            + chrono::Duration::from_std(self.config.share_validity)
                .unwrap_or_else(|_| chrono::Duration::seconds(24 * 3600));
        let url = self
            .sdk
            .share_object(&object, valid_until)
            .map_err(|e| anyhow!("Sdk::share_object failed for {path}: {e:?}"))?;
        Ok(vec![BlobLocation::Url(url.into())])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_id_matches_documented_blake3() {
        // If this assertion ever has to change, you are silently
        // re-keying the AppKey HKDF and losing access to all
        // previously-pinned data. Don't.
        let expected_hex = hex::encode(blake3::hash(b"s5.indexd.app.v1").as_bytes());
        assert_eq!(app_id_hex(), expected_hex);
        assert_eq!(app_id_bytes().len(), 32);
        assert_eq!(S5_INDEXD_APP_ID_PREIMAGE, b"s5.indexd.app.v1");
    }

    #[test]
    fn sealed_object_sia_binary_roundtrip() {
        use crate::encode::{decode_sealed, encode_sealed_to_vec};
        use sia_storage::SealedObject;

        let original = SealedObject {
            encrypted_data_key: vec![9, 9, 9, 9, 9, 9, 9, 9],
            slabs: Vec::new(),
            data_signature: Default::default(),
            encrypted_metadata_key: vec![1, 2, 3],
            encrypted_metadata: vec![4, 5, 6, 7],
            metadata_signature: Default::default(),
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        let bytes = encode_sealed_to_vec(&original);
        let decoded = decode_sealed(&bytes).expect("decode must succeed");
        assert_eq!(original.encrypted_data_key, decoded.encrypted_data_key);
        assert_eq!(original.encrypted_metadata, decoded.encrypted_metadata);
        assert_eq!(
            original.encrypted_metadata_key,
            decoded.encrypted_metadata_key
        );
    }
}
