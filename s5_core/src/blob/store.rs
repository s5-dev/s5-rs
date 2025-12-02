use bytes::Bytes;
use futures_core::Stream;
use std::sync::Arc;
use tokio::io::AsyncRead;
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;

#[cfg(not(target_arch = "wasm32"))]
use std::path::PathBuf;

use crate::{
    BlobId, Hash,
    blob::location::BlobLocation,
    blob::{BlobsRead, BlobsWrite},
    store::{Store, StoreFeatures, StoreResult},
};

use super::import;
use super::paths;
use super::read;

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

    pub fn blob_path_for_hash(&self, hash: Hash) -> String {
        paths::blob_path_for_hash(hash, &self.store.features())
    }

    pub fn obao6_path_for_hash(&self, hash: Hash) -> String {
        debug_assert!(
            self.outboard_store.is_some(),
            "outboard_store must be present when computing obao6 path"
        );
        paths::obao6_path_for_hash(hash, &self.outboard_store.as_ref().unwrap().features())
    }

    pub fn hash_from_blob_path(
        path: &str,
        features: &StoreFeatures,
    ) -> Result<Option<Hash>, std::io::Error> {
        paths::hash_from_blob_path(path, features)
    }

    /// Deletes a blob and its associated outboard data from the store.
    pub async fn delete(&self, hash: Hash) -> StoreResult<()> {
        // Delete the main blob data.
        self.store.delete(&self.blob_path_for_hash(hash)).await?;

        // If an outboard store is configured, delete the outboard data as well.
        if let Some(obao_store) = &self.outboard_store {
            match obao_store.delete(&self.obao6_path_for_hash(hash)).await {
                Ok(()) => {}
                Err(err) => {
                    tracing::warn!(
                        "blobstore: failed to delete outboard data for {}: {err}",
                        hash
                    );
                }
            }
        }
        Ok(())
    }

    pub async fn size(&self, hash: Hash) -> StoreResult<u64> {
        read::size(&self.store, hash).await
    }

    pub async fn contains(&self, hash: Hash) -> StoreResult<bool> {
        read::contains(&self.store, hash).await
    }

    pub async fn contains_obao6(&self, hash: Hash) -> StoreResult<bool> {
        read::contains_obao6(&self.outboard_store, hash).await
    }

    pub async fn provide(&self, hash: Hash) -> StoreResult<Vec<BlobLocation>> {
        read::provide(&self.store, hash).await
    }

    pub async fn provide_obao6(&self, hash: Hash) -> StoreResult<Vec<BlobLocation>> {
        read::provide_obao6(&self.outboard_store, hash).await
    }

    pub async fn read_as_bytes(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        read::read_as_bytes(&self.store, hash, offset, max_len).await
    }

    pub async fn read_stream(
        &self,
        hash: Hash,
    ) -> StoreResult<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
        read::read_stream(&self.store, hash).await
    }

    /// Insert an in-memory blob of bytes to the blob store
    pub async fn import_bytes(&self, bytes: bytes::Bytes) -> StoreResult<BlobId> {
        import::import_bytes(&self.store, &self.outboard_store, bytes).await
    }

    /// Import a blob from a stream of bytes.
    pub async fn import_stream(
        &self,
        stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<BlobId> {
        import::import_stream(&self.store, &self.outboard_store, stream).await
    }

    /// Imports a file from a local path.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn import_file(
        &self,
        path: PathBuf,
        on_progress: impl Fn(u64) -> std::io::Result<()> + Send + Sync + 'static,
    ) -> StoreResult<BlobId> {
        import::import_file(&self.store, &self.outboard_store, path, on_progress).await
    }

    /// Returns all blob hashes currently stored under the `blob3/` prefix.
    pub async fn list_hashes(&self) -> StoreResult<Vec<Hash>> {
        let features = self.store.features();
        let mut hashes = Vec::new();
        let mut stream = self.store.list().await?;

        while let Some(item) = stream.next().await {
            let path = item?;
            if let Some(hash) = Self::hash_from_blob_path(&path, &features)? {
                hashes.push(hash);
            }
        }

        Ok(hashes)
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
        self.read_stream(hash).await
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

    #[cfg(not(target_arch = "wasm32"))]
    async fn blob_upload_file(&self, path: PathBuf) -> StoreResult<BlobId> {
        self.import_file(path, |_| Ok(())).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::{io, sync::Mutex};

    #[derive(Debug, Clone)]
    struct TestStore {
        features: StoreFeatures,
        entries: std::sync::Arc<Mutex<Vec<String>>>,
    }

    impl TestStore {
        fn new(features: StoreFeatures) -> (Self, std::sync::Arc<Mutex<Vec<String>>>) {
            let entries = std::sync::Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    features,
                    entries: entries.clone(),
                },
                entries,
            )
        }
    }

    #[async_trait]
    impl Store for TestStore {
        async fn put_stream(
            &self,
            _path: &str,
            _stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
        ) -> StoreResult<()> {
            unimplemented!("put_stream not used in tests");
        }

        fn features(&self) -> StoreFeatures {
            self.features
        }

        async fn exists(&self, _path: &str) -> StoreResult<bool> {
            unimplemented!("exists not used in tests");
        }

        async fn put_bytes(&self, _path: &str, _bytes: Bytes) -> StoreResult<()> {
            unimplemented!("put_bytes not used in tests");
        }

        async fn open_read_stream(
            &self,
            _path: &str,
            _offset: u64,
            _max_len: Option<u64>,
        ) -> StoreResult<
            Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
        > {
            unimplemented!("open_read_stream not used in tests");
        }

        async fn open_read_bytes(
            &self,
            _path: &str,
            _offset: u64,
            _max_len: Option<u64>,
        ) -> StoreResult<Bytes> {
            unimplemented!("open_read_bytes not used in tests");
        }

        async fn size(&self, _path: &str) -> StoreResult<u64> {
            unimplemented!("size not used in tests");
        }

        async fn list(
            &self,
        ) -> StoreResult<
            Box<dyn Stream<Item = Result<String, std::io::Error>> + Send + Unpin + 'static>,
        > {
            let entries = self.entries.lock().unwrap().clone();
            let stream = tokio_stream::iter(entries.into_iter().map(Ok::<String, io::Error>));
            Ok(Box::new(stream))
        }

        async fn delete(&self, _path: &str) -> StoreResult<()> {
            unimplemented!("delete not used in tests");
        }

        async fn rename(&self, _old_path: &str, _new_path: &str) -> StoreResult<()> {
            unimplemented!("rename not used in tests");
        }

        async fn provide(
            &self,
            _path: &str,
        ) -> StoreResult<Vec<crate::blob::location::BlobLocation>> {
            unimplemented!("provide not used in tests");
        }
    }

    #[tokio::test]
    async fn list_hashes_roundtrip_case_sensitive_segmented() {
        let features = StoreFeatures {
            supports_rename: true,
            case_sensitive: true,
            recommended_max_dir_size: 100,
        };
        let (store, entries) = TestStore::new(features);
        let blob_store = BlobStore::without_outboard(store);

        let h1 = Hash::new(b"one");
        let h2 = Hash::new(b"two");

        let p1 = blob_store.blob_path_for_hash(h1);
        let p2 = blob_store.blob_path_for_hash(h2);

        {
            let mut guard = entries.lock().unwrap();
            guard.push(p1);
            guard.push(p2);
            guard.push("other/prefix/object".to_string());
        }

        let mut hashes = blob_store.list_hashes().await.unwrap();
        hashes.sort();

        let mut expected = vec![h1, h2];
        expected.sort();

        assert_eq!(hashes, expected);
    }

    #[tokio::test]
    async fn list_hashes_roundtrip_case_insensitive_segmented() {
        let features = StoreFeatures {
            supports_rename: true,
            case_sensitive: false,
            recommended_max_dir_size: 100,
        };
        let (store, entries) = TestStore::new(features);
        let blob_store = BlobStore::without_outboard(store);

        let h1 = Hash::new(b"alpha");
        let h2 = Hash::new(b"beta");

        let p1 = blob_store.blob_path_for_hash(h1);
        let p2 = blob_store.blob_path_for_hash(h2);

        {
            let mut guard = entries.lock().unwrap();
            guard.push(p1);
            guard.push(p2);
            guard.push("unrelated/path".to_string());
        }

        let mut hashes = blob_store.list_hashes().await.unwrap();
        hashes.sort();

        let mut expected = vec![h1, h2];
        expected.sort();

        assert_eq!(hashes, expected);
    }

    #[tokio::test]
    async fn list_hashes_roundtrip_case_sensitive_flat() {
        let features = StoreFeatures {
            supports_rename: true,
            case_sensitive: true,
            recommended_max_dir_size: u64::MAX,
        };
        let (store, entries) = TestStore::new(features);
        let blob_store = BlobStore::without_outboard(store);

        let h = Hash::new(b"flat-path");
        let path = blob_store.blob_path_for_hash(h);

        {
            let mut guard = entries.lock().unwrap();
            guard.push(path);
        }

        let hashes = blob_store.list_hashes().await.unwrap();
        assert_eq!(hashes, vec![h]);
    }

    #[test]
    fn hash_from_blob_path_ignores_non_blob_prefix() {
        let features = StoreFeatures {
            supports_rename: true,
            case_sensitive: true,
            recommended_max_dir_size: 100,
        };
        let result = BlobStore::hash_from_blob_path("other/prefix", &features).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn hash_from_blob_path_invalid_base64_is_error() {
        let features = StoreFeatures {
            supports_rename: true,
            case_sensitive: true,
            recommended_max_dir_size: 100,
        };
        let result = BlobStore::hash_from_blob_path("blob3/!!!", &features);
        assert!(result.is_err());
    }

    #[test]
    fn hash_from_blob_path_invalid_base32_is_ignored() {
        let features = StoreFeatures {
            supports_rename: true,
            case_sensitive: false,
            recommended_max_dir_size: 100,
        };
        let result = BlobStore::hash_from_blob_path("blob3/!!!!", &features).unwrap();
        assert!(result.is_none());
    }
}
