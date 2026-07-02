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
    blob::{
        BlobResult, BlobsDelete, BlobsList, BlobsRead, BlobsWrite, HashStream, ReachableStream,
    },
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

    /// Create a `BlobStore` from an already-Arc'd `Store`, using it for
    /// both blob data and outboard data.
    pub fn from_arc(store: Arc<dyn Store>) -> Self {
        Self::from_arc_with_outboard(store, true)
    }

    /// Create a `BlobStore` from an already-Arc'd `Store`, choosing at
    /// runtime whether to also write Bao outboard data alongside blobs.
    /// `with_outboard = false` is identical to [`Self::without_outboard`]
    /// but takes the same already-shared Arc the caller is using for the
    /// main store.
    pub fn from_arc_with_outboard(store: Arc<dyn Store>, with_outboard: bool) -> Self {
        let outboard_store = if with_outboard {
            Some(store.clone())
        } else {
            None
        };
        Self {
            store,
            outboard_store,
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

    /// Last-modification time of the blob's backing object, if the store
    /// tracks one (see [`Store::modified`]). `None` for stores without an
    /// mtime notion. Used by the cold-store GC age gate.
    pub async fn modified(&self, hash: Hash) -> StoreResult<Option<std::time::SystemTime>> {
        self.store.modified(&self.blob_path_for_hash(hash)).await
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

    /// Insert an in-memory blob without checking if it already exists.
    ///
    /// This is faster than `import_bytes` because it skips the existence check
    /// and writes directly. Use this during bulk import/backfill when you know
    /// records are new.
    ///
    /// # Warning
    ///
    /// If the blob already exists, this will overwrite it (which is usually fine
    /// for content-addressed storage since the content is identical).
    pub async fn import_bytes_unchecked(&self, bytes: bytes::Bytes) -> StoreResult<BlobId> {
        import::import_bytes_unchecked(&self.store, &self.outboard_store, bytes).await
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

    /// All blob hashes currently stored under the `blob3/` prefix, collected.
    ///
    /// Convenience over the streaming [`BlobsList::list_hashes`] for callers
    /// that want the whole set in memory (tests, small stores, the
    /// cold-recovery pack-hash list). Drains that one streaming walk — it is
    /// not a second enumeration path.
    pub async fn list_hashes(&self) -> StoreResult<Vec<Hash>> {
        let mut stream = <Self as BlobsList>::list_hashes(self).await?;
        let mut hashes = Vec::new();
        while let Some(item) = stream.next().await {
            hashes.push(item?);
        }
        Ok(hashes)
    }

    /// Ensures all pending writes are durably persisted to storage.
    ///
    /// Call this before creating snapshots or on shutdown to guarantee
    /// that all imported blobs are safely stored.
    pub async fn sync(&self) -> StoreResult<()> {
        self.store.sync().await?;
        if let Some(ref outboard) = self.outboard_store {
            outboard.sync().await?;
        }
        Ok(())
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
        let bytes = self.read_as_bytes(hash, 0, None).await?;
        super::verify_bytes(hash, bytes)
    }

    async fn blob_download_slice(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        let bytes = self.read_as_bytes(hash, offset, max_len).await?;
        if offset == 0 && max_len.is_none() {
            super::verify_bytes(hash, bytes)
        } else {
            Ok(bytes)
        }
    }

    async fn blob_read(&self, hash: Hash) -> StoreResult<Box<dyn AsyncRead + Send + Unpin>> {
        // Streamed full reads verify at EOF (BlobsRead contract).
        let inner = self.read_stream(hash).await?;
        Ok(Box::new(super::VerifyingReader::new(hash, inner)))
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

    async fn blob_sync(&self) -> StoreResult<()> {
        self.sync().await
    }
}

#[async_trait::async_trait]
impl BlobsDelete for BlobStore {
    async fn blob_delete(&self, hash: Hash) -> BlobResult<()> {
        // `BlobStore::delete` is already idempotent for missing blobs at
        // the `Store::delete` layer (LocalStore returns Ok on ENOENT);
        // it also tries to remove the outboard sidecar if configured.
        self.delete(hash).await
    }

    async fn blob_retain(&self, reachable: ReachableStream) -> BlobResult<()> {
        let mut reach = std::collections::HashSet::new();
        let mut stream = reachable;
        while let Some(h) = futures::StreamExt::next(&mut stream).await {
            reach.insert(h);
        }

        // Stream the full inventory rather than materializing it: we only need
        // to hold the reachable set, deleting each unreachable hash as it
        // arrives (the inventory can dwarf the reachable set at TiB scale).
        let mut all = <Self as BlobsList>::list_hashes(self).await?;
        while let Some(hash) = futures::StreamExt::next(&mut all).await {
            let hash = hash?;
            if !reach.contains(&hash) {
                self.delete(hash).await?;
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl BlobsList for BlobStore {
    /// Walk the backing store's listing, decoding each `blob3/` path back to
    /// its `Hash` and skipping non-blob entries (outboards, foreign paths)
    /// lazily. This is the single enumeration path; the inherent
    /// [`BlobStore::list_hashes`] just collects it.
    async fn list_hashes(&self) -> BlobResult<HashStream> {
        let features = self.store.features();
        let inner = self.store.list().await?;
        // `StoreFeatures` is `Copy`, so the closure captures it by value; the
        // decode is synchronous, so `tokio_stream`'s sync `filter_map` fits.
        let stream = inner.filter_map(move |item| match item {
            Ok(path) => match Self::hash_from_blob_path(&path, &features) {
                Ok(Some(hash)) => Some(Ok(hash)),
                Ok(None) => None, // non-blob path (outboard / foreign): skip
                Err(e) => Some(Err(anyhow::Error::from(e))),
            },
            Err(e) => Some(Err(anyhow::Error::from(e))),
        });
        let boxed: HashStream = Box::new(stream);
        Ok(boxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::{collections::HashMap, io, sync::Mutex};

    #[derive(Debug, Clone)]
    struct TestStore {
        features: StoreFeatures,
        entries: std::sync::Arc<Mutex<Vec<String>>>,
        files: std::sync::Arc<Mutex<HashMap<String, Bytes>>>,
    }

    impl TestStore {
        fn new(features: StoreFeatures) -> (Self, std::sync::Arc<Mutex<Vec<String>>>) {
            let entries = std::sync::Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    features,
                    entries: entries.clone(),
                    files: std::sync::Arc::new(Mutex::new(HashMap::new())),
                },
                entries,
            )
        }

        fn insert_bytes(&self, path: String, bytes: Bytes) {
            self.files.lock().unwrap().insert(path, bytes);
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
            self.files.lock().unwrap().insert(_path.to_string(), _bytes);
            Ok(())
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
            path: &str,
            offset: u64,
            max_len: Option<u64>,
        ) -> StoreResult<Bytes> {
            let files = self.files.lock().unwrap();
            let bytes = files.get(path).ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("no such key: {path}"))
            })?;
            let start = std::cmp::min(offset as usize, bytes.len());
            let remaining = bytes.len() - start;
            let len = max_len
                .map(|max| std::cmp::min(max as usize, remaining))
                .unwrap_or(remaining);
            Ok(bytes.slice(start..start + len))
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
            ..Default::default()
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
    async fn blob_download_verifies_full_blob_hash() {
        let features = StoreFeatures {
            supports_rename: true,
            case_sensitive: true,
            recommended_max_dir_size: u64::MAX,
            ..Default::default()
        };
        let (store, _) = TestStore::new(features);
        let blob_store = BlobStore::without_outboard(store.clone());

        let bytes = Bytes::from_static(b"clean");
        let hash = Hash::new(&bytes);
        store.insert_bytes(blob_store.blob_path_for_hash(hash), bytes.clone());

        assert_eq!(blob_store.blob_download(hash).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn blob_download_rejects_wrong_bytes_under_hash() {
        let features = StoreFeatures {
            supports_rename: true,
            case_sensitive: true,
            recommended_max_dir_size: u64::MAX,
            ..Default::default()
        };
        let (store, _) = TestStore::new(features);
        let blob_store = BlobStore::without_outboard(store.clone());

        let expected_hash = Hash::new(b"expected");
        store.insert_bytes(
            blob_store.blob_path_for_hash(expected_hash),
            Bytes::from_static(b"corrupt"),
        );

        let err = blob_store.blob_download(expected_hash).await.unwrap_err();
        assert!(err.to_string().contains("blob integrity check failed for"));
    }

    #[tokio::test]
    async fn list_hashes_roundtrip_case_insensitive_segmented() {
        let features = StoreFeatures {
            supports_rename: true,
            case_sensitive: false,
            recommended_max_dir_size: 100,
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
        };
        let result = BlobStore::hash_from_blob_path("blob3/!!!!", &features).unwrap();
        assert!(result.is_none());
    }
}
