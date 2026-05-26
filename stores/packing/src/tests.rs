use super::*;
use s5_core::blob::store::BlobStore;
use s5_store_memory::MemoryStore;
use std::sync::Arc;

fn small_config() -> PackingConfig {
    PackingConfig {
        min_group_size: 1, // any pending blob is flush-ready
        max_group_size: 64 * 1024 * 1024,
        slab_size: 4 * 1024 * 1024,
        waste_pct: 0.99, // never enter "tight mode" in tests
        interval: Duration::from_secs(3600),
        flush_on_put: false,
        manifests_prefix: "manifests/".to_string(),
    }
}

async fn make_store() -> Arc<PackingStore<BlobStore, MemoryStore, MemoryStore>> {
    let blobs = BlobStore::without_outboard(MemoryStore::new());
    let manifests = MemoryStore::new();
    let staging = MemoryStore::new();
    PackingStore::open(blobs, manifests, staging, small_config())
        .await
        .expect("open packing store")
}

fn blob_path(hash: Hash) -> String {
    s5_core::blob::paths::blob_path_for_hash(
        hash,
        &StoreFeatures {
            supports_rename: false,
            case_sensitive: true,
            recommended_max_dir_size: u64::MAX,
            supports_reflink: false,
        },
    )
}

#[tokio::test]
async fn put_then_read_through_pack() {
    let store = make_store().await;
    let bytes = Bytes::from_static(b"hello packed world");
    let hash = Hash::new(&bytes);
    let path = blob_path(hash);

    store.put_bytes(&path, bytes.clone()).await.unwrap();
    // Read while still in staging — should hit the pending path.
    let read = store.open_read_bytes(&path, 0, None).await.unwrap();
    assert_eq!(read, bytes);

    // Force a flush; read again — now via pack.
    store.sync().await.unwrap();
    let read = store.open_read_bytes(&path, 0, None).await.unwrap();
    assert_eq!(read, bytes);

    // Slice read via pack.
    let slice = store.open_read_bytes(&path, 6, Some(6)).await.unwrap();
    assert_eq!(slice.as_ref(), b"packed");
}

#[tokio::test]
async fn dedup_skips_repeat_put() {
    let store = make_store().await;
    let bytes = Bytes::from_static(b"dedup me");
    let hash = Hash::new(&bytes);
    let path = blob_path(hash);

    store.put_bytes(&path, bytes.clone()).await.unwrap();
    store.put_bytes(&path, bytes.clone()).await.unwrap();
    // Only one pending entry.
    let state = store.state.read().await;
    assert_eq!(state.pending.len(), 1);
}

#[tokio::test]
async fn delete_returns_not_supported() {
    let store = make_store().await;
    let bytes = Bytes::from_static(b"packed");
    let hash = Hash::new(&bytes);
    let path = blob_path(hash);

    store.put_bytes(&path, bytes).await.unwrap();
    store.sync().await.unwrap();
    let err = store.delete(&path).await.unwrap_err();
    assert!(err.to_string().contains("delete is not implemented"));
}

#[tokio::test]
async fn members_are_sorted_by_path_hash_in_pack() {
    let store = make_store().await;
    // Four entries with deliberately scrambled insertion order; the
    // packed manifest must come out sorted by full BLAKE3-of-path.
    let entries: Vec<(&str, Bytes)> = vec![
        ("blob3/alpha", Bytes::from_static(b"AAAA")),
        ("blob3/bravo", Bytes::from_static(b"BBBB")),
        ("blob3/charlie", Bytes::from_static(b"CCCC")),
        ("blob3/delta", Bytes::from_static(b"DDDD")),
    ];
    for (path, b) in &entries {
        store.put_bytes(path, b.clone()).await.unwrap();
    }
    store.sync().await.unwrap();

    let state = store.state.read().await;
    let mut keys_and_offsets: Vec<([u8; HASH_PREFIX_LEN], u32)> = entries
        .iter()
        .map(|(path, _)| {
            let mut key = [0u8; HASH_PREFIX_LEN];
            key.copy_from_slice(&blake3::hash(path.as_bytes()).as_bytes()[..HASH_PREFIX_LEN]);
            let (_, offset, _) = state.locate(&key).expect("packed");
            (key, offset)
        })
        .collect();
    keys_and_offsets.sort_by(|a, b| a.0.cmp(&b.0));
    let offsets: Vec<u32> = keys_and_offsets.iter().map(|(_, o)| *o).collect();
    let mut sorted = offsets.clone();
    sorted.sort();
    assert_eq!(
        offsets, sorted,
        "pack body must be in path-hash-sorted order"
    );
}

#[tokio::test]
async fn flush_on_put_does_not_deadlock_for_small_writes() {
    // Reproduces the bug where a small put with flush_on_put=true and
    // min_group_size > write_size would hang forever waiting for a
    // pack flush that pack_once(false) refuses to perform.
    let blobs = BlobStore::without_outboard(MemoryStore::new());
    let manifests = MemoryStore::new();
    let staging = MemoryStore::new();
    let cfg = PackingConfig {
        min_group_size: 40 * 1024 * 1024, // huge — small writes never reach
        flush_on_put: true,
        ..small_config()
    };
    let store = PackingStore::open(blobs, manifests, staging, cfg)
        .await
        .unwrap();

    let bytes = Bytes::from_static(b"tiny");
    let hash = Hash::new(&bytes);
    let path = blob_path(hash);

    let put = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        store.put_bytes(&path, bytes.clone()),
    )
    .await
    .expect("put_bytes must not hang under flush_on_put");
    put.unwrap();

    // After put returns, the bytes are durable via a pack.
    let mut key = [0u8; HASH_PREFIX_LEN];
    key.copy_from_slice(&blake3::hash(path.as_bytes()).as_bytes()[..HASH_PREFIX_LEN]);
    let state = store.state.read().await;
    assert!(state.by_prefix.contains_key(&key));
    assert!(!state.pending.contains_key(&key));
}

#[tokio::test]
async fn restart_replays_manifests_and_deletion_log() {
    let blobs_inner = Arc::new(MemoryStore::new());
    let manifests_inner = Arc::new(MemoryStore::new());
    let staging_inner = Arc::new(MemoryStore::new());

    let bytes = Bytes::from_static(b"persisted packed bytes");
    let hash = Hash::new(&bytes);
    let path = blob_path(hash);

    {
        let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
        let s = PackingStore::open(
            blobs,
            ArcStore(manifests_inner.clone()),
            ArcStore(staging_inner.clone()),
            small_config(),
        )
        .await
        .unwrap();
        s.put_bytes(&path, bytes.clone()).await.unwrap();
        s.sync().await.unwrap();
    }

    // Reopen and verify index was loaded.
    {
        let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
        let s = PackingStore::open(
            blobs,
            ArcStore(manifests_inner.clone()),
            ArcStore(staging_inner.clone()),
            small_config(),
        )
        .await
        .unwrap();
        let read = s.open_read_bytes(&path, 0, None).await.unwrap();
        assert_eq!(read, bytes);
    }
}

/// Adapter so `Arc<MemoryStore>` can be threaded through both lifetimes
/// of the restart test without rebuilding state in between.
#[derive(Debug)]
struct ArcStore(Arc<MemoryStore>);

#[async_trait::async_trait]
impl Store for ArcStore {
    fn features(&self) -> StoreFeatures {
        self.0.features()
    }
    async fn exists(&self, path: &str) -> StoreResult<bool> {
        self.0.exists(path).await
    }
    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<()> {
        self.0.put_bytes(path, bytes).await
    }
    async fn put_stream(
        &self,
        path: &str,
        stream: Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<()> {
        self.0.put_stream(path, stream).await
    }
    async fn open_read_bytes(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        self.0.open_read_bytes(path, offset, max_len).await
    }
    async fn open_read_stream(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static>>
    {
        self.0.open_read_stream(path, offset, max_len).await
    }
    async fn size(&self, path: &str) -> StoreResult<u64> {
        self.0.size(path).await
    }
    async fn list(
        &self,
    ) -> StoreResult<Box<dyn Stream<Item = Result<String, io::Error>> + Send + Unpin + 'static>>
    {
        self.0.list().await
    }
    async fn delete(&self, path: &str) -> StoreResult<()> {
        self.0.delete(path).await
    }
    async fn rename(&self, old: &str, new: &str) -> StoreResult<()> {
        self.0.rename(old, new).await
    }
    async fn provide(&self, path: &str) -> StoreResult<Vec<BlobLocation>> {
        self.0.provide(path).await
    }
}
