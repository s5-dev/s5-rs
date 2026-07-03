use super::*;
use s5_core::blob::location::BlobLocation;
use s5_core::blob::store::BlobStore;
use s5_core::store::StoreFeatures;
use s5_store_memory::MemoryStore;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

fn small_config() -> PackingConfig {
    PackingConfig {
        min_group_size: 1, // any pending blob is flush-ready
        max_group_size: 64 * 1024 * 1024,
        slab_size: 4 * 1024 * 1024,
        waste_pct: 0.99, // never enter "tight mode" in tests
        interval: Duration::from_secs(3600),
        flush_on_put: false,
        staging_max_packs: 4, // 4 × 64 MiB = 256 MiB watermark — far above any test's staged bytes
        // Max-age flush disabled: tests drive flushes explicitly.
        max_pending_age: Duration::from_secs(3600),
        ..PackingConfig::default()
    }
}

async fn make_store() -> Arc<PackingStore<BlobStore>> {
    let blobs = BlobStore::without_outboard(MemoryStore::new());
    PackingStore::open(
        blobs,
        Arc::new(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        small_config(),
    )
    .await
    .expect("open packing store")
}

#[tokio::test]
async fn upload_then_read_through_pack() {
    let store = make_store().await;
    let bytes = Bytes::from_static(b"hello packed world");
    let id = store.blob_upload_bytes(bytes.clone()).await.unwrap();
    assert_eq!(id.hash, Hash::new(&bytes), "store is content-addressed");

    // Read while still in staging — should hit the pending path.
    let read = store.blob_download(id.hash).await.unwrap();
    assert_eq!(read, bytes);

    // Force a flush; read again — now via pack.
    store.blob_sync().await.unwrap();
    let read = store.blob_download(id.hash).await.unwrap();
    assert_eq!(read, bytes);

    // Slice read via pack.
    let slice = store
        .blob_download_slice(id.hash, 6, Some(6))
        .await
        .unwrap();
    assert_eq!(slice.as_ref(), b"packed");
}

#[tokio::test]
async fn bulk_mphf_indexes_packs_and_rejects_misses() {
    let store = make_store().await;
    let mut entries = Vec::new();
    for i in 0..20u32 {
        let bytes = Bytes::from(format!("packed-blob-number-{i}").into_bytes());
        let id = store.blob_upload_bytes(bytes.clone()).await.unwrap();
        entries.push((id.hash, bytes));
    }
    store.blob_sync().await.unwrap(); // flush all → packs (land in `recent`)
    store.rebuild_index().await.unwrap(); // fold into the compact bulk MPHF

    {
        let st = store.state.read().await;
        assert!(st.bulk.is_some(), "bulk MPHF built");
        assert_eq!(st.bulk_packs, st.packs.len(), "all packs covered by bulk");
        assert!(st.recent.is_empty(), "recent tail folded into bulk");
        // A non-member key must not resolve — the MPHF gives a candidate pack
        // whose member-table binary search then misses (no verify array needed).
        assert!(st.locate(&[0xab; HASH_PREFIX_LEN]).is_none());
    }

    // Every blob still resolves + reads back through the bulk index.
    for (hash, bytes) in &entries {
        let got = store.blob_download(*hash).await.unwrap();
        assert_eq!(&got, bytes);
    }
}

#[tokio::test]
async fn dedup_skips_repeat_upload() {
    let store = make_store().await;
    let bytes = Bytes::from_static(b"dedup me");

    store.blob_upload_bytes(bytes.clone()).await.unwrap();
    store.blob_upload_bytes(bytes.clone()).await.unwrap();
    // Only one pending entry — the second upload sees the hash already staged.
    let state = store.state.read().await;
    assert_eq!(state.pending.len(), 1);
}

#[tokio::test]
async fn delete_returns_not_supported() {
    let store = make_store().await;
    let id = store
        .blob_upload_bytes(Bytes::from_static(b"packed"))
        .await
        .unwrap();
    store.blob_sync().await.unwrap();
    let err = store.blob_delete(id.hash).await.unwrap_err();
    assert!(err.to_string().contains("blob_delete is not implemented"));
}

#[tokio::test]
async fn members_are_sorted_by_hash_in_pack() {
    let store = make_store().await;
    // Four blobs uploaded in scrambled order; the packed member table must come
    // out sorted by the blob's BLAKE3 hash prefix (the in-pack index key).
    let contents: Vec<Bytes> = vec![
        Bytes::from_static(b"AAAA"),
        Bytes::from_static(b"BBBB"),
        Bytes::from_static(b"CCCC"),
        Bytes::from_static(b"DDDD"),
    ];
    let mut hashes = Vec::new();
    for b in &contents {
        let id = store.blob_upload_bytes(b.clone()).await.unwrap();
        hashes.push(id.hash);
    }
    store.blob_sync().await.unwrap();

    let state = store.state.read().await;
    let mut keys_and_offsets: Vec<([u8; HASH_PREFIX_LEN], u32)> = hashes
        .iter()
        .map(|h| {
            let key = hash_key(*h);
            let (_, offset, _) = state.locate(&key).expect("packed");
            (key, offset)
        })
        .collect();
    keys_and_offsets.sort_by_key(|a| a.0);
    let offsets: Vec<u32> = keys_and_offsets.iter().map(|(_, o)| *o).collect();
    let mut sorted = offsets.clone();
    sorted.sort();
    assert_eq!(offsets, sorted, "pack body must be in hash-sorted order");
}

#[tokio::test]
async fn flush_on_put_does_not_deadlock_for_small_writes() {
    // Reproduces the bug where a small upload with flush_on_put=true and
    // min_group_size > write_size would hang forever waiting for a
    // pack flush that pack_once(false) refuses to perform.
    let blobs = BlobStore::without_outboard(MemoryStore::new());
    let cfg = PackingConfig {
        min_group_size: 40 * 1024 * 1024, // huge — small writes never reach
        flush_on_put: true,
        ..small_config()
    };
    let store = PackingStore::open(
        blobs,
        Arc::new(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        cfg,
    )
    .await
    .unwrap();

    let bytes = Bytes::from_static(b"tiny");
    let id = tokio::time::timeout(
        std::time::Duration::from_secs(3),
        store.blob_upload_bytes(bytes.clone()),
    )
    .await
    .expect("blob_upload_bytes must not hang under flush_on_put")
    .unwrap();

    // After upload returns, the bytes are durable via a pack.
    let key = hash_key(id.hash);
    let state = store.state.read().await;
    assert!(state.locate(&key).is_some());
    assert!(!state.pending.contains_key(&key));
}

/// Durability-ordering drill regression: a sub-`min_group_size` tail must still reach
/// durability via the background loop once it is older than
/// `max_pending_age` — before this, tiny staged blobs (identity publishes
/// are a few KB) had NO background durability path at all.
#[tokio::test]
async fn max_age_flush_drains_subminimum_tail() {
    // Below min_group_size AND below max_pending_age: not flushed.
    let fresh = PackingStore::open(
        BlobStore::without_outboard(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        PackingConfig {
            min_group_size: 40 * 1024 * 1024,
            max_pending_age: Duration::from_secs(3600),
            ..small_config()
        },
    )
    .await
    .unwrap();
    let id = fresh
        .blob_upload_bytes(Bytes::from_static(b"tiny tail"))
        .await
        .unwrap();
    fresh.pack_once(false).await.unwrap();
    let key = hash_key(id.hash);
    {
        let state = fresh.state.read().await;
        assert!(
            state.pending.contains_key(&key),
            "a fresh sub-minimum tail must stay staged"
        );
    }

    // Below min_group_size but PAST max_pending_age: the next tick flushes it.
    let stale = PackingStore::open(
        BlobStore::without_outboard(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        PackingConfig {
            min_group_size: 40 * 1024 * 1024,
            max_pending_age: Duration::ZERO,
            ..small_config()
        },
    )
    .await
    .unwrap();
    let id = stale
        .blob_upload_bytes(Bytes::from_static(b"stale tail"))
        .await
        .unwrap();
    stale.pack_once(false).await.unwrap();
    let key = hash_key(id.hash);
    let state = stale.state.read().await;
    assert!(
        state.locate(&key).is_some(),
        "a stale sub-minimum tail must be flushed into a pack"
    );
    assert!(!state.pending.contains_key(&key));
}

/// Delegating wrapper whose `blob_upload_stream` never completes — a wedged
/// backend (degraded host pool, parked upload permit).
#[derive(Debug)]
struct HangingBlobs {
    inner: BlobStore,
}

#[async_trait::async_trait]
impl BlobsRead for HangingBlobs {
    async fn blob_contains(&self, hash: Hash) -> BlobResult<bool> {
        self.inner.blob_contains(hash).await
    }
    async fn blob_get_size(&self, hash: Hash) -> BlobResult<u64> {
        self.inner.blob_get_size(hash).await
    }
    async fn blob_download(&self, hash: Hash) -> BlobResult<Bytes> {
        self.inner.blob_download(hash).await
    }
    async fn blob_download_slice(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> BlobResult<Bytes> {
        self.inner.blob_download_slice(hash, offset, max_len).await
    }
    async fn blob_read(&self, hash: Hash) -> BlobResult<Box<dyn AsyncRead + Send + Unpin>> {
        self.inner.blob_read(hash).await
    }
}

#[async_trait::async_trait]
impl BlobsWrite for HangingBlobs {
    async fn blob_upload_bytes(&self, bytes: Bytes) -> BlobResult<BlobId> {
        self.inner.blob_upload_bytes(bytes).await
    }
    async fn blob_upload_reader<R, F>(
        &self,
        hash: Hash,
        size: u64,
        reader: R,
        on_progress: F,
    ) -> BlobResult<BlobId>
    where
        Self: Sized,
        R: AsyncRead + Send + Unpin + 'static,
        F: Fn(u64) -> io::Result<()> + Send + Sync + 'static,
    {
        self.inner
            .blob_upload_reader(hash, size, reader, on_progress)
            .await
    }
    async fn blob_upload_stream<St>(&self, _stream: St) -> BlobResult<BlobId>
    where
        Self: Sized,
        St: Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static,
    {
        std::future::pending::<()>().await;
        unreachable!()
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn blob_upload_file(&self, path: std::path::PathBuf) -> BlobResult<BlobId> {
        self.inner.blob_upload_file(path).await
    }
    async fn blob_sync(&self) -> BlobResult<()> {
        self.inner.blob_sync().await
    }
}

/// Upload-wedge drill regression: a wedged pack upload must fail the cycle after
/// the outer timeout instead of stalling the store forever — and the staged
/// entries must survive for the next tick's retry.
#[tokio::test]
async fn wedged_upload_times_out_and_retains_staging() {
    let store = PackingStore::open(
        HangingBlobs {
            inner: BlobStore::without_outboard(MemoryStore::new()),
        },
        Arc::new(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        PackingConfig {
            upload_timeout_floor: Duration::from_millis(100),
            ..small_config()
        },
    )
    .await
    .unwrap();

    let id = store
        .blob_upload_bytes(Bytes::from_static(b"will wedge"))
        .await
        .unwrap();
    let err = tokio::time::timeout(Duration::from_secs(5), store.pack_once(true))
        .await
        .expect("pack_once must return once the outer upload timeout fires")
        .expect_err("a timed-out upload must fail the flush cycle");
    assert!(
        err.to_string().contains("timed out"),
        "unexpected error: {err:#}"
    );

    // The WAL keeps the blob for the next attempt.
    let key = hash_key(id.hash);
    let state = store.state.read().await;
    assert!(state.pending.contains_key(&key));
    assert!(state.locate(&key).is_none());
}

#[tokio::test]
async fn restart_replays_manifests() {
    // (No deletion log — deletion/GC is deferred; this only replays manifests.)
    let blobs_inner = Arc::new(MemoryStore::new());
    let manifests_inner = Arc::new(MemoryStore::new());
    let staging_inner = Arc::new(MemoryStore::new());

    let bytes = Bytes::from_static(b"persisted packed bytes");
    let hash = Hash::new(&bytes);

    {
        let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
        let s = PackingStore::open(
            blobs,
            manifests_inner.clone(),
            staging_inner.clone(),
            small_config(),
        )
        .await
        .unwrap();
        s.blob_upload_bytes(bytes.clone()).await.unwrap();
        s.blob_sync().await.unwrap();
    }

    // Reopen and verify index was loaded.
    {
        let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
        let s = PackingStore::open(
            blobs,
            manifests_inner.clone(),
            staging_inner.clone(),
            small_config(),
        )
        .await
        .unwrap();
        let read = s.blob_download(hash).await.unwrap();
        assert_eq!(read, bytes);
    }
}

#[tokio::test]
async fn reconstruct_from_headers_rebuilds_after_cache_wipe() {
    // The manifest-free recovery path: pack bodies survive but the local
    // manifest cache is wiped (a cold device). The index must rebuild purely
    // from each body's self-describing prepended header.
    let blobs_inner = Arc::new(MemoryStore::new());

    let entries: Vec<Bytes> = vec![
        Bytes::from_static(b"first blob bytes"),
        Bytes::from_static(b"second, longer blob of bytes!!"),
        Bytes::from_static(b"three"),
    ];
    let hashes: Vec<Hash> = entries.iter().map(Hash::new).collect();

    // Write + flush against a fresh manifest cache.
    {
        let blobs = BlobStore::from_arc_with_outboard(blobs_inner.clone() as Arc<dyn Store>, false);
        let s = PackingStore::open(
            blobs,
            Arc::new(MemoryStore::new()),
            Arc::new(MemoryStore::new()),
            small_config(),
        )
        .await
        .unwrap();
        for b in &entries {
            s.blob_upload_bytes(b.clone()).await.unwrap();
        }
        s.blob_sync().await.unwrap();
    }

    // Reopen with an EMPTY manifest cache + empty staging — only the durable
    // pack bodies in `blobs_inner` survive.
    let blobs = BlobStore::from_arc_with_outboard(blobs_inner.clone() as Arc<dyn Store>, false);
    let s = PackingStore::open(
        blobs,
        Arc::new(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        small_config(),
    )
    .await
    .unwrap();

    // Cold: nothing is locatable yet.
    assert!(!s.blob_contains(hashes[0]).await.unwrap());

    // Enumerate surviving pack bodies and rebuild from their headers.
    let body_store =
        BlobStore::from_arc_with_outboard(blobs_inner.clone() as Arc<dyn Store>, false);
    let pack_hashes = body_store.list_hashes().await.unwrap();
    let restored = s.reconstruct_from_headers(pack_hashes).await.unwrap();
    assert_eq!(
        restored, 1,
        "the three small blobs packed into a single pack"
    );

    // Every blob is readable again, byte-for-byte, from the rebuilt index.
    for (b, h) in entries.iter().zip(&hashes) {
        let got = s.blob_download(*h).await.unwrap();
        assert_eq!(&got, b, "blob {h} must round-trip after a header rebuild");
    }

    // Idempotent: a second pass restores nothing new.
    let again = s
        .reconstruct_from_headers(body_store.list_hashes().await.unwrap())
        .await
        .unwrap();
    assert_eq!(again, 0);
}

#[tokio::test]
async fn cold_boot_reconcile_recovers_many_packs() {
    // Mirrors exactly what `create_raw_store` now does on a cold device, but at
    // the realistic scale of MANY packs (production enumerates thousands): open
    // over a WIPED manifest cache, enumerate every surviving pack body, then
    // `reconstruct_from_headers`. Guards the multi-pack enumerate path — a regression
    // where the reconcile is dropped would make every packed blob read "not found".
    let blobs_inner = Arc::new(MemoryStore::new());

    // 12 blobs, each flushed on its own → 12 distinct packs (min_group_size = 1,
    // one `blob_sync` per upload force-flushes a single-member pack).
    let entries: Vec<Bytes> = (0..12u8)
        .map(|i| Bytes::from(format!("cold-boot blob number {i} with some padding").into_bytes()))
        .collect();
    let hashes: Vec<Hash> = entries.iter().map(Hash::new).collect();
    {
        let blobs = BlobStore::from_arc_with_outboard(blobs_inner.clone() as Arc<dyn Store>, false);
        let s = PackingStore::open(
            blobs,
            Arc::new(MemoryStore::new()),
            Arc::new(MemoryStore::new()),
            small_config(),
        )
        .await
        .unwrap();
        for b in &entries {
            s.blob_upload_bytes(b.clone()).await.unwrap();
            s.blob_sync().await.unwrap(); // one pack per blob
        }
    }

    // Cold reopen: empty manifest cache + empty staging. `open` does NOT
    // self-reconcile — exactly why `create_raw_store` must enumerate + rebuild.
    let blobs = BlobStore::from_arc_with_outboard(blobs_inner.clone() as Arc<dyn Store>, false);
    let s = PackingStore::open(
        blobs,
        Arc::new(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        small_config(),
    )
    .await
    .unwrap();
    for h in &hashes {
        assert!(
            !s.blob_contains(*h).await.unwrap(),
            "a cold cache must not resolve any packed blob before the reconcile"
        );
    }

    // The production reconcile sequence: enumerate pack bodies → rebuild headers.
    let body_store =
        BlobStore::from_arc_with_outboard(blobs_inner.clone() as Arc<dyn Store>, false);
    let pack_hashes = body_store.list_hashes().await.unwrap();
    assert_eq!(pack_hashes.len(), entries.len(), "one pack body per blob");
    let restored = s.reconstruct_from_headers(pack_hashes).await.unwrap();
    assert_eq!(
        restored,
        entries.len() as u64,
        "every pack rebuilt from its header"
    );

    // All blobs across all packs read back byte-for-byte.
    for (b, h) in entries.iter().zip(&hashes) {
        let got = s.blob_download(*h).await.unwrap();
        assert_eq!(
            &got, b,
            "blob {h} must round-trip after a cold-boot reconcile"
        );
    }
}

#[tokio::test]
async fn snapshot_load_survives_per_pack_manifest_wipe() {
    // The persisted bulk-index snapshot must let a warm open install the index
    // WITHOUT reading per-pack manifests. Prove it by wiping every per-pack
    // manifest and keeping only the snapshot: reads succeed iff the snapshot was
    // used. (Browser-safe: the snapshot is just a blob in the cache `Store`.)
    let blobs_inner = Arc::new(MemoryStore::new());
    let manifests_inner = Arc::new(MemoryStore::new());
    let staging_inner = Arc::new(MemoryStore::new());

    let entries: Vec<Bytes> = (0..6u8)
        .map(|i| Bytes::from(format!("snapshot blob {i} padding").into_bytes()))
        .collect();
    let hashes: Vec<Hash> = entries.iter().map(Hash::new).collect();

    let snap_key = {
        let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
        let s = PackingStore::open(
            blobs,
            manifests_inner.clone(),
            staging_inner.clone(),
            small_config(),
        )
        .await
        .unwrap();
        for b in &entries {
            s.blob_upload_bytes(b.clone()).await.unwrap();
            s.blob_sync().await.unwrap(); // one pack per blob
        }
        s.rebuild_index().await.unwrap(); // builds the bulk MPHF + writes the snapshot
        s.snapshot_key()
    };
    assert!(
        manifests_inner.exists(&snap_key).await.unwrap(),
        "rebuild_index must persist the index snapshot"
    );

    // Wipe every per-pack manifest, keep ONLY the snapshot.
    let mut wiped = 0usize;
    {
        let mut stream = manifests_inner.list().await.unwrap();
        let mut paths = Vec::new();
        while let Some(p) = stream.next().await {
            let p = p.unwrap();
            if p != snap_key {
                paths.push(p);
            }
        }
        for p in &paths {
            manifests_inner.delete(p).await.unwrap();
            wiped += 1;
        }
    }
    assert!(
        wiped >= 6,
        "expected per-pack manifests to wipe, got {wiped}"
    );

    // Reopen: only the snapshot remains, so any successful read proves it loaded
    // the index from the snapshot (not from per-pack manifests, which are gone).
    let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
    let s = PackingStore::open(
        blobs,
        manifests_inner.clone(),
        staging_inner.clone(),
        small_config(),
    )
    .await
    .unwrap();
    for (b, h) in entries.iter().zip(&hashes) {
        let got = s.blob_download(*h).await.unwrap();
        assert_eq!(&got, b, "blob {h} read via the persisted snapshot");
    }
}

#[tokio::test]
async fn snapshot_plus_newer_tail_reads_all() {
    // A snapshot covers packs as of the last rebuild; packs flushed AFTER it
    // live only as per-pack manifests. A warm open must load the snapshot AND
    // fold in that tail, so every blob — old and new — resolves.
    let blobs_inner = Arc::new(MemoryStore::new());
    let manifests_inner = Arc::new(MemoryStore::new());
    let staging_inner = Arc::new(MemoryStore::new());

    let snapshotted: Vec<Bytes> = (0..3u8)
        .map(|i| Bytes::from(format!("pre-snapshot {i}").into_bytes()))
        .collect();
    let tail: Vec<Bytes> = (0..2u8)
        .map(|i| Bytes::from(format!("post-snapshot tail {i}").into_bytes()))
        .collect();

    {
        let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
        let s = PackingStore::open(
            blobs,
            manifests_inner.clone(),
            staging_inner.clone(),
            small_config(),
        )
        .await
        .unwrap();
        for b in &snapshotted {
            s.blob_upload_bytes(b.clone()).await.unwrap();
            s.blob_sync().await.unwrap();
        }
        s.rebuild_index().await.unwrap(); // snapshot covers the first 3 packs
        for b in &tail {
            s.blob_upload_bytes(b.clone()).await.unwrap();
            s.blob_sync().await.unwrap(); // tail packs: per-pack manifests only
        }
    }

    let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
    let s = PackingStore::open(
        blobs,
        manifests_inner.clone(),
        staging_inner.clone(),
        small_config(),
    )
    .await
    .unwrap();
    for b in snapshotted.iter().chain(tail.iter()) {
        let got = s.blob_download(Hash::new(b)).await.unwrap();
        assert_eq!(&got, b, "snapshot + tail must resolve every blob");
    }
}

#[tokio::test]
async fn corrupt_snapshot_falls_back_to_rebuild() {
    // A stale/corrupt snapshot must never break the store: it's a pure cache, so
    // an unreadable blob is ignored and the index rebuilds from per-pack headers.
    let blobs_inner = Arc::new(MemoryStore::new());
    let manifests_inner = Arc::new(MemoryStore::new());
    let staging_inner = Arc::new(MemoryStore::new());

    let entries: Vec<Bytes> = (0..4u8)
        .map(|i| Bytes::from(format!("fallback blob {i}").into_bytes()))
        .collect();
    let hashes: Vec<Hash> = entries.iter().map(Hash::new).collect();

    let snap_key = {
        let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
        let s = PackingStore::open(
            blobs,
            manifests_inner.clone(),
            staging_inner.clone(),
            small_config(),
        )
        .await
        .unwrap();
        for b in &entries {
            s.blob_upload_bytes(b.clone()).await.unwrap();
            s.blob_sync().await.unwrap();
        }
        s.rebuild_index().await.unwrap();
        s.snapshot_key()
    };

    // Corrupt the snapshot blob (per-pack manifests stay intact).
    manifests_inner
        .put_bytes(
            &snap_key,
            Bytes::from_static(b"not a valid postcard snapshot"),
        )
        .await
        .unwrap();

    // Reopen: the snapshot is ignored, the index rebuilds, every blob resolves.
    let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
    let s = PackingStore::open(
        blobs,
        manifests_inner.clone(),
        staging_inner.clone(),
        small_config(),
    )
    .await
    .unwrap();
    for (b, h) in entries.iter().zip(&hashes) {
        let got = s.blob_download(*h).await.unwrap();
        assert_eq!(&got, b, "blob {h} resolves after snapshot fallback");
    }
}

#[tokio::test]
async fn note_then_enrich_drains_todo_and_drops_non_packs() {
    // The discover/enrich split: `note_pack_hashes` records cheap todo markers
    // (no header reads); `enrich` drains them — folding real packs in and
    // DROPPING definitive non-packs so `todo` reaches empty.
    let blobs_inner = Arc::new(MemoryStore::new());

    // A real pack.
    let blob = Bytes::from_static(b"a real packed blob");
    let blob_hash = Hash::new(&blob);
    let pack_hash = {
        let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
        let s = PackingStore::open(
            blobs,
            Arc::new(MemoryStore::new()),
            Arc::new(MemoryStore::new()),
            small_config(),
        )
        .await
        .unwrap();
        s.blob_upload_bytes(blob.clone()).await.unwrap();
        s.blob_sync().await.unwrap();
        let body = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
        body.list_hashes().await.unwrap()[0]
    };
    // A foreign (non-pack) object in the same backend.
    let foreign = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>)
        .import_bytes(Bytes::from_static(b"i am not a pack"))
        .await
        .unwrap();

    // Cold store: fresh caches. Note BOTH hashes; neither is enriched yet.
    let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
    let s = PackingStore::open(
        blobs,
        Arc::new(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        small_config(),
    )
    .await
    .unwrap();
    s.note_pack_hashes([pack_hash, foreign.hash]).await.unwrap();
    assert_eq!(s.state.read().await.todo.len(), 2, "both noted as pending");

    let enriched = s.enrich().await.unwrap();
    assert_eq!(
        enriched, 1,
        "the real pack enriches; the foreign hash does not"
    );
    assert!(
        s.state.read().await.todo.is_empty(),
        "todo must drain to empty — the non-pack is dropped, not retried forever"
    );
    assert_eq!(s.blob_download(blob_hash).await.unwrap(), blob);
}

#[tokio::test]
async fn negative_lookup_gates_on_enrichment() {
    // The correctness rule: while packs are pending, a would-be NEGATIVE answer
    // must enrich first. Here we only `note_pack_hashes` (never call `enrich`);
    // a `blob_contains`/read for a packed blob must still succeed by draining the
    // pending pack on the miss — and a genuinely-absent key returns false after.
    let blobs_inner = Arc::new(MemoryStore::new());
    let blob = Bytes::from_static(b"reachable only after enrichment");
    let blob_hash = Hash::new(&blob);

    let pack_hash = {
        let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
        let s = PackingStore::open(
            blobs,
            Arc::new(MemoryStore::new()),
            Arc::new(MemoryStore::new()),
            small_config(),
        )
        .await
        .unwrap();
        s.blob_upload_bytes(blob.clone()).await.unwrap();
        s.blob_sync().await.unwrap();
        BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>)
            .list_hashes()
            .await
            .unwrap()[0]
    };

    // Cold store: knows the pack hash (noted) but has NOT enriched it.
    let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
    let s = PackingStore::open(
        blobs,
        Arc::new(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        small_config(),
    )
    .await
    .unwrap();
    s.note_pack_hashes([pack_hash]).await.unwrap();
    assert!(
        !s.state.read().await.todo.is_empty(),
        "pack is pending, not enriched"
    );

    // A lookup MISS with a pending pack must not lie: it enriches, then resolves.
    assert!(
        s.blob_contains(blob_hash).await.unwrap(),
        "exists must drain the pending pack rather than return a false negative"
    );
    assert_eq!(s.blob_download(blob_hash).await.unwrap(), blob);
    // Draining cleared the pending set; an absent key now answers false honestly.
    assert!(s.state.read().await.todo.is_empty());
    assert!(
        !s.blob_contains(Hash::new(b"definitely absent"))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn todo_markers_persist_across_restart() {
    // A pack noted but not yet enriched (e.g. an interrupted enrichment) must be
    // remembered across a restart via its persisted `todo/` marker.
    let blobs_inner = Arc::new(MemoryStore::new());
    let manifests_inner = Arc::new(MemoryStore::new());

    let blob = Bytes::from_static(b"survives a restart as a todo");
    let blob_hash = Hash::new(&blob);
    let pack_hash = {
        let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
        let s = PackingStore::open(
            blobs,
            Arc::new(MemoryStore::new()),
            Arc::new(MemoryStore::new()),
            small_config(),
        )
        .await
        .unwrap();
        s.blob_upload_bytes(blob.clone()).await.unwrap();
        s.blob_sync().await.unwrap();
        BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>)
            .list_hashes()
            .await
            .unwrap()[0]
    };

    // Store A: note the pack into a persistent manifests cache, but DON'T enrich.
    {
        let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
        let s = PackingStore::open(
            blobs,
            manifests_inner.clone(),
            Arc::new(MemoryStore::new()),
            small_config(),
        )
        .await
        .unwrap();
        s.note_pack_hashes([pack_hash]).await.unwrap();
    }

    // Store B: reopen over the same manifests — the todo marker is reloaded.
    let blobs = BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>);
    let s = PackingStore::open(
        blobs,
        manifests_inner.clone(),
        Arc::new(MemoryStore::new()),
        small_config(),
    )
    .await
    .unwrap();
    assert_eq!(
        s.state.read().await.todo.len(),
        1,
        "the persisted todo marker resumes the pending enrichment after restart"
    );
    // And the gated read still resolves it.
    assert_eq!(s.blob_download(blob_hash).await.unwrap(), blob);
}

#[tokio::test]
async fn reconstruct_skips_non_pack_bodies() {
    // A shared blob backend can hold non-pack / legacy objects; recovery must
    // skip what isn't a readable pack header rather than abort.
    let blobs_inner = Arc::new(MemoryStore::new());

    // One valid pack.
    let valid = Bytes::from_static(b"valid pack member");
    let valid_hash = Hash::new(&valid);
    {
        let blobs = BlobStore::from_arc_with_outboard(blobs_inner.clone() as Arc<dyn Store>, false);
        let s = PackingStore::open(
            blobs,
            Arc::new(MemoryStore::new()),
            Arc::new(MemoryStore::new()),
            small_config(),
        )
        .await
        .unwrap();
        s.blob_upload_bytes(valid).await.unwrap();
        s.blob_sync().await.unwrap();
    }

    // A foreign blob (no header) written straight into the body store.
    let foreign_store =
        BlobStore::from_arc_with_outboard(blobs_inner.clone() as Arc<dyn Store>, false);
    let foreign = foreign_store
        .import_bytes(Bytes::from_static(b"i am not a pack"))
        .await
        .unwrap();

    // Cold rebuild over BOTH: the pack is restored, the foreign blob is skipped.
    let blobs = BlobStore::from_arc_with_outboard(blobs_inner.clone() as Arc<dyn Store>, false);
    let s = PackingStore::open(
        blobs,
        Arc::new(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        small_config(),
    )
    .await
    .unwrap();
    let all = foreign_store.list_hashes().await.unwrap();
    assert!(all.contains(&foreign.hash));
    let restored = s.reconstruct_from_headers(all).await.unwrap();
    assert_eq!(
        restored, 1,
        "only the valid pack restores; foreign blob skipped"
    );
    assert!(s.blob_contains(valid_hash).await.unwrap());
}

#[tokio::test]
async fn upload_stream_round_trips_via_pack() {
    // Exercises the streaming upload path: blob_upload_stream collects the
    // chunks, hashes them, and stages the blob; reads then work whole + ranged.
    let store = make_store().await;

    let parts: [&[u8]; 3] = [b"streamed ", b"in three ", b"chunks!!"];
    let full = Bytes::from(parts.concat());

    let chunks: Vec<Result<Bytes, io::Error>> = parts
        .iter()
        .map(|p| Ok(Bytes::copy_from_slice(p)))
        .collect();
    let id = store
        .blob_upload_stream(stream::iter(chunks))
        .await
        .unwrap();
    assert_eq!(id.hash, Hash::new(&full));

    // Read back whole while still pending (from staging).
    let got = store.blob_download(id.hash).await.unwrap();
    assert_eq!(got, full);

    // Flush, then read from the pack.
    store.blob_sync().await.unwrap();
    let got = store.blob_download(id.hash).await.unwrap();
    assert_eq!(got, full);

    // Ranged read from the pack.
    let got = store
        .blob_download_slice(id.hash, 9, Some(8))
        .await
        .unwrap();
    assert_eq!(got.as_ref(), b"in three");

    // Whole-blob reader.
    use tokio::io::AsyncReadExt;
    let mut reader = store.blob_read(id.hash).await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(Bytes::from(buf), full);
}

/// Staging wrapper that delays reads, widening the window in which a second
/// concurrent flush could delete a file the first is still streaming.
#[derive(Debug)]
struct SlowStaging(Arc<MemoryStore>, std::time::Duration);

#[async_trait::async_trait]
impl Store for SlowStaging {
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
        tokio::time::sleep(self.1).await;
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

#[tokio::test]
async fn concurrent_pack_once_does_not_race_on_staging() {
    // Regression: two flush cycles racing on the same pending set surfaced live
    // as "staging read <hash>: No such file" when the background tick and the
    // publish sync() barrier overlapped. Slow staging widens the read window so
    // the race would fire without the serializing flush lock; the one-pack
    // assertion also catches a duplicate flush even if the timing doesn't.
    let staging = Arc::new(MemoryStore::new());
    let store = PackingStore::open(
        BlobStore::without_outboard(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        Arc::new(SlowStaging(staging, std::time::Duration::from_millis(25))),
        small_config(),
    )
    .await
    .unwrap();

    let mut entries = Vec::new();
    for i in 0..6 {
        let b = Bytes::from(format!("concurrent-race-blob-{i}").into_bytes());
        let id = store.blob_upload_bytes(b.clone()).await.unwrap();
        entries.push((id.hash, b));
    }

    // Fire two force-flushes concurrently.
    let (s1, s2) = (store.clone(), store.clone());
    let (r1, r2) = tokio::join!(
        tokio::spawn(async move { s1.pack_once(true).await }),
        tokio::spawn(async move { s2.pack_once(true).await }),
    );
    r1.unwrap().expect("flush cycle 1");
    r2.unwrap().expect("flush cycle 2");

    // Exactly one pack, every blob intact — no double flush, no lost staging file.
    {
        let st = store.state.read().await;
        assert_eq!(
            st.packs.len(),
            1,
            "one pack, not a duplicate from a racing flush"
        );
        assert!(st.pending.is_empty(), "all staged entries consumed");
    }
    for (hash, b) in &entries {
        assert_eq!(&store.blob_download(*hash).await.unwrap(), b);
    }
}

// ---- fault injection: a `BlobsReadWrite` whose reads can be made to fail ----

/// Wraps a `BlobStore` and, while its flag is set, fails every READ with a
/// transient error (writes and enumeration still succeed). This exercises the
/// enrich transient-vs-terminal classification and the non-authoritative-negative
/// error semantics — paths an infallible `MemoryStore` can never reach.
#[derive(Debug)]
struct FaultyBlobs {
    inner: BlobStore,
    fail_reads: Arc<AtomicBool>,
}

impl FaultyBlobs {
    fn new(inner: BlobStore) -> (Self, Arc<AtomicBool>) {
        let flag = Arc::new(AtomicBool::new(false));
        (
            Self {
                inner,
                fail_reads: flag.clone(),
            },
            flag,
        )
    }

    fn fail(&self) -> BlobResult<()> {
        if self.fail_reads.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("injected transient read failure"));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl BlobsRead for FaultyBlobs {
    async fn blob_contains(&self, hash: Hash) -> BlobResult<bool> {
        self.fail()?;
        self.inner.blob_contains(hash).await
    }
    async fn blob_get_size(&self, hash: Hash) -> BlobResult<u64> {
        self.fail()?;
        self.inner.blob_get_size(hash).await
    }
    async fn blob_download(&self, hash: Hash) -> BlobResult<Bytes> {
        self.fail()?;
        self.inner.blob_download(hash).await
    }
    async fn blob_download_slice(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> BlobResult<Bytes> {
        self.fail()?;
        self.inner.blob_download_slice(hash, offset, max_len).await
    }
    async fn blob_read(&self, hash: Hash) -> BlobResult<Box<dyn AsyncRead + Send + Unpin>> {
        self.fail()?;
        self.inner.blob_read(hash).await
    }
}

#[async_trait::async_trait]
impl BlobsWrite for FaultyBlobs {
    async fn blob_upload_bytes(&self, bytes: Bytes) -> BlobResult<BlobId> {
        self.inner.blob_upload_bytes(bytes).await
    }
    async fn blob_upload_reader<R, F>(
        &self,
        hash: Hash,
        size: u64,
        reader: R,
        on_progress: F,
    ) -> BlobResult<BlobId>
    where
        Self: Sized,
        R: AsyncRead + Send + Unpin + 'static,
        F: Fn(u64) -> io::Result<()> + Send + Sync + 'static,
    {
        self.inner
            .blob_upload_reader(hash, size, reader, on_progress)
            .await
    }
    async fn blob_upload_stream<St>(&self, stream: St) -> BlobResult<BlobId>
    where
        Self: Sized,
        St: Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static,
    {
        self.inner.blob_upload_stream(stream).await
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn blob_upload_file(&self, path: std::path::PathBuf) -> BlobResult<BlobId> {
        self.inner.blob_upload_file(path).await
    }
    async fn blob_sync(&self) -> BlobResult<()> {
        self.inner.blob_sync().await
    }
}

// ---- integrity: full reads must re-verify the content address ----

/// Wraps a `BlobStore` and, while its flag is set, flips a byte in every
/// slice read — a corrupted (or index-mis-mapped) pack region. Writes pass
/// through untouched.
#[derive(Debug)]
struct CorruptingBlobs {
    inner: BlobStore,
    corrupt_reads: Arc<AtomicBool>,
}

impl CorruptingBlobs {
    fn new(inner: BlobStore) -> (Self, Arc<AtomicBool>) {
        let flag = Arc::new(AtomicBool::new(false));
        (
            Self {
                inner,
                corrupt_reads: flag.clone(),
            },
            flag,
        )
    }

    fn maybe_flip(&self, bytes: Bytes) -> Bytes {
        if !self.corrupt_reads.load(Ordering::SeqCst) || bytes.is_empty() {
            return bytes;
        }
        let mut v = bytes.to_vec();
        let mid = v.len() / 2;
        v[mid] ^= 0xff;
        Bytes::from(v)
    }
}

#[async_trait::async_trait]
impl BlobsRead for CorruptingBlobs {
    async fn blob_contains(&self, hash: Hash) -> BlobResult<bool> {
        self.inner.blob_contains(hash).await
    }
    async fn blob_get_size(&self, hash: Hash) -> BlobResult<u64> {
        self.inner.blob_get_size(hash).await
    }
    async fn blob_download(&self, hash: Hash) -> BlobResult<Bytes> {
        Ok(self.maybe_flip(self.inner.blob_download(hash).await?))
    }
    async fn blob_download_slice(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> BlobResult<Bytes> {
        Ok(self.maybe_flip(
            self.inner
                .blob_download_slice(hash, offset, max_len)
                .await?,
        ))
    }
    async fn blob_read(&self, hash: Hash) -> BlobResult<Box<dyn AsyncRead + Send + Unpin>> {
        self.inner.blob_read(hash).await
    }
}

#[async_trait::async_trait]
impl BlobsWrite for CorruptingBlobs {
    async fn blob_upload_bytes(&self, bytes: Bytes) -> BlobResult<BlobId> {
        self.inner.blob_upload_bytes(bytes).await
    }
    async fn blob_upload_reader<R, F>(
        &self,
        hash: Hash,
        size: u64,
        reader: R,
        on_progress: F,
    ) -> BlobResult<BlobId>
    where
        Self: Sized,
        R: AsyncRead + Send + Unpin + 'static,
        F: Fn(u64) -> io::Result<()> + Send + Sync + 'static,
    {
        self.inner
            .blob_upload_reader(hash, size, reader, on_progress)
            .await
    }
    async fn blob_upload_stream<St>(&self, stream: St) -> BlobResult<BlobId>
    where
        Self: Sized,
        St: Stream<Item = Result<Bytes, io::Error>> + Send + Unpin + 'static,
    {
        self.inner.blob_upload_stream(stream).await
    }
    #[cfg(not(target_arch = "wasm32"))]
    async fn blob_upload_file(&self, path: std::path::PathBuf) -> BlobResult<BlobId> {
        self.inner.blob_upload_file(path).await
    }
    async fn blob_sync(&self) -> BlobResult<()> {
        self.inner.blob_sync().await
    }
}

#[tokio::test]
async fn corrupt_pack_slice_fails_full_read_integrity() {
    // A validly-located pack slice returning WRONG bytes (bit rot, or a
    // mis-mapped index entry) must fail full reads — the BlobsRead
    // integrity contract — never hand wrong bytes to the caller.
    let backend = BlobStore::without_outboard(MemoryStore::new());
    let (blobs, corrupt) = CorruptingBlobs::new(backend);
    let store = PackingStore::open(
        blobs,
        Arc::new(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        small_config(),
    )
    .await
    .expect("open packing store");

    let bytes = Bytes::from_static(b"verify me end to end, byte for byte");
    let id = store.blob_upload_bytes(bytes.clone()).await.unwrap();
    store.blob_sync().await.unwrap(); // flush → the read goes via the pack

    // Sanity: clean read through the pack round-trips.
    assert_eq!(store.blob_download(id.hash).await.unwrap(), bytes);

    corrupt.store(true, Ordering::SeqCst);
    let err = store.blob_download(id.hash).await.unwrap_err();
    assert!(
        err.to_string().contains("integrity check failed"),
        "expected integrity failure, got: {err}"
    );
    assert!(
        store.blob_read(id.hash).await.is_err(),
        "reader form must verify too"
    );

    // Partial slices are exempt by contract (documented on BlobsRead):
    // wrong bytes pass through; higher layers own partial-read integrity.
    let slice = store
        .blob_download_slice(id.hash, 1, Some(4))
        .await
        .unwrap();
    assert_eq!(slice.len(), 4);

    corrupt.store(false, Ordering::SeqCst);
    assert_eq!(
        store.blob_download(id.hash).await.unwrap(),
        bytes,
        "verification is stateless — clean reads succeed again"
    );
}

#[tokio::test]
async fn transient_read_failure_keeps_markers_and_errors_negatives() {
    // Build packs against a shared backend, then reopen COLD over a backend whose
    // reads fail (a Sia/network blip). `enrich` must KEEP the todo markers
    // (transient != definitively-not-a-pack), and a negative-answering lookup
    // must return a retryable ERROR — never a false "not found" — while known
    // packs are unreadable. Recovery drains the markers and restores honesty.
    let backend = Arc::new(MemoryStore::new());
    let entries: Vec<Bytes> = (0..4u8)
        .map(|i| Bytes::from(format!("transient-path blob {i} with padding").into_bytes()))
        .collect();
    let hashes: Vec<Hash> = entries.iter().map(Hash::new).collect();
    {
        let blobs = BlobStore::from_arc(backend.clone() as Arc<dyn Store>);
        let s = PackingStore::open(
            blobs,
            Arc::new(MemoryStore::new()),
            Arc::new(MemoryStore::new()),
            small_config(),
        )
        .await
        .unwrap();
        for b in &entries {
            s.blob_upload_bytes(b.clone()).await.unwrap();
            s.blob_sync().await.unwrap(); // one pack per blob
        }
    }
    let pack_hashes = BlobStore::from_arc(backend.clone() as Arc<dyn Store>)
        .list_hashes()
        .await
        .unwrap();

    let (faulty, fail) = FaultyBlobs::new(BlobStore::from_arc(backend.clone() as Arc<dyn Store>));
    let s = PackingStore::open(
        faulty,
        Arc::new(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        small_config(),
    )
    .await
    .unwrap();
    s.note_pack_hashes(pack_hashes.clone()).await.unwrap();
    assert_eq!(s.state.read().await.todo.len(), pack_hashes.len());

    // Reads fail: enrich folds in nothing and RETAINS every marker.
    fail.store(true, Ordering::SeqCst);
    assert_eq!(
        s.enrich().await.unwrap(),
        0,
        "a transient read failure enriches nothing"
    );
    assert_eq!(
        s.state.read().await.todo.len(),
        pack_hashes.len(),
        "transient failure KEEPS markers — unlike a non-pack, which is dropped"
    );

    // A3: a would-be-negative read cannot prove absence → retryable error.
    let err = s.blob_contains(hashes[0]).await.unwrap_err();
    assert!(
        err.to_string().contains("unreadable"),
        "negative lookup must be a retryable error, not a false 'not found': {err}"
    );
    // And the write path must NOT re-stage a maybe-present blob (that is exactly
    // what would mint a duplicate pack) — it errors and leaves staging untouched.
    let werr = s.blob_upload_bytes(entries[0].clone()).await.unwrap_err();
    assert!(werr.to_string().contains("unreadable"), "{werr}");
    assert!(
        s.state.read().await.pending.is_empty(),
        "must not re-stage a blob while its potential home pack is unreadable"
    );

    // Backend recovers: markers drain, reads work, negatives are honest again.
    fail.store(false, Ordering::SeqCst);
    assert_eq!(s.enrich().await.unwrap() as usize, pack_hashes.len());
    assert!(s.state.read().await.todo.is_empty());
    for (b, h) in entries.iter().zip(&hashes) {
        assert_eq!(&s.blob_download(*h).await.unwrap(), b);
    }
    assert!(
        !s.blob_contains(Hash::new(b"definitely-absent"))
            .await
            .unwrap(),
        "a real miss, with todo drained, is an honest false"
    );
}

#[tokio::test]
async fn duplicate_key_across_packs_survives_rebuild() {
    // A blob can legitimately land in TWO packs — e.g. two devices packing the
    // same content-addressed blob (X) beside different neighbours before they
    // sync. A cold reconstruct then indexes key(X) twice, and the MPHF build must
    // NOT panic on the duplicate (`boomphf::Mphf::new` aborts on dup keys, which
    // would render the store permanently un-openable).
    let backend = Arc::new(MemoryStore::new());
    let x = Bytes::from_static(b"shared blob X, lives in both packs");
    let y = Bytes::from_static(b"neighbour Y, only in pack A");
    let z = Bytes::from_static(b"neighbour Z, only in pack B");
    let (xh, yh, zh) = (Hash::new(&x), Hash::new(&y), Hash::new(&z));

    // "Device A": one pack {X, Y}.
    {
        let blobs = BlobStore::from_arc(backend.clone() as Arc<dyn Store>);
        let s = PackingStore::open(
            blobs,
            Arc::new(MemoryStore::new()),
            Arc::new(MemoryStore::new()),
            small_config(),
        )
        .await
        .unwrap();
        s.blob_upload_bytes(x.clone()).await.unwrap();
        s.blob_upload_bytes(y.clone()).await.unwrap();
        s.blob_sync().await.unwrap();
    }
    // "Device B" (own caches, same backend): a distinct pack {X, Z}.
    {
        let blobs = BlobStore::from_arc(backend.clone() as Arc<dyn Store>);
        let s = PackingStore::open(
            blobs,
            Arc::new(MemoryStore::new()),
            Arc::new(MemoryStore::new()),
            small_config(),
        )
        .await
        .unwrap();
        s.blob_upload_bytes(x.clone()).await.unwrap();
        s.blob_upload_bytes(z.clone()).await.unwrap();
        s.blob_sync().await.unwrap();
    }

    let pack_hashes = BlobStore::from_arc(backend.clone() as Arc<dyn Store>)
        .list_hashes()
        .await
        .unwrap();
    assert_eq!(
        pack_hashes.len(),
        2,
        "two distinct pack bodies, both containing key(X)"
    );

    // Cold reconstruct indexes key(X) from both packs, then FORCE the MPHF build —
    // the panic site absent the `build_bulk` dedup guard.
    let blobs = BlobStore::from_arc(backend.clone() as Arc<dyn Store>);
    let s = PackingStore::open(
        blobs,
        Arc::new(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        small_config(),
    )
    .await
    .unwrap();
    s.reconstruct_from_headers(pack_hashes).await.unwrap();
    assert_eq!(s.state.read().await.packs.len(), 2);
    s.rebuild_index().await.unwrap(); // must NOT panic on the duplicate key

    for (h, want) in [(xh, &x), (yh, &y), (zh, &z)] {
        assert_eq!(
            &s.blob_download(h).await.unwrap(),
            want,
            "blob {h} round-trips after a dup-key rebuild"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn staging_watermark_backpressures_upload_until_flush() {
    // A fast ingest over a slow backend must NOT fill the disk: once staging
    // reaches `staging_max_packs * max_group_size`, uploads block until a flush
    // drains a pack. Tiny watermark here (1 × 128 B = 128 B) so a few small blobs
    // trip it; no flush loop runs, so the over-watermark upload blocks until we
    // flush by hand.
    let cfg = PackingConfig {
        min_group_size: 1,
        max_group_size: 128,
        staging_max_packs: 1, // watermark = 128 bytes
        slab_size: 4 * 1024 * 1024,
        waste_pct: 0.99,
        interval: Duration::from_secs(3600),
        flush_on_put: false,
        max_pending_age: Duration::from_secs(3600),
        ..PackingConfig::default()
    };
    let store = PackingStore::open(
        BlobStore::without_outboard(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        Arc::new(MemoryStore::new()),
        cfg,
    )
    .await
    .unwrap();

    // Two 100-byte blobs → 200 B staged, over the 128 B watermark (each checked
    // the watermark BEFORE adding, and staging was under it at check time).
    store
        .blob_upload_bytes(Bytes::from(vec![b'a'; 100]))
        .await
        .unwrap();
    store
        .blob_upload_bytes(Bytes::from(vec![b'b'; 100]))
        .await
        .unwrap();
    assert!(
        store.staged_bytes.load(Ordering::Acquire) >= 128,
        "staging is over the watermark"
    );

    // A third upload must BLOCK — nothing is draining staging.
    let s2 = store.clone();
    let blocked =
        tokio::spawn(async move { s2.blob_upload_bytes(Bytes::from(vec![b'c'; 100])).await });
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(
        !blocked.is_finished(),
        "upload must block while staging exceeds the watermark"
    );

    // Flush drains staging → wakes the blocked upload.
    store.pack_once(true).await.unwrap();
    let id = tokio::time::timeout(Duration::from_secs(5), blocked)
        .await
        .expect("blocked upload must complete once a flush frees staging")
        .unwrap()
        .unwrap();
    assert_eq!(id.hash, Hash::new([b'c'; 100].as_slice()));

    // All three round-trip (a,b via their packs, c from staging).
    for b in [vec![b'a'; 100], vec![b'b'; 100], vec![b'c'; 100]] {
        let h = Hash::new(b.as_slice());
        assert_eq!(store.blob_download(h).await.unwrap().as_ref(), &b[..]);
    }
}

#[tokio::test]
async fn staging_stats_reflects_staged_then_drained() {
    // The trait-level gauge `vup status` / `vup doctor` read: staged bytes are
    // visible while a write sits in the WAL, and the gauge returns to drained
    // once a flush folds it into a durable pack.
    let store = make_store().await;

    // Fresh store: nothing staged, no flush yet, nothing in flight.
    let s0 = store
        .staging_stats()
        .expect("packing store reports staging");
    assert_eq!(s0.staged_bytes, 0, "nothing staged before any upload");
    assert!(!s0.inflight, "no upload in flight at open");

    // Stage a blob (no flush loop runs) → the gauge shows it as not-yet-durable.
    let payload = vec![b'z'; 4096];
    store
        .blob_upload_bytes(Bytes::from(payload.clone()))
        .await
        .unwrap();
    let s1 = store.staging_stats().expect("staging gauge");
    assert_eq!(
        s1.staged_bytes,
        payload.len() as u64,
        "the staged blob's bytes are visible before it is packed"
    );

    // Flush → staging drains, gauge returns to durable.
    store.blob_sync().await.unwrap();
    let s2 = store.staging_stats().expect("staging gauge");
    assert_eq!(
        s2.staged_bytes, 0,
        "a flushed blob no longer counts as staged"
    );
    assert!(!s2.inflight, "flush completed — nothing in flight");
}

#[tokio::test]
async fn dedup_skips_blob_already_in_a_flushed_pack() {
    // The `cp -r` scenario: a blob whose content is identical to one already
    // uploaded — but now living in a FLUSHED pack, not just staging — must NOT be
    // re-staged / re-packed / re-uploaded. (The existing dedup test only covers
    // the pre-flush case where both are still in `pending`.)
    let store = make_store().await;
    let bytes = Bytes::from_static(b"content that is already packed and uploaded");
    let id1 = store.blob_upload_bytes(bytes.clone()).await.unwrap();
    store.blob_sync().await.unwrap(); // flush into a pack; staging drains
    {
        let st = store.state.read().await;
        assert!(st.pending.is_empty(), "flushed out of pending");
        assert_eq!(st.packs.len(), 1, "one pack");
    }

    // Re-upload the SAME content — must dedup against the flushed pack.
    let id2 = store.blob_upload_bytes(bytes.clone()).await.unwrap();
    assert_eq!(id2.hash, id1.hash);
    assert!(
        store.state.read().await.pending.is_empty(),
        "a blob already in a flushed pack must NOT be re-staged"
    );

    // And a second sync must not mint a new pack for the duplicate.
    store.blob_sync().await.unwrap();
    assert_eq!(
        store.state.read().await.packs.len(),
        1,
        "no new pack should be created for a duplicate blob"
    );
}

#[tokio::test]
async fn dedup_survives_restart_via_manifest_reload() {
    // The `cp -r` AFTER a daemon restart: re-uploading an already-packed blob
    // must still dedup. On reopen the store's index must carry the prior packs'
    // blobs (reloaded from the persisted per-pack manifests), so `contains_honest`
    // finds them and skips the re-stage/re-upload.
    let blobs_inner = Arc::new(MemoryStore::new());
    let manifests_inner = Arc::new(MemoryStore::new());
    let staging_inner = Arc::new(MemoryStore::new());
    let bytes = Bytes::from_static(b"content packed in a previous daemon session");

    {
        let s = PackingStore::open(
            BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>),
            manifests_inner.clone(),
            staging_inner.clone(),
            small_config(),
        )
        .await
        .unwrap();
        s.blob_upload_bytes(bytes.clone()).await.unwrap();
        s.blob_sync().await.unwrap();
    }

    // Reopen over the SAME persistent stores (simulate a daemon restart).
    let s = PackingStore::open(
        BlobStore::from_arc(blobs_inner.clone() as Arc<dyn Store>),
        manifests_inner.clone(),
        staging_inner.clone(),
        small_config(),
    )
    .await
    .unwrap();
    assert_eq!(
        s.state.read().await.packs.len(),
        1,
        "manifest reload must restore the prior pack into the index"
    );

    // Re-upload identical content → must dedup against the reloaded pack.
    s.blob_upload_bytes(bytes.clone()).await.unwrap();
    assert!(
        s.state.read().await.pending.is_empty(),
        "dedup after restart: an already-packed blob must NOT be re-staged"
    );
    s.blob_sync().await.unwrap();
    assert_eq!(
        s.state.read().await.packs.len(),
        1,
        "no new pack should be minted for a duplicate after restart"
    );
}
