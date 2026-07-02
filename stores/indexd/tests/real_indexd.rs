//! Integration tests against a **real** indexd + Sia network.
//!
//! The crate has no mock backend, so these are where the store's behaviour is
//! actually exercised: a put/read/`provide` roundtrip, recovery-metadata
//! seal/unseal, real `object_events` pagination, indexer **tombstones** (which
//! the lazy-delete reconciliation relies on), and cross-AppKey re-pin.
//!
//! They are `#[ignore]`d for two reasons: they need network + credentials, and
//! they **upload real sectors to mainnet Sia, which costs storage**. They also
//! self-skip (return early) when the connection env vars are unset, so an
//! accidental `--ignored` run without creds is harmless.
//!
//! ## Running
//!
//! Provision an AppKey once via the interactive enrollment (`auth::register`,
//! e.g. `examples/end_to_end.rs`) and export its 32 bytes as hex. Then:
//!
//! ```sh
//! export S5_INDEXD_TEST_URL=https://sia.storage     # optional; this is the default
//! export S5_INDEXD_TEST_APP_KEY=<64 hex chars>      # a registered AppKey export
//! export S5_INDEXD_TEST_APP_KEY_2=<64 hex chars>    # 2nd AppKey (distinct account) — migration test
//! cargo test -p s5_store_indexd --test real_indexd -- --ignored --nocapture
//! ```
//!
//! The migration test needs two *distinct* AppKeys; the simplest way to get one
//! indexer to behave as "two indexers with different AppKeys" is two managed
//! accounts — `auth::register` fed two different storage secrets (distinct
//! `stores_seed ‖ label` derivations) — which yield independent mnemonics →
//! independent AppKeys on the same network.

use bytes::Bytes;
use s5_core::Hash;
use s5_core::store::Store;
use s5_store_indexd::{IndexdConfig, IndexdStore};
use s5_store_memory::MemoryStore;

const URL_ENV: &str = "S5_INDEXD_TEST_URL";
const KEY_ENV: &str = "S5_INDEXD_TEST_APP_KEY";
const KEY2_ENV: &str = "S5_INDEXD_TEST_APP_KEY_2";
const DEFAULT_URL: &str = "https://sia.storage";

fn indexer_url() -> String {
    std::env::var(URL_ENV).unwrap_or_else(|_| DEFAULT_URL.to_string())
}

/// A 32-byte AppKey from `env` (hex), or `None` if unset/malformed.
fn app_key(env: &str) -> Option<[u8; 32]> {
    let hex_str = std::env::var(env).ok()?;
    hex::decode(hex_str.trim()).ok()?.try_into().ok()
}

/// Open a store against the live indexer with a fresh in-memory cache, so each
/// store starts cold and any local state it has came purely from the indexer.
async fn open_store(key: [u8; 32]) -> IndexdStore<MemoryStore> {
    let config = IndexdConfig {
        indexer_url: indexer_url(),
        // Tests drive reconstruct/sync explicitly; don't auto-enumerate on open.
        sync_on_open: false,
        ..Default::default()
    };
    IndexdStore::open(config, key, MemoryStore::new(), None)
        .await
        .expect("open IndexdStore against live indexer (is the AppKey registered?)")
}

fn hash(bytes: &[u8]) -> Hash {
    Hash::from_bytes(*blake3::hash(bytes).as_bytes())
}

/// Content-addressed key scheme shared by these tests, reproduced on
/// reconstruction (the store is path-scheme agnostic; the test owns the scheme).
fn path_for_hash(h: &Hash) -> String {
    format!("s5-it/{}", hex::encode(h.as_bytes()))
}

/// A blob unique to this run, so reruns don't collide and `tag` keeps two blobs
/// in the same test distinct even under a coarse clock.
fn unique_blob(tag: &str) -> Vec<u8> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("s5 indexd integration test :: {tag} :: {nanos}").into_bytes()
}

/// Put a blob, then rebuild a *fresh* store purely by enumerating the indexer —
/// validates the real recovery-metadata round-trip and `object_events`.
#[tokio::test]
#[ignore = "hits live indexd + Sia (costs storage); set S5_INDEXD_TEST_APP_KEY"]
async fn reconstruct_roundtrip() {
    let Some(key) = app_key(KEY_ENV) else {
        eprintln!("SKIP reconstruct_roundtrip: set {KEY_ENV}");
        return;
    };

    let writer = open_store(key).await;
    let body = unique_blob("reconstruct");
    let path = path_for_hash(&hash(&body));
    writer
        .put_bytes(&path, Bytes::from(body.clone()))
        .await
        .expect("put");

    // Fresh store, same AppKey, empty cache — must serve the blob using only
    // state rebuilt from the indexer.
    let recovered = open_store(key).await;
    assert!(!recovered.exists(&path).await.unwrap());
    let stats = recovered
        .reconstruct_from_indexer()
        .await
        .expect("reconstruct");
    assert!(
        stats.restored >= 1,
        "expected to recover at least the blob we just put, got {stats:?}"
    );
    assert!(recovered.exists(&path).await.unwrap());
    let got = recovered.open_read_bytes(&path, 0, None).await.unwrap();
    assert_eq!(&got[..], &body[..], "recovered bytes must match");

    writer.delete(&path).await.ok(); // best-effort cleanup
}

/// Lazy deletes against a real indexer: `sync_from_indexer` does **not** evict a
/// remotely-deleted object (we keep no reverse index to map the delete event's
/// object id back to a path), but a from-empty `reconstruct_from_indexer`
/// omits it — the indexer returns a deleted object only as a tombstone, with no
/// live object to restore. This is the tombstone behaviour the mock can't fake.
#[tokio::test]
#[ignore = "hits live indexd + Sia (costs storage); set S5_INDEXD_TEST_APP_KEY"]
async fn lazy_delete_reconciled_by_cold_reconstruct() {
    let Some(key) = app_key(KEY_ENV) else {
        eprintln!("SKIP lazy_delete_reconciled_by_cold_reconstruct: set {KEY_ENV}");
        return;
    };

    let writer = open_store(key).await;
    let keep = unique_blob("keep");
    let doomed = unique_blob("doomed");
    let keep_path = path_for_hash(&hash(&keep));
    let doomed_path = path_for_hash(&hash(&doomed));
    writer.put_bytes(&keep_path, keep.into()).await.unwrap();
    writer.put_bytes(&doomed_path, doomed.into()).await.unwrap();

    // A warm follower learns both, then a remote delete + incremental sync.
    let follower = open_store(key).await;
    follower.reconstruct_from_indexer().await.unwrap();
    assert!(follower.exists(&doomed_path).await.unwrap());

    writer.delete(&doomed_path).await.unwrap();
    follower.sync_from_indexer().await.expect("sync");
    // Lazy: the warm follower still lists the doomed path — the delete event is
    // skipped (no reverse index), so the stale entry lingers until rebuilt.
    assert!(
        follower.exists(&doomed_path).await.unwrap(),
        "lazy sync should leave the remotely-deleted entry in a warm cache"
    );

    // A cold store reconstructing from scratch never sees it: the indexer
    // returns the deleted object only as a tombstone.
    let cold = open_store(key).await;
    cold.reconstruct_from_indexer().await.expect("reconstruct");
    assert!(
        !cold.exists(&doomed_path).await.unwrap(),
        "a from-empty reconstruct must omit the remotely-deleted object"
    );
    assert!(
        cold.exists(&keep_path).await.unwrap(),
        "the surviving blob must still reconstruct"
    );

    writer.delete(&keep_path).await.ok(); // cleanup
}

/// Migrate by re-pin between two **different AppKeys** (distinct accounts) on
/// the same network — no blob bytes re-uploaded, object id preserved.
#[tokio::test]
#[ignore = "hits live indexd + Sia (costs storage); set S5_INDEXD_TEST_APP_KEY and _2"]
async fn migrate_cross_appkey() {
    let (Some(src_key), Some(dst_key)) = (app_key(KEY_ENV), app_key(KEY2_ENV)) else {
        eprintln!("SKIP migrate_cross_appkey: set {KEY_ENV} and {KEY2_ENV} (two distinct AppKeys)");
        return;
    };
    assert_ne!(
        src_key, dst_key,
        "the two AppKeys must differ to exercise cross-AppKey re-pin"
    );

    let src = open_store(src_key).await;
    let dst = open_store(dst_key).await;

    let body = unique_blob("migrate");
    let path = path_for_hash(&hash(&body));
    src.put_bytes(&path, Bytes::from(body.clone()))
        .await
        .unwrap();

    let stats = src.migrate_to(&dst).await.expect("migrate");
    assert!(
        stats.migrated >= 1,
        "expected to migrate the blob, got {stats:?}"
    );

    // dst serves it, re-pinned under its own AppKey (no re-upload), same id.
    let got = dst.open_read_bytes(&path, 0, None).await.unwrap();
    assert_eq!(&got[..], &body[..]);
    assert_eq!(
        src.object_id(&path).await.unwrap(),
        dst.object_id(&path).await.unwrap(),
        "object id (blake2b(slabs)) is preserved across the re-pin"
    );

    src.delete(&path).await.ok(); // cleanup both accounts
    dst.delete(&path).await.ok();
}
