use bytes::Bytes;
use s5_core::{Hash, PinContext};
use s5_fs::{DirContext, FS5, FileRef};
use s5_store_local::{LocalStore, LocalStoreConfig};
use tempfile::tempdir;

#[tokio::test]
async fn snapshot_hash_persists_in_meta_store() {
    let _ = env_logger::builder().is_test(true).try_init();

    let temp_dir = tempdir().expect("tmp");
    let base = temp_dir.path().to_path_buf();

    let ctx = DirContext::open_local_root(&base).expect("ctx");
    let fs = FS5::open(ctx);

    fs.file_put_sync(
        "foo.txt",
        FileRef::new_inline_blob(Bytes::from_static(b"foo")),
    )
    .await
    .unwrap();

    let h: Hash = fs.snapshot_hash().await.expect("snapshot_hash");

    // The meta blob store for this root is a LocalStore rooted at `base`.
    let meta = LocalStore::create(LocalStoreConfig {
        base_path: base.to_string_lossy().into(),
    })
    .to_blob_store();

    assert!(meta.contains(h).await.expect("contains"));
}

#[tokio::test]
async fn local_head_pinning_tracks_latest_root() {
    let _ = env_logger::builder().is_test(true).try_init();

    let temp_dir = tempdir().expect("tmp");
    let base = temp_dir.path().to_path_buf();

    let ctx = DirContext::open_local_root(&base).expect("ctx");
    // Clone the pins handle before moving ctx into FS5, so we can verify pins later
    // without trying to re-open the Redb registry (which would fail with "Database already open").
    let pins = ctx.pins.clone().expect("pins");
    let fs = FS5::open(ctx);

    // First version
    fs.file_put_sync("a.txt", FileRef::new_inline_blob(Bytes::from_static(b"a")))
        .await
        .unwrap();
    fs.save().await.unwrap();

    let root_bytes_v1 = std::fs::read(base.join("root.fs5.cbor")).expect("root v1 bytes");
    let root_hash_v1 = Hash::new(&root_bytes_v1);

    // Second version
    fs.file_put_sync("b.txt", FileRef::new_inline_blob(Bytes::from_static(b"b")))
        .await
        .unwrap();
    fs.save().await.unwrap();

    let root_bytes_v2 = std::fs::read(base.join("root.fs5.cbor")).expect("root v2 bytes");
    let root_hash_v2 = Hash::new(&root_bytes_v2);
    assert_ne!(root_hash_v1, root_hash_v2);

    // Inspect pins via the existing handle.
    let pinners_v1 = pins
        .get_pinners(root_hash_v1)
        .await
        .expect("get_pinners v1");
    let pinners_v2 = pins
        .get_pinners(root_hash_v2)
        .await
        .expect("get_pinners v2");

    // Old root should no longer be pinned as LocalFsHead.
    assert!(
        !pinners_v1.contains(&PinContext::LocalFsHead),
        "previous head should have been unpinned"
    );

    // New root must be pinned as LocalFsHead.
    assert!(
        pinners_v2.contains(&PinContext::LocalFsHead),
        "latest head should be pinned as LocalFsHead"
    );
}

#[tokio::test]
async fn create_snapshot_updates_index_and_pins_snapshot() {
    let _ = env_logger::builder().is_test(true).try_init();

    let temp_dir = tempdir().expect("tmp");
    let base = temp_dir.path().to_path_buf();

    let ctx = DirContext::open_local_root(&base).expect("ctx");
    let pins = ctx.pins.clone().expect("pins");
    let fs = FS5::open(ctx);

    fs.file_put_sync(
        "snap.txt",
        FileRef::new_inline_blob(Bytes::from_static(b"s")),
    )
    .await
    .unwrap();

    let (name, root_hash) = fs.create_snapshot().await.expect("create_snapshot");

    // snapshots.fs5.cbor should now contain a DirRef under the returned name
    // pointing at root_hash.
    let snapshots_bytes =
        std::fs::read(base.join("snapshots.fs5.cbor")).expect("snapshots.fs5.cbor bytes");
    let dir = s5_fs::dir::DirV1::from_bytes(&snapshots_bytes).expect("decode snapshots");

    let entry = dir
        .dirs
        .get(&name)
        .expect("snapshot entry should exist in index");
    assert_eq!(entry.hash, *root_hash.as_bytes());

    // Snapshot root must be pinned under LocalFsSnapshot.
    let pinners = pins
        .get_pinners(root_hash)
        .await
        .expect("get_pinners snapshot");

    assert!(
        pinners.contains(&PinContext::LocalFsSnapshot {
            root_hash: *root_hash.as_bytes(),
        }),
        "snapshot root should be pinned as LocalFsSnapshot"
    );
}

#[tokio::test]
async fn delete_snapshot_removes_index_and_unpins() {
    let _ = env_logger::builder().is_test(true).try_init();

    let temp_dir = tempdir().expect("tmp");
    let base = temp_dir.path().to_path_buf();

    let ctx = DirContext::open_local_root(&base).expect("ctx");
    let pins = ctx.pins.clone().expect("pins");
    let fs = FS5::open(ctx);

    fs.file_put_sync(
        "del.txt",
        FileRef::new_inline_blob(Bytes::from_static(b"d")),
    )
    .await
    .unwrap();

    let (name, root_hash) = fs.create_snapshot().await.expect("create_snapshot");

    // Sanity check: entry exists before deletion.
    let snapshots_bytes =
        std::fs::read(base.join("snapshots.fs5.cbor")).expect("snapshots.fs5.cbor bytes");
    let dir = s5_fs::dir::DirV1::from_bytes(&snapshots_bytes).expect("decode snapshots");
    assert!(dir.dirs.contains_key(&name));

    // Delete the snapshot and ensure it is removed from the index.
    fs.delete_snapshot(&name)
        .await
        .expect("delete_snapshot should succeed");

    let snapshots_bytes =
        std::fs::read(base.join("snapshots.fs5.cbor")).expect("snapshots.fs5.cbor bytes");
    let dir = s5_fs::dir::DirV1::from_bytes(&snapshots_bytes).expect("decode snapshots");
    assert!(
        !dir.dirs.contains_key(&name),
        "snapshot entry should be removed after delete_snapshot",
    );

    // The LocalFsSnapshot pin for this hash should be removed.
    let pinners = pins
        .get_pinners(root_hash)
        .await
        .expect("get_pinners snapshot");

    assert!(
        !pinners.contains(&PinContext::LocalFsSnapshot {
            root_hash: *root_hash.as_bytes(),
        }),
        "snapshot LocalFsSnapshot pin should be removed after delete_snapshot",
    );
}
