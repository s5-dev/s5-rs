use s5_core::Hash;
use s5_fs::{
    DirContext, FS5,
    dir::{DirRef, DirV1, FileRef},
};
use tempfile::tempdir;

#[tokio::test]
async fn test_delete_creates_tombstone_and_hides_from_live_view() {
    let tmp = tempdir().unwrap();
    let ctx = DirContext::open_local_root(tmp.path()).unwrap();
    let fs = FS5::open(ctx);

    // Create a file and persist it
    let fr = FileRef::new_inline_blob(bytes::Bytes::from_static(b"v1"));
    fs.file_put_sync("a.txt", fr).await.unwrap();
    fs.save().await.unwrap();

    // Verify it is visible via the live API
    assert!(fs.file_exists("a.txt").await);

    // Delete and ensure live view no longer sees it
    fs.file_delete("a.txt").await.unwrap();
    fs.save().await.unwrap();
    assert!(!fs.file_exists("a.txt").await);

    // But the snapshot should still carry a tombstone with history
    let snap = fs.export_snapshot().await.unwrap();
    let head = snap.files.get("a.txt").expect("missing tombstone entry");
    assert!(head.is_tombstone());
    assert!(head.prev.is_some());
    assert!(head.first_version.is_some());
}

#[tokio::test]
async fn test_conflict_resolution_lww() {
    let tmp = tempdir().unwrap();
    let ctx = DirContext::open_local_root(tmp.path()).unwrap();
    let fs = FS5::open(ctx);

    // 1. Setup local state: File "foo" at T=100
    let mut local_file = FileRef::new(Hash::from_bytes([1u8; 32]), 100);
    local_file.timestamp = Some(100);
    local_file.timestamp_subsec_nanos = Some(0);

    fs.file_put_sync("foo", local_file).await.unwrap();
    fs.save().await.unwrap();

    // 2. Create remote snapshot: File "foo" at T=200 (Newer)
    let mut remote_snapshot = DirV1::new();
    let mut remote_file = FileRef::new(Hash::from_bytes([2u8; 32]), 200);
    remote_file.timestamp = Some(200);
    remote_file.timestamp_subsec_nanos = Some(0);
    remote_snapshot.files.insert("foo".to_string(), remote_file);

    // 3. Merge
    fs.merge_from_snapshot(remote_snapshot).await.unwrap();

    // 4. Verify "foo" is now the remote one (Hash 2)
    let merged_file = fs.file_get("foo").await.expect("file not found");
    assert_eq!(merged_file.hash, [2u8; 32]);
    assert_eq!(merged_file.timestamp, Some(200));
}

#[tokio::test]
async fn test_conflict_resolution_local_wins() {
    let tmp = tempdir().unwrap();
    let ctx = DirContext::open_local_root(tmp.path()).unwrap();
    let fs = FS5::open(ctx);

    // 1. Setup local state: File "bar" at T=200
    let mut local_file = FileRef::new(Hash::from_bytes([1u8; 32]), 100);
    local_file.timestamp = Some(200);

    fs.file_put_sync("bar", local_file).await.unwrap();
    fs.save().await.unwrap();

    // 2. Create remote snapshot: File "bar" at T=100 (Older)
    let mut remote_snapshot = DirV1::new();
    let mut remote_file = FileRef::new(Hash::from_bytes([2u8; 32]), 200);
    remote_file.timestamp = Some(100);
    remote_snapshot.files.insert("bar".to_string(), remote_file);

    // 3. Merge
    fs.merge_from_snapshot(remote_snapshot).await.unwrap();

    // 4. Verify "bar" is still the local one (Hash 1)
    let merged_file = fs.file_get("bar").await.expect("file not found");
    assert_eq!(merged_file.hash, [1u8; 32]);
    assert_eq!(merged_file.timestamp, Some(200));
}

#[tokio::test]
async fn test_conflict_resolution_type_mismatch() {
    let tmp = tempdir().unwrap();
    let ctx = DirContext::open_local_root(tmp.path()).unwrap();
    let fs = FS5::open(ctx);

    // 1. Setup local state: File "baz" at T=100
    let mut local_file = FileRef::new(Hash::from_bytes([1u8; 32]), 100);
    local_file.timestamp = Some(100);
    fs.file_put_sync("baz", local_file).await.unwrap();
    fs.save().await.unwrap();

    // 2. Create remote snapshot: Directory "baz" at T=200 (Newer)
    let mut remote_snapshot = DirV1::new();
    let mut remote_dir = DirRef::from_hash(Hash::from_bytes([3u8; 32]));
    remote_dir.ts_seconds = Some(200);
    remote_snapshot.dirs.insert("baz".to_string(), remote_dir);

    // 3. Merge
    fs.merge_from_snapshot(remote_snapshot).await.unwrap();

    // 4. Verify "baz" is now a directory
    let res = fs.file_get("baz").await;
    assert!(res.is_none()); // Should not be a file

    // Verify it is in dirs
    let (entries, _) = fs.list(None, 100).await.unwrap();
    let entry = entries
        .iter()
        .find(|(name, _)| name == "baz")
        .expect("baz not found");
    assert!(matches!(entry.1, s5_fs::CursorKind::Directory));
}
