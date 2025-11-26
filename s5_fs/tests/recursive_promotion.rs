use s5_core::BlobStore;
use s5_fs::{DirContext, FS5, FileRef};
use s5_store_memory::MemoryStore;
use tempfile::tempdir;

#[tokio::test]
async fn test_recursive_auto_promotion() {
    let fs_dir = tempdir().unwrap();
    let ctx = DirContext::open_local_root(fs_dir.path()).unwrap();
    let fs = FS5::open(ctx).with_autosave(50).await.unwrap();
    let _blob_store = BlobStore::new(MemoryStore::new());

    // Create 20 files in "home/red/..."
    // This should trigger promotion of "home" (because > FS5_PROMOTION_THRESHOLD files start with "home/")
    // And recursively promotion of "red" inside "home" (because > FS5_PROMOTION_THRESHOLD files start with "red/")
    for i in 0..20 {
        let path = format!("home/red/file_{}.txt", i);
        let file_ref = FileRef {
            hash: [0u8; 32],
            size: 100,
            timestamp: None,
            timestamp_subsec_nanos: None,
            prev: None,
            first_version: None,
            version_count: Some(0),
            locations: None,
            extra: None,
            media_type: None,
            ref_type: None,
            warc: None,
        };
        fs.file_put(&path, file_ref).await.unwrap();
    }

    // Force save to ensure everything is flushed
    fs.save().await.unwrap();

    // Verify structure
    // "home" should be a directory
    // "home/red" should be a directory
    // "home/red/file_0.txt" should exist

    // We can verify this by listing "home" and seeing if it contains "red" as a directory
    let (entries, _) = fs.list_at("home", None, 100).await.unwrap();

    // Should contain "red" as a directory
    let red_entry = entries
        .iter()
        .find(|(name, kind)| name == "red" && matches!(kind, s5_fs::CursorKind::Directory));
    assert!(red_entry.is_some(), "home should contain red directory");

    // List "home/red"
    let (entries_red, _) = fs.list_at("home/red", None, 100).await.unwrap();
    assert_eq!(entries_red.len(), 20);

    // Verify file existence
    assert!(fs.file_exists("home/red/file_0.txt").await);
}
