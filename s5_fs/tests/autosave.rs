use bytes::Bytes;
use s5_fs::{DirContext, FS5, FileRef};
use std::time::Duration;
use tempfile::tempdir;

#[tokio::test]
async fn test_autosave_debouncing() {
    let _ = env_logger::builder().is_test(true).try_init();

    let fs_dir = tempdir().unwrap();
    let ctx = DirContext::open_local_root(fs_dir.path()).unwrap();
    // 200ms debounce
    let fs = FS5::open(ctx).with_autosave(200).await.unwrap();

    // 1. Write a file
    fs.file_put("a.txt", FileRef::new_inline_blob(Bytes::from_static(b"a")))
        .await
        .unwrap();

    // 2. Wait a bit (less than debounce)
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 3. Write another file
    fs.file_put("b.txt", FileRef::new_inline_blob(Bytes::from_static(b"b")))
        .await
        .unwrap();

    // At this point, nothing should be on disk yet (conceptually),
    // or at least the second write shouldn't have triggered a save immediately.
    // But we can't easily check internal state.
    // We can check that after the debounce period, it IS saved.

    // 4. Wait for debounce to fire
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Shutdown to release lock
    fs.shutdown().await.unwrap();
    // Give the actor a moment to fully drop and release the lock
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 5. Verify persistence by reloading
    let ctx2 = DirContext::open_local_root(fs_dir.path()).unwrap();
    let fs2 = FS5::open(ctx2);

    assert!(fs2.file_exists("a.txt").await);
    assert!(fs2.file_exists("b.txt").await);
}

#[tokio::test]
async fn test_autosave_shutdown_safety() {
    let _ = env_logger::builder().is_test(true).try_init();

    let fs_dir = tempdir().unwrap();
    let ctx = DirContext::open_local_root(fs_dir.path()).unwrap();
    // Long debounce, so it won't fire naturally during the test
    let fs = FS5::open(ctx).with_autosave(5000).await.unwrap();

    fs.file_put(
        "shutdown.txt",
        FileRef::new_inline_blob(Bytes::from_static(b"s")),
    )
    .await
    .unwrap();

    // Immediate shutdown
    fs.shutdown().await.unwrap();
    // Give the actor a moment to fully drop and release the lock
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify persistence
    let ctx2 = DirContext::open_local_root(fs_dir.path()).unwrap();
    let fs2 = FS5::open(ctx2);

    assert!(
        fs2.file_exists("shutdown.txt").await,
        "Data should be saved on shutdown even if debounce timer hasn't fired"
    );
}

#[tokio::test]
async fn test_autosave_drop_safety() {
    let _ = env_logger::builder().is_test(true).try_init();

    let fs_dir = tempdir().unwrap();
    let ctx = DirContext::open_local_root(fs_dir.path()).unwrap();
    // Long debounce
    let fs = FS5::open(ctx).with_autosave(5000).await.unwrap();

    fs.file_put(
        "drop.txt",
        FileRef::new_inline_blob(Bytes::from_static(b"d")),
    )
    .await
    .unwrap();

    // Drop the FS instance without calling shutdown
    drop(fs);

    // Give the actor a moment to detect the channel close and save
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify persistence
    let ctx2 = DirContext::open_local_root(fs_dir.path()).unwrap();
    let fs2 = FS5::open(ctx2);

    assert!(
        fs2.file_exists("drop.txt").await,
        "Data should be saved on drop even if debounce timer hasn't fired"
    );
}

#[tokio::test]
async fn test_root_file_contains_dirv1_after_shutdown() {
    let _ = env_logger::builder().is_test(true).try_init();

    let fs_dir = tempdir().unwrap();
    let ctx = DirContext::open_local_root(fs_dir.path()).unwrap();
    let fs = FS5::open(ctx).with_autosave(5000).await.unwrap();

    fs.file_put(
        "inspect.txt",
        FileRef::new_inline_blob(Bytes::from_static(b"i")),
    )
    .await
    .unwrap();

    fs.shutdown().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let root_path = fs_dir.path().join("root.fs5.cbor");
    let bytes = std::fs::read(&root_path).expect("failed to read root.fs5.cbor");
    let dir = s5_fs::dir::DirV1::from_bytes(&bytes).expect("root.fs5.cbor should decode");
    eprintln!("root.fs5.cbor dir: {:?}", dir);
    assert!(dir.files.contains_key("inspect.txt"));
}
