use bytes::Bytes;
use s5_fs::{DirContext, FS5, FileRef};
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread")]
async fn subdir_scopes_paths_and_auto_creates() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let ctx = DirContext::open_local_root(tmp.path())?;
    let fs = FS5::open(ctx).with_autosave(0).await?;

    // Create a nested scoped handle and write via it.
    let scoped = fs.subdir("foo/bar").await?;
    scoped
        .file_put_sync(
            "baz.txt",
            FileRef::new_inline_blob(Bytes::from_static(b"hello")),
        )
        .await?;
    fs.save().await?;

    // Root should see the file at the full path.
    assert!(fs.file_exists("foo/bar/baz.txt").await);

    let (entries, _cursor) = fs.list_at("foo/bar", None, 10).await?;
    assert!(entries.iter().any(|(name, _)| name == "baz.txt"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn subdir_root_equivalence_for_empty_or_slash() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let ctx = DirContext::open_local_root(tmp.path())?;
    let fs = FS5::open(ctx).with_autosave(0).await?;

    let scoped_empty = fs.subdir("").await?;
    let scoped_slash = fs.subdir("/").await?;

    scoped_empty
        .file_put_sync("a.txt", FileRef::new_inline_blob(Bytes::from_static(b"a")))
        .await?;
    scoped_slash
        .file_put_sync("b.txt", FileRef::new_inline_blob(Bytes::from_static(b"b")))
        .await?;

    fs.save().await?;

    assert!(fs.file_exists("a.txt").await);
    assert!(fs.file_exists("b.txt").await);

    Ok(())
}
