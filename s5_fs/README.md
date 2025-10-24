# S5 File-system (FS5)

High-level, content-addressed, optionally encrypted directory tree. Everything is an immutable DirV1 snapshot; mutability is simulated through actors that rewrite parent snapshots atomically.

## Quick Start

- Open a local root, put/get a file, create an encrypted subdir, and save changes.

```rust
use s5_fs::{DirContext, FS5, FileRef};
use bytes::Bytes;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let ctx = DirContext::open_local_root(tmp.path())?;

    // Default: open without autosave; call save() when ready
    let fs = FS5::open(ctx);

    // Put and get a file
    let blob = Bytes::from("hello fs5");
    let file_ref = FileRef::new_inline_blob(blob.clone());
    fs.file_put("greeting.txt", file_ref).await; // fire-and-forget
    let got = fs.file_get("greeting.txt").await.unwrap();

    // Create encrypted sub-directory
    fs.create_dir("secret", true).await?;
    fs.file_put_sync("secret/plan.txt", FileRef::new_inline_blob(Bytes::from("top secret"))).await?;

    // Batch multiple ops, then persist once
    fs.batch(|fs| async move {
        fs.file_put_sync("a.txt", FileRef::new_inline_blob(Bytes::from("A"))).await?;
        fs.file_put_sync("b.txt", FileRef::new_inline_blob(Bytes::from("B"))).await?;
        fs.file_move("b.txt", "secret/b.txt").await?;
        fs.delete("a.txt").await?;
        Ok(())
    }).await?;

    // Persist metadata snapshots
    fs.save().await?;
    Ok(())
}
```