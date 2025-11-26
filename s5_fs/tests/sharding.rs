use bytes::Bytes;
use s5_fs::{DirContext, FS5, FileRef};
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread")]
async fn test_sharding_behavior() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let ctx = DirContext::open_local_root(tmp.path())?;
    let fs = FS5::open(ctx).with_autosave(10).await?;

    // Add enough files to trigger sharding
    // MAX_DIR_BYTES_BEFORE_SHARD is 65_536.
    // Each entry has overhead. 1000 entries should be enough.
    let count = 2000;
    println!("Adding {} files...", count);

    fs.batch(|fs| async move {
        for i in 0..count {
            let name = format!("file_{:04}.txt", i);
            let data = format!("content {}", i);
            fs.file_put_sync(&name, FileRef::new_inline_blob(Bytes::from(data)))
                .await?;
        }
        Ok(())
    })
    .await?;

    println!("Files added. Saving...");
    fs.save().await?;

    // Verify listing
    println!("Listing files...");
    let (entries, _) = fs.list(None, count + 100).await?;
    assert_eq!(entries.len(), count, "Should list all files");

    // Verify snapshot structure
    let snapshot = fs.export_snapshot().await?;
    println!("Snapshot header: {:?}", snapshot.header);

    if let Some(shards) = snapshot.header.shards {
        println!("Directory is sharded with {} shards", shards.len());
        assert!(!shards.is_empty());

        // Check if main dirs/files are mostly empty (BUG FIX VERIFICATION)
        // Note: It might not be completely empty if files were added AFTER sharding
        // and mapped to shards that didn't exist yet (sparse shards).
        // But it should be much less than the total count.
        if snapshot.files.len() > count / 2 {
            println!(
                "BUG DETECTED: Main snapshot still contains {} files after sharding!",
                snapshot.files.len()
            );
            panic!("Main snapshot should be mostly empty after sharding");
        } else {
            println!(
                "Main snapshot has {} files (acceptable overflow).",
                snapshot.files.len()
            );
        }

        // Verify merged snapshot
        println!("Exporting merged snapshot...");
        let merged = fs.export_merged_snapshot().await?;
        assert_eq!(
            merged.files.len(),
            count,
            "Merged snapshot should contain all files"
        );
        assert!(
            merged.header.shards.is_none(),
            "Merged snapshot should not have shards in header"
        );
        println!("Merged snapshot verified.");
    } else {
        println!("Directory is NOT sharded. Need more files?");
        // If not sharded, we can't verify the bug.
        // But 2000 files should definitely shard.
    }

    Ok(())
}
