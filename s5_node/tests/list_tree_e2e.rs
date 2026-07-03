//! Store-agnostic E2E for the `list vault:[path]` tree view (Stage 3 of the
//! D20 CLI cutover).
//!
//! A single warm device backs up a small nested source tree, then drives the
//! `ListTree` core (`s5_node::tasks::list::list_tree` — the exact function the
//! daemon's `ListTree` RPC handler calls) with:
//!
//!   - no subtree → the whole vault listing, asserting every authored path is
//!     present with the right dir/file classification;
//!   - a **subtree** (`sub`) → only that subtree, re-rooted so the prefix is
//!     stripped and files outside it are absent (the inverse of subtree
//!     restore);
//!   - a **depth bound** → only the top level;
//!   - a **missing subtree** → a loud error, not an empty listing.
//!
//! Runs against the Memory and Local `DurableBackend`s (no live network), the
//! same seam the restore / share / recovery E2Es ride on.

mod common;

use anyhow::{Context, Result};
use common::{
    DurableBackend, LocalBackend, MemoryBackend, age_identity, build_ctx, make_config, run_task,
};
use s5_node::config::TaskSpec;
use s5_node::tasks::TaskExecutor;
use s5_node::tasks::list::list_tree;

fn backup_spec() -> TaskSpec {
    TaskSpec::Backup {
        vault: "backup".to_string(),
        source: "docs".to_string(),
        blob_store: "durable".to_string(),
        keys: vec!["device".to_string(), "paper".to_string()],
        target_path: None,
        changed_paths: None,
    }
}

/// Collect a listing into a sorted `(path, is_dir)` vec for order-independent
/// assertions.
fn paths(entries: &[s5_node_api::TreeEntry]) -> Vec<(String, bool)> {
    let mut v: Vec<(String, bool)> = entries.iter().map(|e| (e.path.clone(), e.is_dir)).collect();
    v.sort();
    v
}

async fn list_tree_roundtrip(backend: &dyn DurableBackend) -> Result<()> {
    let label = backend.label();
    let scratch = tempfile::tempdir()?;
    let (paper_recipient, paper_id) = age_identity(scratch.path(), "paper");
    let (device_recipient, device_id) = age_identity(scratch.path(), "device");

    // -- Author the source tree --
    //   top.txt
    //   sub/keep.txt
    //   sub/child.txt
    //   sub/deep/leaf.txt
    let source = scratch.path().join("source");
    std::fs::create_dir_all(source.join("sub/deep"))?;
    std::fs::write(source.join("top.txt"), b"top")?;
    std::fs::write(source.join("sub/keep.txt"), b"keep-content")?;
    std::fs::write(source.join("sub/child.txt"), b"child")?;
    std::fs::write(source.join("sub/deep/leaf.txt"), b"leaf")?;

    let vault_root = scratch.path().join("vault");
    std::fs::create_dir_all(&vault_root)?;

    let (blobs, registry) = backend.open();
    let config = make_config(
        &vault_root.to_string_lossy(),
        &paper_recipient,
        &paper_id,
        &device_recipient,
        &device_id,
        &source.to_string_lossy(),
    );
    let ctx = build_ctx(config, blobs, registry, [0x51u8; 32]);
    let executor = TaskExecutor::new(ctx.clone());

    run_task(&executor, backup_spec())
        .await
        .with_context(|| format!("[{label}] backup"))?;

    // ================= Whole-vault listing =================
    let all = list_tree(&ctx, "backup", None, None, None)
        .await
        .with_context(|| format!("[{label}] list whole vault"))?;
    assert_eq!(
        paths(&all),
        vec![
            ("sub".to_string(), true),
            ("sub/child.txt".to_string(), false),
            ("sub/deep".to_string(), true),
            ("sub/deep/leaf.txt".to_string(), false),
            ("sub/keep.txt".to_string(), false),
            ("top.txt".to_string(), false),
        ],
        "[{label}] whole-vault listing must include every path with correct dir/file flags"
    );
    // Files carry their plaintext size; dirs report 0.
    let keep = all.iter().find(|e| e.path == "sub/keep.txt").unwrap();
    assert_eq!(
        keep.size,
        b"keep-content".len() as u64,
        "[{label}] file size"
    );
    let sub = all.iter().find(|e| e.path == "sub").unwrap();
    assert_eq!(sub.size, 0, "[{label}] directory size is 0");

    // ================= Subtree listing =================
    // `sub` is re-rooted: its own entry is dropped and children lose the prefix.
    let subt = list_tree(&ctx, "backup", None, Some("sub"), None)
        .await
        .with_context(|| format!("[{label}] list subtree"))?;
    assert_eq!(
        paths(&subt),
        vec![
            ("child.txt".to_string(), false),
            ("deep".to_string(), true),
            ("deep/leaf.txt".to_string(), false),
            ("keep.txt".to_string(), false),
        ],
        "[{label}] subtree listing must be re-rooted under `sub` and exclude `top.txt`"
    );
    assert!(
        !subt.iter().any(|e| e.path == "top.txt"),
        "[{label}] subtree listing must not leak files outside the subtree"
    );

    // A trailing slash on the subtree selector is tolerated (same result).
    let subt_slash = list_tree(&ctx, "backup", None, Some("sub/"), None)
        .await
        .with_context(|| format!("[{label}] list subtree (trailing slash)"))?;
    assert_eq!(
        paths(&subt_slash),
        paths(&subt),
        "[{label}] a trailing slash on the subtree must not change the result"
    );

    // ================= Depth bound =================
    // Depth 1 = the immediate children of the listing root only.
    let shallow = list_tree(&ctx, "backup", None, None, Some(1))
        .await
        .with_context(|| format!("[{label}] list depth 1"))?;
    assert_eq!(
        paths(&shallow),
        vec![("sub".to_string(), true), ("top.txt".to_string(), false)],
        "[{label}] depth-1 listing must include only top-level entries"
    );

    // ================= Missing subtree fails loudly =================
    let missing = list_tree(&ctx, "backup", None, Some("does/not/exist"), None).await;
    assert!(
        missing.is_err(),
        "[{label}] listing a nonexistent subtree must error, not return an empty listing"
    );

    // ================= Unknown vault fails =================
    let no_vault = list_tree(&ctx, "nope", None, None, None).await;
    assert!(
        no_vault.is_err(),
        "[{label}] listing an unknown vault must error"
    );

    Ok(())
}

#[tokio::test]
async fn list_tree_roundtrip_memory() {
    list_tree_roundtrip(&MemoryBackend::new()).await.unwrap();
}

#[tokio::test]
async fn list_tree_roundtrip_local() {
    list_tree_roundtrip(&LocalBackend::new()).await.unwrap();
}
