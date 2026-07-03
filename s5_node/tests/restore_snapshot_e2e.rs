//! Store-agnostic E2E for restore-by-`#snap` and subtree restore (Stage 2 of
//! the D20 CLI cutover).
//!
//! A single warm device backs up a source tree **twice** — the first snapshot
//! is captured, the source is then mutated (one file changed, one added), and a
//! second snapshot is published. Both snapshots now live in the vault's
//! published registry history. The test then drives `TaskSpec::Restore` with:
//!
//!   - a **`#snap` selector** (`"1"`, the oldest published snapshot) and asserts
//!     the *older* bytes come back and the later-added file is absent — i.e. the
//!     resolver followed the history chain to the earlier encrypted TN, not the
//!     current local root;
//!   - the current revision (`"2"`) to exercise the current-snapshot branch of
//!     the resolver end-to-end through the registry;
//!   - a **subtree** selector, asserting only that path is written, re-rooted
//!     under the target (the inverse of `backup SRC vault:sub`);
//!   - `#snap` + subtree together, and a missing-subtree failure.
//!
//! Runs against the Memory and Local `DurableBackend`s (no live network), the
//! same seam the recovery / share / durability E2Es ride on.

mod common;

use anyhow::{Context, Result};
use common::{
    DurableBackend, LocalBackend, MemoryBackend, age_identity, build_ctx, make_config, run_task,
};
use s5_node::config::TaskSpec;
use s5_node::tasks::TaskExecutor;

/// Build the vault's `Backup` spec (ingest + publish for the whole source).
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

/// A `Restore` spec with the given optional `#snap` / subtree selectors.
fn restore_spec(
    target: &std::path::Path,
    snapshot: Option<&str>,
    subtree: Option<&str>,
) -> TaskSpec {
    TaskSpec::Restore {
        vault: "backup".to_string(),
        target_path: target.to_string_lossy().into_owned(),
        blob_store: None,
        snapshot: snapshot.map(String::from),
        subtree: subtree.map(String::from),
    }
}

async fn restore_snapshot_roundtrip(backend: &dyn DurableBackend) -> Result<()> {
    let label = backend.label();
    let scratch = tempfile::tempdir()?;
    let (paper_recipient, paper_id) = age_identity(scratch.path(), "paper");
    let (device_recipient, device_id) = age_identity(scratch.path(), "device");

    // -- Author the source tree (snapshot 1) --
    let source = scratch.path().join("source");
    std::fs::create_dir_all(source.join("sub"))?;
    std::fs::write(source.join("top.txt"), b"top-v1")?;
    std::fs::write(source.join("sub/keep.txt"), b"keep")?;
    std::fs::write(source.join("sub/child.txt"), b"child-v1")?;

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
    let ctx = build_ctx(config, blobs, registry, [0x33u8; 32]);
    let executor = TaskExecutor::new(ctx);

    // -- Backup #1 → published snapshot revision 1 --
    run_task(&executor, backup_spec())
        .await
        .with_context(|| format!("[{label}] first backup"))?;

    // -- Mutate the source: change one file, change another, add a third --
    std::fs::write(source.join("top.txt"), b"top-v2")?;
    std::fs::write(source.join("sub/child.txt"), b"child-v2")?;
    std::fs::write(source.join("sub/added.txt"), b"added")?;

    // -- Backup #2 → published snapshot revision 2 (history now: #1 → #2) --
    run_task(&executor, backup_spec())
        .await
        .with_context(|| format!("[{label}] second backup"))?;

    // ================= Restore the FIRST snapshot by selector =================
    // `#1` resolves through the registry history to the earlier encrypted TN —
    // NOT the current local root — so we must see snapshot-1 bytes.
    let old = scratch.path().join("restore_old");
    run_task(&executor, restore_spec(&old, Some("1"), None))
        .await
        .with_context(|| format!("[{label}] restore #1"))?;
    assert_eq!(
        std::fs::read(old.join("top.txt"))?,
        b"top-v1",
        "[{label}] restore #1 must yield the ORIGINAL top.txt"
    );
    assert_eq!(
        std::fs::read(old.join("sub/child.txt"))?,
        b"child-v1",
        "[{label}] restore #1 must yield the ORIGINAL child.txt"
    );
    assert!(
        !old.join("sub/added.txt").exists(),
        "[{label}] snapshot #1 predates added.txt — it must NOT be restored"
    );

    // ================= Restore the CURRENT snapshot via registry ==============
    // `#2` is the newest revision; exercises the resolver's current-snapshot
    // branch and confirms the newest bytes come back.
    let cur = scratch.path().join("restore_cur");
    run_task(&executor, restore_spec(&cur, Some("2"), None))
        .await
        .with_context(|| format!("[{label}] restore #2"))?;
    assert_eq!(
        std::fs::read(cur.join("top.txt"))?,
        b"top-v2",
        "[{label}] restore #2 must yield the UPDATED top.txt"
    );
    assert_eq!(
        std::fs::read(cur.join("sub/added.txt"))?,
        b"added",
        "[{label}] restore #2 must include the later-added file"
    );

    // ================= Subtree restore (current) ==============================
    // `sub` is re-rooted so its contents land directly under the target.
    let subt = scratch.path().join("restore_sub");
    run_task(&executor, restore_spec(&subt, None, Some("sub")))
        .await
        .with_context(|| format!("[{label}] subtree restore"))?;
    assert_eq!(std::fs::read(subt.join("keep.txt"))?, b"keep", "[{label}]");
    assert_eq!(
        std::fs::read(subt.join("child.txt"))?,
        b"child-v2",
        "[{label}] subtree restore reflects the current snapshot"
    );
    assert_eq!(
        std::fs::read(subt.join("added.txt"))?,
        b"added",
        "[{label}]"
    );
    assert!(
        !subt.join("top.txt").exists(),
        "[{label}] subtree restore must exclude files outside sub/"
    );
    assert!(
        !subt.join("sub").exists(),
        "[{label}] subtree contents are re-rooted, not nested under sub/"
    );

    // ================= Subtree restore AT snapshot #1 =========================
    // Composition of both selectors: the OLD subtree — child-v1, no added.txt.
    let subt_old = scratch.path().join("restore_sub_old");
    run_task(&executor, restore_spec(&subt_old, Some("1"), Some("sub")))
        .await
        .with_context(|| format!("[{label}] subtree restore at #1"))?;
    assert_eq!(
        std::fs::read(subt_old.join("child.txt"))?,
        b"child-v1",
        "[{label}] #1 subtree restore yields the original child.txt"
    );
    assert!(
        !subt_old.join("added.txt").exists(),
        "[{label}] #1 predates added.txt inside the subtree too"
    );

    // ================= A missing subtree fails loudly =========================
    let bad = scratch.path().join("restore_bad");
    let res = run_task(&executor, restore_spec(&bad, None, Some("does/not/exist"))).await;
    assert!(
        res.is_err(),
        "[{label}] restoring a nonexistent subtree must fail, not silently write nothing"
    );

    Ok(())
}

#[tokio::test]
async fn restore_snapshot_roundtrip_memory() {
    restore_snapshot_roundtrip(&MemoryBackend::new())
        .await
        .unwrap();
}

#[tokio::test]
async fn restore_snapshot_roundtrip_local() {
    restore_snapshot_roundtrip(&LocalBackend::new())
        .await
        .unwrap();
}
