//! Store-agnostic E2E for the D21 `copy` primitive (TaskSpec::Copy).
//!
//! Producer backs a corpus into a source vault, then copies it into a
//! DIFFERENT destination vault with independently-generated keys. A shallow
//! copy must:
//!   * round-trip byte-for-byte when the destination reader restores it,
//!   * reuse the SAME data-blob ciphertext (content-addressed hashes match) —
//!     while a deep copy re-encrypts to DIFFERENT hashes,
//!   * be undecodable to a source-only reader (cross-reader isolation),
//!   * move ZERO bytes when the stores are shared (the `blob_contains` gate),
//!   * refuse to widen the reader set without confirmation (honesty gate),
//!   * refuse to shallow-copy into a plaintext destination (plaintext-dest
//!     guard).
//!
//! Runs over the shared [`common::DurableBackend`] seam, so shallow+deep each
//! run against Memory and Local without a live network.

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, Result};
use async_trait::async_trait;
use common::{
    Corpus, DurableBackend, LocalBackend, MemoryBackend, age_identity, build_ctx, make_config,
    run_task,
};
use futures_util::StreamExt;
use s5_core::blob::{BlobStore, Blobs};
use s5_core::{BlobsRead, Hash};
use s5_fs_v2::copy::{BlobReplicator, shallow_copy_into};
use s5_fs_v2::node::NodeEntry;
use s5_fs_v2::snapshot::Snapshot;
use s5_node::config::{NodeConfigKey, NodeConfigVault, S5NodeConfig, TaskSpec};
use s5_node::tasks::TaskExecutor;
use s5_node::tasks::vault_persist::{load_vault_root, vault_root_path};
use s5_store_memory::MemoryStore;

// ---------------------------------------------------------------------------
// Two-vault config
// ---------------------------------------------------------------------------

/// Extend the single-vault harness config with a second, independently-keyed
/// destination vault `share`. `plaintext_dst` makes `share`'s metadata tree
/// unencrypted (to exercise the plaintext-dest guard).
#[allow(clippy::too_many_arguments)]
fn two_vault_config(
    src_root: &str,
    dst_root: &str,
    device_r: &str,
    device_id: &str,
    paper_r: &str,
    paper_id: &str,
    share_r: &str,
    share_id: &str,
    source_path: &str,
    plaintext_dst: bool,
) -> S5NodeConfig {
    // `make_config` builds the `backup` source vault (recipients device+paper).
    let mut config = make_config(
        src_root,
        paper_r,
        paper_id,
        device_r,
        device_id,
        source_path,
    );
    config.key.insert(
        "share".to_string(),
        NodeConfigKey {
            public_key: share_r.to_string(),
            identity_file: Some(share_id.to_string()),
        },
    );
    config.vault.insert(
        "share".to_string(),
        NodeConfigVault {
            root_path: dst_root.to_string(),
            key: "share".to_string(),
            data_store: Some("durable".to_string()),
            recipients: vec!["share".to_string()],
            sources: vec![],
            plaintext_tree: plaintext_dst,
            ..Default::default()
        },
    );
    config
}

fn copy_spec(deep: bool, confirm_widen: bool) -> TaskSpec {
    TaskSpec::Copy {
        src_vault: "backup".to_string(),
        src_path: None,
        src_snap: None,
        dst_vault: "share".to_string(),
        dst_path: None,
        blob_store: "durable".to_string(),
        keys: vec!["share".to_string()],
        deep,
        confirm_widen,
    }
}

// ---------------------------------------------------------------------------
// Full round-trip (shallow + deep × memory + local)
// ---------------------------------------------------------------------------

async fn copy_roundtrip(
    backend: &dyn DurableBackend,
    corpus: &Corpus,
    deep: bool,
) -> Result<usize> {
    let scratch = tempfile::tempdir()?;
    let (paper_r, paper_id) = age_identity(scratch.path(), "paper");
    let (device_r, device_id) = age_identity(scratch.path(), "device");
    let (share_r, share_id) = age_identity(scratch.path(), "share");

    let src_root = scratch.path().join("src_vault");
    let dst_root = scratch.path().join("dst_vault");
    std::fs::create_dir_all(&src_root)?;
    std::fs::create_dir_all(&dst_root)?;

    let (blobs, registry) = backend.open();
    let config = two_vault_config(
        &src_root.to_string_lossy(),
        &dst_root.to_string_lossy(),
        &device_r,
        &device_id,
        &paper_r,
        &paper_id,
        &share_r,
        &share_id,
        &corpus.source_path(),
        false,
    );
    let ctx = build_ctx(config, blobs.clone(), registry.clone(), [0x11u8; 32]);
    let executor = TaskExecutor::new(ctx.clone());

    // 1. Back the corpus into the source vault.
    run_task(
        &executor,
        TaskSpec::Backup {
            vault: "backup".to_string(),
            source: "docs".to_string(),
            blob_store: "durable".to_string(),
            keys: vec!["device".to_string(), "paper".to_string()],
            target_path: None,
            changed_paths: None,
        },
    )
    .await
    .context("backup source")?;

    // 2. Copy source → destination (confirm_widen: the share reader is new).
    run_task(&executor, copy_spec(deep, true))
        .await
        .with_context(|| format!("copy (deep={deep})"))?;

    // 3. The destination reader restores it byte-for-byte.
    let restore_target = scratch.path().join("restored");
    std::fs::create_dir_all(&restore_target)?;
    run_task(
        &executor,
        TaskSpec::Restore {
            vault: "share".to_string(),
            target_path: restore_target.to_string_lossy().into_owned(),
            blob_store: None,
            snapshot: None,
            subtree: None,
        },
    )
    .await
    .context("restore destination")?;
    let verified = corpus.verify_restored(&restore_target)?;

    // 4. Cross-reader isolation + ciphertext-reuse assertions.
    let store_read: Arc<dyn BlobsRead> = blobs.clone();
    let (share_root, share_ph, share_ctx) = load_vault_root(
        &vault_root_path(&dst_root.to_string_lossy()),
        std::slice::from_ref(&share_id),
    )?
    .expect("destination root saved");
    let (backup_root, backup_ph, backup_ctx) = load_vault_root(
        &vault_root_path(&src_root.to_string_lossy()),
        std::slice::from_ref(&device_id),
    )?
    .expect("source root saved");

    let share_snap = Snapshot::new(share_root, store_read.clone(), share_ctx.clone(), share_ph);
    assert!(
        walk_ok(&share_snap).await,
        "[{}] destination reader must decode the copied tree",
        backend.label()
    );

    // A source-only reader (source keys) MUST NOT be able to decode the
    // destination's nodes — the inlined per-blob keys ride inside nodes
    // encrypted under the DESTINATION node key only.
    let wrong = Snapshot::new(share_root, store_read.clone(), backup_ctx.clone(), share_ph);
    assert!(
        !walk_ok(&wrong).await,
        "[{}] source-only reader must NOT decode destination nodes",
        backend.label()
    );

    // Ciphertext reuse: shallow shares every data blob hash with the source;
    // deep re-encrypts to a disjoint set.
    let backup_snap = Snapshot::new(backup_root, store_read.clone(), backup_ctx, backup_ph);
    let src_data = collect_data_hashes(&backup_snap).await?;
    let dst_data = collect_data_hashes(&share_snap).await?;
    assert!(!src_data.is_empty(), "source must reference data blobs");
    assert!(
        !dst_data.is_empty(),
        "destination must reference data blobs"
    );
    if deep {
        assert!(
            src_data.is_disjoint(&dst_data),
            "[{}] deep copy must NOT reuse source ciphertext",
            backend.label()
        );
    } else {
        assert_eq!(
            dst_data,
            src_data,
            "[{}] shallow copy must reuse the SAME data-blob ciphertext",
            backend.label()
        );
    }

    Ok(verified)
}

#[tokio::test]
async fn copy_roundtrip_shallow_memory() {
    let corpus = Corpus::author(30).unwrap();
    let backend = MemoryBackend::new();
    assert_eq!(
        copy_roundtrip(&backend, &corpus, false).await.unwrap(),
        corpus.hashes.len()
    );
}

#[tokio::test]
async fn copy_roundtrip_shallow_local() {
    let corpus = Corpus::author(30).unwrap();
    let backend = LocalBackend::new();
    assert_eq!(
        copy_roundtrip(&backend, &corpus, false).await.unwrap(),
        corpus.hashes.len()
    );
}

#[tokio::test]
async fn copy_roundtrip_deep_memory() {
    let corpus = Corpus::author(30).unwrap();
    let backend = MemoryBackend::new();
    assert_eq!(
        copy_roundtrip(&backend, &corpus, true).await.unwrap(),
        corpus.hashes.len()
    );
}

#[tokio::test]
async fn copy_roundtrip_deep_local() {
    let corpus = Corpus::author(30).unwrap();
    let backend = LocalBackend::new();
    assert_eq!(
        copy_roundtrip(&backend, &corpus, true).await.unwrap(),
        corpus.hashes.len()
    );
}

// ---------------------------------------------------------------------------
// Honesty gate: refuse-then-proceed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn honesty_gate_refuses_widening_then_proceeds() {
    let corpus = Corpus::author(8).unwrap();
    let backend = MemoryBackend::new();
    let (executor, _scratch) = fixture_backed(&backend, &corpus, false).await;

    // `share` has a reader `backup` does not → widening. Without confirmation
    // a SHALLOW copy must be refused.
    let refused = run_task(&executor, copy_spec(false, false)).await;
    assert!(
        refused.is_err(),
        "widening shallow copy must be refused without confirm_widen"
    );
    let msg = format!("{:#}", refused.unwrap_err());
    assert!(
        msg.contains("widen") || msg.contains("reader"),
        "gate error should explain the widening: {msg}"
    );

    // With confirmation it proceeds.
    run_task(&executor, copy_spec(false, true))
        .await
        .expect("confirmed widening shallow copy must proceed");
}

// ---------------------------------------------------------------------------
// Plaintext-dest guard
// ---------------------------------------------------------------------------

#[tokio::test]
async fn plaintext_dest_guard_refuses_shallow() {
    let corpus = Corpus::author(6).unwrap();
    let backend = MemoryBackend::new();
    let (executor, _scratch) = fixture_backed(&backend, &corpus, true).await;

    // Even WITH confirm_widen, a shallow copy into a plaintext destination is
    // refused (the inlined keys would land in plaintext nodes).
    let refused = run_task(&executor, copy_spec(false, true)).await;
    assert!(
        refused.is_err(),
        "shallow copy into plaintext dest must be refused"
    );
    let msg = format!("{:#}", refused.unwrap_err());
    assert!(
        msg.contains("encrypt"),
        "guard error should mention encryption: {msg}"
    );
    // (A deep copy into a plaintext vault is a separate matter: a plaintext,
    // non-`plaintext_published_tn` vault has no recovery slot and so cannot be
    // published at all — orthogonal to the shallow-copy key-safety guard.)
}

/// Build a two-vault fixture and back the corpus into the source vault.
/// Returns the executor and the scratch dir (kept alive by the caller).
async fn fixture_backed(
    backend: &dyn DurableBackend,
    corpus: &Corpus,
    plaintext_dst: bool,
) -> (TaskExecutor, tempfile::TempDir) {
    let scratch = tempfile::tempdir().unwrap();
    let (paper_r, paper_id) = age_identity(scratch.path(), "paper");
    let (device_r, device_id) = age_identity(scratch.path(), "device");
    let (share_r, share_id) = age_identity(scratch.path(), "share");

    let src_root = scratch.path().join("src_vault");
    let dst_root = scratch.path().join("dst_vault");
    std::fs::create_dir_all(&src_root).unwrap();
    std::fs::create_dir_all(&dst_root).unwrap();

    let (blobs, registry) = backend.open();
    let config = two_vault_config(
        &src_root.to_string_lossy(),
        &dst_root.to_string_lossy(),
        &device_r,
        &device_id,
        &paper_r,
        &paper_id,
        &share_r,
        &share_id,
        &corpus.source_path(),
        plaintext_dst,
    );
    let ctx = build_ctx(config, blobs, registry, [0x11u8; 32]);
    let executor = TaskExecutor::new(ctx);

    run_task(
        &executor,
        TaskSpec::Backup {
            vault: "backup".to_string(),
            source: "docs".to_string(),
            blob_store: "durable".to_string(),
            keys: vec!["device".to_string(), "paper".to_string()],
            target_path: None,
            changed_paths: None,
        },
    )
    .await
    .expect("backup source");

    (executor, scratch)
}

// ---------------------------------------------------------------------------
// Shared-store zero-copy (fs-level, counting replicator over the gate)
// ---------------------------------------------------------------------------

/// A replicator that mirrors the daemon's contents-gate + upload counter, so a
/// SHARED store yields zero uploads and a DISTINCT store yields > 0.
struct CountingReplicator {
    src: Arc<dyn Blobs>,
    dst: Arc<dyn Blobs>,
    uploads: AtomicUsize,
}

#[async_trait]
impl BlobReplicator for CountingReplicator {
    async fn replicate(&self, hash: Hash) -> Result<()> {
        if self.dst.blob_contains(hash).await? {
            return Ok(());
        }
        let bytes = self.src.blob_download(hash).await?;
        self.dst.blob_upload_bytes(bytes).await?;
        self.uploads.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test]
async fn shared_store_zero_copy_vs_distinct() {
    // Author a single-leaf file in an encrypted source snapshot persisted in a
    // shared store.
    let shared: Arc<dyn Blobs> = Arc::new(BlobStore::new(MemoryStore::new()));
    let shared_read: Arc<dyn BlobsRead> = shared.clone();

    let src = Snapshot::empty_encrypted(shared_read.clone(), [0x21u8; 32]);
    let file = src
        .import_bytes(b"shared-store copy fixture payload", shared.as_ref(), None)
        .await
        .unwrap();
    let mut map = std::collections::BTreeMap::new();
    map.insert("file.bin".to_string(), file);
    let (root, ph, _) = src
        .merge_and_persist(&s5_fs_v2::layer::MapLayer::new(map), shared.as_ref())
        .await
        .unwrap()
        .unwrap();
    let src = Snapshot::new(root, shared_read.clone(), src.context().clone(), Some(ph));
    let src_master = leaf_master_of(&src);

    let dst_ctx = Snapshot::empty_encrypted(shared_read.clone(), [0x22u8; 32])
        .context()
        .clone();

    // Shared store: the data blob is already present → zero uploads.
    let shared_repl = CountingReplicator {
        src: shared.clone(),
        dst: shared.clone(),
        uploads: AtomicUsize::new(0),
    };
    let entries = shallow_copy_into(
        &src,
        None,
        src_master.as_ref(),
        &dst_ctx,
        shared.as_ref(),
        shared_read.clone(),
        None,
        &shared_repl,
    )
    .await
    .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        shared_repl.uploads.load(Ordering::Relaxed),
        0,
        "shared store must move ZERO data bytes"
    );

    // Distinct destination store: the same copy must actually replicate the
    // one data blob (proves the counter is non-vacuous).
    let other: Arc<dyn Blobs> = Arc::new(BlobStore::new(MemoryStore::new()));
    let other_read: Arc<dyn BlobsRead> = other.clone();
    let distinct_repl = CountingReplicator {
        src: shared.clone(),
        dst: other.clone(),
        uploads: AtomicUsize::new(0),
    };
    let _ = shallow_copy_into(
        &src,
        None,
        src_master.as_ref(),
        &dst_ctx,
        other.as_ref(),
        other_read,
        None,
        &distinct_repl,
    )
    .await
    .unwrap();
    assert!(
        distinct_repl.uploads.load(Ordering::Relaxed) >= 1,
        "distinct destination store must replicate the data blob"
    );
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// True iff a full `walk()` completes without a decode error.
async fn walk_ok(snap: &Snapshot) -> bool {
    let mut s = snap.walk();
    while let Some(item) = s.next().await {
        if item.is_err() {
            return false;
        }
    }
    true
}

/// Every DATA-blob (leaf/chunk) CAS hash a snapshot references — the set that
/// is shared on a shallow copy and disjoint on a deep one. Node hashes are
/// deliberately excluded (they always differ, re-encrypted under dest keys).
async fn collect_data_hashes(snap: &Snapshot) -> Result<HashSet<Hash>> {
    let pipe = snap.as_pipeline();
    let mut set = HashSet::new();
    let mut w = snap.walk();
    while let Some(item) = w.next().await {
        let (_, entry) = item?;
        let Some(content) = entry.content.as_ref() else {
            continue;
        };
        if entry.is_leaf() {
            set.insert(content.hash());
        } else if entry.is_link() {
            // Chunked file: descend the byte-stream and record each chunk.
            let child = pipe.child_for(&entry);
            let mut cs = child.walk_byte_stream(content.hash(), content.plaintext_hash);
            while let Some(c) = cs.next().await {
                let ce: NodeEntry = c?;
                if let Some(cc) = ce.content.as_ref() {
                    set.insert(cc.hash());
                }
            }
        }
    }
    Ok(set)
}

/// The effective leaf master of an encrypted snapshot (the key its leaf
/// pipeline references).
fn leaf_master_of(snap: &Snapshot) -> Option<[u8; 32]> {
    let (_strat, slot) = snap.context().leaf.as_ref()?.encryption.as_ref()?;
    snap.context().keys.as_ref()?.get(slot).copied()
}
