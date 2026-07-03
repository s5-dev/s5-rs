//! Store-agnostic E2E for the D21 **subtree share** composition
//! (`vup share docs:Photos`).
//!
//! The CLI sugar bottoms out in three existing mechanisms — a share-vault,
//! `copy`, and (optionally) an automation — so this test drives that exact
//! composition at the daemon seam:
//!
//!   1. producer backs a corpus (a `Photos/` subtree PLUS files outside it)
//!      into the source vault,
//!   2. a share-vault is minted (owner's own keys — no reader-set widening),
//!   3. `TaskSpec::Copy { src_path: "Photos", .. }` shallow-copies just that
//!      subtree into the share-vault (re-rooted to its root),
//!   4. the share-vault is exported to a frozen anonymous URL,
//!   5. a **non-recipient** consumer joins the URL and restores.
//!
//! Composition-honesty assertions:
//!   * the consumer sees ONLY the `Photos/` subtree — every file outside it is
//!     absent (per-path scoping falls out of the copy, not a crypto path), and
//!   * the source vault's master key never reaches the consumer — its identity
//!     cannot open the source vault root at all.
//!
//! Runs over the shared [`common::DurableBackend`] seam (Memory + Local), no
//! live network.

mod common;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Context, Result};
use common::{
    DurableBackend, LocalBackend, MemoryBackend, age_identity, build_ctx, make_config, run_task,
};
use futures_util::StreamExt;
use s5_core::blob::Blobs;
use s5_core::{BlobsRead, Hash};
use s5_fs_v2::snapshot::Snapshot;
use s5_node::config::{NodeConfigVault, S5NodeConfig, TaskSpec};
use s5_node::share;
use s5_node::tasks::TaskExecutor;
use s5_node::tasks::vault_persist::{load_vault_root, vault_root_path};

/// A corpus with an in-subtree part (`Photos/…`, remembered by its
/// share-vault-relative path after the `Photos/` prefix is stripped) and an
/// out-of-subtree part (paths that MUST be absent from the share).
struct SubtreeCorpus {
    dir: tempfile::TempDir,
    /// share-relative path (post-reroot) → content hash, for the shared files.
    in_subtree: BTreeMap<String, Hash>,
    /// source-relative paths that must NOT appear in the share.
    outside: Vec<String>,
}

impl SubtreeCorpus {
    fn author() -> Result<Self> {
        let dir = tempfile::tempdir()?;
        std::fs::create_dir_all(dir.path().join("Photos/holiday"))?;
        std::fs::create_dir_all(dir.path().join("work"))?;

        let mut in_subtree = BTreeMap::new();
        // (source path, share-relative path after stripping `Photos/`)
        let shared = [
            ("Photos/a.bin", "a.bin", 4096usize, 0xA1u8),
            ("Photos/b.txt", "b.txt", 321, 0xB2),
            ("Photos/holiday/c.bin", "holiday/c.bin", 9001, 0xC3),
        ];
        for (src_rel, share_rel, len, byte) in shared {
            let content = vec![byte; len];
            std::fs::write(dir.path().join(src_rel), &content)?;
            in_subtree.insert(share_rel.to_string(), Hash::new(&content));
        }

        // Files OUTSIDE the shared subtree — must never reach the consumer.
        let outside = ["secret.txt", "work/report.bin"];
        for rel in outside {
            std::fs::write(dir.path().join(rel), format!("PRIVATE {rel}").as_bytes())?;
        }

        Ok(Self {
            dir,
            in_subtree,
            outside: outside.iter().map(|s| s.to_string()).collect(),
        })
    }

    fn source_path(&self) -> String {
        self.dir.path().to_string_lossy().into_owned()
    }
}

/// Add the owner-keyed share-vault `share` to a single-vault producer config.
/// Its recipients are the SAME identities as the source (`device` + `paper`),
/// so the shallow copy does not widen the reader set (the honest default: you
/// share to yourself, then the export URL admits the outside consumer).
fn add_share_vault(config: &mut S5NodeConfig, share_root: &str) {
    config.vault.insert(
        "share".to_string(),
        NodeConfigVault {
            root_path: share_root.to_string(),
            key: "device".to_string(),
            data_store: Some("durable".to_string()),
            recipients: vec!["device".to_string(), "paper".to_string()],
            sources: vec![],
            ..Default::default()
        },
    );
}

async fn share_subtree_roundtrip(backend: &dyn DurableBackend) -> Result<()> {
    let corpus = SubtreeCorpus::author()?;
    let scratch = tempfile::tempdir()?;
    let (paper_r, paper_id) = age_identity(scratch.path(), "paper");
    let (device_r, device_id) = age_identity(scratch.path(), "device");

    let src_root = scratch.path().join("src_vault");
    let share_root = scratch.path().join("share_vault");
    std::fs::create_dir_all(&src_root)?;
    std::fs::create_dir_all(&share_root)?;

    // ===================== PRODUCER: back up docs, then compose =============
    let (blobs, registry) = backend.open();
    let mut config = make_config(
        &src_root.to_string_lossy(),
        &paper_r,
        &paper_id,
        &device_r,
        &device_id,
        &corpus.source_path(),
    );
    add_share_vault(&mut config, &share_root.to_string_lossy());
    let ctx = build_ctx(config, blobs.clone(), registry.clone(), [0x11u8; 32]);
    let executor = TaskExecutor::new(ctx.clone());

    // 1. Back the full corpus into the source vault `backup`.
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

    // 2. Shallow-copy ONLY `backup:Photos` into the share-vault, re-rooted to
    //    its root. Owner keys on both sides → confirm_widen: false (no widening).
    run_task(
        &executor,
        TaskSpec::Copy {
            src_vault: "backup".to_string(),
            src_path: Some("Photos".to_string()),
            src_snap: None,
            dst_vault: "share".to_string(),
            dst_path: None,
            blob_store: "durable".to_string(),
            keys: vec!["device".to_string(), "paper".to_string()],
            deep: false,
            confirm_widen: false,
        },
    )
    .await
    .context("copy subtree into share-vault")?;

    // 3. Export the share-vault → a frozen `s5://export/share?m=…#secret` URL.
    let stores: HashMap<String, Arc<dyn Blobs>> = {
        let mut m = HashMap::new();
        m.insert("durable".to_string(), blobs.clone());
        m
    };
    let export = {
        let cfg = ctx.config.read().await;
        s5_node::export::run_export(&cfg, &stores, "share", None)
            .await
            .context("export share-vault")?
    };
    assert!(
        export.url.starts_with("s5://export/share?m="),
        "[{}] export URL shape: {}",
        backend.label(),
        export.url
    );

    // ===================== CONSUMER: join + restore (non-recipient) ========
    let (consumer_r, consumer_id) = age_identity(scratch.path(), "consumer");
    let (blobs_consumer, reg_consumer) = backend.open();
    let consumer_stores: HashMap<String, Arc<dyn Blobs>> = {
        let mut m = HashMap::new();
        m.insert("durable".to_string(), blobs_consumer.clone());
        m
    };

    let joined_root = scratch.path().join("joined_vault");
    std::fs::create_dir_all(&joined_root)?;
    let parsed = share::join_export(
        &export.url,
        &consumer_stores,
        std::slice::from_ref(&consumer_r),
        &joined_root,
    )
    .await
    .context("join_export")?;
    assert_eq!(parsed.label, "share");

    // The consumer restores the joined share-vault. `device` slot = consumer.
    let restore_target = scratch.path().join("restored");
    std::fs::create_dir_all(&restore_target)?;
    let consumer_config = make_config(
        &joined_root.to_string_lossy(),
        &paper_r, // unused on the consumer side
        &paper_id,
        &consumer_r,
        &consumer_id,
        &corpus.source_path(),
    );
    let consumer_ctx = build_ctx(consumer_config, blobs_consumer, reg_consumer, [0x33u8; 32]);
    let consumer_executor = TaskExecutor::new(consumer_ctx);
    run_task(
        &consumer_executor,
        TaskSpec::Restore {
            vault: "backup".to_string(),
            target_path: restore_target.to_string_lossy().into_owned(),
            blob_store: None,
            snapshot: None,
            subtree: None,
        },
    )
    .await
    .context("restore joined share-vault")?;

    // ===================== ASSERT: only Photos, and no master leak =========
    // (a) Every in-subtree file is present, byte-for-byte, re-rooted to the
    //     share-vault root (the `Photos/` prefix stripped).
    for (share_rel, expected) in &corpus.in_subtree {
        let got = std::fs::read(restore_target.join(share_rel))
            .with_context(|| format!("[{}] shared file missing: {share_rel}", backend.label()))?;
        assert_eq!(
            &Hash::new(&got),
            expected,
            "[{}] byte mismatch for shared file {share_rel}",
            backend.label()
        );
    }

    // (b) Every file OUTSIDE the shared subtree is absent — the share leaked
    //     nothing beyond `Photos/`.
    for rel in &corpus.outside {
        let leaked = restore_target.join(rel);
        assert!(
            !leaked.exists(),
            "[{}] file outside the shared subtree leaked into the share: {rel}",
            backend.label()
        );
        // Also check its basename didn't slip in at the share root.
        if let Some(base) = std::path::Path::new(rel).file_name() {
            assert!(
                !restore_target.join(base).exists(),
                "[{}] outside file '{rel}' leaked (as {:?}) into the share root",
                backend.label(),
                base
            );
        }
    }

    // (c1) age-wrapping isolation: the consumer identity cannot open the SOURCE
    //      vault root at all (it was wrapped only for device+paper).
    let source_open = load_vault_root(
        &vault_root_path(&src_root.to_string_lossy()),
        std::slice::from_ref(&consumer_id),
    );
    assert!(
        source_open.is_err(),
        "[{}] consumer must NOT be able to open the source vault root (age-wrapping isolation)",
        backend.label()
    );

    // (c2) key-inlining isolation (the crux D21 invariant): scan the SHARE-vault
    //      nodes the consumer CAN decrypt and assert NO source master/secret key
    //      appears in any inlined `child_context` — only per-blob keys were
    //      disclosed. This is the check assertion (c1) can't make: a regression
    //      that inlined `src_leaf_master` instead of the per-blob key would keep
    //      (c1) green (the source ROOT is still wrapped for device+paper) yet
    //      hand the consumer the master for the whole source vault.
    let (_src_root_hash, _src_ph, src_ctx) = load_vault_root(
        &vault_root_path(&src_root.to_string_lossy()),
        &[device_id.clone(), paper_id.clone()],
    )
    .context("owner opens source root")?
    .expect("source root saved");
    let (_leaf_strat, src_leaf_slot) = src_ctx
        .leaf
        .as_ref()
        .and_then(|p| p.encryption.as_ref())
        .expect("source leaf is encrypted");
    let src_leaf_master = *src_ctx
        .keys
        .as_ref()
        .and_then(|k| k.get(src_leaf_slot))
        .expect("source leaf master present");
    let src_secrets: HashSet<[u8; 32]> = src_ctx
        .keys
        .as_ref()
        .map(|k| k.values().copied().collect())
        .unwrap_or_default();

    let share_read: Arc<dyn BlobsRead> = consumer_stores
        .get("durable")
        .expect("consumer durable store")
        .clone();
    let (share_root_hash, share_ph, share_ctx) = load_vault_root(
        &vault_root_path(&joined_root.to_string_lossy()),
        std::slice::from_ref(&consumer_id),
    )
    .context("consumer opens joined share root")?
    .expect("joined share root saved");
    let share_snap = Snapshot::new(share_root_hash, share_read, share_ctx, share_ph);

    let mut inlined = 0usize;
    let mut walk = share_snap.walk();
    while let Some(item) = walk.next().await {
        let (path, entry) =
            item.with_context(|| format!("[{}] walking share vault", backend.label()))?;
        let Some(keys) = entry.child_context.as_ref().and_then(|c| c.keys.as_ref()) else {
            continue;
        };
        for (slot, key) in keys {
            assert_ne!(
                key,
                &src_leaf_master,
                "[{}] source LEAF MASTER leaked into share entry '{path}' at slot {slot:#04x}",
                backend.label()
            );
            assert!(
                !src_secrets.contains(key),
                "[{}] a source master/secret key leaked into share entry '{path}' at slot {slot:#04x}",
                backend.label()
            );
            inlined += 1;
        }
    }
    assert!(
        inlined > 0,
        "[{}] shallow share must inline per-blob keys — else the master-absence scan is vacuous",
        backend.label()
    );

    Ok(())
}

#[tokio::test]
async fn share_subtree_roundtrip_memory() {
    share_subtree_roundtrip(&MemoryBackend::new())
        .await
        .unwrap();
}

#[tokio::test]
async fn share_subtree_roundtrip_local() {
    share_subtree_roundtrip(&LocalBackend::new()).await.unwrap();
}
