//! Store-agnostic share-link E2E: producer exports a frozen snapshot,
//! consumer joins it from the same store, restore round-trips byte-for-byte.
//!
//! Closes the share loop (`docs/reference/share-links.md`): the export
//! producer ([`s5_node::export`]) and the join consumer ([`s5_node::share`])
//! are exercised end to end over the shared [`common::DurableBackend`] seam,
//! so the same flow runs against Memory and Local (and later a live store).

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use common::{
    Corpus, DurableBackend, LocalBackend, MemoryBackend, age_identity, build_ctx, make_config,
    run_task,
};
use s5_core::blob::Blobs;
use s5_node::config::TaskSpec;
use s5_node::share;
use s5_node::tasks::TaskExecutor;

/// Producer snaps + exports; consumer joins the URL from the same store and
/// restores. Returns files verified.
async fn share_roundtrip(backend: &dyn DurableBackend, corpus: &Corpus) -> Result<usize> {
    let scratch = tempfile::tempdir()?;
    let (paper_recipient, paper_id) = age_identity(scratch.path(), "paper");
    let (device_recipient, device_id) = age_identity(scratch.path(), "device");

    let pub_vault_root = scratch.path().join("pub_vault");
    std::fs::create_dir_all(&pub_vault_root)?;

    // ===================== PRODUCER: snap + export =====================
    let (blobs, registry) = backend.open();
    let config = make_config(
        &pub_vault_root.to_string_lossy(),
        &paper_recipient,
        &paper_id,
        &device_recipient,
        &device_id,
        &corpus.source_path(),
    );
    let ctx = build_ctx(config, blobs.clone(), registry.clone(), [0x11u8; 32]);
    let executor = TaskExecutor::new(ctx.clone());
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
    .context("snap")?;

    // Export the whole vault → a frozen `s5://export/backup?m=…#secret` URL.
    let stores: HashMap<String, Arc<dyn Blobs>> = {
        let mut m = HashMap::new();
        m.insert("durable".to_string(), blobs.clone());
        m
    };
    let cfg = ctx.config.read().await;
    let export = s5_node::export::run_export(&cfg, &stores, "backup", None)
        .await
        .context("export")?;
    drop(cfg);
    assert!(
        export.url.starts_with("s5://export/backup?m="),
        "[{}] export URL shape: {}",
        backend.label(),
        export.url
    );

    // ===================== CONSUMER: join from the same store =====================
    // A fresh consumer identity (NOT a recipient of the original vault) — it
    // must decrypt purely via the URL's ephemeral secret.
    let (consumer_recipient, consumer_id) = age_identity(scratch.path(), "consumer");
    let (blobs_consumer, _reg_consumer) = backend.open();
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
        std::slice::from_ref(&consumer_recipient),
        &joined_root,
    )
    .await
    .context("join_export")?;
    assert_eq!(parsed.label, "backup");

    // ===================== RESTORE + VERIFY =====================
    // The consumer's config: a read-only vault at the joined root, keyed to
    // the consumer's own identity (which the join re-wrapped the TN to).
    let restore_target = scratch.path().join("restored");
    std::fs::create_dir_all(&restore_target)?;
    let consumer_config = make_config(
        &joined_root.to_string_lossy(),
        &paper_recipient, // unused on the consumer side
        &paper_id,
        &consumer_recipient,
        &consumer_id,
        &corpus.source_path(),
    );
    let consumer_ctx = build_ctx(consumer_config, blobs_consumer, _reg_consumer, [0x33u8; 32]);
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
    .context("restore joined vault")?;

    corpus.verify_restored(&restore_target)
}

#[tokio::test]
async fn share_roundtrip_memory() {
    let corpus = Corpus::author(30).unwrap();
    let backend = MemoryBackend::new();
    assert_eq!(
        share_roundtrip(&backend, &corpus).await.unwrap(),
        corpus.hashes.len()
    );
}

#[tokio::test]
async fn share_roundtrip_local() {
    let corpus = Corpus::author(30).unwrap();
    let backend = LocalBackend::new();
    assert_eq!(
        share_roundtrip(&backend, &corpus).await.unwrap(),
        corpus.hashes.len()
    );
}
