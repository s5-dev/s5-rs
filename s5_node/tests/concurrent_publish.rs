//! Concurrent publish race: two flows produce divergent Transparent
//! Nodes against the same vault, both publish under the same
//! `(device_pubkey, vault_id)` registry stream, and we observe what
//! happens.
//!
//! The user's claim: the prolly-tree merge primitives + CAS dedup mean
//! divergence reconciles cleanly with no operational coordination
//! needed. This test pins what currently happens vs. what should
//! happen, so the gap is concrete.
//!
//! Expectation in this test:
//!   1. A fresh vault is set up with no entries.
//!   2. Two flows independently produce snapshots — flow A adds
//!      `a.txt`, flow B adds `b.txt`.
//!   3. Both flows publish: each builds an encrypted Transparent Node,
//!      uploads it, computes `prev_revision + 1`, signs, and calls
//!      `registry.set`.
//!   4. The registry's `should_store` rule (`MemoryRegistry::set` →
//!      `StreamMessage::should_store`) deterministically picks one
//!      winner on the (revision, hash) tie.
//!   5. Subsequent reads via `load_peer_snapshot` see the winner's
//!      tree only — the loser's TN is durable in CAS but unreachable
//!      from the registry walk.
//!
//! If/when the convergence loop is implemented (read registry back
//! after publish; if our entry didn't win, fetch the winner's tree,
//! merge our changes on top, re-publish), the assertion will flip
//! to: the latest TN contains *both* `a.txt` and `b.txt`. Until then
//! we assert the gap so any future change to publish is forced to
//! address it.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Arc;

use age::secrecy::ExposeSecret;
use anyhow::{Context, Result, anyhow};
use ed25519_dalek::{SigningKey, VerifyingKey};
use futures_util::StreamExt;
use s5_core::RegistryApi;
use s5_core::blob::BlobStore;
use s5_core::store::Store;
use s5_fs_v2::layer::ReadableLayer;
use s5_fs_v2::overlay::WritableOverlay;
use s5_fs_v2::snapshot::{KEY_SLOT_LEAF, KEY_SLOT_NODE, KEY_SLOT_RECOVERY, Snapshot};
use s5_node::config::{
    NodeConfigIdentity, NodeConfigKey, NodeConfigSource, NodeConfigVault, S5NodeConfig,
};
use s5_node::tasks::TaskExecutorContext;
use s5_node::tasks::peer_load::load_peer_snapshot;
use s5_node::tasks::publish::{derive_vault_id, device_signing_key, run_publish};
use s5_node::tasks::vault_persist::{save_vault_root, vault_root_path};
use s5_registry::{MemoryRegistry, MultiRegistry, WritePolicy};
use s5_registry_store::StoreRegistry;
use s5_store_local::{LocalStore, LocalStoreConfig};
use tempfile::tempdir;
use tokio::sync::RwLock;

/// Build a vault context for a single device. Registry is `Multi[Memory,
/// Store(relay)]` so the on-disk relay can serve registry reads from
/// fresh runtimes too (peer-load round-trip).
fn build_ctx(
    config: S5NodeConfig,
    relay_blob: BlobStore,
    relay_raw: Arc<dyn Store>,
    node_secret: [u8; 32],
) -> Arc<TaskExecutorContext> {
    let mut stores = HashMap::new();
    stores.insert("relay".to_string(), relay_blob);

    let memory: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());
    let store_reg: Arc<dyn RegistryApi + Send + Sync> =
        Arc::new(StoreRegistry::new(relay_raw, None));
    let multi = MultiRegistry::with_policy(vec![memory, store_reg], WritePolicy::All);

    Arc::new(TaskExecutorContext {
        config: Arc::new(RwLock::new(config)),
        stores,
        node_secret,
        registry: Some(Arc::new(multi)),
    })
}

fn vault_config(
    vault_root: &str,
    paper_recipient: &str,
    paper_identity_file: &str,
) -> S5NodeConfig {
    let mut key = BTreeMap::new();
    key.insert(
        "paper".to_string(),
        NodeConfigKey {
            public_key: paper_recipient.to_string(),
            identity_file: Some(paper_identity_file.to_string()),
        },
    );

    let mut source = BTreeMap::new();
    source.insert(
        "docs".to_string(),
        NodeConfigSource {
            paths: Vec::new(),
            include_caches: false,
            skip_hidden: false,
            respect_ignore_files: false,
            exclude: Vec::new(),
            one_file_system: false,
        },
    );

    let mut vault = BTreeMap::new();
    vault.insert(
        "test".to_string(),
        NodeConfigVault {
            root_path: vault_root.to_string(),
            key: "paper".to_string(),
            blob_stores: vec!["relay".to_string()],
            preset: None,
            recipients: vec!["paper".to_string()],
            sources: vec!["docs".to_string()],
            meta_targets: vec![],
            plaintext_tree: false,
            watch: false,
        },
    );

    S5NodeConfig {
        identity: NodeConfigIdentity {
            secret_key_file: None,
            secret_key: None,
            encrypted_with: None,
        },
        key,
        store: BTreeMap::new(),
        registry: BTreeMap::new(),
        source,
        vault,
        task: BTreeMap::new(),
    }
}

/// Build an empty initial snapshot whose context carries the
/// per-vault `recovery_secret` (slot 0x12). Without that slot the
/// publish task's `recovery_secret_from_vault_root` errors with
/// "was this vault created before the v3 schema?" — populating it
/// here matches what a real `vup init` flow ultimately produces.
fn make_initial_snapshot(
    store: &BlobStore,
    read_store: Arc<dyn s5_core::BlobsRead>,
    recovery_secret: [u8; 32],
) -> Snapshot {
    use s5_fs_v2::node::{
        BlobPipeline, CompressionStrategy, EncryptionStrategy, PaddingStrategy, TraversalContext,
    };
    let _ = store;

    let mut keys = BTreeMap::new();
    keys.insert(KEY_SLOT_LEAF, [0xAAu8; 32]);
    keys.insert(KEY_SLOT_NODE, [0xBBu8; 32]);
    keys.insert(KEY_SLOT_RECOVERY, recovery_secret);
    let pad = Some(PaddingStrategy { block_size: 1024 });
    let leaf_pipeline = BlobPipeline {
        compression: Some(CompressionStrategy::Zstd),
        padding: pad.clone(),
        encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_LEAF)),
        skip_when_unhelpful: None,
    };
    let node_pipeline = BlobPipeline {
        compression: Some(CompressionStrategy::Zstd),
        padding: pad,
        encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_NODE)),
        skip_when_unhelpful: None,
    };
    let ctx = TraversalContext {
        keys: Some(keys),
        leaf: Some(leaf_pipeline),
        node: Some(node_pipeline),
        chunking: None,
    };
    Snapshot::empty(read_store, ctx)
}

/// Mirror of the rw-mount flush + publish flow: build a snapshot from a
/// `WritableOverlay` over the given base, save the new vault root, run
/// the publish task. This is what `s5_fuse::WritableFs::flush_overlay`
/// + `vup_cli::publish_after_flush` do at the end of every debounce
///   window.
async fn flush_and_publish(
    base: Snapshot,
    overlay_writes: &[(&str, &[u8])],
    store: &BlobStore,
    ctx: &TaskExecutorContext,
    vault_root_file: &std::path::Path,
    recipient_pubkeys: &[String],
) -> Result<Snapshot> {
    let pipeline = Arc::new(base.as_pipeline());
    let base_layer: Arc<dyn ReadableLayer> = Arc::new(base);
    let overlay = WritableOverlay::new(base_layer, Arc::clone(&pipeline));

    for (path, bytes) in overlay_writes {
        let entry = pipeline
            .import_bytes(bytes, store, None)
            .await
            .with_context(|| format!("import {path}"))?;
        overlay.put((*path).to_string(), entry);
    }

    let result = overlay
        .flush(store)
        .await
        .context("flush")?
        .ok_or_else(|| anyhow!("flush produced empty tree"))?;

    let read_store: Arc<dyn s5_core::BlobsRead> = Arc::new(store.clone());
    let new_snap = Snapshot::new(
        result.0,
        read_store,
        pipeline.context().clone(),
        Some(result.1),
    );

    save_vault_root(vault_root_file, &new_snap, recipient_pubkeys).context("save vault root")?;
    run_publish(ctx, "test", &["paper".to_string()])
        .await
        .context("run_publish")?;

    Ok(new_snap)
}

/// Returns the keys present in the latest published Transparent Node
/// for our device, fetched from the registry → relay store path. Any
/// post-publish merge convergence — if/when we add it — should make
/// this set the union of all writers' overlays, not just the winner's.
async fn list_keys_in_latest_published(
    ctx: &TaskExecutorContext,
    recovery_secret: [u8; 32],
    identity_file: &str,
) -> Result<Vec<String>> {
    let signing: SigningKey = device_signing_key(&ctx.node_secret);
    let pubkey: VerifyingKey = (&signing).into();
    let pubkey_bytes: [u8; 32] = pubkey.to_bytes();
    let vault_id = derive_vault_id(&recovery_secret);

    let registry = ctx
        .registry
        .as_ref()
        .ok_or_else(|| anyhow!("no registry"))?;
    let blob_store = ctx
        .stores
        .get("relay")
        .ok_or_else(|| anyhow!("no relay store"))?;
    let read_store: Arc<dyn s5_core::BlobsRead> = Arc::new(blob_store.clone());

    let snap = load_peer_snapshot(
        pubkey_bytes,
        vault_id,
        registry.as_ref(),
        blob_store,
        &[identity_file.to_string()],
        read_store,
    )
    .await
    .context("load_peer_snapshot")?
    .ok_or_else(|| anyhow!("no published TN found for our pubkey"))?;

    let mut keys = Vec::new();
    let mut stream = snap.scan(Bound::Unbounded, Bound::Unbounded);
    while let Some(item) = stream.next().await {
        let (key, _entry) = item?;
        keys.push(key);
    }
    Ok(keys)
}

/// Two flows publish divergent TNs back to back from the same shared
/// base. Without the `run_publish` convergence step the second writer
/// would silently overwrite the first (no merge against `prev_node`).
/// With the convergence step, flow B detects flow A's published TN as
/// `prev`, merges A's tree into B's, and publishes the union.
#[tokio::test]
async fn back_to_back_divergent_publishes_converge() -> Result<()> {
    // Shared infrastructure
    let relay_dir = tempdir()?;
    let vault_dir = tempdir()?;
    let identity_dir = tempdir()?;
    std::fs::create_dir_all(vault_dir.path())?;

    let paper = age::x25519::Identity::generate();
    let paper_recipient = paper.to_public().to_string();
    let paper_secret = paper.to_string().expose_secret().to_string();
    let identity_path = identity_dir.path().join("paper.txt");
    std::fs::write(&identity_path, &paper_secret)?;
    let identity_path_str = identity_path.to_string_lossy().into_owned();

    let relay_raw: Arc<dyn Store> = Arc::new(LocalStore::create(LocalStoreConfig {
        base_path: relay_dir.path().to_string_lossy().into_owned(),
    }));
    let relay_blob = BlobStore::from_arc(Arc::clone(&relay_raw));

    let cfg = vault_config(
        &vault_dir.path().to_string_lossy(),
        &paper_recipient,
        &identity_path_str,
    );
    let recovery_secret: [u8; 32] = *blake3::hash(b"test-vault-recovery-secret").as_bytes();
    let ctx = build_ctx(
        cfg,
        relay_blob.clone(),
        Arc::clone(&relay_raw),
        [0x11u8; 32],
    );
    let read_store: Arc<dyn s5_core::BlobsRead> = Arc::new(relay_blob.clone());

    // Initial empty snapshot — same on both flows so they diverge from
    // a shared root.
    let s0 = make_initial_snapshot(&relay_blob, Arc::clone(&read_store), recovery_secret);

    let vault_root = vault_root_path(&vault_dir.path().to_string_lossy());
    let recipients = vec![paper_recipient.clone()];

    // --- Flow A: adds a.txt, then publishes ---
    flush_and_publish(
        s0.clone(),
        &[("a.txt", b"alpha")],
        &relay_blob,
        &ctx,
        &vault_root,
        &recipients,
    )
    .await
    .context("flow A")?;

    // --- Flow B: adds b.txt to the *same* base s0 (not s0+a), then publishes ---
    flush_and_publish(
        s0.clone(),
        &[("b.txt", b"beta")],
        &relay_blob,
        &ctx,
        &vault_root,
        &recipients,
    )
    .await
    .context("flow B")?;

    // --- Inspect what's actually reachable via the registry ---
    let keys = list_keys_in_latest_published(&ctx, recovery_secret, &identity_path_str).await?;
    eprintln!("keys reachable in latest published TN: {keys:?}");

    let has_a = keys.iter().any(|k| k == "a.txt");
    let has_b = keys.iter().any(|k| k == "b.txt");

    // With the convergence step in run_publish, flow B's publish
    // detects flow A's already-published TN, merges A+B into a
    // unified tree, and publishes that. Both files end up reachable.
    assert!(
        has_a,
        "a.txt missing from latest TN — convergence dropped flow A"
    );
    assert!(
        has_b,
        "b.txt missing from latest TN — convergence dropped flow B"
    );
    Ok(())
}

/// Truly concurrent: both flows run via `tokio::join`, each starts from
/// the same base, both call `run_publish`. Stresses the case where the
/// *second* publish fetches `prev_node` after the *first*'s registry
/// entry has landed but before the first's local-disk merged TN write
/// completes (or vice versa). The convergence step in `run_publish`
/// fires twice if both interleave: first publish wins solo, second
/// publish detects divergence vs. first's now-published tree, merges,
/// re-publishes at `revision + 1`. Both files end up reachable.
#[tokio::test]
async fn parallel_divergent_publishes_converge() -> Result<()> {
    let relay_dir = tempdir()?;
    let vault_dir = tempdir()?;
    let identity_dir = tempdir()?;
    std::fs::create_dir_all(vault_dir.path())?;

    let paper = age::x25519::Identity::generate();
    let paper_recipient = paper.to_public().to_string();
    let paper_secret = paper.to_string().expose_secret().to_string();
    let identity_path = identity_dir.path().join("paper.txt");
    std::fs::write(&identity_path, &paper_secret)?;
    let identity_path_str = identity_path.to_string_lossy().into_owned();

    let relay_raw: Arc<dyn Store> = Arc::new(LocalStore::create(LocalStoreConfig {
        base_path: relay_dir.path().to_string_lossy().into_owned(),
    }));
    let relay_blob = BlobStore::from_arc(Arc::clone(&relay_raw));

    let cfg = vault_config(
        &vault_dir.path().to_string_lossy(),
        &paper_recipient,
        &identity_path_str,
    );
    let recovery_secret: [u8; 32] = *blake3::hash(b"parallel-test-recovery").as_bytes();
    let ctx = build_ctx(
        cfg,
        relay_blob.clone(),
        Arc::clone(&relay_raw),
        [0x22u8; 32],
    );
    let read_store: Arc<dyn s5_core::BlobsRead> = Arc::new(relay_blob.clone());

    let s0 = make_initial_snapshot(&relay_blob, Arc::clone(&read_store), recovery_secret);

    let vault_root = vault_root_path(&vault_dir.path().to_string_lossy());
    let recipients = vec![paper_recipient.clone()];

    // Both flows run in parallel. Each builds a divergent TN from the
    // same base, races to save_vault_root + run_publish.
    let ctx_a = Arc::clone(&ctx);
    let ctx_b = Arc::clone(&ctx);
    let store_a = relay_blob.clone();
    let store_b = relay_blob.clone();
    let vault_root_a = vault_root.clone();
    let vault_root_b = vault_root.clone();
    let recipients_a = recipients.clone();
    let recipients_b = recipients.clone();
    let s0_a = s0.clone();
    let s0_b = s0.clone();

    let (res_a, res_b) = tokio::join!(
        tokio::spawn(async move {
            flush_and_publish(
                s0_a,
                &[("a.txt", b"alpha")],
                &store_a,
                &ctx_a,
                &vault_root_a,
                &recipients_a,
            )
            .await
        }),
        tokio::spawn(async move {
            flush_and_publish(
                s0_b,
                &[("b.txt", b"beta")],
                &store_b,
                &ctx_b,
                &vault_root_b,
                &recipients_b,
            )
            .await
        }),
    );
    res_a.context("flow A panic")?.context("flow A error")?;
    res_b.context("flow B panic")?.context("flow B error")?;

    let keys = list_keys_in_latest_published(&ctx, recovery_secret, &identity_path_str).await?;
    eprintln!("parallel: keys reachable in latest published TN: {keys:?}");

    let has_a = keys.iter().any(|k| k == "a.txt");
    let has_b = keys.iter().any(|k| k == "b.txt");
    assert!(
        has_a && has_b,
        "parallel publishes lost data: a={has_a}, b={has_b} — keys={keys:?}"
    );
    Ok(())
}

/// Ten flows publishing in parallel, each adding one file. The
/// retry-and-merge convergence loop folds all ten into the final
/// published TN. Strict-CAS in `should_store` (same-revision-
/// different-hash rejects rather than silently overwriting via the
/// old "smaller hash wins" tie-break) means a loser's verify-after-
/// set reliably observes "not our hash" and they retry. Without
/// strict-CAS this used to flake at 5+ contestants because a writer
/// could verify, exit happy, then get silently overwritten by a
/// later smaller-hash writer that hadn't merged them in.
#[tokio::test]
async fn many_parallel_divergent_publishes_all_converge() -> Result<()> {
    let relay_dir = tempdir()?;
    let vault_dir = tempdir()?;
    let identity_dir = tempdir()?;
    std::fs::create_dir_all(vault_dir.path())?;

    let paper = age::x25519::Identity::generate();
    let paper_recipient = paper.to_public().to_string();
    let paper_secret = paper.to_string().expose_secret().to_string();
    let identity_path = identity_dir.path().join("paper.txt");
    std::fs::write(&identity_path, &paper_secret)?;
    let identity_path_str = identity_path.to_string_lossy().into_owned();

    let relay_raw: Arc<dyn Store> = Arc::new(LocalStore::create(LocalStoreConfig {
        base_path: relay_dir.path().to_string_lossy().into_owned(),
    }));
    let relay_blob = BlobStore::from_arc(Arc::clone(&relay_raw));

    let cfg = vault_config(
        &vault_dir.path().to_string_lossy(),
        &paper_recipient,
        &identity_path_str,
    );
    let recovery_secret: [u8; 32] = *blake3::hash(b"many-parallel-test").as_bytes();
    let ctx = build_ctx(
        cfg,
        relay_blob.clone(),
        Arc::clone(&relay_raw),
        [0x33u8; 32],
    );
    let read_store: Arc<dyn s5_core::BlobsRead> = Arc::new(relay_blob.clone());

    let s0 = make_initial_snapshot(&relay_blob, Arc::clone(&read_store), recovery_secret);

    let vault_root = vault_root_path(&vault_dir.path().to_string_lossy());
    let recipients = vec![paper_recipient.clone()];

    let names = [
        "one.txt",
        "two.txt",
        "three.txt",
        "four.txt",
        "five.txt",
        "six.txt",
        "seven.txt",
        "eight.txt",
        "nine.txt",
        "ten.txt",
    ];
    let mut handles = Vec::new();
    for name in &names {
        let s0 = s0.clone();
        let store = relay_blob.clone();
        let ctx = Arc::clone(&ctx);
        let vault_root = vault_root.clone();
        let recipients = recipients.clone();
        let name = name.to_string();
        let body = format!("payload for {name}").into_bytes();
        handles.push(tokio::spawn(async move {
            flush_and_publish(
                s0,
                &[(&name, body.as_slice())],
                &store,
                &ctx,
                &vault_root,
                &recipients,
            )
            .await
        }));
    }
    for h in handles {
        h.await
            .context("flow panic")?
            .context("flow returned error")?;
    }

    let keys = list_keys_in_latest_published(&ctx, recovery_secret, &identity_path_str).await?;
    eprintln!("five-way parallel: keys reachable = {keys:?}");

    for expected in names.iter() {
        assert!(
            keys.iter().any(|k| k == expected),
            "missing {expected} after 5-way parallel publish — keys={keys:?}"
        );
    }
    Ok(())
}
