//! Async-relay end-to-end: device A publishes via a remote store, device B
//! pulls from the same remote store after A is offline. No iroh between A and
//! B; the only shared resource is a `Store` that both can reach.
//!
//! Steps:
//!   1. A creates files
//!   2. A snaps → file blobs + encrypted Transparent Node land in the relay
//!      store; the registry entry mirrors to the relay via a `Multi`
//!      registry whose backends are `[Memory, Store(relay)]`.
//!   3. A's runtime is dropped (offline simulation).
//!   4. B starts up with its own (empty) memory backend + the same relay
//!      store. `RemoteRestore` finds the recovery + vault registry entries
//!      via the Multi-registry fallback to relay, downloads the encrypted
//!      Transparent Node from the relay, decrypts with the shared paper
//!      age key, and restores.
//!   5. Test asserts that the SHA-256 of every restored file matches its
//!      original.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use age::secrecy::ExposeSecret;
use anyhow::{Context, Result, anyhow};
use s5_core::RegistryApi;
use s5_core::blob::BlobStore;
use s5_core::store::Store;
use s5_node::config::{
    NodeConfigIdentity, NodeConfigKey, NodeConfigSource, NodeConfigVault, S5NodeConfig, TaskSpec,
};
use s5_node::tasks::{TaskExecutor, TaskExecutorContext};
use s5_node_api::TaskState;
use s5_registry::{MemoryRegistry, MultiRegistry, WritePolicy};
use s5_registry_store::StoreRegistry;
use s5_store_local::{LocalStore, LocalStoreConfig};
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use tokio::sync::RwLock;

/// Build a `TaskExecutorContext` with `relay` (primary, file blobs + TN
/// primary copy) and `mirror` (encrypted-TN mirror target via meta_targets).
/// The Multi registry has `[Memory, Store(relay)]` backends so the relay
/// store serves both as a CAS for blobs and as a key-value backend for the
/// registry — the "remote store as relay" shape exercised end-to-end.
///
/// `node_secret` lets the test distinguish A from B: each gets its own
/// derivation of `vault_signing_key`, but the recovery bridge
/// (paper-age-derived) is the same on both sides, which is what makes B's
/// `RemoteRestore` resolve A's vault root.
fn build_ctx(
    config: S5NodeConfig,
    relay_blob: BlobStore,
    mirror_blob: BlobStore,
    relay_raw: Arc<dyn Store>,
    node_secret: [u8; 32],
) -> Arc<TaskExecutorContext> {
    let mut stores = HashMap::new();
    stores.insert("relay".to_string(), relay_blob);
    stores.insert("mirror".to_string(), mirror_blob);

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

/// Minimal config for a vault that publishes everything to `relay`.
fn vault_config(
    vault_root: &str,
    paper_recipient: &str,
    paper_identity_file: &str,
    source_paths: Vec<String>,
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
            paths: source_paths,
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
            // Distinct from blob_stores: forces publish through the
            // meta_targets mirror loop (otherwise it dedups the duplicate).
            meta_targets: vec!["mirror".to_string()],
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

/// Spawn `spec` and block until the task reaches a terminal state.
async fn run_to_completion(executor: &TaskExecutor, spec: TaskSpec) -> Result<()> {
    let (id, _) = executor.spawn(spec).await?;
    let mut rx = executor
        .watch_status(id)
        .await
        .ok_or_else(|| anyhow!("task vanished after spawn"))?;
    loop {
        let state = rx.borrow().state.clone();
        match state {
            TaskState::Completed => return Ok(()),
            TaskState::Failed { error } => return Err(anyhow!("task {id} failed: {error}")),
            TaskState::Cancelled => return Err(anyhow!("task {id} cancelled")),
            _ => {
                rx.changed()
                    .await
                    .map_err(|_| anyhow!("task {id} status channel closed"))?;
            }
        }
    }
}

/// SHA-256 every regular file under `dir` (relative paths, sorted). Symlinks
/// are skipped so the assertion doesn't depend on follow semantics.
fn hash_tree(dir: &std::path::Path) -> Result<BTreeMap<String, [u8; 32]>> {
    let mut out = BTreeMap::new();
    walk_files(dir, dir, &mut out)?;
    Ok(out)
}

fn walk_files(
    base: &std::path::Path,
    dir: &std::path::Path,
    out: &mut BTreeMap<String, [u8; 32]>,
) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let ft = entry.file_type()?;
        if ft.is_dir() {
            walk_files(base, &path, out)?;
        } else if ft.is_file() {
            let rel = path
                .strip_prefix(base)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            let bytes = std::fs::read(&path)?;
            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            let digest: [u8; 32] = hasher.finalize().into();
            out.insert(rel, digest);
        }
        // skip symlinks; not part of this test's surface
    }
    Ok(())
}

#[tokio::test]
async fn async_relay_via_remote_store() -> Result<()> {
    // ---- shared ground -----------------------------------------------------
    let relay_dir = tempdir()?;
    let mirror_dir = tempdir()?;
    let source_dir = tempdir()?;
    let a_vault_dir = tempdir()?;
    let b_vault_dir = tempdir()?;
    let restore_dir = tempdir()?;
    let identity_dir = tempdir()?;

    // Author the source tree A is going to back up.
    std::fs::create_dir_all(source_dir.path().join("nested"))?;
    std::fs::write(
        source_dir.path().join("readme.md"),
        b"# vault test\n\nSome text content.\n",
    )?;
    std::fs::write(
        source_dir.path().join("nested/data.bin"),
        // Non-trivial size so we exercise CDC + chunk upload, not just inline.
        vec![0xAB; 256 * 1024],
    )?;
    std::fs::write(source_dir.path().join("nested/hello.txt"), b"hello world")?;

    // Paper age key — the only shared secret between A and B.
    let paper = age::x25519::Identity::generate();
    let paper_recipient = paper.to_public().to_string();
    let paper_secret = paper.to_string().expose_secret().to_string();
    let identity_path = identity_dir.path().join("paper.txt");
    std::fs::write(&identity_path, &paper_secret)?;
    let identity_path_str = identity_path.to_string_lossy().into_owned();

    // The relay + mirror stores: A writes here, B reads from here. Same
    // backing dirs, separate Store instances per role so we don't share
    // runtime state across the offline boundary.
    let relay_a_raw: Arc<dyn Store> = Arc::new(LocalStore::create(LocalStoreConfig {
        base_path: relay_dir.path().to_string_lossy().into_owned(),
    }));
    let relay_a_blob = BlobStore::from_arc(Arc::clone(&relay_a_raw));
    let mirror_a_blob = BlobStore::new(LocalStore::create(LocalStoreConfig {
        base_path: mirror_dir.path().to_string_lossy().into_owned(),
    }));

    // ---- A publishes -------------------------------------------------------
    let a_cfg = vault_config(
        &a_vault_dir.path().to_string_lossy(),
        &paper_recipient,
        &identity_path_str,
        vec![source_dir.path().to_string_lossy().into_owned()],
    );

    let a_node_secret = [0x11u8; 32];
    let a_ctx = build_ctx(
        a_cfg,
        relay_a_blob,
        mirror_a_blob,
        Arc::clone(&relay_a_raw),
        a_node_secret,
    );
    let a_executor = TaskExecutor::new(Arc::clone(&a_ctx));

    run_to_completion(
        &a_executor,
        TaskSpec::Backup {
            vault: "test".to_string(),
            source: "docs".to_string(),
            blob_store: "relay".to_string(),
            keys: vec!["paper".to_string()],
            target_path: None,
        },
    )
    .await?;

    // ---- A goes offline ----------------------------------------------------
    drop(a_executor);
    drop(a_ctx);
    drop(relay_a_raw);

    // Sanity check: registry entries did land on the relay store. The Store
    // backing the relay is independent of A, so we can read it directly here.
    let relay_check: Arc<dyn Store> = Arc::new(LocalStore::create(LocalStoreConfig {
        base_path: relay_dir.path().to_string_lossy().into_owned(),
    }));
    let mut count = 0u32;
    let mut stream = relay_check.list().await?;
    use futures_util::StreamExt;
    while let Some(item) = stream.next().await {
        let _ = item?;
        count += 1;
    }
    assert!(
        count > 0,
        "relay store has no entries — publish never wrote to it"
    );

    // ---- B restores --------------------------------------------------------
    let relay_b_raw: Arc<dyn Store> = Arc::new(LocalStore::create(LocalStoreConfig {
        base_path: relay_dir.path().to_string_lossy().into_owned(),
    }));
    let relay_b_blob = BlobStore::from_arc(Arc::clone(&relay_b_raw));
    let mirror_b_blob = BlobStore::new(LocalStore::create(LocalStoreConfig {
        base_path: mirror_dir.path().to_string_lossy().into_owned(),
    }));

    // B's vault config still references the same vault name (used for
    // signing-key derivation in the recovery flow) and the same paper key.
    let b_cfg = vault_config(
        &b_vault_dir.path().to_string_lossy(),
        &paper_recipient,
        &identity_path_str,
        Vec::new(),
    );

    let b_node_secret = [0x22u8; 32]; // distinct from A's — important
    let b_ctx = build_ctx(
        b_cfg,
        relay_b_blob,
        mirror_b_blob.clone(),
        Arc::clone(&relay_b_raw),
        b_node_secret,
    );
    let b_executor = TaskExecutor::new(Arc::clone(&b_ctx));

    run_to_completion(
        &b_executor,
        TaskSpec::RemoteRestore {
            vault: "test".to_string(),
            age_secret_key: paper_secret.clone(),
            blob_store: "relay".to_string(),
            target_path: restore_dir.path().to_string_lossy().into_owned(),
        },
    )
    .await?;

    // Allow filesystem flushes (atomic renames) to settle before walking.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // ---- Verify meta_targets mirror -----------------------------------------
    // The mirror store should have received its own copy of the encrypted
    // Transparent Node — proving the publish path mirrored to meta_targets,
    // not just to blob_stores[0].
    let mirror_hashes = mirror_b_blob.list_hashes().await?;
    assert!(
        !mirror_hashes.is_empty(),
        "mirror store has no blobs — meta_targets mirror loop did not run",
    );

    // ---- Verify content round-trip -----------------------------------------
    let originals = hash_tree(source_dir.path())?;
    let restored = hash_tree(restore_dir.path())?;

    assert_eq!(
        originals.keys().collect::<Vec<_>>(),
        restored.keys().collect::<Vec<_>>(),
        "restored file set does not match original",
    );
    for (path, original_hash) in &originals {
        let restored_hash = restored
            .get(path)
            .ok_or_else(|| anyhow!("missing on restore: {path}"))?;
        assert_eq!(
            original_hash, restored_hash,
            "SHA-256 mismatch for restored file {path}",
        );
    }

    Ok(())
}
