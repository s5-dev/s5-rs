//! Round-trip a peer's snapshot through the publish + load pipeline.
//!
//! Steps:
//!   1. A daemon-ctx publishes a backup → encrypted Transparent Node lands
//!      in the relay store, registry entry under `(A_pubkey, vault_id)`
//!      lands in the relay-backed registry.
//!   2. From a *separate* code path (no daemon, no per-device state),
//!      we call `load_peer_snapshot(A_pubkey, vault_id, …)` against the
//!      same relay store + registry + the shared paper age identity.
//!   3. The returned `Snapshot` is wrapped in a `MergedView` and we
//!      verify the file tree we backed up is reachable through that
//!      merged view — which is what a multi-peer mount would do on the
//!      receiving side.
//!
//! This is the unit covering `vup +<vault> mount` showing peers' files.
//! The CLI / config glue (peer enumeration, layered mount entry point)
//! sits on top of this primitive in subsequent commits.

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
use s5_fs_v2::merge::MergedView;
use s5_node::config::{
    NodeConfigIdentity, NodeConfigKey, NodeConfigSource, NodeConfigVault, S5NodeConfig, TaskSpec,
};
use s5_node::tasks::peer_load::load_peer_snapshot;
use s5_node::tasks::publish::{derive_vault_id, device_signing_key};
use s5_node::tasks::vault_persist::{load_vault_root, vault_root_path};
use s5_node::tasks::{TaskExecutor, TaskExecutorContext};
use s5_node_api::TaskState;
use s5_registry::{MemoryRegistry, MultiRegistry, WritePolicy};
use s5_registry_store::StoreRegistry;
use s5_store_local::{LocalStore, LocalStoreConfig};
use tempfile::tempdir;
use tokio::sync::RwLock;

/// Mirror of `async_relay::build_ctx`. Mints a TaskExecutorContext with
/// `relay` (file blobs + TN primary) and `mirror` (encrypted-TN mirror
/// target via `meta_targets`); `relay` is also the registry key-value
/// backend so peer-side reads find published entries even when the
/// publishing node is gone.
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

#[tokio::test]
async fn peer_snapshot_round_trip_via_relay() -> Result<()> {
    // ---- shared ground -----------------------------------------------------
    let relay_dir = tempdir()?;
    let mirror_dir = tempdir()?;
    let source_dir = tempdir()?;
    let a_vault_dir = tempdir()?;
    let identity_dir = tempdir()?;

    // Author the source tree A is going to back up — kept small but
    // varied so the merged-view assertion is non-trivial (root files,
    // a nested file).
    std::fs::create_dir_all(source_dir.path().join("nested"))?;
    std::fs::write(source_dir.path().join("readme.md"), b"# peer-load test\n")?;
    std::fs::write(
        source_dir.path().join("nested/data.bin"),
        vec![0xAB; 8 * 1024],
    )?;

    // The age identity that A signs as a recipient and the peer-load
    // path uses to age-decrypt. Same paper key on both sides — this is
    // the "shared vault recipient" model.
    let paper = age::x25519::Identity::generate();
    let paper_recipient = paper.to_public().to_string();
    let paper_secret = paper.to_string().expose_secret().to_string();
    let identity_path = identity_dir.path().join("paper.txt");
    std::fs::write(&identity_path, &paper_secret)?;
    let identity_path_str = identity_path.to_string_lossy().into_owned();

    // Relay + mirror stores: A writes here on publish; the peer-load
    // path reads from `relay` (both for the encrypted blob and via the
    // store-backed registry).
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

    // ---- Extract A's pubkey + the vault_id ---------------------------------
    let a_signing: SigningKey = device_signing_key(&a_node_secret);
    let a_pubkey: VerifyingKey = (&a_signing).into();
    let a_pubkey_bytes: [u8; 32] = a_pubkey.to_bytes();

    // vault_id is derived from the vault root's KEY_SLOT_RECOVERY. In a
    // real two-device deploy, B reads its own local vault root to get
    // the same value (it's shared across all peers of a vault). Here
    // we crack open A's vault root file directly — same secret, just a
    // different code path to it.
    let a_root_file = vault_root_path(&a_vault_dir.path().to_string_lossy());
    let (root_node_a, _, ctx_a) =
        load_vault_root(&a_root_file, std::slice::from_ref(&identity_path_str))
            .context("loading A's vault root for vault_id extraction")?
            .ok_or_else(|| anyhow!("A's vault root file missing after publish"))?;
    let recovery_secret: [u8; 32] = ctx_a
        .keys
        .as_ref()
        .and_then(|m| m.get(&s5_fs_v2::snapshot::KEY_SLOT_RECOVERY))
        .copied()
        .ok_or_else(|| anyhow!("A's vault root has no KEY_SLOT_RECOVERY"))?;
    let vault_id = derive_vault_id(&recovery_secret);
    // load_vault_root returned (root_hash, plaintext_hash, ctx) — we
    // don't need the root_hash here (the registry lookup uses it via
    // the published TN), but binding it stops the lint about unused.
    let _ = root_node_a;

    // Hand A's executor's runtime to garbage so we're sure the load
    // path doesn't accidentally lean on it.
    drop(a_executor);

    // ---- Peer-load A's snapshot from B's perspective -----------------------
    //
    // We rebuild fresh handles (relay_b_*) bound to the same on-disk
    // stores so the load path mirrors what a real second device would
    // see — independent runtime state, shared CAS only.
    let relay_b_raw: Arc<dyn Store> = Arc::new(LocalStore::create(LocalStoreConfig {
        base_path: relay_dir.path().to_string_lossy().into_owned(),
    }));
    let relay_b_blob = BlobStore::from_arc(Arc::clone(&relay_b_raw));
    let read_store: Arc<dyn s5_core::BlobsRead> = Arc::new(relay_b_blob.clone());

    // Same multi-registry shape as on the publish side — Memory
    // (always empty here, on a fresh runtime) + Store-backed (where
    // A's entries actually live). This proves the registry is reachable
    // via the relay store, not just via leftover in-memory state.
    let memory_b: Arc<dyn RegistryApi + Send + Sync> = Arc::new(MemoryRegistry::new());
    let store_reg_b: Arc<dyn RegistryApi + Send + Sync> =
        Arc::new(StoreRegistry::new(Arc::clone(&relay_b_raw), None));
    let registry_b = MultiRegistry::with_policy(vec![memory_b, store_reg_b], WritePolicy::All);

    let peer_snap = load_peer_snapshot(
        a_pubkey_bytes,
        vault_id,
        &registry_b,
        &relay_b_blob,
        std::slice::from_ref(&identity_path_str),
        Arc::clone(&read_store),
    )
    .await
    .context("load_peer_snapshot")?
    .ok_or_else(|| anyhow!("load_peer_snapshot returned None — A's TN was not found"))?;

    // ---- Build a MergedView and verify A's tree surfaces -------------------
    //
    // A real mount would compose [my_snapshot, peer_snap_a, peer_snap_b, …].
    // For this test we wrap just A — a length-1 merged view exercises
    // the same code path the FUSE adapter takes.
    let layers: Vec<Arc<dyn ReadableLayer>> = vec![Arc::new(peer_snap)];
    let merged = MergedView::new(layers);

    // Walk the merged view and collect file keys.
    let mut keys = Vec::new();
    let mut stream = merged.scan(Bound::Unbounded, Bound::Unbounded);
    while let Some(item) = stream.next().await {
        let (key, _entry) = item?;
        keys.push(key);
    }
    drop(stream);

    assert!(
        keys.iter().any(|k| k == "readme.md"),
        "merged peer view missing readme.md; got {keys:?}",
    );
    assert!(
        keys.iter().any(|k| k == "nested/data.bin"),
        "merged peer view missing nested/data.bin; got {keys:?}",
    );

    Ok(())
}
