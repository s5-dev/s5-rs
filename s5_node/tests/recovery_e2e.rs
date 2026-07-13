//! Store-agnostic, modular end-to-end recovery harness.
//!
//! Proves the grant's headline disaster-recovery loop as a single automated
//! run, no daemon and no live network — the FULL composed walk `vup recover`
//! performs from the paper phrase, on the D17 cold/warm split:
//!
//! ```text
//! onboard-equivalent → cold+warm split: cold pointer published at
//!                      (DID, IDENTITY_ANCHOR_ID); warm seed escrowed in
//!                      identity_secrets (sealed to device + PAPER);
//!                      config vault authored, warm-signed
//! snap               → ingest a file corpus, publish the encrypted TN + HEAD
//! wipe               → drop ALL device-local state (vault root, caches,
//!                      every in-RAM key — the fresh device starts from the
//!                      mnemonic-equivalent: the DID + the paper age key)
//! recover            → resolve_cold_pointer(DID) → warm_pub
//!                      → identity_secrets escrow opened with the PAPER key
//!                        → the warm seed (it is random — the phrase alone
//!                        cannot re-derive it)
//!                      → bootstrap_from_identity(warm) → HEADs
//!                      → materialise each vault's local root (paper key)
//! restore            → pull blobs, write files
//! verify             → restored bytes hash-match the originals
//! ```
//!
//! (`enroll_e2e` stage 4 walks the same shape with the DEVICE key; this is
//! the paper-recipient variant.)
//!
//! The store-agnostic seam ([`common::DurableBackend`]) is shared with the
//! share-link E2E; the same flow runs against Memory and Local here, and the
//! live Sia/indexd backend ([`IndexdBackend`], env-gated + `#[ignore]`) drops
//! in behind the same trait for the live-network validation run.

mod common;

use anyhow::{Context, Result, anyhow};
use common::{
    Corpus, DurableBackend, LocalBackend, MemoryBackend, age_identity, build_ctx, make_config,
    run_task,
};
use ed25519_dalek::{SigningKey, VerifyingKey};
use s5_core::RegistryApi;
use s5_core::blob::Blobs;
use s5_core::identity::{Did, DidMasterPubkey};
use s5_core::{StreamKey, StreamMessage};
use s5_node::bootstrap::{bootstrap_from_identity, materialise_vault_root};
use s5_node::config::TaskSpec;
use s5_node::config_vault::VaultDirEntry;
use s5_node::identity_anchor::{ColdPointer, publish_cold_pointer, resolve_cold_pointer};
use s5_node::identity_secrets_vault::{IdentitySecretsVault, MASTER_KEY};
use s5_node::special_vaults::{
    config_vault_id, identity_secrets_vault_id, publish_vault_entries, read_vault_entries,
};
use s5_node::tasks::TaskExecutor;
use s5_node::tasks::publish::{derive_vault_id, device_signing_key, discovery_signing_key};
use s5_node::tasks::vault_persist::{load_vault_root, vault_root_path};
use std::collections::BTreeMap;

/// The full recovery round-trip against one backend. Returns files verified.
async fn recovery_roundtrip(backend: &dyn DurableBackend, corpus: &Corpus) -> Result<usize> {
    let scratch = tempfile::tempdir()?;

    // The identity, on the real cold/warm split (D17): the mnemonic-
    // equivalent is (cold key → the DID) + the paper age key — exactly
    // what `vup recover` re-derives from the 12 words. The warm master
    // is RANDOM (never phrase-derived); paper recovery must fetch it
    // from the escrow.
    let cold = SigningKey::from_bytes(&[0xC1u8; 32]);
    let did = Did::from_pubkey(DidMasterPubkey::from_verifying_key(&cold.verifying_key()));
    let warm = SigningKey::from_bytes(&[0x5eu8; 32]);
    let warm_seed: [u8; 32] = warm.to_bytes();
    let (paper_recipient, paper_id) = age_identity(scratch.path(), "paper");
    let (device_recipient, device_id) = age_identity(scratch.path(), "device");

    let pub_vault_root = scratch.path().join("pub_vault");
    std::fs::create_dir_all(&pub_vault_root)?;

    // ===================== ONBOARD-equivalent + SNAP =====================
    let (blobs_pub, registry_pub) = backend.open();

    // Cold pointer: the one signature the cold key ever produces —
    // (DID, IDENTITY_ANCHOR_ID) → warm_pub. Recovery starts here.
    publish_cold_pointer(
        registry_pub.as_ref(),
        &cold,
        &ColdPointer {
            warm_pub: warm.verifying_key().to_bytes(),
            next_cold_pub: [0u8; 32],
        },
    )
    .await
    .context("publish cold pointer")?;

    // Warm-seed escrow in identity_secrets, sealed to {device, paper} —
    // what the daemon publishes on first boot. The paper walk below
    // reads it back with the paper key alone.
    IdentitySecretsVault::new(
        warm.clone(),
        blobs_pub.clone(),
        registry_pub.clone(),
        vec![device_recipient.clone(), paper_recipient.clone()],
        vec![device_id.clone()],
    )
    .publish(&warm_seed)
    .await
    .context("escrow the warm seed")?;

    let config = make_config(
        &pub_vault_root.to_string_lossy(),
        &paper_recipient,
        &paper_id,
        &device_recipient,
        &device_id,
        &corpus.source_path(),
    );
    let node_secret = [0x11u8; 32];
    let ctx = build_ctx(config, blobs_pub.clone(), registry_pub.clone(), node_secret);
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
    .context("snap (backup task)")?;

    // Seed the config vault + discovery breadcrumb as the daemon's bootstrap
    // would for a remote store.
    let device_signing = device_signing_key(&node_secret);
    let device_pubkey = VerifyingKey::from(&device_signing).to_bytes();

    let root_file = vault_root_path(&pub_vault_root.to_string_lossy());
    let (_root, _ph, root_ctx) = load_vault_root(&root_file, std::slice::from_ref(&device_id))?
        .ok_or_else(|| anyhow!("no local vault root after snap"))?;
    let recovery_secret: [u8; 32] = root_ctx
        .keys
        .as_ref()
        .and_then(|m| m.get(&s5_fs_v2::snapshot::KEY_SLOT_RECOVERY))
        .copied()
        .ok_or_else(|| anyhow!("vault root has no KEY_SLOT_RECOVERY"))?;
    let vault_id = derive_vault_id(&recovery_secret);

    let head_hash = registry_pub
        .get(&StreamKey::Vault {
            pubkey: device_pubkey,
            vault_id,
        })
        .await?
        .ok_or_else(|| anyhow!("no published HEAD under (device_pubkey, vault_id)"))?
        .hash;

    let seed = [0x9au8; 32];
    let raw = BTreeMap::from([
        ("seed".to_string(), seed.to_vec()),
        (
            "vaults/backup".to_string(),
            serde_json::to_vec(&VaultDirEntry {
                vault_id,
                stores: vec!["durable".to_string()],
            })?,
        ),
    ]);
    publish_vault_entries(
        raw,
        config_vault_id(),
        &warm,
        blobs_pub.clone(),
        registry_pub.as_ref(),
        std::slice::from_ref(&paper_recipient),
    )
    .await
    .context("publish config vault")?;

    let discovery_key = discovery_signing_key(&seed, &vault_id);
    let disc_msg = StreamMessage::sign_ed25519_registry(&discovery_key, vault_id, head_hash, 1)?;
    registry_pub.set(disc_msg).await?;

    // Durability barrier before the wipe — mirrors the daemon publish
    // task's barrier: on a packing (live-Sia) backend the special-vault
    // blobs written after the Backup task's own sync (the config vault
    // above) sit in local staging until a `sync()` force-flushes them into
    // packs, and a cold device can only read what reached the durable
    // store. No-op for the memory/local backends.
    blobs_pub
        .blob_sync()
        .await
        .context("durability barrier before wipe")?;

    // ===================== WIPE (fresh device) =====================
    // Drop everything device-local — including the warm key handle. The
    // recovering side starts from the mnemonic-equivalent only: the DID
    // (cold pubkey) + the paper age identity file.
    drop(executor);
    drop(ctx);
    drop(warm);
    std::fs::remove_dir_all(&pub_vault_root).ok();

    // ===================== RECOVER (the composed D17 walk) =====================
    let (blobs_cold, registry_cold) = backend.open();

    // Step 1: DID → cold pointer → the current warm pubkey.
    let (pointer, _revision) = resolve_cold_pointer(registry_cold.as_ref(), &did)
        .await
        .context("resolve_cold_pointer (recover)")?;

    // Step 2: the warm seed from the identity_secrets escrow, opened
    // with the PAPER age key (the warm seed is random — this read is
    // the only way paper recovery obtains it).
    let secrets = read_vault_entries(
        pointer.warm_pub,
        identity_secrets_vault_id(),
        blobs_cold.clone(),
        registry_cold.as_ref(),
        std::slice::from_ref(&paper_id),
    )
    .await
    .context("read identity_secrets with the paper key")?;
    let recovered_seed: [u8; 32] = secrets
        .get(MASTER_KEY)
        .and_then(|v| v.as_slice().try_into().ok())
        .ok_or_else(|| anyhow!("no warm-master escrow readable with the paper key"))?;
    assert_eq!(
        recovered_seed,
        warm_seed,
        "[{}] the paper walk recovered the escrowed warm seed",
        backend.label()
    );
    let warm_recovered = SigningKey::from_bytes(&recovered_seed);
    assert_eq!(
        warm_recovered.verifying_key().to_bytes(),
        pointer.warm_pub,
        "[{}] the escrowed seed is the warm key the anchor names",
        backend.label()
    );

    // Step 3: the same bootstrap core enrollment runs, under the
    // recovered warm key + the paper identity file.
    let recovered = bootstrap_from_identity(
        &warm_recovered,
        blobs_cold.clone(),
        registry_cold.clone(),
        std::slice::from_ref(&paper_id),
    )
    .await
    .context("bootstrap_from_identity (recover)")?;

    assert_eq!(
        recovered.vaults.len(),
        1,
        "[{}] recovery discovered the vault",
        backend.label()
    );
    let rv = &recovered.vaults[0];
    assert_eq!(rv.head_hash, head_hash, "recovered HEAD == published HEAD");

    let cold_vault_root = scratch.path().join("cold_vault");
    std::fs::create_dir_all(&cold_vault_root)?;
    materialise_vault_root(
        blobs_cold.as_ref(),
        rv.head_hash,
        std::slice::from_ref(&paper_id),
        &[device_recipient.clone(), paper_recipient.clone()],
        &cold_vault_root,
    )
    .await
    .context("materialise cold-device root")?;

    // ===================== RESTORE + VERIFY =====================
    let restore_target = scratch.path().join("restored");
    std::fs::create_dir_all(&restore_target)?;
    let cold_config = make_config(
        &cold_vault_root.to_string_lossy(),
        &paper_recipient,
        &paper_id,
        &device_recipient,
        &device_id,
        &corpus.source_path(),
    );
    let cold_ctx = build_ctx(cold_config, blobs_cold, registry_cold, [0x22u8; 32]);
    let cold_executor = TaskExecutor::new(cold_ctx);
    run_task(
        &cold_executor,
        TaskSpec::Restore {
            vault: "backup".to_string(),
            target_path: restore_target.to_string_lossy().into_owned(),
            blob_store: None,
            snapshot: None,
            subtree: None,
        },
    )
    .await
    .context("restore task")?;

    corpus.verify_restored(&restore_target)
}

#[tokio::test]
async fn recovery_roundtrip_memory() {
    let corpus = Corpus::author(40).unwrap();
    let backend = MemoryBackend::new();
    assert_eq!(
        recovery_roundtrip(&backend, &corpus).await.unwrap(),
        corpus.hashes.len()
    );
}

#[tokio::test]
async fn recovery_roundtrip_local() {
    let corpus = Corpus::author(40).unwrap();
    let backend = LocalBackend::new();
    assert_eq!(
        recovery_roundtrip(&backend, &corpus).await.unwrap(),
        corpus.hashes.len()
    );
}

// ---------------------------------------------------------------------------
// Live Sia/indexd backend (env-gated; NEVER runs by default)
// ---------------------------------------------------------------------------

/// Env var holding a registered indexd AppKey (64 hex chars) — the same
/// convention as `stores/indexd/tests/real_indexd.rs`.
const LIVE_KEY_ENV: &str = "S5_INDEXD_TEST_APP_KEY";
/// Optional indexer-URL override; defaults to the production indexer.
const LIVE_URL_ENV: &str = "S5_INDEXD_TEST_URL";
const LIVE_DEFAULT_URL: &str = "https://sia.storage";

/// Live Sia/indexd durable backend: the PRODUCTION packing-over-indexd
/// wiring, built by [`s5_node::create_raw_store`] from the same
/// `NodeConfigStoreBackend::Indexd` shape `vup onboard` writes to
/// `[store.sia]` — one `IndexdStore` of self-describing packs, the
/// staging + local-manifest layout, the cold-boot pack reconcile, and the
/// metadata-pointer registry sibling (durable HEADs in the same account).
///
/// Every `open()` uses a FRESH local cache/staging/manifests directory, so
/// the second open models a cold device sharing only the remote durable
/// state (indexd metadata + Sia slabs): the pack-membership index is
/// rebuilt from pack headers by the reconcile inside `create_raw_store`,
/// exactly as a real `vup recover` would.
struct IndexdBackend {
    app_key_hex: String,
    indexer_url: String,
    /// Root for the per-open cache dirs; lives as long as the backend.
    scratch: tempfile::TempDir,
    opens: std::sync::atomic::AtomicUsize,
}

impl IndexdBackend {
    /// `None` when `S5_INDEXD_TEST_APP_KEY` is unset — the live test then
    /// skips (it is additionally `#[ignore]`d so it never runs in CI).
    fn from_env() -> Option<Self> {
        let app_key_hex = std::env::var(LIVE_KEY_ENV).ok()?;
        Some(Self {
            app_key_hex,
            indexer_url: std::env::var(LIVE_URL_ENV)
                .unwrap_or_else(|_| LIVE_DEFAULT_URL.to_string()),
            scratch: tempfile::tempdir().expect("temp dir for indexd caches"),
            opens: std::sync::atomic::AtomicUsize::new(0),
        })
    }
}

impl DurableBackend for IndexdBackend {
    fn label(&self) -> &'static str {
        "indexd"
    }

    /// NB: blocks in place on the (async) production store constructor, so
    /// callers must run on a multi-thread tokio runtime
    /// (`#[tokio::test(flavor = "multi_thread")]`).
    fn open(
        &self,
    ) -> (
        std::sync::Arc<dyn Blobs>,
        std::sync::Arc<dyn RegistryApi + Send + Sync>,
    ) {
        use s5_node::config::{IndexdStoreConfig, NodeConfigStore, NodeConfigStoreBackend};
        let n = self.opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let cache_path = self.scratch.path().join(format!("open{n}/index-cache"));
        let cfg =
            NodeConfigStore::from_backend(NodeConfigStoreBackend::Indexd(IndexdStoreConfig {
                indexer_url: self.indexer_url.clone(),
                account: String::new(),
                app_key: self.app_key_hex.clone(),
                cache_path: cache_path.to_string_lossy().into_owned(),
                ..Default::default()
            }));
        let created = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(s5_node::create_raw_store(
                cfg,
                &std::collections::HashMap::new(),
            ))
        })
        .expect("open live indexd store (is the AppKey registered?)");
        let registry = created
            .registry
            .expect("the indexd backend surfaces its native metadata-pointer registry");
        (created.blobs, registry)
    }
}

/// The live-network validation gate: the SAME composed D17
/// recovery walk as above, over the production packing/indexd wiring on
/// real Sia, with a tiny corpus (~50 small files). Costs real storage and
/// minutes of slab uploads, so it NEVER runs by default (`#[ignore]` +
/// env-gated). Run explicitly with:
///
/// ```text
/// S5_INDEXD_TEST_APP_KEY=<64 hex> cargo test -p s5_node --test recovery_e2e -- --ignored
/// ```
#[tokio::test(flavor = "multi_thread")]
#[ignore = "hits live indexd + Sia (costs storage); set S5_INDEXD_TEST_APP_KEY and run with --ignored"]
async fn recovery_roundtrip_live_indexd() {
    let Some(backend) = IndexdBackend::from_env() else {
        eprintln!("skipping recovery_roundtrip_live_indexd: {LIVE_KEY_ENV} not set");
        return;
    };
    let corpus = Corpus::author(50).unwrap();
    assert_eq!(
        recovery_roundtrip(&backend, &corpus).await.unwrap(),
        corpus.hashes.len()
    );
}
