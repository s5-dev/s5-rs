//! Durability-ordering regression E2E (the recovery-drill
//! brick, proven live 2026-07-02 on production sia.storage).
//!
//! The bug: identity-metadata publishes (`publish_vault` for the
//! identity_secrets escrow / config vault, `edit_bundle` for the identity
//! bundle) set their registry HEAD as soon as the blob was *accepted* by the
//! store — but on the packing (Sia) backend "accepted" means staged in a
//! device-local spool, and a sub-minimum tail had no background flush at
//! all. Kill the device between HEAD-set and flush and the published HEAD
//! points at bytes that exist nowhere durable: `vup recover` walks
//! DID → anchor → escrow and dies on `no such blob` — a healthy, fully
//! "backed-up" identity that cannot be recovered.
//!
//! The fix under test: both write-halves run a `blob_sync()` durability
//! barrier between upload and `registry.set` (the same contract the
//! snapshot publish path has always had).
//!
//! The harness models the drill exactly: pack bodies + registry are the
//! shared durable side; each `open()` builds a fresh `PackingStore` whose
//! staging spool and index cache are device-local RAM, so dropping the
//! handles IS the device wipe. No background flush loop runs and the
//! packing thresholds guarantee nothing flushes on size — durability is
//! reachable ONLY through the write-halves' own barriers.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use common::age_identity;
use ed25519_dalek::SigningKey;
use s5_core::RegistryApi;
use s5_core::blob::{BlobStore, Blobs};
use s5_core::store::Store;
use s5_node::admission::{DeviceKeys, admit_device_keys, read_current_bundle};
use s5_node::identity_secrets_vault::{IdentitySecretsVault, MASTER_KEY};
use s5_node::special_vaults::{identity_secrets_vault_id, read_vault_entries};
use s5_registry::MemoryRegistry;
use s5_store_memory::MemoryStore;
use s5_store_packing::{PackingConfig, PackingStore};

/// Shared durable side (pack bodies + registry); every `open()` is a new
/// device: a fresh packing store over the shared bodies with RAM-fresh
/// staging + index cache, cold-boot-reconciled from the pack headers the
/// way `create_raw_store` does it.
struct SpoolBackend {
    bodies: Arc<MemoryStore>,
    registry: Arc<MemoryRegistry>,
}

impl SpoolBackend {
    fn new() -> Self {
        Self {
            bodies: Arc::new(MemoryStore::new()),
            registry: Arc::new(MemoryRegistry::new()),
        }
    }

    async fn open(&self) -> Result<(Arc<dyn Blobs>, Arc<dyn RegistryApi + Send + Sync>)> {
        let bodies = BlobStore::from_arc(self.bodies.clone() as Arc<dyn Store>);
        let packing = PackingStore::open(
            bodies,
            Arc::new(MemoryStore::new()),
            Arc::new(MemoryStore::new()),
            PackingConfig {
                // Nothing may flush on size or age, and no background loop is
                // spawned: the ONLY path to durability is a write-half's own
                // blob_sync barrier — the invariant under test.
                min_group_size: u64::MAX,
                max_pending_age: std::time::Duration::from_secs(3600),
                ..PackingConfig::default()
            },
        )
        .await
        .map_err(|e| anyhow!("open packing store: {e}"))?;
        // Cold-boot reconcile (as the daemon does): discover pack bodies in
        // the durable CAS and rebuild the membership index from headers.
        let hashes = BlobStore::from_arc(self.bodies.clone() as Arc<dyn Store>)
            .list_hashes()
            .await
            .map_err(|e| anyhow!("list pack bodies: {e}"))?;
        packing
            .note_pack_hashes(hashes)
            .await
            .map_err(|e| anyhow!("note pack hashes: {e}"))?;
        packing
            .enrich()
            .await
            .map_err(|e| anyhow!("enrich pack index: {e}"))?;
        Ok((packing as Arc<dyn Blobs>, self.registry.clone() as _))
    }
}

/// `publish_vault` half: the identity_secrets escrow — the exact publish
/// that bricked drill attempt 1 — must survive losing the publishing
/// device the instant its HEAD is set.
#[tokio::test]
async fn escrow_publish_survives_device_loss_before_any_flush() -> Result<()> {
    let backend = SpoolBackend::new();
    let scratch = tempfile::tempdir()?;

    let warm = SigningKey::from_bytes(&[0x5eu8; 32]);
    let warm_seed: [u8; 32] = warm.to_bytes();
    let (paper_recipient, paper_id) = age_identity(scratch.path(), "paper");
    let (device_recipient, device_id) = age_identity(scratch.path(), "device");

    // Publishing device: escrow the warm seed, then die. No snap follows,
    // so no snapshot barrier ever piggybacks the staged blobs to safety.
    {
        let (blobs_pub, registry_pub) = backend.open().await?;
        IdentitySecretsVault::new(
            warm.clone(),
            blobs_pub,
            registry_pub,
            vec![device_recipient, paper_recipient],
            vec![device_id],
        )
        .publish(&warm_seed)
        .await
        .context("escrow the warm seed")?;
        // Handles dropped here: the device — and its staging spool — is gone.
    }

    // Cold device: the paper walk must read the escrow from durable state.
    let (blobs_cold, registry_cold) = backend.open().await?;
    let secrets = read_vault_entries(
        warm.verifying_key().to_bytes(),
        identity_secrets_vault_id(),
        blobs_cold,
        registry_cold.as_ref(),
        std::slice::from_ref(&paper_id),
    )
    .await
    .context("published escrow HEAD must resolve to a durable blob after device loss")?;
    let recovered: [u8; 32] = secrets
        .get(MASTER_KEY)
        .and_then(|v| v.as_slice().try_into().ok())
        .ok_or_else(|| anyhow!("no warm-master escrow readable with the paper key"))?;
    assert_eq!(recovered, warm_seed);
    Ok(())
}

/// `edit_bundle` half: an admitted device's bundle must be fetchable by
/// peers even if the admitting daemon dies right after the HEAD lands —
/// otherwise enrolls don't propagate and (worse, mirror-image) a revoked
/// device stays authorized on every peer.
#[tokio::test]
async fn bundle_publish_survives_device_loss_before_any_flush() -> Result<()> {
    let backend = SpoolBackend::new();
    let warm = SigningKey::from_bytes(&[0x77u8; 32]);

    let keys = DeviceKeys {
        signing: [1u8; 32],
        acl: [2u8; 32],
        iroh: [3u8; 32],
        age_recipient: "age1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqql4d340"
            .to_string(),
    };

    {
        let (blobs_pub, registry_pub) = backend.open().await?;
        let stores: HashMap<String, Arc<dyn Blobs>> =
            HashMap::from([("spool".to_string(), blobs_pub)]);
        admit_device_keys(&warm, registry_pub.as_ref(), &stores, &keys)
            .await
            .context("admit device keys")?;
        // Admitting device dies; its staging spool is gone.
    }

    let (blobs_cold, registry_cold) = backend.open().await?;
    let stores: HashMap<String, Arc<dyn Blobs>> =
        HashMap::from([("spool".to_string(), blobs_cold)]);
    let (bundle, _entry) = read_current_bundle(
        warm.verifying_key().to_bytes(),
        registry_cold.as_ref(),
        &stores,
    )
    .await
    .context("published bundle HEAD must resolve to a durable blob after device loss")?
    .ok_or_else(|| anyhow!("no identity bundle published"))?;
    assert!(bundle.signers.contains(&keys.signing));
    assert!(bundle.acl_keys.contains(&keys.acl));
    Ok(())
}
