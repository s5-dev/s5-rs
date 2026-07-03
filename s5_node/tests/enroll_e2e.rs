//! Store-agnostic, modular end-to-end multi-device enrollment (D10).
//!
//! Drives the LIBRARY functions (not the interactive CLI) through the
//! shared [`common::DurableBackend`] seam, against Memory and Local:
//!
//! ```text
//! identity   → real cold+warm split: cold pointer published, warm signs
//! device A   → admitted via admit_device_keys (the one admission path),
//!              warm-seed escrow + device catalogue in identity_secrets
//! admit B    → admit_device_keys + catalogue + rewrap_special_vaults
//! snap       → device A publishes the corpus, TN sealed to {A, B, paper}
//! device B   → resolves the anchor (two-hop), reads the warm seed from
//!              identity_secrets with ITS OWN age key, reads the config
//!              vault, materialises + restores the vault byte-for-byte,
//!              and its device_signing key is accepted by allow_write
//! revoke B   → by catalogue LABEL (D18's `vup device revoke` shape):
//!              label → keys, remove_device_keys, catalogue entry out,
//!              rewrap WITHOUT B + a fresh snap: B is excluded from
//!              every NEW root (bundle, escrow, TN) and its writes are
//!              rejected; data it already fetched stays readable —
//!              EXPECTED (D3, restic-comparable model)
//! ```

mod common;

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use common::{
    Corpus, DurableBackend, LocalBackend, MemoryBackend, age_identity, build_ctx, run_task,
};
use ed25519_dalek::SigningKey;
use s5_core::blob::Blobs;
use s5_core::identity::{Did, DidMasterPubkey};
use s5_core::{RegistryApi, StreamKey, StreamMessage};
use s5_node::admission::{
    DeviceKeys, admit_device_keys, read_current_bundle, remove_device_keys, rewrap_special_vaults,
};
use s5_node::bootstrap::{bootstrap_from_identity, materialise_vault_root};
use s5_node::config::{
    NodeConfigIdentity, NodeConfigKey, NodeConfigSource, NodeConfigVault, S5NodeConfig, TaskSpec,
};
use s5_node::config_vault::VaultDirEntry;
use s5_node::identity_anchor::{ColdPointer, publish_cold_pointer, resolve_cold_pointer};
use s5_node::identity_secrets_vault::{IdentitySecretsVault, MASTER_KEY};
use s5_node::membership::{MembershipRegistryAcl, build_membership_state};
use s5_node::special_vaults::{
    config_vault_id, identity_secrets_vault_id, publish_vault_entries, read_vault_entries,
};
use s5_node::tasks::TaskExecutor;
use s5_node::tasks::publish::{derive_vault_id, device_signing_key, discovery_signing_key};
use s5_node::tasks::vault_persist::{load_vault_root, vault_root_path};
use s5_registry::RegistryAcl;

/// One enrolled device's secrets for the test: its signing seed doubles
/// as the executor `node_secret` (that is exactly how the daemon wires
/// `device_signing_key`), so registry entries it publishes verify
/// against the bundle entry admitted for it.
struct TestDevice {
    node_secret: [u8; 32],
    keys: DeviceKeys,
    age_recipient: String,
    age_identity_file: String,
    iroh_pub: [u8; 32],
}

fn test_device(scratch: &std::path::Path, name: &str, seed: u8) -> TestDevice {
    let (age_recipient, age_identity_file) = age_identity(scratch, name);
    let node_secret = [seed; 32];
    let signing = device_signing_key(&node_secret).verifying_key().to_bytes();
    let acl = SigningKey::from_bytes(&[seed.wrapping_add(1); 32])
        .verifying_key()
        .to_bytes();
    let iroh_pub = SigningKey::from_bytes(&[seed.wrapping_add(2); 32])
        .verifying_key()
        .to_bytes();
    TestDevice {
        node_secret,
        keys: DeviceKeys {
            signing,
            acl,
            iroh: iroh_pub,
            age_recipient: age_recipient.clone(),
        },
        age_recipient,
        age_identity_file,
        iroh_pub,
    }
}

/// A config with THREE keys (device_a, device_b, paper), one vault
/// (`backup`, members = self), sealing to `recipients` (key names).
#[allow(clippy::too_many_arguments)]
fn enroll_config(
    vault_root: &str,
    vault_key: &str,
    recipients: &[&str],
    a: (&str, Option<&str>),
    b: (&str, Option<&str>),
    paper: (&str, Option<&str>),
    source_path: &str,
) -> S5NodeConfig {
    let mut key = BTreeMap::new();
    for (name, (public, id_file)) in [("device_a", a), ("device_b", b), ("paper", paper)] {
        key.insert(
            name.to_string(),
            NodeConfigKey {
                public_key: public.to_string(),
                identity_file: id_file.map(str::to_string),
            },
        );
    }

    let mut source = BTreeMap::new();
    source.insert(
        "docs".to_string(),
        NodeConfigSource {
            paths: vec![source_path.to_string()],
            include_caches: false,
            skip_hidden: false,
            respect_ignore_files: false,
            exclude: vec![],
            one_file_system: false,
            max_concurrent_ops: None,
            follow_symlinks: false,
            detect_deletions: false,
        },
    );

    let mut vault = BTreeMap::new();
    vault.insert(
        "backup".to_string(),
        NodeConfigVault {
            root_path: vault_root.to_string(),
            key: vault_key.to_string(),
            data_store: Some("durable".to_string()),
            recipients: recipients.iter().map(|s| s.to_string()).collect(),
            sources: vec!["docs".to_string()],
            members: vec!["self".to_string()],
            ..Default::default()
        },
    );

    S5NodeConfig {
        identity: NodeConfigIdentity {
            secret_key_file: None,
            secret_key: None,
            encrypted_with: None,
            master_key_file: None,
            anchor_entry_file: None,
            keyset_file: None,
            bootstrap_store: Some("durable".to_string()),
        },
        key,
        store: BTreeMap::new(),
        default_store: Some("durable".to_string()),
        registry: BTreeMap::new(),
        source,
        vault,
        task: BTreeMap::new(),
        friend: BTreeMap::new(),
    }
}

/// `admit → enroll B → B bootstraps + writes → revoke B` against one
/// backend.
async fn enroll_roundtrip(backend: &dyn DurableBackend, corpus: &Corpus) -> Result<()> {
    let scratch = tempfile::tempdir()?;
    let label = backend.label();

    // ===================== IDENTITY: real cold/warm split ===================
    let cold = SigningKey::from_bytes(&[0xC0u8; 32]);
    let warm = SigningKey::from_bytes(&[0xAAu8; 32]);
    let warm_seed: [u8; 32] = warm.to_bytes();
    let warm_pub = warm.verifying_key().to_bytes();
    let did = Did::from_pubkey(DidMasterPubkey::from_verifying_key(&cold.verifying_key()));
    let (paper_recipient, paper_id) = age_identity(scratch.path(), "paper");

    let (blobs_a, registry_a) = backend.open();
    let stores_a: HashMap<String, Arc<dyn Blobs>> =
        HashMap::from([("durable".to_string(), blobs_a.clone())]);

    publish_cold_pointer(
        registry_a.as_ref(),
        &cold,
        &ColdPointer {
            warm_pub,
            next_cold_pub: [0u8; 32],
        },
    )
    .await
    .context("publish cold pointer")?;

    // ===================== DEVICE A onboard-equivalent =====================
    // Admitted through the SAME admission path enrollment uses (D9: one
    // primitive), then the warm-seed escrow + catalogue entry.
    let dev_a = test_device(scratch.path(), "device_a", 0x11);
    let rev = admit_device_keys(&warm, registry_a.as_ref(), &stores_a, &dev_a.keys)
        .await
        .context("admit device A")?;
    assert_eq!(rev, 1, "[{label}] first admission is revision 1");

    let secrets_a = IdentitySecretsVault::new(
        warm.clone(),
        blobs_a.clone(),
        registry_a.clone(),
        vec![dev_a.age_recipient.clone(), paper_recipient.clone()],
        vec![dev_a.age_identity_file.clone()],
    );
    secrets_a.publish(&warm_seed).await.context("warm escrow")?;
    secrets_a
        .upsert_device("laptop", &dev_a.keys)
        .await
        .context("catalogue A")?;

    // Config vault: the discovery seed, sealed to {A, paper} for now —
    // the enrollment re-wrap below must extend it to B.
    let seed = [0x9au8; 32];
    publish_vault_entries(
        BTreeMap::from([("seed".to_string(), seed.to_vec())]),
        config_vault_id(),
        &warm,
        blobs_a.clone(),
        registry_a.as_ref(),
        &[dev_a.age_recipient.clone(), paper_recipient.clone()],
    )
    .await
    .context("publish config vault (seed)")?;

    // ===================== ADMIT DEVICE B (the §6.1 add) ====================
    let dev_b = test_device(scratch.path(), "device_b", 0x22);
    let rev = admit_device_keys(&warm, registry_a.as_ref(), &stores_a, &dev_b.keys)
        .await
        .context("admit device B")?;
    assert_eq!(rev, 2, "[{label}] second admission bumps to revision 2");

    let full_recipients = [
        dev_a.age_recipient.clone(),
        dev_b.age_recipient.clone(),
        paper_recipient.clone(),
    ];
    let secrets_full = IdentitySecretsVault::new(
        warm.clone(),
        blobs_a.clone(),
        registry_a.clone(),
        full_recipients.to_vec(),
        vec![dev_a.age_identity_file.clone()],
    );
    secrets_full
        .upsert_device("phone", &dev_b.keys)
        .await
        .context("catalogue B")?;
    rewrap_special_vaults(
        &warm,
        registry_a.as_ref(),
        blobs_a.clone(),
        std::slice::from_ref(&dev_a.age_identity_file),
        &full_recipients,
    )
    .await
    .context("rewrap for B")?;

    // ===================== SNAP as device A (post-add re-seal) =============
    // The next snap after a device add seals the published TN to the new
    // recipient set — that is what makes NEW vault data readable by B.
    let vault_root_a = scratch.path().join("vault_a");
    std::fs::create_dir_all(&vault_root_a)?;
    let config_a = enroll_config(
        &vault_root_a.to_string_lossy(),
        "device_a",
        &["device_a", "device_b", "paper"],
        (&dev_a.age_recipient, Some(&dev_a.age_identity_file)),
        (&dev_b.age_recipient, None),
        (&paper_recipient, Some(&paper_id)),
        &corpus.source_path(),
    );
    let ctx_a = build_ctx(
        config_a,
        blobs_a.clone(),
        registry_a.clone(),
        dev_a.node_secret,
    );
    let executor_a = TaskExecutor::new(ctx_a);
    run_task(
        &executor_a,
        TaskSpec::Backup {
            vault: "backup".to_string(),
            source: "docs".to_string(),
            blob_store: "durable".to_string(),
            keys: vec![
                "device_a".to_string(),
                "device_b".to_string(),
                "paper".to_string(),
            ],
            target_path: None,
            changed_paths: None,
        },
    )
    .await
    .context("snap as A")?;

    // Derive the vault_id + HEAD, then complete the config vault
    // (directory + seed) and the discovery breadcrumb — what the
    // daemon's bootstrap publish does after a snap.
    let root_file = vault_root_path(&vault_root_a.to_string_lossy());
    let (_root, _ph, root_ctx) =
        load_vault_root(&root_file, std::slice::from_ref(&dev_a.age_identity_file))?
            .ok_or_else(|| anyhow!("no local vault root after snap"))?;
    let recovery_secret: [u8; 32] = root_ctx
        .keys
        .as_ref()
        .and_then(|m| m.get(&s5_fs_v2::snapshot::KEY_SLOT_RECOVERY))
        .copied()
        .ok_or_else(|| anyhow!("vault root has no KEY_SLOT_RECOVERY"))?;
    let vault_id = derive_vault_id(&recovery_secret);
    let head1 = registry_a
        .get(&StreamKey::Vault {
            pubkey: dev_a.keys.signing,
            vault_id,
        })
        .await?
        .ok_or_else(|| anyhow!("no HEAD under (A_signing, vault_id)"))?
        .hash;

    let mut config_raw = read_vault_entries(
        warm_pub,
        config_vault_id(),
        blobs_a.clone(),
        registry_a.as_ref(),
        std::slice::from_ref(&dev_a.age_identity_file),
    )
    .await?;
    config_raw.insert(
        "vaults/backup".to_string(),
        serde_json::to_vec(&VaultDirEntry {
            vault_id,
            stores: vec!["durable".to_string()],
        })?,
    );
    publish_vault_entries(
        config_raw,
        config_vault_id(),
        &warm,
        blobs_a.clone(),
        registry_a.as_ref(),
        &full_recipients,
    )
    .await
    .context("publish config vault (directory)")?;
    let discovery_key = discovery_signing_key(&seed, &vault_id);
    registry_a
        .set(StreamMessage::sign_ed25519_registry(
            &discovery_key,
            vault_id,
            head1,
            1,
        )?)
        .await?;

    // ===================== DEVICE B: the enrollment walk ====================
    // Fresh handles over the same durable state — a cold device shares
    // only the durable store, nothing in-RAM.
    let (blobs_b, registry_b) = backend.open();
    let stores_b: HashMap<String, Arc<dyn Blobs>> =
        HashMap::from([("durable".to_string(), blobs_b.clone())]);

    // 1. Two-hop anchor resolution from the DID alone.
    let (pointer, _rev) = resolve_cold_pointer(registry_b.as_ref(), &did)
        .await
        .context("B resolves the anchor")?;
    assert_eq!(
        pointer.warm_pub, warm_pub,
        "[{label}] anchor names the warm key"
    );

    // 2. The warm seed from identity_secrets with B's OWN age key (the
    // same read path recovery runs with the paper key), plus the
    // catalogue that survived the re-wrap.
    let secrets = read_vault_entries(
        pointer.warm_pub,
        identity_secrets_vault_id(),
        blobs_b.clone(),
        registry_b.as_ref(),
        std::slice::from_ref(&dev_b.age_identity_file),
    )
    .await
    .context("B reads identity_secrets")?;
    let escrowed: [u8; 32] = secrets
        .get(MASTER_KEY)
        .and_then(|v| v.as_slice().try_into().ok())
        .ok_or_else(|| anyhow!("no warm escrow readable by B"))?;
    assert_eq!(escrowed, warm_seed, "[{label}] B recovered the warm seed");
    let catalogue = s5_node::identity_secrets_vault::decode_device_catalogue(
        secrets
            .get(s5_node::identity_secrets_vault::DEVICES_KEY)
            .ok_or_else(|| anyhow!("no device catalogue"))?,
    )?;
    assert_eq!(
        catalogue.keys().cloned().collect::<Vec<_>>(),
        vec!["laptop".to_string(), "phone".to_string()],
        "[{label}] catalogue survived the re-wrap with both devices"
    );

    // 3. The config vault via bootstrap_from_identity (warm key + B's
    // own age identity — recover.rs minus the phrase).
    let warm_b = SigningKey::from_bytes(&escrowed);
    let bootstrap = bootstrap_from_identity(
        &warm_b,
        blobs_b.clone(),
        registry_b.clone(),
        std::slice::from_ref(&dev_b.age_identity_file),
    )
    .await
    .context("B bootstraps from identity")?;
    assert_eq!(
        bootstrap.vaults.len(),
        1,
        "[{label}] B discovered the vault"
    );
    assert_eq!(bootstrap.vaults[0].head_hash, head1);

    // 4. B proves it can READ vault data: materialise the root with its
    // own key and restore the corpus byte-for-byte.
    let vault_root_b = scratch.path().join("vault_b");
    std::fs::create_dir_all(&vault_root_b)?;
    materialise_vault_root(
        blobs_b.as_ref(),
        head1,
        std::slice::from_ref(&dev_b.age_identity_file),
        &[dev_b.age_recipient.clone(), paper_recipient.clone()],
        &vault_root_b,
    )
    .await
    .context("B materialises the vault root with its own key")?;
    let restore_target = scratch.path().join("restored_b");
    std::fs::create_dir_all(&restore_target)?;
    let config_b = enroll_config(
        &vault_root_b.to_string_lossy(),
        "device_b",
        &["device_a", "device_b", "paper"],
        (&dev_a.age_recipient, None),
        (&dev_b.age_recipient, Some(&dev_b.age_identity_file)),
        (&paper_recipient, None),
        &corpus.source_path(),
    );
    let ctx_b = build_ctx(
        config_b.clone(),
        blobs_b.clone(),
        registry_b.clone(),
        dev_b.node_secret,
    );
    let executor_b = TaskExecutor::new(ctx_b);
    run_task(
        &executor_b,
        TaskSpec::Restore {
            vault: "backup".to_string(),
            target_path: restore_target.to_string_lossy().into_owned(),
            blob_store: None,
            snapshot: None,
            subtree: None,
        },
    )
    .await
    .context("B restores")?;
    assert_eq!(
        corpus.verify_restored(&restore_target)?,
        corpus.hashes.len(),
        "[{label}] B restored every byte with its own keys"
    );

    // 5. B proves it can WRITE: its device_signing key is in the bundle,
    // so the registry ACL accepts its stream — and a publish under it
    // lands.
    let mut state = build_membership_state(&did, &config_b, registry_b.as_ref(), &stores_b).await;
    state.register_vault_id("backup", vault_id);
    assert!(
        state
            .did_for_device_signing
            .contains_key(&dev_b.keys.signing),
        "[{label}] B's signer is recognised after admission"
    );
    let state = Arc::new(tokio::sync::RwLock::new(state));
    let acl = MembershipRegistryAcl::new(state.clone());
    let b_stream = StreamKey::Vault {
        pubkey: dev_b.keys.signing,
        vault_id,
    };
    assert!(
        acl.allow_write(&dev_b.iroh_pub, &b_stream).await,
        "[{label}] allow_write accepts B's device_signing stream"
    );
    registry_b
        .set(StreamMessage::sign_ed25519_registry(
            &device_signing_key(&dev_b.node_secret),
            vault_id,
            head1,
            1,
        )?)
        .await
        .context("B publishes under its own signing key")?;

    // Keep a pre-revocation copy of a sealed root B could open — the
    // D3 "old fetched data stays readable" check below.
    let old_root_file = vault_root_path(&vault_root_b.to_string_lossy());
    assert!(old_root_file.exists());

    // ===================== REVOKE B (D18's core, routine §6.1) ==============
    // By catalogue LABEL, the way `vup device revoke @phone` works: the
    // label resolves to the key 4-tuple; the label itself never
    // authorizes anything.
    let keys_from_label = secrets_full
        .read_devices()
        .await
        .context("read catalogue for revoke")?
        .get("phone")
        .cloned()
        .ok_or_else(|| anyhow!("no 'phone' in the catalogue"))?;
    assert_eq!(
        keys_from_label, dev_b.keys,
        "[{label}] the catalogue label resolves to exactly B's four keys"
    );
    let rev = remove_device_keys(&warm, registry_a.as_ref(), &stores_a, &keys_from_label)
        .await
        .context("remove device B")?;
    assert_eq!(rev, 3, "[{label}] removal bumps the bundle revision");
    let (bundle, _) = read_current_bundle(warm_pub, registry_a.as_ref(), &stores_a)
        .await?
        .ok_or_else(|| anyhow!("bundle vanished"))?;
    assert!(!bundle.signers.contains(&dev_b.keys.signing));
    assert!(!bundle.acl_keys.contains(&dev_b.keys.acl));
    assert!(!bundle.iroh_pubkeys.contains(&dev_b.keys.iroh));
    assert!(!bundle.age_recipients.contains(&dev_b.age_recipient));
    assert!(bundle.signers.contains(&dev_a.keys.signing), "A survives");

    let survivor_recipients = [dev_a.age_recipient.clone(), paper_recipient.clone()];
    // Catalogue entry out (sealed to the survivors), then the re-wrap —
    // the daemon's revoke order.
    let secrets_survivors = IdentitySecretsVault::new(
        warm.clone(),
        blobs_a.clone(),
        registry_a.clone(),
        survivor_recipients.to_vec(),
        vec![dev_a.age_identity_file.clone()],
    );
    let removed = secrets_survivors
        .remove_device("phone")
        .await
        .context("remove catalogue entry for B")?;
    assert_eq!(removed, Some(dev_b.keys.clone()));
    rewrap_special_vaults(
        &warm,
        registry_a.as_ref(),
        blobs_a.clone(),
        std::slice::from_ref(&dev_a.age_identity_file),
        &survivor_recipients,
    )
    .await
    .context("rewrap without B")?;
    let catalogue_after = secrets_survivors
        .read_devices()
        .await
        .context("read catalogue after revoke")?;
    assert_eq!(
        catalogue_after.keys().cloned().collect::<Vec<_>>(),
        vec!["laptop".to_string()],
        "[{label}] only the survivor remains in the catalogue"
    );
    assert_eq!(
        secrets_survivors.read().await?,
        Some(warm_seed),
        "[{label}] the warm escrow survived the catalogue removal + re-wrap"
    );

    // A's next snap seals the NEW TN to the survivors only.
    std::fs::write(corpus.dir.path().join("post_revoke.txt"), b"survivors-only")?;
    let config_a2 = enroll_config(
        &vault_root_a.to_string_lossy(),
        "device_a",
        &["device_a", "paper"],
        (&dev_a.age_recipient, Some(&dev_a.age_identity_file)),
        (&dev_b.age_recipient, None),
        (&paper_recipient, Some(&paper_id)),
        &corpus.source_path(),
    );
    let ctx_a2 = build_ctx(
        config_a2,
        blobs_a.clone(),
        registry_a.clone(),
        dev_a.node_secret,
    );
    let executor_a2 = TaskExecutor::new(ctx_a2);
    run_task(
        &executor_a2,
        TaskSpec::Backup {
            vault: "backup".to_string(),
            source: "docs".to_string(),
            blob_store: "durable".to_string(),
            keys: vec!["device_a".to_string(), "paper".to_string()],
            target_path: None,
            changed_paths: None,
        },
    )
    .await
    .context("snap as A after revocation")?;
    let head2 = registry_a
        .get(&StreamKey::Vault {
            pubkey: dev_a.keys.signing,
            vault_id,
        })
        .await?
        .ok_or_else(|| anyhow!("no HEAD after re-snap"))?
        .hash;
    assert_ne!(head2, head1, "[{label}] the re-snap produced a new root");

    // -- B is excluded from every NEW root -----------------------------------
    // New identity_secrets: sealed to survivors only.
    assert!(
        read_vault_entries(
            warm_pub,
            identity_secrets_vault_id(),
            blobs_b.clone(),
            registry_b.as_ref(),
            std::slice::from_ref(&dev_b.age_identity_file),
        )
        .await
        .is_err(),
        "[{label}] B must not open the re-wrapped identity_secrets"
    );
    // New published TN: B's key is not a recipient.
    let vault_root_b2 = scratch.path().join("vault_b2");
    std::fs::create_dir_all(&vault_root_b2)?;
    assert!(
        materialise_vault_root(
            blobs_b.as_ref(),
            head2,
            std::slice::from_ref(&dev_b.age_identity_file),
            std::slice::from_ref(&dev_b.age_recipient),
            &vault_root_b2,
        )
        .await
        .is_err(),
        "[{label}] B must not decrypt the post-revocation TN"
    );
    // Write authority: rebuilding membership from the new bundle drops
    // B's signer, so allow_write rejects its stream.
    let mut state2 = build_membership_state(&did, &config_b, registry_b.as_ref(), &stores_b).await;
    state2.register_vault_id("backup", vault_id);
    assert!(
        !state2
            .did_for_device_signing
            .contains_key(&dev_b.keys.signing),
        "[{label}] B's signer is gone from the resolved bundle"
    );
    let acl2 = MembershipRegistryAcl::new(Arc::new(tokio::sync::RwLock::new(state2)));
    assert!(
        !acl2.allow_write(&dev_b.iroh_pub, &b_stream).await,
        "[{label}] allow_write rejects B after revocation"
    );

    // -- D3: what B already fetched stays readable (EXPECTED) ----------------
    // Its locally materialised root (re-encrypted to its own key at
    // enrollment time) still opens; content keys are not rotated.
    assert!(
        load_vault_root(
            &old_root_file,
            std::slice::from_ref(&dev_b.age_identity_file)
        )?
        .is_some(),
        "[{label}] B's pre-revocation root stays readable (D3 model)"
    );

    Ok(())
}

#[tokio::test]
async fn enroll_roundtrip_memory() {
    let corpus = Corpus::author(40).unwrap();
    let backend = MemoryBackend::new();
    enroll_roundtrip(&backend, &corpus).await.unwrap();
}

#[tokio::test]
async fn enroll_roundtrip_local() {
    let corpus = Corpus::author(40).unwrap();
    let backend = LocalBackend::new();
    enroll_roundtrip(&backend, &corpus).await.unwrap();
}
