//! `vup recover` — paper-phrase disaster recovery.
//!
//! Daemon-less, like `onboard`. The user enters their recovery phrase (hidden)
//! and re-creates their durable store via the **same** storage dialog `onboard`
//! uses. The phrase re-derives only the COLD key (= the DID) and the paper age
//! key; the operational WARM master is random and comes back via the recovery
//! walk (`identity-rotation.md §8`): cold pointer at `(DID,
//! IDENTITY_ANCHOR_ID)` → `warm_pub` → `identity_secrets` escrow at
//! `(warm_pub, identity_secrets_vault_id())`, age-opened with the paper key →
//! the warm seed. From there the shared device-bootstrap tail
//! ([`super::device_bootstrap`]) runs `bootstrap_from_identity` — the same
//! core a freshly enrolled device runs (`vup device join`, D10) — then
//! scaffolds a working `config.toml` + key material so the daemon can resume.
//!
//! The one irreducible manual step is the storage dialog: the indexd AppKey is
//! not offline-derivable, so recovery re-runs the one-time OAuth (deterministic
//! → the same AppKey), exactly as `onboard` did.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use age::secrecy::ExposeSecret;
use anyhow::{Context, Result, bail};

use super::device_bootstrap::{
    FinishArgs, durable_handles, finish_device_bootstrap, generate_device_key_files, scaffold,
};
use super::onboard::{ask_store_type, write_secret_file};

/// Run the interactive recovery flow.
pub async fn run_recover(config_path: &Path) -> Result<()> {
    if config_path.exists() {
        bail!(
            "Config already exists at {}.\n\
             Recovery scaffolds a fresh config — remove it first if you really \
             want to recover over this machine.",
            config_path.display()
        );
    }

    let dirs = scaffold(config_path)?;

    // -- Recovery phrase (hidden) → identity + paper keys --------------------
    // The phrase yields the COLD key (its pubkey IS the DID) and the paper age
    // key. The warm master is NOT derivable — it is fetched from the
    // `identity_secrets` escrow below (identity-rotation.md §3/§8).
    let phrase = crate::interact::password("Recovery phrase (12 words)")
        .context("reading recovery phrase")?;
    let root_master = s5_node::mnemonic::root_master(phrase.trim())
        .context("deriving identity from the phrase — check the words and spacing")?;
    let cold = s5_node::mnemonic::identity_cold_signing_key(&root_master, 0);
    let did = s5_core::Did::from_pubkey(s5_core::identity::DidMasterPubkey::from_verifying_key(
        &cold.verifying_key(),
    ));
    let paper_identity =
        s5_node::mnemonic::paper_age_identity(&root_master).context("deriving paper age key")?;
    let paper_public = paper_identity.to_public().to_string();
    let stores_seed = s5_node::mnemonic::storage_root_seed(&root_master);

    // The paper age identity opens the identity_secrets escrow and the config
    // vault. Persist it so the recovery walk below (and later boots) can
    // decrypt the bootstrap vaults.
    let paper_id_path = dirs.keys_dir.join("recovery.txt");
    write_secret_file(
        &paper_id_path,
        format!("{}\n", paper_identity.to_string().expose_secret()).as_bytes(),
    )?;

    // -- Re-create the durable store (the same dialog as onboard) ------------
    println!();
    println!("Select the SAME storage backend you originally set up.");
    println!("(For Sia this re-runs the one-time authorization; it re-derives the");
    println!(" same key, so your existing data unlocks.)");
    println!();
    let store_choice = ask_store_type(&dirs.store_path, &stores_seed).await?;
    fs::create_dir_all(store_choice.local_path())?;

    // -- Build a live store + registry over it, then resolve -----------------
    let created = s5_node::create_raw_store(store_choice.to_node_config_store(), &HashMap::new())
        .await
        .context("opening the recovery store")?;
    // The vault-facing `dyn Blobs` handle plus the registry the daemon writes
    // HEADs through (indexd native pointers, or StoreRegistry over a path
    // store for S3/local).
    let (blob_store, registry) = durable_handles(created)?;

    // -- Recovery walk (identity-rotation.md §8): DID → warm master ----------
    // Step 1: the cold pointer at (DID, IDENTITY_ANCHOR_ID) names the current
    // warm master pubkey.
    println!();
    println!("Resolving your identity anchor…");
    let (pointer, _revision) =
        s5_node::identity_anchor::resolve_cold_pointer(registry.as_ref(), &did)
            .await
            .context(
                "could not resolve the cold-pointer anchor — make sure this is the \
                 same storage backend and recovery phrase you originally used",
            )?;

    // Persist the anchor entry for the daemon (it verifies it against the warm
    // key and republishes it at startup). Plain file — the entry is exactly
    // what sits in the registry; the cold signature carries the authority.
    let anchor_msg = registry
        .get(&s5_core::StreamKey::Vault {
            pubkey: *did.pubkey(),
            vault_id: s5_node::identity_anchor::identity_anchor_id(),
        })
        .await
        .map_err(|e| anyhow::anyhow!("fetching anchor entry: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("anchor entry disappeared between resolve and fetch"))?;
    let anchor_entry_path = dirs.keys_dir.join("identity_anchor.entry");
    fs::write(&anchor_entry_path, anchor_msg.serialize())
        .with_context(|| format!("writing anchor entry: {}", anchor_entry_path.display()))?;

    // Step 2: the warm seed is random (never phrase-derivable) — open the
    // `identity_secrets` escrow at (warm_pub, identity_secrets_vault_id())
    // with the paper age key.
    let paper_id_files = vec![paper_id_path.to_string_lossy().into_owned()];
    let secrets = s5_node::special_vaults::read_vault_entries(
        pointer.warm_pub,
        s5_node::special_vaults::identity_secrets_vault_id(),
        blob_store.clone(),
        registry.as_ref(),
        &paper_id_files,
    )
    .await
    .context("reading the identity_secrets vault")?;
    let warm_seed: [u8; 32] = secrets
        .get(s5_node::identity_secrets_vault::MASTER_KEY)
        .and_then(|v| v.as_slice().try_into().ok())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no warm-master escrow found in the identity_secrets vault. This \
                 identity was onboarded before the escrow publisher existed; there \
                 is no legacy recovery path (pre-release) — re-run `vup onboard`."
            )
        })?;

    // -- Generate THIS device's keys + run the shared bootstrap tail ---------
    let key_files = generate_device_key_files(&dirs.keys_dir)?;
    finish_device_bootstrap(FinishArgs {
        config_path,
        dirs: &dirs,
        key_files: &key_files,
        anchor_entry_path: &anchor_entry_path,
        store_choice: &store_choice,
        blob_store,
        registry,
        warm_seed,
        // The paper key is a permanent recipient of every vault root, so
        // recovery can materialise all of them.
        reader_identity_files: paper_id_files,
        recovery_public: paper_public,
        did,
        headline: "Identity recovered",
    })
    .await
}
