//! Shared scaffolding for the two "fresh device joins an existing
//! identity" flows — `vup recover` (paper phrase) and `vup device join`
//! (enrollment code). D10: enrollment is paper-recovery's sibling; the
//! flows differ only in *how the warm master arrives* (read from the
//! re-wrapped `identity_secrets` escrow with this device's own age key,
//! vs. located via the phrase + paper age key). Everything downstream —
//! dirs, per-device key material, the `bootstrap_from_identity` walk,
//! vault-root materialisation, `config.toml` scaffolding — is this one
//! shared tail.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use s5_core::RegistryApi;
use s5_core::blob::Blobs;

use super::onboard::{StoreChoice, age_encrypt_to, build_config, write_secret_file};

/// The standard directory layout both flows create.
pub(crate) struct Dirs {
    pub data_dir: PathBuf,
    pub store_path: PathBuf,
    pub registry_path: PathBuf,
    pub keys_dir: PathBuf,
}

/// Create the config/data/store/registry/keys directories (same layout
/// as `vup onboard`).
pub(crate) fn scaffold(config_path: &Path) -> Result<Dirs> {
    let dirs = directories::ProjectDirs::from("pro", "s5", "s5")
        .context("could not determine application directories")?;
    let config_dir = config_path
        .parent()
        .unwrap_or_else(|| Path::new(dirs.config_dir()))
        .to_path_buf();
    let data_dir = dirs.data_dir().to_path_buf();

    std::fs::create_dir_all(&config_dir)
        .with_context(|| format!("creating config dir: {}", config_dir.display()))?;
    std::fs::create_dir_all(&data_dir)
        .with_context(|| format!("creating data dir: {}", data_dir.display()))?;
    let store_path = data_dir.join("store");
    let registry_path = data_dir.join("registry");
    let keys_dir = config_dir.join("keys");
    std::fs::create_dir_all(&store_path)?;
    std::fs::create_dir_all(&registry_path)?;
    std::fs::create_dir_all(&keys_dir)?;

    Ok(Dirs {
        data_dir,
        store_path,
        registry_path,
        keys_dir,
    })
}

/// This device's freshly-generated local key material.
pub(crate) struct DeviceKeyFiles {
    /// The main age recipient (this device's `[key.main].public_key`).
    pub main_public: String,
    /// `keys/main.txt` — the age identity file.
    pub identity_path: PathBuf,
    /// `keys/node-identity.key` — the random node secret (the device
    /// keyset file resolves as its sibling).
    pub node_secret_path: PathBuf,
}

/// Generate THIS device's local keys: the main age identity
/// (`keys/main.txt`) and a random node secret (`keys/node-identity.key`,
/// age-encrypted to main). Per-device randomness — never derived from
/// the identity (`mnemonic-derivation.md` §6).
pub(crate) fn generate_device_key_files(keys_dir: &Path) -> Result<DeviceKeyFiles> {
    use age::secrecy::ExposeSecret;

    let main_identity = age::x25519::Identity::generate();
    let main_public = main_identity.to_public().to_string();
    let identity_path = keys_dir.join("main.txt");
    write_secret_file(
        &identity_path,
        format!("{}\n", main_identity.to_string().expose_secret()).as_bytes(),
    )?;

    let mut node_secret = [0u8; 32];
    rand::Rng::fill_bytes(&mut rand::rng(), &mut node_secret);
    let node_secret_path = keys_dir.join("node-identity.key");
    write_secret_file(
        &node_secret_path,
        &age_encrypt_to(&main_public, &node_secret)?,
    )?;

    Ok(DeviceKeyFiles {
        main_public,
        identity_path,
        node_secret_path,
    })
}

/// Turn a [`s5_node::CreatedStore`] into the (blobs, registry) pair the
/// bootstrap walk reads: prefer the store's native registry (indexd
/// metadata pointers), fall back to a generic `StoreRegistry` over the
/// path-store view.
pub(crate) fn durable_handles(
    created: s5_node::CreatedStore,
) -> Result<(Arc<dyn Blobs>, Arc<dyn RegistryApi + Send + Sync>)> {
    let blob_store = created.blobs.clone();
    let registry: Arc<dyn RegistryApi + Send + Sync> = match (created.registry, created.store) {
        (Some(reg), _) => reg,
        (None, Some(store)) => Arc::new(s5_registry_store::StoreRegistry::new(
            store,
            Some("registry".to_string()),
        )),
        (None, None) => {
            bail!("bootstrap store has neither a native registry nor a path-store view")
        }
    };
    Ok((blob_store, registry))
}

/// Everything the shared tail needs once the warm seed is in hand.
pub(crate) struct FinishArgs<'a> {
    pub config_path: &'a Path,
    pub dirs: &'a Dirs,
    pub key_files: &'a DeviceKeyFiles,
    /// `keys/identity_anchor.entry`, already written by the caller.
    pub anchor_entry_path: &'a Path,
    pub store_choice: &'a StoreChoice,
    pub blob_store: Arc<dyn Blobs>,
    pub registry: Arc<dyn RegistryApi + Send + Sync>,
    /// The recovered/escrowed WARM master seed.
    pub warm_seed: [u8; 32],
    /// age identity files that decrypt the config vault and (where
    /// possible) the published vault roots: the paper key on recovery;
    /// this device's main key on enrollment — the latter only opens
    /// roots republished after the enrollment re-wrap, so per-vault
    /// materialisation may legitimately defer to the next snap.
    pub reader_identity_files: Vec<String>,
    /// The paper recovery age recipient (`[key.recovery].public_key`).
    pub recovery_public: String,
    pub did: s5_core::Did,
    /// Report headline, e.g. "Identity recovered" / "Device enrolled".
    pub headline: &'a str,
}

/// The shared tail: persist the warm seed for the daemon, run
/// `bootstrap_from_identity`, scaffold + materialise each discovered
/// vault, write `config.toml`, and print the report.
pub(crate) async fn finish_device_bootstrap(args: FinishArgs<'_>) -> Result<()> {
    // The WARM master seed, re-encrypted to this device's age key —
    // the same `[identity].master_key_file` shape `onboard` writes.
    let master_key_path = args.dirs.keys_dir.join("identity_master.key");
    write_secret_file(
        &master_key_path,
        &age_encrypt_to(&args.key_files.main_public, &args.warm_seed)?,
    )?;
    let warm_signing = ed25519_dalek::SigningKey::from_bytes(&args.warm_seed);

    println!();
    println!("Resolving your identity and vaults from the config vault…");
    let blob_store_for_roots = args.blob_store.clone();
    let recovered = s5_node::bootstrap::bootstrap_from_identity(
        &warm_signing,
        args.blob_store,
        args.registry,
        &args.reader_identity_files,
    )
    .await
    .context(
        "could not read the config vault — make sure this is the same storage \
         backend the identity was set up with",
    )?;

    // -- Scaffold one [vault.<name>] per discovered vault -------------------
    let vaults: Vec<(String, PathBuf)> = recovered
        .vaults
        .iter()
        .map(|v| {
            (
                v.name.clone(),
                args.dirs.data_dir.join("vaults").join(&v.name),
            )
        })
        .collect();
    for (_, root) in &vaults {
        std::fs::create_dir_all(root)?;
    }

    // -- Materialise each vault's local root from its published HEAD --------
    // Fetch the published Transparent Node by HEAD hash, decrypt with the
    // reader identity, re-encrypt to this device's keys — the exact
    // on-disk shape the daemon writes, so `vup <vault> restore` works on
    // a cold device. Best-effort per vault: a root this reader cannot
    // open yet (enrollment before the next snap re-seals it) is deferred
    // to the daemon's re-sync.
    let new_recipients = [
        args.key_files.main_public.clone(),
        args.recovery_public.clone(),
    ];
    let mut materialised = 0usize;
    for (v, (_, root_dir)) in recovered.vaults.iter().zip(vaults.iter()) {
        match s5_node::bootstrap::materialise_vault_root(
            blob_store_for_roots.as_ref(),
            v.head_hash,
            &args.reader_identity_files,
            &new_recipients,
            root_dir,
        )
        .await
        {
            Ok(()) => materialised += 1,
            Err(e) => {
                println!("  ⚠ could not materialise root for '{}': {e:#}", v.name);
                println!("    (its file contents can be restored once the daemon re-syncs it)");
            }
        }
    }

    // -- Write config.toml (onboard's writer, generalised over vaults) ------
    // NB: the store sections are scaffolded from the bootstrap store,
    // which for the common single-remote-store setup is exactly the
    // original. Further remote stores re-sync once the daemon resolves
    // the config vault on boot.
    let config_content = build_config(
        args.store_choice.local_path(),
        &args.dirs.registry_path,
        &args.key_files.identity_path,
        &args.key_files.main_public,
        &args.recovery_public,
        &args.key_files.node_secret_path,
        &master_key_path,
        args.anchor_entry_path,
        args.store_choice,
        &vaults,
    );
    std::fs::write(args.config_path, &config_content)
        .with_context(|| format!("writing config: {}", args.config_path.display()))?;

    // -- Report -------------------------------------------------------------
    println!();
    println!("✓ {}: {}", args.headline, args.did);
    println!("✓ Config:             {}", args.config_path.display());
    if !recovered.stores.is_empty() {
        println!(
            "✓ Store configs recovered: {}",
            recovered
                .stores
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    if recovered.vaults.is_empty() {
        println!("  No vaults discovered yet (none had been snapped to a durable store).");
    } else {
        println!(
            "✓ Vaults discovered ({} root(s) materialised for restore):",
            materialised
        );
        for v in &recovered.vaults {
            println!("    {}  (HEAD {})", v.name, v.head_hash.fmt_short());
        }
    }
    println!();
    println!("Next steps:");
    println!("  1. Restore file contents now:  vup restore <vault>: <target-path>");
    println!("  2. Or start the daemon — it resumes publishing and re-syncs your vaults.");

    Ok(())
}
