//! `vup onboard` — first-run setup wizard.
//!
//! Creates the config directory, generates age keys, asks where to store
//! backups (local or S3), scaffolds `config.toml` with the vault schema
//! (recipients/sources/default_store), and prints the recovery secret
//! for the user to write down.
//!
//! `onboard` is one of two CLI verbs that runs without a daemon
//! connection (`_daemon` is the other) — it bootstraps the config the
//! daemon needs to start.

use std::fs;
use std::io::Write;
use std::path::Path;

use age::secrecy::ExposeSecret;
use anyhow::{Context, Result, bail};

/// Run the interactive onboard flow.
///
/// `config_path` is the resolved config file location (default or `--config`).
pub async fn run_onboard(config_path: &Path) -> Result<()> {
    if config_path.exists() {
        bail!(
            "Config already exists at {}.\n\
             Remove it first if you want to re-initialise.",
            config_path.display()
        );
    }

    let dirs = directories::ProjectDirs::from("pro", "s5", "s5")
        .context("could not determine application directories")?;
    let config_dir = config_path
        .parent()
        .unwrap_or_else(|| Path::new(dirs.config_dir()));
    let data_dir = dirs.data_dir();

    // -- Create directories --------------------------------------------------
    fs::create_dir_all(config_dir)
        .with_context(|| format!("creating config dir: {}", config_dir.display()))?;
    fs::create_dir_all(data_dir)
        .with_context(|| format!("creating data dir: {}", data_dir.display()))?;

    let store_path = data_dir.join("store");
    let registry_path = data_dir.join("registry");
    let keys_dir = config_dir.join("keys");
    fs::create_dir_all(&store_path)?;
    fs::create_dir_all(&registry_path)?;
    fs::create_dir_all(&keys_dir)?;

    // -- Generate main age key -----------------------------------------------
    let main_identity = age::x25519::Identity::generate();
    let main_public = main_identity.to_public().to_string();
    let main_secret = main_identity.to_string().expose_secret().to_string();

    let identity_path = keys_dir.join("main.txt");
    fs::write(&identity_path, format!("{}\n", main_secret))
        .with_context(|| format!("writing identity file: {}", identity_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&identity_path, fs::Permissions::from_mode(0o600))?;
    }

    // -- Generate paper recovery mnemonic ------------------------------------
    // The phrase is the user's paper recovery token: BOTH the recovery age key
    // (a permanent recipient of every vault root → vault *content* recovery) and
    // the COLD identity key (whose pubkey IS the DID → *identity* recovery)
    // derive from it, so the words alone reconstruct the identity and decrypt
    // the data (identity-rotation.md §3/§8).
    let mnemonic =
        s5_node::mnemonic::generate_mnemonic().context("generating recovery mnemonic")?;
    let root_master =
        s5_node::mnemonic::root_master(&mnemonic).context("deriving root from mnemonic")?;
    let recovery_public = s5_node::mnemonic::paper_age_identity(&root_master)
        .context("deriving recovery age key")?
        .to_public()
        .to_string();

    // D17 cold/warm split: the cold key signs exactly one thing — the cold
    // pointer below — and is NEVER written to disk (paper-only; the phrase
    // re-derives it). Epoch 1's pubkey is the pre-committed successor
    // (identity-rotation.md §6.3), derivable now while the mnemonic is in hand.
    let cold = s5_node::mnemonic::identity_cold_signing_key(&root_master, 0);
    let did = s5_core::Did::from_pubkey(s5_core::identity::DidMasterPubkey::from_verifying_key(
        &cold.verifying_key(),
    ));
    let next_cold_pub = s5_node::mnemonic::identity_cold_signing_key(&root_master, 1)
        .verifying_key()
        .to_bytes();

    // -- Persist the per-device + identity secrets, age-encrypted at rest ----
    // Node/device identity: random per device, never recovered from the phrase.
    let mut node_secret_bytes = [0u8; 32];
    rand::Rng::fill_bytes(&mut rand::rng(), &mut node_secret_bytes);
    let node_secret_path = keys_dir.join("node-identity.key");
    write_secret_file(
        &node_secret_path,
        &age_encrypt_to(&main_public, &node_secret_bytes)?,
    )?;

    // WARM identity master (the operational signer): random per identity —
    // deliberately NOT phrase-derived, so warm rotation never needs the
    // mnemonic (spec §3). The daemon loads it from `[identity].master_key_file`
    // and escrows the seed into the `identity_secrets` vault on first boot;
    // paper recovery reads it back from there (spec §8).
    let mut warm_seed = [0u8; 32];
    rand::Rng::fill_bytes(&mut rand::rng(), &mut warm_seed);
    let warm = ed25519_dalek::SigningKey::from_bytes(&warm_seed);
    let master_key_path = keys_dir.join("identity_master.key");
    write_secret_file(&master_key_path, &age_encrypt_to(&main_public, &warm_seed)?)?;

    // The cold pointer (DID → warm binding): the ONLY signature the cold key
    // ever produces. Plain file — it is exactly the public registry entry;
    // the daemon verifies + republishes it at startup, so onboarding needs
    // no network.
    let anchor_entry = s5_node::identity_anchor::sign_cold_pointer(
        &cold,
        &s5_node::identity_anchor::ColdPointer {
            warm_pub: warm.verifying_key().to_bytes(),
            next_cold_pub,
        },
        1,
    )
    .context("signing the cold-pointer anchor")?;
    let anchor_entry_path = keys_dir.join("identity_anchor.entry");
    fs::write(&anchor_entry_path, anchor_entry.serialize())
        .with_context(|| format!("writing anchor entry: {}", anchor_entry_path.display()))?;

    // -- Choose backup store -------------------------------------------------
    // Managed storage credentials derive from the storage seed (NOT the cold
    // identity master) — see mnemonic-derivation.md § Layer C.
    let stores_seed = s5_node::mnemonic::storage_root_seed(&root_master);
    let store_choice = ask_store_type(&store_path, &stores_seed).await?;

    // -- Write config --------------------------------------------------------
    let local_store_path = store_choice.local_path();
    let vault_root_path = data_dir.join("vaults/backup");
    fs::create_dir_all(&vault_root_path)?;
    let config_content = build_config(
        local_store_path,
        &registry_path,
        &identity_path,
        &main_public,
        &recovery_public,
        &node_secret_path,
        &master_key_path,
        &anchor_entry_path,
        &store_choice,
        &[("backup".to_string(), vault_root_path.clone())],
    );

    // Ensure the chosen local store dir exists
    fs::create_dir_all(local_store_path)?;

    fs::write(config_path, &config_content)
        .with_context(|| format!("writing config: {}", config_path.display()))?;

    // -- Print summary -------------------------------------------------------
    println!();
    println!("✓ Config file:      {}", config_path.display());
    // The shareable identity: the COLD pubkey (D17) — stable across warm
    // rotations, so it is safe to hand out from day one.
    println!("✓ Identity DID:     {did}");
    println!("✓ Data directory:   {}/", data_dir.display());
    println!(
        "✓ Encryption keys:  {}/ (age keys, 0600 — lost with this machine; \
         the recovery phrase below is the offline backup)",
        keys_dir.display()
    );
    match &store_choice {
        StoreChoice::Local { path } => {
            println!("✓ Backup store:     {}/", path.display());
        }
        StoreChoice::S3 { local_cache, s3 } => {
            println!("✓ Backup store:     s3://{}", s3.bucket_name);
            println!("✓ Local cache:      {}/", local_cache.display());
        }
        StoreChoice::Sia {
            indexer_url,
            cache_path,
            ..
        } => {
            println!("✓ Backup store:     {indexer_url} (Sia, via indexd)");
            println!("✓ Index cache:      {}/", cache_path.display());
            println!(
                "⚠ The indexer AppKey is stored inline in {} — if that file is\n\
                 \x20 lost, `vup recover` re-derives the same AppKey from your recovery\n\
                 \x20 phrase via a one-time browser re-authorization.",
                config_path.display()
            );
        }
    }
    // D17 honesty: only the Sia path configures a durable
    // [identity].bootstrap_store (build_config's bootstrap_line — keep in
    // sync). Without one, the config vault and the warm-key escrow have
    // nowhere durable to live — and the warm master is RANDOM, so the 12
    // words alone cannot reconstruct it. The phrase still derives the DID
    // and the paper age key, but there is nothing off-machine to walk.
    let durable_bootstrap = matches!(store_choice, StoreChoice::Sia { .. });
    if !durable_bootstrap {
        println!();
        println!("⚠  WARNING — no durable bootstrap store with this storage choice.");
        println!("   The config vault and the warm-key escrow will NOT be published");
        println!("   anywhere off this machine, and the operational (warm) identity");
        println!("   key is random: the 12-word phrase alone CANNOT recover this");
        println!("   identity if the machine is lost. `vup recover` and paper");
        println!("   recovery only become available once a durable store (e.g. Sia)");
        println!("   is configured as [identity].bootstrap_store.");
        println!("   Still write the words down — they are the anchor everything");
        println!("   else attaches to once a durable store exists.");
    }
    println!();
    println!("========================================================================");
    println!();
    println!("  RECOVERY PHRASE — write these 12 words down, in order, and keep");
    println!("  them somewhere safe and offline.");
    if durable_bootstrap {
        println!("  This is the ONLY way to recover your data if you lose access to");
        println!("  this machine.");
    } else {
        println!("  (Mind the warning above: until a durable store is configured,");
        println!("   the words alone cannot perform a full recovery.)");
    }
    println!();
    println!("  {}", mnemonic);
    println!();
    println!("========================================================================");
    println!();
    while !crate::interact::confirm("I have written down the 12 words", false)? {
        println!("  Take your time — write them down before continuing.");
    }
    println!();
    crate::cmd::service::offer_install_during_onboarding(config_path).await;
    println!();
    println!("Next steps:");
    println!("  1. Back up:     vup backup ~/Documents ~/Photos docs:");
    println!("  2. Keep it up:  vup automate");

    Ok(())
}

// Store-backend selection lives in [`super::store_config`] so `store add`
// (a new named store against the live daemon) shares the exact collectors +
// validation `onboard`/`recover`/`device join` use. Re-exported here so the
// established `super::onboard::{…}` import sites keep resolving.
pub(crate) use super::store_config::{StoreChoice, ask_store_type, store_choice_from_synced};

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_config(
    store_path: &Path,
    registry_path: &Path,
    identity_path: &Path,
    main_public: &str,
    recovery_public: &str,
    node_secret_path: &Path,
    master_key_path: &Path,
    anchor_entry_path: &Path,
    store_choice: &StoreChoice,
    vaults: &[(String, std::path::PathBuf)],
) -> String {
    let store_section = match store_choice {
        StoreChoice::S3 { s3, .. } => {
            format!(
                r#"[store.local]
type = "local"
base_path = "{store}"

[store.s3]
type = "s3"
endpoint = "{endpoint}"
bucket_name = "{bucket}"
access_key = "{access_key}"
secret_key = "{secret_key}"
region = "{region}""#,
                store = store_path.display(),
                endpoint = s3.endpoint,
                bucket = s3.bucket_name,
                access_key = s3.access_key,
                secret_key = s3.secret_key,
                region = s3.region,
            )
        }
        StoreChoice::Local { .. } => {
            format!(
                r#"[store.local]
type = "local"
base_path = "{store}""#,
                store = store_path.display(),
            )
        }
        StoreChoice::Sia {
            indexer_url,
            app_key,
            cache_path,
        } => {
            format!(
                r#"[store.sia]
type = "indexd"
indexer_url = "{indexer_url}"
app_key = "{app_key}"
cache_path = "{cache_path}""#,
                app_key = hex::encode(app_key),
                cache_path = cache_path.display(),
            )
        }
    };

    // The chosen backend is the node-wide default store (D1): every vault
    // resolves its data + meta primaries to it unless overridden per-vault.
    let default_store = match store_choice {
        StoreChoice::S3 { .. } => "s3",
        StoreChoice::Local { .. } => "local",
        StoreChoice::Sia { .. } => "sia",
    };

    // For a remote (Sia) store, make the registry durable: a Multi over the
    // local redb (fast) AND a StoreRegistry over the remote store (recoverable),
    // and mark that store the bootstrap host. So every HEAD lands in the remote
    // store and `vup recover` can read the identity back from paper alone. A
    // local-only setup keeps the plain redb registry (nothing off-machine to
    // recover from).
    let (registry_section, bootstrap_line) = match store_choice {
        StoreChoice::Sia { .. } => (
            format!(
                r#"[registry.default]
type = "multi"
write_policy = "all"

[[registry.default.backends]]
type = "redb"
path = "{registry}"

[[registry.default.backends]]
type = "store"
store = "sia"
prefix = "registry""#,
                registry = registry_path.display(),
            ),
            "bootstrap_store = \"sia\"".to_string(),
        ),
        _ => (
            format!(
                r#"[registry.default]
type = "redb"
path = "{registry}""#,
                registry = registry_path.display(),
            ),
            String::new(),
        ),
    };

    // One `[vault.<name>]` per vault — `onboard` passes a single "backup"; on
    // `recover` these are the vaults discovered in the config-vault directory.
    // No per-vault store overrides: all ride the node `default_store`.
    let vault_sections: String = vaults
        .iter()
        .map(|(name, root)| {
            format!(
                r#"[vault.{name}]
root_path = "{root}"
key = "main"
recipients = ["main", "recovery"]
sources = ["default"]
"#,
                name = name,
                root = root.display(),
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        r#"# vup configuration — generated by `vup onboard`

default_store = "{default_store}"

[identity]
secret_key_file = "{node_secret}"
master_key_file = "{master_key}"
anchor_entry_file = "{anchor_entry}"
encrypted_with = "main"
{bootstrap_line}
{store_section}

{registry_section}

[key.main]
public_key = "{main_pub}"
identity_file = "{identity}"

[key.recovery]
public_key = "{recovery_pub}"

{vault_sections}
[source.default]
paths = []
one_file_system = false
exclude = [
    "/proc",
    "/sys",
    "/dev",
    "/run",
    "/tmp",
    "/var/run",
    "/var/tmp",
    "**/.cache",
    "**/node_modules"
]
"#,
        default_store = default_store,
        node_secret = node_secret_path.display(),
        master_key = master_key_path.display(),
        anchor_entry = anchor_entry_path.display(),
        bootstrap_line = bootstrap_line,
        store_section = store_section,
        registry_section = registry_section,
        main_pub = main_public,
        identity = identity_path.display(),
        recovery_pub = recovery_public,
        vault_sections = vault_sections,
    )
}

/// Age-encrypt `plaintext` to a single recipient public-key string.
pub(crate) fn age_encrypt_to(recipient_pub: &str, plaintext: &[u8]) -> Result<Vec<u8>> {
    let recipient: age::x25519::Recipient = recipient_pub
        .parse()
        .map_err(|e| anyhow::anyhow!("parsing recipient '{recipient_pub}': {e}"))?;
    let encryptor =
        age::Encryptor::with_recipients(std::iter::once(&recipient as &dyn age::Recipient))
            .map_err(|e| anyhow::anyhow!("creating age encryptor: {e}"))?;
    let mut ciphertext = vec![];
    let mut writer = encryptor
        .wrap_output(&mut ciphertext)
        .map_err(|e| anyhow::anyhow!("age encrypt: {e}"))?;
    writer.write_all(plaintext)?;
    writer.finish().context("finishing age encryption")?;
    Ok(ciphertext)
}

/// Write `bytes` to `path`, restricting to owner-only (0o600) on unix.
pub(crate) fn write_secret_file(path: &Path, bytes: &[u8]) -> Result<()> {
    fs::write(path, bytes).with_context(|| format!("writing {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// `build_config` must emit a config that parses back as `S5NodeConfig`,
    /// with one `[vault.<name>]` per supplied vault and the bootstrap store set.
    /// This is the shape both `onboard` (one vault) and `recover` (the discovered
    /// vaults) depend on.
    #[test]
    fn build_config_parses_with_multiple_vaults() {
        let choice = StoreChoice::Sia {
            cache_path: PathBuf::from("/cache"),
            indexer_url: "https://sia.storage".to_string(),
            app_key: [7u8; 32],
        };
        let vaults = [
            ("backup".to_string(), PathBuf::from("/data/vaults/backup")),
            ("photos".to_string(), PathBuf::from("/data/vaults/photos")),
        ];
        let toml = build_config(
            choice.local_path(),
            Path::new("/data/registry"),
            Path::new("/keys/main.txt"),
            "age1main",
            "age1paper",
            Path::new("/keys/node.key"),
            Path::new("/keys/master.key"),
            Path::new("/keys/identity_anchor.entry"),
            &choice,
            &vaults,
        );

        let cfg: s5_node::config::S5NodeConfig =
            toml::from_str(&toml).expect("generated config must parse");
        assert!(cfg.vault.contains_key("backup"));
        assert!(cfg.vault.contains_key("photos"));
        // The daemon resolves its DID from the anchor entry file (D17).
        assert_eq!(
            cfg.identity.anchor_entry_file.as_deref(),
            Some("/keys/identity_anchor.entry")
        );
        // Vaults carry no per-vault store overrides — they resolve to the
        // node default store (D1).
        assert_eq!(cfg.default_store.as_deref(), Some("sia"));
        assert_eq!(
            cfg.vault_data_store("photos", &cfg.vault["photos"])
                .unwrap(),
            "sia"
        );
        assert_eq!(
            cfg.vault_meta_store("photos", &cfg.vault["photos"])
                .unwrap(),
            "sia"
        );
        assert_eq!(cfg.identity.bootstrap_store.as_deref(), Some("sia"));
        // The Sia store config round-trips with the inline AppKey.
        match &cfg.store["sia"].backend {
            s5_node::config::NodeConfigStoreBackend::Indexd(c) => {
                assert_eq!(c.app_key, hex::encode([7u8; 32]));
            }
            other => panic!("expected Indexd, got {other:?}"),
        }
    }
}
