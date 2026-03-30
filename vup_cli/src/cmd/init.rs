//! `vup init` — first-run setup wizard.
//!
//! Creates the config directory, generates age keys, asks where to store
//! backups (local or S3), scaffolds `config.toml`, and prints the recovery
//! secret for the user to write down.

use std::fs;
use std::io::Write;
use std::path::Path;

use age::secrecy::ExposeSecret;
use anyhow::{Context, Result, bail};

/// Run the interactive init flow.
///
/// `config_path` is the resolved config file location (default or `--config`).
pub async fn run_init(config_path: &Path) -> Result<()> {
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

    // -- Generate recovery key -----------------------------------------------
    let (recovery_public, recovery_secret) = crate::recovery::generate_recovery_key();

    // -- Generate node identity secret (32 random bytes, age-encrypted) --------
    let mut node_secret_bytes = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut node_secret_bytes);

    // Encrypt with main age key so the identity is protected at rest
    let recipient: age::x25519::Recipient = main_public
        .parse()
        .map_err(|e| anyhow::anyhow!("parsing main public key as age recipient: {e}"))?;
    let encryptor =
        age::Encryptor::with_recipients(std::iter::once(&recipient as &dyn age::Recipient))
            .map_err(|e| anyhow::anyhow!("creating age encryptor: {e}"))?;
    let mut ciphertext = vec![];
    let mut writer = encryptor
        .wrap_output(&mut ciphertext)
        .map_err(|e| anyhow::anyhow!("age encrypt: {e}"))?;
    writer.write_all(&node_secret_bytes)?;
    writer.finish().context("finishing age encryption")?;

    let node_secret_path = keys_dir.join("node-identity.key");
    fs::write(&node_secret_path, &ciphertext)
        .with_context(|| format!("writing node secret: {}", node_secret_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&node_secret_path, fs::Permissions::from_mode(0o600))?;
    }

    // -- Choose backup store -------------------------------------------------
    let store_choice = ask_store_type(&store_path).await?;

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
        &store_choice,
        &vault_root_path,
    );

    // Ensure the chosen local store dir exists
    fs::create_dir_all(local_store_path)?;

    fs::write(config_path, &config_content)
        .with_context(|| format!("writing config: {}", config_path.display()))?;

    // -- Print summary -------------------------------------------------------
    println!();
    println!("✓ Config directory: {}/", config_dir.display());
    println!("✓ Data directory:   {}/", data_dir.display());
    match &store_choice {
        StoreChoice::Local { path } => {
            println!("✓ Backup store:     {}/", path.display());
        }
        StoreChoice::S3 { local_cache, s3 } => {
            println!("✓ Backup store:     s3://{}", s3.bucket_name);
            println!("✓ Local cache:      {}/", local_cache.display());
        }
    }
    println!();
    println!("========================================================================");
    println!();
    println!("  RECOVERY KEY — write this down and store it safely!");
    println!("  This is the ONLY way to recover your data if you lose");
    println!("  access to this machine.");
    println!();
    println!("  {}", recovery_secret);
    println!();
    println!("========================================================================");
    println!();
    println!("Next steps:");
    println!("  1. Write down your recovery key on paper");
    println!("  2. Add paths:   vup add ~/Documents ~/Photos");
    println!("  3. Run backup:  vup backup");

    Ok(())
}

/// S3 credentials collected from the user.
struct S3Config {
    endpoint: String,
    bucket_name: String,
    access_key: String,
    secret_key: String,
    region: String,
}

/// The user's store choice from the init wizard.
enum StoreChoice {
    Local {
        path: std::path::PathBuf,
    },
    S3 {
        local_cache: std::path::PathBuf,
        s3: S3Config,
    },
}

impl StoreChoice {
    fn local_path(&self) -> &std::path::Path {
        match self {
            StoreChoice::Local { path } => path,
            StoreChoice::S3 { local_cache, .. } => local_cache,
        }
    }
}

/// Ask the user where to store backups and collect the relevant details.
async fn ask_store_type(default_local_path: &std::path::Path) -> Result<StoreChoice> {
    use dialoguer::{Input, Select};

    let choices = &["Local (this machine)", "S3-compatible storage"];
    let selection = Select::new()
        .with_prompt("Where do you want to store backups?")
        .items(choices)
        .default(0)
        .interact()?;

    if selection == 0 {
        let path: String = Input::new()
            .with_prompt("Local store path")
            .default(default_local_path.to_string_lossy().into_owned())
            .interact_text()?;
        return Ok(StoreChoice::Local {
            path: std::path::PathBuf::from(path),
        });
    }

    // -- S3 prompts ----------------------------------------------------------
    let endpoint: String = Input::new().with_prompt("Endpoint URL").interact_text()?;

    let bucket_name: String = Input::new().with_prompt("Bucket name").interact_text()?;

    let region: String = Input::new()
        .with_prompt("Region")
        .default("us-east-1".into())
        .interact_text()?;

    let access_key: String = Input::new().with_prompt("Access Key ID").interact_text()?;

    let secret_key: String = Input::new()
        .with_prompt("Secret Access Key")
        .interact_text()?;

    // -- Test connection -----------------------------------------------------
    println!("Testing S3 connection...");
    let s3_cfg = s5_store_s3::S3StoreConfig::new(
        endpoint.clone(),
        bucket_name.clone(),
        access_key.clone(),
        secret_key.clone(),
        region.clone(),
    );
    let store = s5_store_s3::S3Store::create(s3_cfg);

    use s5_core::store::Store;
    use tokio_stream::StreamExt;

    // Try to list — this validates endpoint, credentials, and bucket access.
    let mut stream = store
        .list()
        .await
        .context("S3 connection failed. Check endpoint, bucket, and credentials.")?;
    // Consume the first item (or empty is fine) to confirm the stream works.
    let _first = stream.next().await;
    println!("✓ S3 connection successful");

    Ok(StoreChoice::S3 {
        local_cache: default_local_path.to_path_buf(),
        s3: S3Config {
            endpoint,
            bucket_name,
            access_key,
            secret_key,
            region,
        },
    })
}

#[allow(clippy::too_many_arguments)]
fn build_config(
    store_path: &Path,
    registry_path: &Path,
    identity_path: &Path,
    main_public: &str,
    recovery_public: &str,
    node_secret_path: &Path,
    store_choice: &StoreChoice,
    vault_root_path: &Path,
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
    };

    let blob_stores = match store_choice {
        StoreChoice::S3 { .. } => r#"["local", "s3"]"#,
        StoreChoice::Local { .. } => r#"["local"]"#,
    };

    format!(
        r#"# vup configuration — generated by `vup init`

[identity]
secret_key_file = "{node_secret}"
encrypted_with = "main"

{store_section}

[registry.default]
type = "redb"
path = "{registry}"

[key.main]
public_key = "{main_pub}"
identity_file = "{identity}"

[key.recovery]
public_key = "{recovery_pub}"

[vault.backup]
root_path = "{vault_root}"
key = "main"
blob_stores = {blob_stores}

[source.default]
paths = []
"#,
        node_secret = node_secret_path.display(),
        store_section = store_section,
        registry = registry_path.display(),
        main_pub = main_public,
        identity = identity_path.display(),
        recovery_pub = recovery_public,
        blob_stores = blob_stores,
        vault_root = vault_root_path.display(),
    )
}
