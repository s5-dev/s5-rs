//! Store-backend selection, shared by every verb that has to stand up a
//! `[store.*]` entry: `onboard` (first store), `recover` / `device join`
//! (re-create the durable store to read the config vault), and
//! `store add` (a new named store against the live daemon).
//!
//! A [`StoreChoice`] is the backend-agnostic result of collecting a
//! store's config — from flags, from the interactive wizard, or rebuilt
//! from a synced `[store.*]`. It converts to a
//! [`s5_node::config::NodeConfigStore`] (→ `serde_json` → a
//! `patch_config` op, or a live store via `create_raw_store`).

use anyhow::{Context, Result, bail};

/// S3 credentials collected from the user.
pub(crate) struct S3Config {
    pub(crate) endpoint: String,
    pub(crate) bucket_name: String,
    pub(crate) access_key: String,
    pub(crate) secret_key: String,
    pub(crate) region: String,
}

/// A store backend the user has chosen and fully specified. Shared by
/// `onboard` (writes config), `recover`/`device join` (re-creates the
/// durable store to read the config vault), and `store add` (patches a
/// new `[store.*]` into the live config).
pub(crate) enum StoreChoice {
    Local {
        path: std::path::PathBuf,
    },
    S3 {
        local_cache: std::path::PathBuf,
        s3: S3Config,
    },
    /// Sia via an indexd service. `app_key` is the 32-byte AppKey returned by
    /// the one-time OAuth registration; it is written inline into the config.
    Sia {
        cache_path: std::path::PathBuf,
        indexer_url: String,
        app_key: [u8; 32],
    },
}

impl StoreChoice {
    /// The local directory this choice needs created — the local store path, or
    /// the indexd index/capability cache for Sia.
    pub(crate) fn local_path(&self) -> &std::path::Path {
        match self {
            StoreChoice::Local { path } => path,
            StoreChoice::S3 { local_cache, .. } => local_cache,
            StoreChoice::Sia { cache_path, .. } => cache_path,
        }
    }

    /// The inline `NodeConfigStore` for this choice, so callers can build a
    /// live store (via `create_raw_store`) or serialise it into a
    /// `patch_config /store/<name>` op. For Sia the re-OAuth re-derives the
    /// *same* AppKey, so this matches the published config byte for byte.
    pub(crate) fn to_node_config_store(&self) -> s5_node::config::NodeConfigStore {
        use s5_node::config::{IndexdStoreConfig, NodeConfigStore, NodeConfigStoreBackend};
        let backend = match self {
            StoreChoice::Local { path } => {
                NodeConfigStoreBackend::Local(s5_store_local::LocalStoreConfig {
                    base_path: path.to_string_lossy().into_owned(),
                })
            }
            StoreChoice::S3 { s3, .. } => {
                NodeConfigStoreBackend::S3(s5_store_s3::S3StoreConfig::new(
                    s3.endpoint.clone(),
                    s3.bucket_name.clone(),
                    s3.access_key.clone(),
                    s3.secret_key.clone(),
                    s3.region.clone(),
                ))
            }
            StoreChoice::Sia {
                indexer_url,
                app_key,
                cache_path,
            } => NodeConfigStoreBackend::Indexd(IndexdStoreConfig {
                indexer_url: indexer_url.clone(),
                account: String::new(),
                app_key: hex::encode(app_key),
                cache_path: cache_path.to_string_lossy().into_owned(),
                ..Default::default()
            }),
        };
        NodeConfigStore::from_backend(backend)
    }
}

/// Rebuild a `StoreChoice` from a **synced** `NodeConfigStore` — the
/// shape the enroll grant (and the config vault) carries. The inverse
/// of [`StoreChoice::to_node_config_store`] for the backends `onboard`
/// can produce, with device-local paths (the Sia index cache, the S3
/// local cache) re-rooted at THIS device's data dir instead of the
/// inviter's. A `local` backend passes through as-is: it only makes
/// sense when both devices share a filesystem (dev/test), and there is
/// no better path to guess.
pub(crate) fn store_choice_from_synced(
    store: &s5_node::config::NodeConfigStore,
    default_local_path: &std::path::Path,
) -> Result<StoreChoice> {
    use s5_node::config::NodeConfigStoreBackend as B;
    match &store.backend {
        B::Local(cfg) => Ok(StoreChoice::Local {
            path: std::path::PathBuf::from(&cfg.base_path),
        }),
        B::S3(cfg) => {
            // The s3 crate keeps its config fields private; round-trip
            // through serde_json (the same encoding the config vault uses).
            let v = serde_json::to_value(cfg).context("encoding synced S3 store config")?;
            let field = |name: &str| -> Result<String> {
                v.get(name)
                    .and_then(|s| s.as_str())
                    .map(str::to_string)
                    .ok_or_else(|| anyhow::anyhow!("synced S3 store config missing '{name}'"))
            };
            Ok(StoreChoice::S3 {
                local_cache: default_local_path.to_path_buf(),
                s3: S3Config {
                    endpoint: field("endpoint")?,
                    bucket_name: field("bucket_name")?,
                    access_key: field("access_key")?,
                    secret_key: field("secret_key")?,
                    region: field("region").unwrap_or_default(),
                },
            })
        }
        B::Indexd(cfg) => {
            let bytes = hex::decode(cfg.app_key.trim()).context("synced indexd app_key hex")?;
            let app_key: [u8; 32] = bytes.try_into().map_err(|v: Vec<u8>| {
                anyhow::anyhow!("app_key must be 32 bytes, got {}", v.len())
            })?;
            Ok(StoreChoice::Sia {
                cache_path: default_local_path.with_file_name("indexd-cache"),
                indexer_url: cfg.indexer_url.clone(),
                app_key,
            })
        }
        other => bail!(
            "unsupported bootstrap store type for device join: {other:?} \
             (expected local, s3, or indexd)"
        ),
    }
}

/// Ask the user which backend to store backups on, then collect its
/// details. The `onboard`/`recover` entry point — the menu picks the
/// backend, then delegates to the same per-backend collectors
/// `store add <backend>` uses.
pub(crate) async fn ask_store_type(
    default_local_path: &std::path::Path,
    stores_seed: &[u8; 32],
) -> Result<StoreChoice> {
    use crate::interact;

    let choices = &[
        "Local (this machine)",
        "S3-compatible storage",
        "Sia (decentralized, via indexd)",
    ];
    let selection = interact::select("Where do you want to store backups?", choices, 0)?;

    match selection {
        0 => ask_local_store(default_local_path),
        2 => ask_sia_store(default_local_path, stores_seed).await,
        _ => ask_s3_store(default_local_path).await,
    }
}

/// Collect a local store path (default offered, Enter accepts).
pub(crate) fn ask_local_store(default_local_path: &std::path::Path) -> Result<StoreChoice> {
    let path = crate::interact::input_with_default(
        "Local store path",
        default_local_path.to_string_lossy().into_owned(),
    )?;
    Ok(StoreChoice::Local {
        path: std::path::PathBuf::from(path),
    })
}

/// Collect S3 credentials interactively and validate them with a live
/// `list` before returning.
pub(crate) async fn ask_s3_store(default_local_path: &std::path::Path) -> Result<StoreChoice> {
    use crate::interact;

    let endpoint = interact::input_required("Endpoint URL")?;
    let bucket_name = interact::input_required("Bucket name")?;
    let region = interact::input_with_default("Region", "us-east-1".into())?;
    let access_key = interact::input_required("Access Key ID")?;
    let secret_key = interact::input_required("Secret Access Key")?;

    build_and_test_s3(
        default_local_path,
        S3Config {
            endpoint,
            bucket_name,
            access_key,
            secret_key,
            region,
        },
    )
    .await
}

/// Build an S3 `StoreChoice` from already-collected credentials, first
/// validating endpoint + credentials + bucket access with a live `list`
/// (the same probe `onboard` runs). Shared by the interactive wizard and
/// the flag-driven `store add s3` path.
pub(crate) async fn build_and_test_s3(
    default_local_path: &std::path::Path,
    s3: S3Config,
) -> Result<StoreChoice> {
    use s5_core::store::Store;
    use tokio_stream::StreamExt;

    println!("Testing S3 connection...");
    let s3_cfg = s5_store_s3::S3StoreConfig::new(
        s3.endpoint.clone(),
        s3.bucket_name.clone(),
        s3.access_key.clone(),
        s3.secret_key.clone(),
        s3.region.clone(),
    );
    let store = s5_store_s3::S3Store::create(s3_cfg);

    // A `list` validates endpoint, credentials, and bucket access.
    let mut stream = store
        .list()
        .await
        .context("S3 connection failed. Check endpoint, bucket, and credentials.")?;
    let _first = stream.next().await;
    println!("✓ S3 connection successful");

    Ok(StoreChoice::S3 {
        local_cache: default_local_path.to_path_buf(),
        s3,
    })
}

/// The application identity Vup presents in the indexer's OAuth approval
/// dialog: the **S5 app id** ([`s5_store_indexd::app_id`], so the account and
/// AppKey derivation are unchanged) with Vup's name, site, and logo. Only the
/// presentational fields differ from [`s5_store_indexd::auth::app_metadata`];
/// the id is load-bearing and must not change.
pub fn vup_app_metadata() -> s5_store_indexd::AppMetadata {
    s5_store_indexd::AppMetadata {
        id: s5_store_indexd::app_id(),
        name: "Vup Vault",
        description: "Content-addressed backup, sync, and archive with end-to-end encryption, \
                      versioned snapshots, and secure sharing.",
        service_url: "https://vup.app",
        logo_url: Some("https://vup.app/android-chrome-512x512.png"),
        callback_url: None,
    }
}

/// Run the one-time indexd OAuth registration and collect the Sia store config.
///
/// The registration secret is the **managed storage secret** derived from
/// `stores_seed ‖ "sia"` (mnemonic-derivation.md § Layer C) — never the cold
/// identity master. The same derivation re-creates this AppKey on recovery.
pub(crate) async fn ask_sia_store(
    default_local_path: &std::path::Path,
    stores_seed: &[u8; 32],
) -> Result<StoreChoice> {
    let indexer_url =
        crate::interact::input_with_default("Indexer URL", "https://sia.storage".to_string())?;
    register_sia(default_local_path, stores_seed, indexer_url).await
}

/// The OAuth-registration tail of [`ask_sia_store`], with the indexer URL
/// already resolved (a flag, or the interactive default). Shared with the
/// flag-driven `store add sia` path.
pub(crate) async fn register_sia(
    default_local_path: &std::path::Path,
    stores_seed: &[u8; 32],
    indexer_url: String,
) -> Result<StoreChoice> {
    // The store is named "sia"; that name is also the managed-account label.
    let secret = s5_node::mnemonic::managed_storage_secret(stores_seed, "indexd", "sia");

    println!();
    println!("Registering with the indexer — a one-time authorization.");
    let app_key =
        s5_store_indexd::auth::register(&indexer_url, &secret, Some(vup_app_metadata()), |url| {
            println!();
            println!("  Open this URL in your browser to authorize Vup Vault, then return here:");
            println!("    {url}");
            println!();
            println!("  Waiting for approval…");
        })
        .await
        .context("indexd registration failed")?;
    println!("✓ Registered with the indexer");

    // The indexd index/capability cache lives beside the default local store.
    let cache_path = default_local_path.with_file_name("indexd-cache");
    Ok(StoreChoice::Sia {
        cache_path,
        indexer_url,
        app_key,
    })
}
