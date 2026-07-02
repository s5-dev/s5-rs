//! End-to-end backup/restore against a real indexd.
//!
//! Flow:
//!   1. Load (or generate) a 32-byte identity master secret, then load
//!      (or register) the AppKey. On first run the AppKey is obtained
//!      via a one-time OAuth round (prints a URL) using a mnemonic
//!      *secretly derived* from the master secret, and cached.
//!   2. Upload a small blob, read it back, and `provide()` a share URL.
//!
//! The crate's `auth` is filesystem-free, so this example does its own
//! tiny file persistence. In a real node the master secret lives in the
//! cold `identity_secrets` vault and the AppKey in the warm `stores` vault
//! (both age-encrypted), not loose files.
//!
//! Running (defaults to https://sia.storage):
//! ```sh
//! cargo run --example end_to_end -p s5_store_indexd
//! ```
//!
//! Env overrides: `INDEXD_URL`, `INDEXD_SECRET`, `INDEXD_APPKEY`,
//! `INDEXD_POINTERS_DIR`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use bytes::Bytes;
use rand::Rng;
use s5_core::blob::location::BlobLocation;
use s5_core::store::Store;
use s5_store_indexd::{DEFAULT_INDEXER_URL, IndexdConfig, IndexdStore, auth};
use s5_store_local::LocalStore;

#[tokio::main]
async fn main() -> Result<()> {
    let indexer_url =
        std::env::var("INDEXD_URL").unwrap_or_else(|_| DEFAULT_INDEXER_URL.to_string());
    let secret_path = env_path("INDEXD_SECRET", ".config/s5/keys/indexd_master_secret.bin");
    let appkey_path = env_path("INDEXD_APPKEY", ".config/s5/keys/indexd_appkey.bin");
    let cache_dir = env_path("INDEXD_CACHE_DIR", ".config/s5/keys/indexd_cache");

    let master_secret = load_or_create_secret(&secret_path)?;

    // Reuse a cached AppKey if the indexer still recognises it; otherwise
    // register (one-time OAuth) with the secret-derived mnemonic.
    let app_key = match load_secret_file(&appkey_path)? {
        Some(k) if auth::connect(&indexer_url, k, None).await? => {
            eprintln!("reusing cached AppKey");
            k
        }
        _ => {
            eprintln!("registering with {indexer_url} …");
            let k = auth::register(&indexer_url, &master_secret, None, |url| {
                eprintln!("\n=== visit this URL to authorise the app ===");
                eprintln!("{url}");
                eprintln!("===========================================\n");
            })
            .await?;
            save_secret_file(&appkey_path, &k)?;
            k
        }
    };

    // A single durable cache (one `path -> capability` map) persists across
    // restarts. Pass a `MemoryStore` instead for an ephemeral cache rebuilt
    // from the indexer via `reconstruct_from_indexer`.
    let cache = LocalStore::new(&cache_dir);
    let config = IndexdConfig {
        indexer_url: indexer_url.clone(),
        ..IndexdConfig::default()
    };
    let store = IndexdStore::open(config, app_key, cache, None).await?;

    let blob = Bytes::from_static(b"hello from end_to_end.rs");
    let path = "blob3/end-to-end-demo";
    eprintln!("uploading {} bytes …", blob.len());
    store.put_bytes(path, blob.clone()).await?;

    eprintln!("reading back …");
    let read = store.open_read_bytes(path, 0, None).await?;
    assert_eq!(read, blob, "uploaded and downloaded bytes must match");
    eprintln!("✓ roundtrip matched ({} bytes)", read.len());

    for loc in store.provide(path).await? {
        match loc {
            BlobLocation::Url(u) => {
                let preview = u.split('?').next().unwrap_or(u.as_str());
                eprintln!("share url: {preview}…");
            }
            other => eprintln!("location: {other:?}"),
        }
    }
    eprintln!("\nE2E backup/restore cycle succeeded.");
    Ok(())
}

fn env_path(var: &str, rel: &str) -> PathBuf {
    std::env::var_os(var)
        .map(PathBuf::from)
        .unwrap_or_else(|| home_path(rel))
}

fn home_path(rel: &str) -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/root"));
    home.join(rel)
}

/// Load a 32-byte secret from `path`, generating + persisting a random
/// one (the identity master secret) on first run.
fn load_or_create_secret(path: &Path) -> Result<[u8; 32]> {
    if let Some(bytes) = load_secret_file(path)? {
        return Ok(bytes);
    }
    let mut secret = [0u8; 32];
    rand::rng().fill_bytes(&mut secret);
    save_secret_file(path, &secret)?;
    eprintln!(
        "generated a new identity master secret at {}",
        path.display()
    );
    Ok(secret)
}

fn load_secret_file(path: &Path) -> Result<Option<[u8; 32]>> {
    match std::fs::read(path) {
        Ok(raw) => raw
            .as_slice()
            .try_into()
            .map(Some)
            .map_err(|_| anyhow::anyhow!("{} is not 32 bytes", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

fn save_secret_file(path: &Path, bytes: &[u8; 32]) -> Result<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    std::fs::write(path, bytes).with_context(|| format!("writing {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}
