//! End-to-end backup/restore against a real indexd.
//!
//! Flow:
//!   1. Connect or register against `INDEXD_URL` — first run prints an
//!      OAuth URL, subsequent runs reuse the cached AppKey from
//!      `~/.config/s5/keys/indexd_appkey.bin`.
//!   2. Upload a small blob via `IndexdStore::put_bytes`.
//!   3. Read it back via `IndexdStore::open_read_bytes`.
//!   4. Call `IndexdStore::provide` to surface a signed share URL.
//!
//! Stores wiring (example chooses for the user):
//!   - **pointers**: `LocalStore` rooted at
//!     `~/.config/s5/keys/indexd_pointers/`. 72-byte records per
//!     blob; durable across restarts.
//!   - **metadata**: in-memory `BlobStore` over `MemoryStore`.
//!     SealedObject cache; misses fall through to
//!     `Sdk::object(object_id)` and write-through into the cache.
//!
//! Running (defaults to https://sia.storage):
//! ```sh
//! cargo run --example end_to_end -p s5_store_indexd
//! ```
//!
//! Overrides via env: `INDEXD_URL`, `APPKEY_VAULT`,
//! `INDEXD_POINTERS_DIR`.

use std::path::PathBuf;

use anyhow::Result;
use bytes::Bytes;
use s5_core::blob::location::BlobLocation;
use s5_core::blob::store::BlobStore;
use s5_core::store::Store;
use s5_store_indexd::{
    DEFAULT_INDEXER_URL, IndexdConfig, IndexdStore,
    auth::{AppKeyVault, connect_or_register},
};
use s5_store_local::LocalStore;
use s5_store_memory::MemoryStore;

#[tokio::main]
async fn main() -> Result<()> {
    let indexer_url =
        std::env::var("INDEXD_URL").unwrap_or_else(|_| DEFAULT_INDEXER_URL.to_string());
    let appkey_vault_path = std::env::var("APPKEY_VAULT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| home_path(".config/s5/keys/indexd_appkey.bin"));
    let pointers_dir = std::env::var("INDEXD_POINTERS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| home_path(".config/s5/keys/indexd_pointers"));

    let vault = AppKeyVault::new(&appkey_vault_path, Vec::new(), Vec::new());

    let sdk = connect_or_register(&indexer_url, &vault, |url| {
        eprintln!("\n=== visit this URL to authorise the app ===");
        eprintln!("{url}");
        eprintln!("===========================================\n");
    })
    .await?;

    // Pointers persist across restarts so we can find previously
    // uploaded blobs by their caller-path on the next run.
    let pointers = LocalStore::new(&pointers_dir);

    // SealedObject cache lives in memory for this example. On a fresh
    // process the cache starts empty and the first read of each blob
    // re-fetches the SealedObject from indexd via `Sdk::object` (with
    // write-through into this in-memory cache for subsequent reads).
    let metadata = BlobStore::without_outboard(MemoryStore::new());

    let config = IndexdConfig {
        indexer_url: indexer_url.clone(),
        ..IndexdConfig::default()
    };
    let store = IndexdStore::with_config(sdk, pointers, metadata, config);

    let blob = Bytes::from_static(b"hello from end_to_end.rs");
    let path = "blob3/end-to-end-demo";
    eprintln!("uploading {} bytes …", blob.len());
    store.put_bytes(path, blob.clone()).await?;

    eprintln!("reading back …");
    let read = store.open_read_bytes(path, 0, None).await?;
    assert_eq!(read, blob, "uploaded and downloaded bytes must match");
    eprintln!("✓ roundtrip matched ({} bytes)", read.len());

    let locations = store.provide(path).await?;
    eprintln!("got {} location(s) from provide():", locations.len());
    for loc in &locations {
        match loc {
            BlobLocation::Url(u) => {
                let preview = u.split('?').next().unwrap_or(u.as_str());
                eprintln!("  - Url(<{preview}>)");
            }
            other => eprintln!("  - {other:?}"),
        }
    }

    eprintln!("\nE2E backup/restore cycle succeeded.");
    Ok(())
}

fn home_path(rel: &str) -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/root"));
    home.join(rel)
}
