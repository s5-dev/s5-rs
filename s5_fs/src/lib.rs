//! # S5 File-system (FS5)
//!
//! High-level, *content addressed*, optionally *encrypted* directory tree
//! Everything is an immutable [`DirV1`] snapshot; mutability is simulated
//! through **actors** that rewrite parent snapshots atomically.
//!
//! ## Layers
//! 1. `dir`   – pure data structures (CBOR encoded).  
//! 2. `actor` – single-threaded state machine that owns one directory
//!    snapshot and processes commands sequentially.  
//! 3. `api`   – ergonomic façade (`FS5`) that applications use.  
//! 4. `context` – wiring (blob-store, registry, encryption keys …).  

mod actor;
mod api;
mod context;
pub mod debug;
pub mod dir;
pub mod gc;
pub mod snapshots;
mod spawn;

pub use api::{CursorKind, FS5};
pub use context::{DirContext, DirContextParentLink, SigningKey};
pub use dir::FileRef;

/// Number of entries sharing a path prefix
/// required before FS5 auto-promotes that prefix
/// into a dedicated subdirectory.
pub const FS5_PROMOTION_THRESHOLD: usize = 10;

/// Crate-wide result alias that bubbles up [`anyhow::Error`].
pub type FSResult<T> = anyhow::Result<T>;

/// Derives sync keys (encryption key, signing key, public key) from a shared secret.
///
/// This is the canonical derivation used by all S5 clients (native and WASM).
///
/// # Key Derivation
/// - `encryption_key` = BLAKE3 derive_key("s5/fs/sync/xchacha20", secret)
/// - `signing_key` = BLAKE3 derive_key("s5/fs/sync/ed25519", secret)
/// - `public_key` = Ed25519 public key from signing_key
///
/// # Returns
/// Tuple of (encryption_key, signing_key_bytes, public_key) as `[u8; 32]` arrays.
pub fn derive_sync_keys(secret: &[u8]) -> ([u8; 32], [u8; 32], [u8; 32]) {
    use blake3::derive_key;
    use ed25519_dalek::SigningKey as Ed25519SigningKey;

    let encryption_key = derive_key("s5/fs/sync/xchacha20", secret);
    let signing_key_bytes = derive_key("s5/fs/sync/ed25519", secret);
    let signing_key = Ed25519SigningKey::from_bytes(&signing_key_bytes);
    let public_key: [u8; 32] = *signing_key.verifying_key().as_bytes();

    (encryption_key, signing_key_bytes, public_key)
}
