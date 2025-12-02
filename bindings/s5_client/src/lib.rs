//! Shared client logic for S5 FFI bindings.
//!
//! This crate contains platform-agnostic code that is shared between:
//! - `s5_ffi` (UniFFI for native platforms: iOS, Android, desktop)
//! - `s5_wasm` (wasm-bindgen for browsers)
//!
//! ## Modules
//!
//! - [`keys`] - BIP39 seed phrase and key derivation
//! - [`crypto`] - Encryption/decryption primitives

pub mod crypto;
pub mod keys;

pub use crypto::{
    decrypt_chunk, decrypt_xchacha20poly1305, decrypt_xchacha20poly1305_with_nonce, encrypt_chunk,
    encrypt_xchacha20poly1305_with_nonce, hash_blake3,
};
pub use keys::{DerivedKeys, SyncKeys};

#[cfg(feature = "std")]
pub use crypto::encrypt_xchacha20poly1305;
