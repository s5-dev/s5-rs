//! S5 WebAssembly bindings for browser clients.
//!
//! This crate provides a WASM-compatible S5 client that can:
//! - Generate and recover from BIP39 seed phrases
//! - Derive both iroh node identity and FS encryption keys from the seed
//! - Connect to a remote S5 node via iroh (through relay)
//! - Perform encrypted file operations on FS5

use wasm_bindgen::prelude::*;

mod client;

// Re-export from s5_client with WASM wrappers
pub use client::*;

/// Initialize console logging for debugging
#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();
}

// Expose crypto functions from s5_client

/// Generate a new 12-word BIP39 mnemonic seed phrase
#[wasm_bindgen]
pub fn generate_seed_phrase() -> Result<String, JsError> {
    // For WASM, we need to generate entropy ourselves using getrandom
    let mut entropy = [0u8; 16]; // 128 bits = 12 words
    getrandom::getrandom(&mut entropy).map_err(|e| JsError::new(&format!("RNG failed: {}", e)))?;

    use bip39::{Language, Mnemonic};
    let mnemonic = Mnemonic::from_entropy_in(Language::English, &entropy)
        .map_err(|e| JsError::new(&format!("Mnemonic generation failed: {}", e)))?;
    Ok(mnemonic.to_string())
}

/// Validate a BIP39 seed phrase
#[wasm_bindgen]
pub fn validate_seed_phrase(phrase: &str) -> bool {
    s5_client::keys::validate_seed_phrase(phrase)
}

/// Compute BLAKE3 hash of data
#[wasm_bindgen]
pub fn hash_blake3(data: &[u8]) -> Vec<u8> {
    s5_client::crypto::hash_blake3(data).to_vec()
}

/// Encrypt data with XChaCha20-Poly1305
///
/// Returns: nonce (24 bytes) || ciphertext
#[wasm_bindgen]
pub fn encrypt_xchacha20poly1305(key: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, JsError> {
    if key.len() != 32 {
        return Err(JsError::new("Key must be 32 bytes"));
    }

    // Generate random nonce
    let mut nonce = [0u8; 24];
    getrandom::getrandom(&mut nonce).map_err(|e| JsError::new(&format!("RNG failed: {}", e)))?;

    let ciphertext =
        s5_client::crypto::encrypt_xchacha20poly1305_with_nonce(key, &nonce, plaintext)
            .map_err(|e| JsError::new(&format!("Encryption failed: {}", e)))?;

    let mut result = Vec::with_capacity(24 + ciphertext.len());
    result.extend_from_slice(&nonce);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

/// Decrypt data with XChaCha20-Poly1305
///
/// Input: nonce (24 bytes) || ciphertext
#[wasm_bindgen]
pub fn decrypt_xchacha20poly1305(key: &[u8], data: &[u8]) -> Result<Vec<u8>, JsError> {
    s5_client::crypto::decrypt_xchacha20poly1305(key, data)
        .map_err(|e| JsError::new(&format!("{}", e)))
}

/// Decrypt a chunk with XChaCha20-Poly1305 using chunk index as nonce
///
/// This is the format used by FS5 encrypted files:
/// - Nonce is derived from chunk index (little-endian, 24 bytes)
/// - No nonce prefix in the ciphertext
#[wasm_bindgen]
pub fn decrypt_chunk_xchacha20poly1305(
    key: &[u8],
    chunk_index: u32,
    ciphertext: &[u8],
) -> Result<Vec<u8>, JsError> {
    s5_client::crypto::decrypt_chunk(key, chunk_index as u64, ciphertext)
        .map_err(|e| JsError::new(&format!("{}", e)))
}
