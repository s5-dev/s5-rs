//! Cryptographic primitives for S5 client.
//!
//! Provides encryption/decryption using XChaCha20-Poly1305 and BLAKE3 hashing.

use chacha20poly1305::{
    XChaCha20Poly1305,
    aead::{Aead, KeyInit},
};
use thiserror::Error;

/// Errors that can occur during cryptographic operations.
#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("Invalid key length: expected 32 bytes")]
    InvalidKeyLength,
    #[error("Data too short: expected at least {0} bytes")]
    DataTooShort(usize),
    #[error("Encryption failed")]
    EncryptionFailed,
    #[error("Decryption failed: invalid ciphertext or wrong key")]
    DecryptionFailed,
    #[error("Random number generation failed")]
    RngFailed,
}

/// Compute BLAKE3 hash of data.
///
/// Returns a 32-byte hash.
pub fn hash_blake3(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}

/// Generate a random 32-byte encryption key.
///
/// This function is only available when the `std` feature is enabled.
#[cfg(feature = "std")]
pub fn generate_key() -> Result<[u8; 32], CryptoError> {
    let mut key = [0u8; 32];
    getrandom::getrandom(&mut key).map_err(|_| CryptoError::RngFailed)?;
    Ok(key)
}

/// Encrypt data with XChaCha20-Poly1305 (requires random nonce generation).
///
/// This function is only available when the `std` feature is enabled,
/// as it requires access to a secure random number generator.
///
/// # Arguments
/// * `key` - 32-byte encryption key
/// * `plaintext` - Data to encrypt
///
/// # Returns
/// `nonce (24 bytes) || ciphertext` or an error.
#[cfg(feature = "std")]
pub fn encrypt_xchacha20poly1305(key: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, CryptoError> {
    if key.len() != 32 {
        return Err(CryptoError::InvalidKeyLength);
    }

    let key_array: [u8; 32] = key.try_into().unwrap();
    let cipher = XChaCha20Poly1305::new(&key_array.into());

    // Generate random nonce
    let mut nonce = [0u8; 24];
    getrandom::getrandom(&mut nonce).map_err(|_| CryptoError::RngFailed)?;

    let ciphertext = cipher
        .encrypt(&nonce.into(), plaintext)
        .map_err(|_| CryptoError::EncryptionFailed)?;

    // Return nonce || ciphertext
    let mut result = Vec::with_capacity(24 + ciphertext.len());
    result.extend_from_slice(&nonce);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

/// Encrypt data with XChaCha20-Poly1305 using a provided nonce.
///
/// # Arguments
/// * `key` - 32-byte encryption key
/// * `nonce` - 24-byte nonce
/// * `plaintext` - Data to encrypt
///
/// # Returns
/// Ciphertext (without nonce prefix) or an error.
pub fn encrypt_xchacha20poly1305_with_nonce(
    key: &[u8],
    nonce: &[u8; 24],
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    if key.len() != 32 {
        return Err(CryptoError::InvalidKeyLength);
    }

    let key_array: [u8; 32] = key.try_into().unwrap();
    let cipher = XChaCha20Poly1305::new(&key_array.into());

    cipher
        .encrypt(nonce.into(), plaintext)
        .map_err(|_| CryptoError::EncryptionFailed)
}

/// Decrypt data with XChaCha20-Poly1305.
///
/// # Arguments
/// * `key` - 32-byte encryption key
/// * `data` - `nonce (24 bytes) || ciphertext`
///
/// # Returns
/// Plaintext or an error.
pub fn decrypt_xchacha20poly1305(key: &[u8], data: &[u8]) -> Result<Vec<u8>, CryptoError> {
    if key.len() != 32 {
        return Err(CryptoError::InvalidKeyLength);
    }
    if data.len() < 24 {
        return Err(CryptoError::DataTooShort(24));
    }

    let key_array: [u8; 32] = key.try_into().unwrap();
    let cipher = XChaCha20Poly1305::new(&key_array.into());

    let nonce: [u8; 24] = data[..24].try_into().unwrap();
    let ciphertext = &data[24..];

    cipher
        .decrypt(&nonce.into(), ciphertext)
        .map_err(|_| CryptoError::DecryptionFailed)
}

/// Decrypt data with XChaCha20-Poly1305 using a provided nonce.
///
/// # Arguments
/// * `key` - 32-byte encryption key
/// * `nonce` - 24-byte nonce
/// * `ciphertext` - Encrypted data (without nonce prefix)
///
/// # Returns
/// Plaintext or an error.
pub fn decrypt_xchacha20poly1305_with_nonce(
    key: &[u8],
    nonce: &[u8; 24],
    ciphertext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    if key.len() != 32 {
        return Err(CryptoError::InvalidKeyLength);
    }

    let key_array: [u8; 32] = key.try_into().unwrap();
    let cipher = XChaCha20Poly1305::new(&key_array.into());

    cipher
        .decrypt(nonce.into(), ciphertext)
        .map_err(|_| CryptoError::DecryptionFailed)
}

/// Decrypt an FS5 encrypted chunk using chunk index as nonce.
///
/// FS5 uses chunk-based encryption where:
/// - Each chunk is encrypted separately
/// - The nonce is derived from the chunk index (little-endian, padded to 24 bytes)
/// - No nonce prefix in the ciphertext
///
/// # Arguments
/// * `key` - 32-byte encryption key
/// * `chunk_index` - Index of the chunk (0, 1, 2, ...)
/// * `ciphertext` - Encrypted chunk data
///
/// # Returns
/// Decrypted chunk data or an error.
pub fn decrypt_chunk(
    key: &[u8],
    chunk_index: u64,
    ciphertext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    if key.len() != 32 {
        return Err(CryptoError::InvalidKeyLength);
    }

    // Create nonce from chunk index (little-endian, padded to 24 bytes)
    let mut nonce = [0u8; 24];
    nonce[..8].copy_from_slice(&chunk_index.to_le_bytes());

    decrypt_xchacha20poly1305_with_nonce(key, &nonce, ciphertext)
}

/// Encrypt an FS5 chunk using chunk index as nonce.
///
/// # Arguments
/// * `key` - 32-byte encryption key
/// * `chunk_index` - Index of the chunk (0, 1, 2, ...)
/// * `plaintext` - Chunk data to encrypt
///
/// # Returns
/// Encrypted chunk data (without nonce prefix) or an error.
pub fn encrypt_chunk(
    key: &[u8],
    chunk_index: u64,
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptoError> {
    if key.len() != 32 {
        return Err(CryptoError::InvalidKeyLength);
    }

    // Create nonce from chunk index (little-endian, padded to 24 bytes)
    let mut nonce = [0u8; 24];
    nonce[..8].copy_from_slice(&chunk_index.to_le_bytes());

    encrypt_xchacha20poly1305_with_nonce(key, &nonce, plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_blake3() {
        let hash = hash_blake3(b"hello");
        assert_eq!(hash.len(), 32);

        // Same input should produce same hash
        let hash2 = hash_blake3(b"hello");
        assert_eq!(hash, hash2);

        // Different input should produce different hash
        let hash3 = hash_blake3(b"world");
        assert_ne!(hash, hash3);
    }

    #[test]
    fn test_chunk_encryption_roundtrip() {
        let key = [42u8; 32];
        let plaintext = b"Hello, S5!";

        let ciphertext = encrypt_chunk(&key, 0, plaintext).unwrap();
        let decrypted = decrypt_chunk(&key, 0, &ciphertext).unwrap();

        assert_eq!(plaintext.as_slice(), decrypted.as_slice());
    }

    #[test]
    fn test_chunk_index_matters() {
        let key = [42u8; 32];
        let plaintext = b"Hello, S5!";

        let ciphertext = encrypt_chunk(&key, 0, plaintext).unwrap();

        // Decrypting with wrong chunk index should fail
        let result = decrypt_chunk(&key, 1, &ciphertext);
        assert!(result.is_err());
    }

    #[test]
    fn test_nonce_encryption_roundtrip() {
        let key = [42u8; 32];
        let nonce = [1u8; 24];
        let plaintext = b"Hello, S5!";

        let ciphertext = encrypt_xchacha20poly1305_with_nonce(&key, &nonce, plaintext).unwrap();
        let decrypted = decrypt_xchacha20poly1305_with_nonce(&key, &nonce, &ciphertext).unwrap();

        assert_eq!(plaintext.as_slice(), decrypted.as_slice());
    }

    #[test]
    fn test_invalid_key_length() {
        let short_key = [0u8; 16];
        let result = encrypt_chunk(&short_key, 0, b"test");
        assert!(matches!(result, Err(CryptoError::InvalidKeyLength)));
    }
}
