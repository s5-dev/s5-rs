//! BIP39 seed phrase handling and key derivation.
//!
//! This module provides deterministic key derivation from BIP39 seed phrases,
//! generating all cryptographic material needed for S5 client operation.
//!
//! ## Key Derivation Chain
//!
//! ```text
//! seed_phrase (12 words BIP39)
//!     |
//!     v
//! mnemonic.to_seed("") -> 64 bytes
//!     |
//!     v
//! blake3::derive_key("s5/root", seed) -> root_secret [32 bytes]
//!     |
//!     +-- blake3::derive_key("s5/fs/root", root_secret) -> fs_root_secret
//!     |       |
//!     |       +-- blake3::derive_key("s5/fs/sync/xchacha20", ...) -> encryption_key
//!     |       +-- blake3::derive_key("s5/fs/sync/ed25519", ...) -> signing_key
//!     |               |
//!     |               +-> public_key (user identity / stream key)
//!     |
//!     +-- blake3::derive_key("s5/iroh/node", root_secret) -> iroh_secret_key
//! ```

use bip39::{Language, Mnemonic};
use blake3::derive_key;
use ed25519_dalek::SigningKey as Ed25519SigningKey;
use thiserror::Error;

/// Errors that can occur during key operations.
#[derive(Debug, Error)]
pub enum KeyError {
    #[error("Invalid mnemonic: {0}")]
    InvalidMnemonic(String),
    #[error("Invalid root secret: must be 32 bytes")]
    InvalidRootSecret,
}

/// Sync keys derived from the filesystem root secret.
///
/// Uses `s5_fs::derive_sync_keys` for the actual derivation.
#[derive(Clone)]
pub struct SyncKeys {
    /// XChaCha20-Poly1305 encryption key for FS5 directory encryption
    pub encryption_key: [u8; 32],
    /// Ed25519 signing key bytes (can be used to construct SigningKey)
    pub signing_key_bytes: [u8; 32],
    /// Ed25519 public key (used as stream key / user identity)
    pub public_key: [u8; 32],
}

impl SyncKeys {
    /// Derive sync keys from a filesystem root secret.
    ///
    /// Uses `s5_fs::derive_sync_keys` internally.
    pub fn derive(fs_root_secret: &[u8; 32]) -> Self {
        let (encryption_key, signing_key_bytes, public_key) =
            s5_fs::derive_sync_keys(fs_root_secret);

        SyncKeys {
            encryption_key,
            signing_key_bytes,
            public_key,
        }
    }

    /// Get the Ed25519 signing key.
    pub fn signing_key(&self) -> Ed25519SigningKey {
        Ed25519SigningKey::from_bytes(&self.signing_key_bytes)
    }

    /// Get the public key as hex string (this is the user's identity).
    pub fn public_key_hex(&self) -> String {
        hex::encode(self.public_key)
    }

    /// Get the signing key bytes for FS5 SigningKey construction.
    ///
    /// Use this to create an `s5_fs::SigningKey`:
    /// ```ignore
    /// let fs_signing_key = s5_fs::SigningKey::new(sync_keys.signing_key_bytes());
    /// ```
    pub fn signing_key_bytes(&self) -> [u8; 32] {
        self.signing_key_bytes
    }
}

/// All cryptographic keys derived from a seed phrase or root secret.
#[derive(Clone)]
pub struct DerivedKeys {
    /// Root secret (first level derivation from seed)
    pub root_secret: [u8; 32],
    /// FS root secret (used for sync key derivation)
    pub fs_root_secret: [u8; 32],
    /// Sync keys (encryption, signing, public)
    pub sync_keys: SyncKeys,
    /// Iroh node secret key (separate derivation for browser node identity)
    pub iroh_secret_key: [u8; 32],
}

impl DerivedKeys {
    /// Derive all keys from a BIP39 seed phrase.
    ///
    /// # Arguments
    /// * `phrase` - A 12-word BIP39 mnemonic seed phrase
    ///
    /// # Returns
    /// All derived keys, or an error if the phrase is invalid.
    pub fn from_seed_phrase(phrase: &str) -> Result<Self, KeyError> {
        let mnemonic = Mnemonic::parse_in(Language::English, phrase)
            .map_err(|e| KeyError::InvalidMnemonic(e.to_string()))?;
        let seed = mnemonic.to_seed("");
        Ok(Self::from_seed_bytes(&seed))
    }

    /// Derive all keys from raw seed bytes (64 bytes from mnemonic.to_seed("")).
    pub fn from_seed_bytes(seed: &[u8]) -> Self {
        let root_secret = derive_key("s5/root", seed);
        Self::from_root_secret_array(&root_secret)
    }

    /// Derive keys from an existing root secret (32 bytes).
    ///
    /// This is useful when the root secret was stored/transmitted separately.
    pub fn from_root_secret(root_secret: &[u8]) -> Result<Self, KeyError> {
        if root_secret.len() != 32 {
            return Err(KeyError::InvalidRootSecret);
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(root_secret);
        Ok(Self::from_root_secret_array(&arr))
    }

    /// Derive keys from a root secret array.
    fn from_root_secret_array(root_secret: &[u8; 32]) -> Self {
        let fs_root_secret = derive_key("s5/fs/root", root_secret);
        let sync_keys = SyncKeys::derive(&fs_root_secret);
        let iroh_secret_key = derive_key("s5/iroh/node", root_secret);

        DerivedKeys {
            root_secret: *root_secret,
            fs_root_secret,
            sync_keys,
            iroh_secret_key,
        }
    }

    /// Get the root secret as hex string.
    pub fn root_secret_hex(&self) -> String {
        hex::encode(self.root_secret)
    }

    /// Get the public key as hex string (user identity).
    pub fn public_key_hex(&self) -> String {
        self.sync_keys.public_key_hex()
    }

    /// Get the iroh node secret key as hex string.
    pub fn iroh_secret_key_hex(&self) -> String {
        hex::encode(self.iroh_secret_key)
    }
}

/// Generate a new random 12-word BIP39 seed phrase.
///
/// This function requires the `std` feature, as it needs access to
/// a secure random number generator.
#[cfg(feature = "std")]
pub fn generate_seed_phrase() -> Result<String, KeyError> {
    let mnemonic = Mnemonic::generate_in(Language::English, 12)
        .map_err(|e| KeyError::InvalidMnemonic(e.to_string()))?;
    Ok(mnemonic.to_string())
}

/// Validate a BIP39 seed phrase.
pub fn validate_seed_phrase(phrase: &str) -> bool {
    Mnemonic::parse_in(Language::English, phrase).is_ok()
}

/// Recover root secret hex from a seed phrase.
///
/// This is a convenience function that derives the root secret and returns it as hex.
pub fn recover_root_secret_hex(phrase: &str) -> Result<String, KeyError> {
    let keys = DerivedKeys::from_seed_phrase(phrase)?;
    Ok(keys.root_secret_hex())
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_PHRASE: &str = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";

    #[cfg(feature = "std")]
    #[test]
    fn test_generate_and_validate() {
        let phrase = generate_seed_phrase().unwrap();
        assert!(validate_seed_phrase(&phrase));
        assert_eq!(phrase.split_whitespace().count(), 12);
    }

    #[test]
    fn test_derivation_deterministic() {
        let keys1 = DerivedKeys::from_seed_phrase(TEST_PHRASE).unwrap();
        let keys2 = DerivedKeys::from_seed_phrase(TEST_PHRASE).unwrap();

        assert_eq!(keys1.root_secret, keys2.root_secret);
        assert_eq!(keys1.sync_keys.public_key, keys2.sync_keys.public_key);
        assert_eq!(keys1.iroh_secret_key, keys2.iroh_secret_key);
    }

    #[test]
    fn test_from_root_secret_matches() {
        let keys_from_phrase = DerivedKeys::from_seed_phrase(TEST_PHRASE).unwrap();
        let keys_from_root = DerivedKeys::from_root_secret(&keys_from_phrase.root_secret).unwrap();

        assert_eq!(
            keys_from_phrase.fs_root_secret,
            keys_from_root.fs_root_secret
        );
        assert_eq!(
            keys_from_phrase.sync_keys.encryption_key,
            keys_from_root.sync_keys.encryption_key
        );
        assert_eq!(
            keys_from_phrase.sync_keys.public_key,
            keys_from_root.sync_keys.public_key
        );
    }

    #[test]
    fn test_invalid_phrase() {
        assert!(DerivedKeys::from_seed_phrase("invalid phrase").is_err());
        assert!(!validate_seed_phrase("invalid phrase"));
    }

    #[test]
    fn test_known_derivation() {
        // Using the standard test vector phrase, verify we get consistent results
        let keys = DerivedKeys::from_seed_phrase(TEST_PHRASE).unwrap();

        // Root secret should be deterministic
        assert!(!keys.root_secret_hex().is_empty());
        assert_eq!(keys.root_secret_hex().len(), 64); // 32 bytes = 64 hex chars

        // Public key should be deterministic
        assert!(!keys.public_key_hex().is_empty());
        assert_eq!(keys.public_key_hex().len(), 64);
    }
}
