//! Recovery key generation and derivation using native age keys.
//!
//! Generates an age X25519 keypair. The secret key (`AGE-SECRET-KEY-1...`)
//! is the user's offline recovery secret — designed to be written on paper
//! (Bech32 avoids ambiguous characters, includes error-checking checksum).
//!
//! The public key (`age1...`) is stored in the node config as a
//! `[key.recovery]` entry.
//!
//! For disaster recovery, the `recovery_signing_key` function derives a
//! deterministic Ed25519 key from the paper key + vault name, used to look
//! up the vault's registry entry.

use age::secrecy::ExposeSecret;

// Re-export the recovery signing key derivation from s5_node.
#[cfg(test)]
use s5_node::tasks::publish::recovery_signing_key;

/// Generate a new age X25519 keypair for recovery.
///
/// Returns `(public_key, secret_key)` where:
/// - `public_key` is `"age1..."` — stored in config
/// - `secret_key` is `"AGE-SECRET-KEY-1..."` — shown once, written on paper
pub fn generate_recovery_key() -> (String, String) {
    let identity = age::x25519::Identity::generate();
    let public_key = identity.to_public().to_string();
    let secret_key = identity.to_string().expose_secret().to_string();
    (public_key, secret_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_keys_have_correct_prefixes() {
        let (pubkey, secret) = generate_recovery_key();
        assert!(pubkey.starts_with("age1"), "got: {pubkey}");
        assert!(secret.starts_with("AGE-SECRET-KEY-1"), "got: {secret}");
    }

    #[test]
    fn different_calls_produce_different_keys() {
        let (pub1, _) = generate_recovery_key();
        let (pub2, _) = generate_recovery_key();
        assert_ne!(pub1, pub2);
    }

    #[test]
    fn secret_key_can_roundtrip_to_identity() {
        let (pubkey, secret) = generate_recovery_key();
        let restored: age::x25519::Identity = secret.parse().expect("parse secret key");
        assert_eq!(restored.to_public().to_string(), pubkey);
    }

    #[test]
    fn recovery_signing_key_is_deterministic() {
        // v3: recovery_signing_key derives from the vault root's
        // KEY_SLOT_RECOVERY value (32 raw bytes), not from age secret +
        // vault name (see docs/reference/snapshot-publication.md
        // § Vault ID derivation).
        let recovery_secret = [42u8; 32];
        let k1 = recovery_signing_key(&recovery_secret);
        let k2 = recovery_signing_key(&recovery_secret);
        assert_eq!(k1.to_bytes(), k2.to_bytes());

        let other = [43u8; 32];
        let k3 = recovery_signing_key(&other);
        assert_ne!(k1.to_bytes(), k3.to_bytes());
    }
}
