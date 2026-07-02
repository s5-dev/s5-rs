//! Canonical key derivation for s5.
//!
//! Every *secret* derived in s5 — identity master seeds, per-blob
//! content/metadata keys, and (incoming) the special-vault locators — funnels
//! through [`derive_secret`] so the complete set of derivations is greppable
//! and auditable from one place. It is a thin, intentionally boring wrapper
//! over BLAKE3 in KDF mode ([`blake3::derive_key`]); the value is the single
//! call-site convention, not the (trivial) implementation.
//!
//! `context` MUST be a stable, globally-unique, human-readable domain string.
//! The s5 convention is `"s5/<purpose>/v<n>"` so a schema change can bump the
//! version without colliding with an old derivation. Changing a context string
//! changes every secret derived under it — treat a shipped context as frozen.
//!
//! ## Derivations that deliberately do NOT use this helper
//!
//! Three derivations are intentionally *not* `derive_secret` calls and should
//! not be "consolidated" onto it without understanding why:
//!
//! - **`vault_id` / `recovery_signing_key`** (`s5_node::tasks::publish`) use a
//!   plain tagged BLAKE3 hash, `blake3(tag ‖ recovery_secret)`. `vault_id` is a
//!   *public locator* (not a secret), and that construction is a frozen
//!   wire-format identifier — routing it through KDF mode would change every
//!   vault's id and orphan its registry entries.
//! - **The F02 channel binding** (`s5_blobs`) is a multi-part BLAKE3-KDF over
//!   three distinct fixed-length transport inputs; it reads more honestly as
//!   explicit `Hasher::new_derive_key(..).update(..)` calls than as a
//!   concatenation.
//! - **The indexd registration mnemonic** (`s5_store_indexd::auth`) is a
//!   length-prefixed multi-part XOF to 16 bytes of BIP-39 entropy, not a
//!   32-byte key.
//!
//! Auditing "where do s5 secrets come from" is therefore:
//! `rg 'derive_secret|new_derive_key|Hasher::new'`.

/// Derive a 32-byte secret from a domain-separation `context` and the keying
/// `input`. Equivalent to [`blake3::derive_key(context, input)`](blake3::derive_key);
/// see the module docs for the `"s5/<purpose>/v<n>"` context convention.
#[inline]
pub fn derive_secret(context: &str, input: &[u8]) -> [u8; 32] {
    blake3::derive_key(context, input)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_blake3_derive_key() {
        // Must stay byte-identical to `blake3::derive_key` so the call sites
        // migrated onto this helper keep deriving exactly the same secrets.
        let input = b"keying material";
        assert_eq!(
            derive_secret("s5/test/v1", input),
            blake3::derive_key("s5/test/v1", input)
        );
    }

    #[test]
    fn context_is_domain_separating() {
        let input = b"same input";
        assert_ne!(
            derive_secret("s5/a/v1", input),
            derive_secret("s5/b/v1", input)
        );
    }
}
