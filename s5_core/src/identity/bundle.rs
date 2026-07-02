//! Identity bundle — the keyset record an `s5` identity publishes.
//!
//! Per `docs/reference/identity-model.md` § Identity bundle, a bundle
//! is a flat CBOR map carrying a monotone `revision` plus the four
//! parallel keysets of the four-key model (one entry per authorised
//! device per role):
//!
//! - `signers` — ed25519 signing/write pubkeys (`acl-and-revocation.md
//!   §3b`: per-entry registry signature must be `∈ signers[]`).
//! - `acl_keys` — ed25519 ACL/read pubkeys (`§3a`: blob-fetch connection
//!   challenge must be `∈ acl_keys[]`).
//! - `iroh_pubkeys` — ed25519 transport pubkeys for QUIC handshake.
//! - `age_recipients` — X25519 (bech32 `age1...`) encryption recipients.
//!
//! The bundle blob itself is **unsigned** — the master signature lives
//! on the registry entry that points at the blob's BLAKE3 hash. The
//! integrity chain is: F01 verifies the registry entry's sig under the
//! DID master pubkey ⇒ blob hash is trusted ⇒ `BlobStore::blob_download`
//! BLAKE3-verifies the bytes ⇒ bundle contents are trusted. No
//! `Signed<T>` envelope is needed at this layer.
//!
//! Supersedes the W3C-shaped `DidDocument` + `VerificationMethod` +
//! `Service` types from before 2026-05-19 (see DECISION 09 / TODO 09
//! § Cross-impact: the slim/flat bundle replaces the verbose W3C form).

use minicbor::{Decode, Encode};

use crate::Hash;
use crate::stream::types::PublicKeyEd25519;

/// Master-signed identity bundle (CBOR map). One blob per identity per
/// `revision`; the registry entry under `(master_pubkey, IDENTITY_VAULT_ID)`
/// carries the master ed25519 signature attesting to this blob's hash.
///
/// CBOR map keys are assigned deliberately, not sequentially, leaving gaps so a
/// related future field (e.g. a post-quantum key variant beside its ed25519
/// counterpart) can slot in next to its siblings: `0x00` version, `0x01`
/// revision, `0x03` iroh transport, `0x0a` age recipients, `0x11`/`0x12`
/// signing / ACL authority.
#[derive(Clone, Debug, PartialEq, Eq, Encode, Decode)]
#[cbor(map)]
pub struct IdentityBundle {
    /// Wire-format version, `1` today — the first field, so a reader sees it
    /// up front. A future *incompatible* change (e.g. self-describing
    /// multi-algorithm keys for a post-quantum migration) bumps this so a
    /// reader branches on the encoding instead of mis-parsing; purely additive
    /// fields don't need a bump (the CBOR map skips unknown entries). See
    /// [`IdentityBundle::CURRENT_VERSION`].
    #[n(0x00)]
    pub version: u16,
    /// Strictly monotone. Verifiers MUST reject any newly-fetched bundle
    /// whose revision is ≤ the previously-seen one for this DID
    /// (`acl-and-revocation.md §4`). Anti-rollback is load-bearing for
    /// revocation correctness.
    #[n(0x01)]
    pub revision: u64,
    /// Ed25519 signing/write pubkeys. A device's registry write is
    /// accepted iff its per-entry signature verifies under a key in
    /// this set. Empty on read-only/service-DID provisioning.
    #[n(0x11)]
    pub signers: Vec<PublicKeyEd25519>,
    /// Ed25519 ACL/read pubkeys. A blob-fetch connection is served iff
    /// it proved possession of a key in this set (channel-bound
    /// challenge per `acl-and-revocation.md §3a`).
    #[n(0x12)]
    pub acl_keys: Vec<PublicKeyEd25519>,
    /// Ed25519 iroh transport pubkeys. Authentication of *the QUIC
    /// channel*, never an authorisation principal (`identity-model.md`
    /// § Per-device keys: "the most reachable key (iroh transport) can
    /// do the least").
    #[n(0x03)]
    pub iroh_pubkeys: Vec<PublicKeyEd25519>,
    /// Bech32 `age1...` recipient strings. Content encrypted for the
    /// identity wraps a key for each of these.
    #[n(0x0a)]
    pub age_recipients: Vec<String>,
}

impl IdentityBundle {
    /// The wire-format [`version`](Self::version) this build writes.
    pub const CURRENT_VERSION: u16 = 1;

    pub fn encode_cbor(&self) -> Vec<u8> {
        minicbor::to_vec(self).expect("CBOR encoding into Vec is infallible")
    }

    pub fn decode_cbor(bytes: &[u8]) -> Result<Self, minicbor::decode::Error> {
        minicbor::decode(bytes)
    }

    /// BLAKE3 of the canonical CBOR — what the registry entry's
    /// payload hash attests to.
    pub fn content_hash(&self) -> Hash {
        Hash::new(self.encode_cbor())
    }

    /// True iff the four keyset arrays are bitwise equal — used by the
    /// publisher's idempotency check (skip republishing when the
    /// keysets haven't changed even if `revision` would otherwise bump).
    pub fn keysets_eq(&self, other: &Self) -> bool {
        self.signers == other.signers
            && self.acl_keys == other.acl_keys
            && self.iroh_pubkeys == other.iroh_pubkeys
            && self.age_recipients == other.age_recipients
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> IdentityBundle {
        IdentityBundle {
            version: IdentityBundle::CURRENT_VERSION,
            revision: 1,
            signers: vec![[7u8; 32]],
            acl_keys: vec![[8u8; 32]],
            iroh_pubkeys: vec![[9u8; 32]],
            age_recipients: vec!["age1example".to_string()],
        }
    }

    #[test]
    fn cbor_round_trips() {
        let b = sample();
        let parsed = IdentityBundle::decode_cbor(&b.encode_cbor()).unwrap();
        assert_eq!(parsed, b);
    }

    #[test]
    fn cbor_encoding_is_deterministic() {
        // The content_hash → registry-entry-attestation chain requires
        // identical bytes from identical inputs.
        let b = sample();
        assert_eq!(b.encode_cbor(), b.encode_cbor());
    }

    #[test]
    fn content_hash_differs_when_any_keyset_differs() {
        let base = sample();
        let h_base = base.content_hash();

        let mut a = base.clone();
        a.signers.push([0u8; 32]);
        assert_ne!(a.content_hash(), h_base);

        let mut b = base.clone();
        b.acl_keys.push([0u8; 32]);
        assert_ne!(b.content_hash(), h_base);

        let mut c = base.clone();
        c.iroh_pubkeys.push([0u8; 32]);
        assert_ne!(c.content_hash(), h_base);

        let mut d = base;
        d.age_recipients.push("age1other".to_string());
        assert_ne!(d.content_hash(), h_base);
    }

    #[test]
    fn keysets_eq_ignores_revision() {
        let a = sample();
        let mut b = sample();
        b.revision = 42;
        assert!(a.keysets_eq(&b));
        b.signers.push([1u8; 32]);
        assert!(!a.keysets_eq(&b));
    }
}
