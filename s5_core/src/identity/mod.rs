//! DID-based identity primitives.
//!
//! `did:s5:b<multibase(0xed||master_pubkey)>` derives the DID from a
//! 32-byte ed25519 master signing pubkey. The resolved
//! [`IdentityBundle`] carries the four parallel keysets — signers,
//! ACL/read keys, iroh transport keys, age recipients — that authorise
//! writes, reads, connections, and content decryption under that DID.
//!
//! See `docs/reference/identity-model.md` for the data model and
//! `docs/reference/acl-and-revocation.md` for the verify chain and
//! revocation semantics.

mod bundle;
mod did;

pub use bundle::IdentityBundle;
pub use did::{Did, DidMasterPubkey, DidParseError, ED25519_PUB_MULTICODEC, ed25519_to_multikey};
