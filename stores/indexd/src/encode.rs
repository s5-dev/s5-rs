//! Local `SiaEncodable` helper for `SealedObject`.
//!
//! `sia_storage::SealedObject` ships with `SiaDecodable` but no
//! matching `SiaEncodable` (the Go SDK has both `MarshalSia` /
//! `UnmarshalSia`; the Rust SDK only has decode). Until upstreamed,
//! we provide the encoder here, mirroring the decode order field for
//! field — see `sia_storage/src/slabs.rs:161-183`.
//!
//! SealedObject persistence is load-bearing: it's the "trustless
//! capability" — with these bytes plus the AppKey, a client can read
//! data direct from Sia hosts even if the indexd provider goes
//! away (spec §5.1).
//!
//! TODO: upstream `SiaEncodable for SealedObject` to `sia_storage`.

use anyhow::{Result, anyhow};
use sia_core::encoding::{SiaDecodable, SiaEncodable};
use sia_storage::SealedObject;
use std::io::Write;

fn encode<W: Write>(sealed: &SealedObject, w: &mut W) -> sia_core::encoding::Result<()> {
    sealed.encrypted_data_key.encode(w)?;
    sealed.slabs.encode(w)?;
    sealed.data_signature.encode(w)?;
    sealed.encrypted_metadata_key.encode(w)?;
    sealed.encrypted_metadata.encode(w)?;
    sealed.metadata_signature.encode(w)?;
    sealed.created_at.encode(w)?;
    sealed.updated_at.encode(w)?;
    Ok(())
}

fn encoded_len(sealed: &SealedObject) -> usize {
    sealed.encrypted_data_key.encoded_length()
        + sealed.slabs.encoded_length()
        + sealed.data_signature.encoded_length()
        + sealed.encrypted_metadata_key.encoded_length()
        + sealed.encrypted_metadata.encoded_length()
        + sealed.metadata_signature.encoded_length()
        + sealed.created_at.encoded_length()
        + sealed.updated_at.encoded_length()
}

/// Encode a `SealedObject` to bytes for persistence in the
/// `associated` Store.
pub fn encode_sealed_to_vec(sealed: &SealedObject) -> Vec<u8> {
    let mut v = Vec::with_capacity(encoded_len(sealed));
    encode(sealed, &mut v).expect("Sia encoding to Vec is infallible");
    v
}

/// Decode from the same format `encode_sealed_to_vec` produces.
pub fn decode_sealed(bytes: &[u8]) -> Result<SealedObject> {
    SealedObject::decode(&mut &bytes[..]).map_err(|e| anyhow!("decoding SealedObject failed: {e}"))
}
