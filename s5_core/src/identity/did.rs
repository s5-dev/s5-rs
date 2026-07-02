use std::fmt;
use std::str::FromStr;

use crate::stream::types::PublicKeyEd25519;

/// Multicodec varint for `ed25519-pub` (0xed) — the unsigned-varint
/// of 0xed encodes as two bytes: `0xED 0x01`.
pub const ED25519_PUB_MULTICODEC: [u8; 2] = [0xED, 0x01];

const DID_S5_PREFIX: &str = "did:s5:";

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum DidParseError {
    #[error("DID does not start with `did:s5:`")]
    BadPrefix,
    #[error("invalid multibase encoding: {0}")]
    Multibase(#[from] multibase::Error),
    #[error("expected ed25519-pub multicodec [0xED 0x01]")]
    BadMulticodec,
    #[error("expected 32-byte ed25519 pubkey, got {0}")]
    BadPubkeyLength(usize),
}

/// A 32-byte ed25519 **master signing** pubkey — the key a `did:s5:`
/// encodes (`identity-model.md` § four-key model).
///
/// A newtype so [`Did::from_pubkey`] cannot be fed the **iroh transport
/// key** by accident: the two were the same key in the pre-four-key
/// model and the conflation is the classic identity bug (a DID derived
/// from the transport key won't match the master-derived DID used
/// everywhere else, breaking resolution and pairing). Constructing one
/// is a visible, greppable assertion "these bytes are a master pubkey".
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct DidMasterPubkey([u8; 32]);

impl DidMasterPubkey {
    /// Wrap raw bytes asserted to be a master signing pubkey. Prefer
    /// [`Self::from_verifying_key`] when a key is in hand.
    pub fn new(pubkey: [u8; 32]) -> Self {
        Self(pubkey)
    }

    /// From an ed25519 verifying key known to be the identity master.
    pub fn from_verifying_key(vk: &ed25519_dalek::VerifyingKey) -> Self {
        Self(vk.to_bytes())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// A `did:s5:` identifier.
///
/// The DID literally encodes a 32-byte ed25519 master signing pubkey;
/// resolution is "look up the registry under this pubkey to find the
/// current [`super::IdentityBundle`] hash."
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Did(PublicKeyEd25519);

impl Did {
    /// Build a DID from a [`DidMasterPubkey`]. The newtype is the guard
    /// against feeding the iroh transport key (see its docs).
    pub fn from_pubkey(pubkey: DidMasterPubkey) -> Self {
        Self(pubkey.0)
    }

    pub fn pubkey(&self) -> &PublicKeyEd25519 {
        &self.0
    }

    pub fn parse(s: &str) -> Result<Self, DidParseError> {
        let body = s
            .strip_prefix(DID_S5_PREFIX)
            .ok_or(DidParseError::BadPrefix)?;
        let (_base, bytes) = multibase::decode(body)?;
        if bytes.len() < ED25519_PUB_MULTICODEC.len()
            || bytes[..ED25519_PUB_MULTICODEC.len()] != ED25519_PUB_MULTICODEC
        {
            return Err(DidParseError::BadMulticodec);
        }
        let payload = &bytes[ED25519_PUB_MULTICODEC.len()..];
        if payload.len() != 32 {
            return Err(DidParseError::BadPubkeyLength(payload.len()));
        }
        let mut pk = [0u8; 32];
        pk.copy_from_slice(payload);
        Ok(Self(pk))
    }
}

impl fmt::Display for Did {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut bytes = Vec::with_capacity(ED25519_PUB_MULTICODEC.len() + 32);
        bytes.extend_from_slice(&ED25519_PUB_MULTICODEC);
        bytes.extend_from_slice(&self.0);
        write!(
            f,
            "{}{}",
            DID_S5_PREFIX,
            multibase::encode(multibase::Base::Base32Lower, &bytes)
        )
    }
}

impl fmt::Debug for Did {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Did({self})")
    }
}

impl FromStr for Did {
    type Err = DidParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Encode an ed25519 public key as a W3C Multikey string:
/// `z` (multibase base58btc) + `0xED 0x01` (ed25519-pub multicodec) +
/// 32 raw key bytes.
///
/// This is the format DID Documents expect in
/// `verificationMethod[].publicKeyMultibase` for `kind = "Multikey"`.
pub fn ed25519_to_multikey(pubkey: &PublicKeyEd25519) -> String {
    let mut bytes = Vec::with_capacity(ED25519_PUB_MULTICODEC.len() + 32);
    bytes.extend_from_slice(&ED25519_PUB_MULTICODEC);
    bytes.extend_from_slice(pubkey);
    multibase::encode(multibase::Base::Base58Btc, &bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_pubkey() -> PublicKeyEd25519 {
        let mut pk = [0u8; 32];
        for (i, b) in pk.iter_mut().enumerate() {
            *b = i as u8;
        }
        pk
    }

    #[test]
    fn round_trip() {
        let did = Did::from_pubkey(DidMasterPubkey::new(sample_pubkey()));
        let s = did.to_string();
        assert!(s.starts_with("did:s5:b"));
        let parsed = Did::parse(&s).unwrap();
        assert_eq!(parsed, did);
    }

    #[test]
    fn from_str_matches_parse() {
        let did = Did::from_pubkey(DidMasterPubkey::new(sample_pubkey()));
        let s = did.to_string();
        assert_eq!(Did::from_str(&s).unwrap(), did);
    }

    #[test]
    fn rejects_bad_prefix() {
        assert_eq!(Did::parse("did:key:z6Mk..."), Err(DidParseError::BadPrefix));
        assert_eq!(Did::parse(""), Err(DidParseError::BadPrefix));
    }

    #[test]
    fn rejects_bad_multicodec() {
        // Encode 32 bytes with a different multicodec prefix.
        let mut bytes = vec![0x12, 0x00];
        bytes.extend_from_slice(&[0u8; 32]);
        let s = format!(
            "{}{}",
            DID_S5_PREFIX,
            multibase::encode(multibase::Base::Base32Lower, &bytes)
        );
        assert_eq!(Did::parse(&s), Err(DidParseError::BadMulticodec));
    }

    #[test]
    fn rejects_short_payload() {
        let mut bytes = Vec::from(ED25519_PUB_MULTICODEC);
        bytes.extend_from_slice(&[0u8; 31]);
        let s = format!(
            "{}{}",
            DID_S5_PREFIX,
            multibase::encode(multibase::Base::Base32Lower, &bytes)
        );
        assert_eq!(Did::parse(&s), Err(DidParseError::BadPubkeyLength(31)));
    }

    #[test]
    fn rejects_long_payload() {
        let mut bytes = Vec::from(ED25519_PUB_MULTICODEC);
        bytes.extend_from_slice(&[0u8; 33]);
        let s = format!(
            "{}{}",
            DID_S5_PREFIX,
            multibase::encode(multibase::Base::Base32Lower, &bytes)
        );
        assert_eq!(Did::parse(&s), Err(DidParseError::BadPubkeyLength(33)));
    }

    #[test]
    fn distinct_pubkeys_distinct_dids() {
        let a = Did::from_pubkey(DidMasterPubkey::new([0xAAu8; 32]));
        let b = Did::from_pubkey(DidMasterPubkey::new([0xBBu8; 32]));
        assert_ne!(a.to_string(), b.to_string());
    }
}
