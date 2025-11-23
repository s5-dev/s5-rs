use bytes::Bytes;
use minicbor::{CborLen, Decode, Encode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

impl BlobLocation {
    pub fn deserialize(bytes: &[u8]) -> Result<BlobLocation, minicbor::decode::Error> {
        minicbor::decode(bytes)
    }
    pub fn to_vec(&self) -> Result<Vec<u8>, minicbor::encode::Error<std::convert::Infallible>> {
        minicbor::to_vec(self)
    }
    pub fn serialize(&self) -> Result<Bytes, minicbor::encode::Error<std::convert::Infallible>> {
        Ok(self.to_vec()?.into())
    }

    pub fn location_type(&self) -> BlobLocationType {
        match self {
            BlobLocation::IdentityRawBinary(_) => BlobLocationType::IdentityRawBinary,
            BlobLocation::Url(_) => BlobLocationType::Url,
            BlobLocation::Iroh(_) => BlobLocationType::Iroh,
            BlobLocation::SiaFile(_) => BlobLocationType::SiaFile,
            BlobLocation::MultihashSha1(_) => BlobLocationType::MultihashSha1,
            BlobLocation::MultihashSha2_256(_) => BlobLocationType::MultihashSha2_256,
            BlobLocation::MultihashBlake3(_) => BlobLocationType::MultihashBlake3,
            BlobLocation::MultihashMd5(_) => BlobLocationType::MultihashMd5,
            BlobLocation::EncryptionXChaCha20Poly1305(_) => {
                BlobLocationType::EncryptionXChaCha20Poly1305
            }
            BlobLocation::CompressionZstd(_) => BlobLocationType::CompressionZstd,
            BlobLocation::CompressionBrotli(_) => BlobLocationType::CompressionBrotli,
        }
    }
}

#[repr(u8)]
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlobLocationType {
    IdentityRawBinary = 0,
    Url = 1,
    Iroh = 4,
    // hash types
    MultihashSha1 = 0x11,
    MultihashSha2_256 = 0x12,
    MultihashBlake3 = 0x1e,
    MultihashMd5 = 0xd5,
    // sia file (renterd)
    SiaFile = 0x41,
    // compression formats
    CompressionZstd = 0xc2,
    CompressionBrotli = 0xcb,
    // encryption methods
    EncryptionXChaCha20Poly1305 = 0xe2,
}

#[derive(
    Encode,
    Decode,
    Serialize,
    Deserialize,
    CborLen,
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
#[cbor(flat)]
#[non_exhaustive]
/// Describes where and how a blob can be fetched.
///
/// `BlobLocation` is CBOR-encoded and can represent inline data, URLs,
/// Iroh locations, Sia metadata, and various multihash- and wrapper-based
/// indirections (encryption, compression, etc.).
///
/// Note that some variants (for example `SiaFile` and
/// `EncryptionXChaCha20Poly1305Location`) may embed encryption keys or
/// other secret material. Callers should treat `BlobLocation` values as
/// potentially sensitive and take care when logging, persisting, or
/// transmitting them.
pub enum BlobLocation {
    #[n(0)]
    IdentityRawBinary(
        #[n(0)]
        #[cbor(with = "minicbor::bytes")]
        Vec<u8>,
    ),

    #[n(1)]
    #[cbor(array)]
    Url(#[n(0)] String),

    #[n(4)]
    Iroh(#[n(0)] IrohLocation),

    #[n(0x41)]
    SiaFile(#[n(0)] SiaFile),
    #[n(0x11)]
    MultihashSha1(
        #[n(0)]
        #[cbor(with = "minicbor::bytes")]
        [u8; 20],
    ),

    #[n(0x12)]
    MultihashSha2_256(
        #[n(0)]
        #[cbor(with = "minicbor::bytes")]
        [u8; 32],
    ),

    #[n(0x1e)]
    MultihashBlake3(
        #[n(0)]
        #[cbor(with = "minicbor::bytes")]
        [u8; 32],
    ),

    #[n(0xd5)]
    MultihashMd5(
        #[n(0)]
        #[cbor(with = "minicbor::bytes")]
        [u8; 16],
    ),
    #[n(0xe2)]
    EncryptionXChaCha20Poly1305(#[n(0)] EncryptionXChaCha20Poly1305Location),

    #[n(0xc2)]
    CompressionZstd(#[n(0)] Box<BlobLocation>),

    #[n(0xcb)]
    CompressionBrotli(#[n(0)] Box<BlobLocation>),
}

#[derive(
    Encode,
    Decode,
    Serialize,
    Deserialize,
    CborLen,
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
pub struct IrohLocation {
    #[n(0)]
    pub host: [u8; 32], //NodeId,

    /// The kind of the announcement.
    #[n(1)]
    #[cbor(default)]
    pub partial: bool,
}

#[derive(
    Encode,
    Decode,
    Serialize,
    Deserialize,
    CborLen,
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
#[cbor(array)]
pub struct SiaFile {
    #[n(0)]
    pub size: u64,
    #[n(1)]
    pub slab_size: u32,
    #[n(2)]
    pub min_shards: u8,
    #[n(3)]
    pub hosts: BTreeMap<u8, SiaFileHost>,
    #[n(4)]
    #[cbor(with = "minicbor::bytes")]
    pub file_encryption_key: [u8; 32],
    #[n(5)]
    pub slabs: Vec<SiaFileSlab>,
}

#[derive(
    Encode,
    Decode,
    Serialize,
    Deserialize,
    CborLen,
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
#[cbor(array)]
pub struct SiaFileHost {
    #[n(0)]
    pub hostkey: String,
    #[n(1)]
    pub v2_siamux_addresses: Vec<String>,
    #[n(2)]
    #[cbor(with = "minicbor::bytes")]
    pub ephemeral_account_private_key: [u8; 32],
}

#[derive(
    Encode,
    Decode,
    Serialize,
    Deserialize,
    CborLen,
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
#[cbor(array)]
pub struct SiaFileSlab {
    #[n(0)]
    #[cbor(with = "minicbor::bytes")]
    pub slab_encryption_key: [u8; 32],

    #[n(1)]
    // TODO minicbor should serialize these as byte arrays
    pub shard_roots: BTreeMap<u8, [u8; 32]>,
}

#[derive(
    Encode,
    Decode,
    Serialize,
    Deserialize,
    CborLen,
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
#[cbor(array)]
pub struct EncryptionXChaCha20Poly1305Location {
    #[n(0)]
    pub inner: Box<BlobLocation>,
    #[n(1)]
    #[cbor(with = "minicbor::bytes")]
    pub key: [u8; 32],
    #[n(2)]
    pub block_size: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_location_type() {
        assert_eq!(
            BlobLocation::IdentityRawBinary(vec![]).location_type(),
            BlobLocationType::IdentityRawBinary
        );
        assert_eq!(
            BlobLocation::Url("".to_string()).location_type(),
            BlobLocationType::Url
        );
        assert_eq!(
            BlobLocation::Iroh(IrohLocation {
                host: [0; 32],
                partial: false
            })
            .location_type(),
            BlobLocationType::Iroh
        );
        assert_eq!(BlobLocationType::MultihashSha2_256 as u8, 0x12);
    }
}
