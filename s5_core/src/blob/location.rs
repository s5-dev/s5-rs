//! Blob location types for the S5 protocol.
//!
//! A [`BlobLocation`] describes *where* and *how* to retrieve a blob's content.
//! Locations can be:
//!
//! - **Inline**: Raw bytes embedded directly in the location (small blobs).
//! - **Network**: A URL, Iroh endpoint, or Sia renterd reference.
//! - **Transforms**: Encryption (XChaCha20-Poly1305) or compression (Zstd, Brotli)
//!   wrapping another location.
//!
//! `BlobLocation` is CBOR-encoded for wire transport and persistent storage.
//!
//! # Security Notes
//!
//! Some variants (e.g. `SiaFile`, `EncryptionXChaCha20Poly1305`) may embed
//! encryption keys or other secret material. Callers should treat `BlobLocation`
//! values as potentially sensitive and take care when logging, persisting, or
//! transmitting them.
//!
//! ## Debug Output
//!
//! Types containing secret keys (`SiaFile`, `SiaFileHost`, `SiaFileSlab`,
//! `EncryptionXChaCha20Poly1305Location`) implement `Debug` with key fields
//! redacted as `[REDACTED]` to prevent accidental exposure in logs.
//!
//! ## Key Zeroization
//!
//! Secret key fields use plain `[u8; 32]` arrays for CBOR compatibility. These
//! are **not** automatically zeroized on drop. If your threat model requires
//! defense against memory scraping attacks, consider copying keys into a
//! zeroizing wrapper (e.g. `zeroize::Zeroizing`) immediately after decoding
//! and clearing the original.

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
    Encode, Decode, Serialize, Deserialize, CborLen, Clone, PartialEq, Eq, PartialOrd, Ord, Hash,
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

impl std::fmt::Debug for SiaFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SiaFile")
            .field("size", &self.size)
            .field("slab_size", &self.slab_size)
            .field("min_shards", &self.min_shards)
            .field("hosts", &self.hosts)
            .field("file_encryption_key", &"[REDACTED]")
            .field("slabs", &self.slabs)
            .finish()
    }
}

#[derive(
    Encode, Decode, Serialize, Deserialize, CborLen, Clone, PartialEq, Eq, PartialOrd, Ord, Hash,
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

impl std::fmt::Debug for SiaFileHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SiaFileHost")
            .field("hostkey", &self.hostkey)
            .field("v2_siamux_addresses", &self.v2_siamux_addresses)
            .field("ephemeral_account_private_key", &"[REDACTED]")
            .finish()
    }
}

#[derive(
    Encode, Decode, Serialize, Deserialize, CborLen, Clone, PartialEq, Eq, PartialOrd, Ord, Hash,
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

impl std::fmt::Debug for SiaFileSlab {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SiaFileSlab")
            .field("slab_encryption_key", &"[REDACTED]")
            .field("shard_roots", &self.shard_roots)
            .finish()
    }
}

/// Encrypted blob location using XChaCha20-Poly1305.
///
/// The blob is split into chunks of `block_size` bytes, each encrypted separately.
/// The nonce for each chunk is derived from its zero-based chunk index encoded as
/// little-endian u64, zero-padded to 24 bytes (XChaCha20 nonce size).
///
/// # Security Note
///
/// The `key` field contains secret key material. The `Debug` implementation
/// redacts this field to prevent accidental exposure in logs. Note that cloned
/// instances share the same key bytes in memory; proper key zeroization on drop
/// is not currently implemented.
#[derive(
    Encode, Decode, Serialize, Deserialize, CborLen, Clone, PartialEq, Eq, PartialOrd, Ord, Hash,
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

impl std::fmt::Debug for EncryptionXChaCha20Poly1305Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionXChaCha20Poly1305Location")
            .field("inner", &self.inner)
            .field("key", &"[REDACTED]")
            .field("block_size", &self.block_size)
            .finish()
    }
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

    // CBOR roundtrip tests for all BlobLocation variants

    fn roundtrip(loc: &BlobLocation) -> BlobLocation {
        let bytes = loc.to_vec().expect("encode");
        BlobLocation::deserialize(&bytes).expect("decode")
    }

    #[test]
    fn test_roundtrip_identity_raw() {
        let loc = BlobLocation::IdentityRawBinary(vec![1, 2, 3, 4, 5]);
        assert_eq!(roundtrip(&loc), loc);

        // Empty case
        let loc_empty = BlobLocation::IdentityRawBinary(vec![]);
        assert_eq!(roundtrip(&loc_empty), loc_empty);
    }

    #[test]
    fn test_roundtrip_url() {
        let loc = BlobLocation::Url("https://example.com/blob".to_string());
        assert_eq!(roundtrip(&loc), loc);

        // Empty URL
        let loc_empty = BlobLocation::Url(String::new());
        assert_eq!(roundtrip(&loc_empty), loc_empty);
    }

    #[test]
    fn test_roundtrip_iroh() {
        let loc = BlobLocation::Iroh(IrohLocation {
            host: [42; 32],
            partial: true,
        });
        assert_eq!(roundtrip(&loc), loc);

        // With partial=false (default)
        let loc_full = BlobLocation::Iroh(IrohLocation {
            host: [0; 32],
            partial: false,
        });
        assert_eq!(roundtrip(&loc_full), loc_full);
    }

    #[test]
    fn test_roundtrip_multihash_variants() {
        let sha1 = BlobLocation::MultihashSha1([0xaa; 20]);
        assert_eq!(roundtrip(&sha1), sha1);

        let sha256 = BlobLocation::MultihashSha2_256([0xbb; 32]);
        assert_eq!(roundtrip(&sha256), sha256);

        let blake3 = BlobLocation::MultihashBlake3([0xcc; 32]);
        assert_eq!(roundtrip(&blake3), blake3);

        let md5 = BlobLocation::MultihashMd5([0xdd; 16]);
        assert_eq!(roundtrip(&md5), md5);
    }

    #[test]
    fn test_roundtrip_sia_file() {
        let mut hosts = BTreeMap::new();
        hosts.insert(
            0,
            SiaFileHost {
                hostkey: "ed25519:abc123".to_string(),
                v2_siamux_addresses: vec!["127.0.0.1:9981".to_string()],
                ephemeral_account_private_key: [0x11; 32],
            },
        );

        let mut shard_roots = BTreeMap::new();
        shard_roots.insert(0, [0xaa; 32]);
        shard_roots.insert(1, [0xbb; 32]);

        let loc = BlobLocation::SiaFile(SiaFile {
            size: 1024 * 1024,
            slab_size: 4096,
            min_shards: 10,
            hosts,
            file_encryption_key: [0x22; 32],
            slabs: vec![SiaFileSlab {
                slab_encryption_key: [0x33; 32],
                shard_roots,
            }],
        });
        assert_eq!(roundtrip(&loc), loc);
    }

    #[test]
    fn test_roundtrip_encryption() {
        let inner = BlobLocation::MultihashBlake3([0xee; 32]);
        let loc = BlobLocation::EncryptionXChaCha20Poly1305(EncryptionXChaCha20Poly1305Location {
            inner: Box::new(inner),
            key: [0xff; 32],
            block_size: 65536,
        });
        assert_eq!(roundtrip(&loc), loc);
    }

    #[test]
    fn test_roundtrip_compression() {
        let inner = BlobLocation::Url("https://example.com/compressed".to_string());

        let zstd = BlobLocation::CompressionZstd(Box::new(inner.clone()));
        assert_eq!(roundtrip(&zstd), zstd);

        let brotli = BlobLocation::CompressionBrotli(Box::new(inner));
        assert_eq!(roundtrip(&brotli), brotli);
    }

    #[test]
    fn test_roundtrip_nested_transforms() {
        // Encryption wrapping compression wrapping a URL
        let url = BlobLocation::Url("https://example.com/data".to_string());
        let compressed = BlobLocation::CompressionZstd(Box::new(url));
        let encrypted =
            BlobLocation::EncryptionXChaCha20Poly1305(EncryptionXChaCha20Poly1305Location {
                inner: Box::new(compressed),
                key: [0x42; 32],
                block_size: 1024 * 64,
            });
        assert_eq!(roundtrip(&encrypted), encrypted);
    }

    #[test]
    fn test_debug_redacts_keys() {
        let loc = BlobLocation::EncryptionXChaCha20Poly1305(EncryptionXChaCha20Poly1305Location {
            inner: Box::new(BlobLocation::Url("test".to_string())),
            key: [0x42; 32],
            block_size: 1024,
        });
        let debug = format!("{:?}", loc);
        assert!(debug.contains("[REDACTED]"));
        // Make sure the key bytes don't appear in the output
        assert!(!debug.contains("42"));
    }

    #[test]
    fn test_debug_redacts_sia_keys() {
        let mut hosts = BTreeMap::new();
        hosts.insert(
            0,
            SiaFileHost {
                hostkey: "test".to_string(),
                v2_siamux_addresses: vec![],
                ephemeral_account_private_key: [0xab; 32],
            },
        );
        let loc = BlobLocation::SiaFile(SiaFile {
            size: 100,
            slab_size: 10,
            min_shards: 1,
            hosts,
            file_encryption_key: [0xcd; 32],
            slabs: vec![SiaFileSlab {
                slab_encryption_key: [0xef; 32],
                shard_roots: BTreeMap::new(),
            }],
        });
        let debug = format!("{:?}", loc);
        // Check that REDACTED appears for all key fields
        assert!(debug.contains("[REDACTED]"));
        // Key bytes should not appear
        assert!(!debug.contains("171")); // 0xab = 171
        assert!(!debug.contains("205")); // 0xcd = 205
        assert!(!debug.contains("239")); // 0xef = 239
    }
}
