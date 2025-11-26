//! This implementation follows the S5 v1 spec at https://docs.s5.pro/spec/blobs.html

use crate::Hash;
use std::fmt;
use std::str::FromStr;

const S5_MAGIC_BYTE: u8 = 0x5b;
const BLOB_TYPE_DEFAULT: u8 = 0x82;
const MULTIHASH_BLAKE3: u8 = 0x1e;

#[derive(thiserror::Error, Debug)]
pub enum BlobIdError {
    #[error("invalid multibase string: {0}")]
    Multibase(#[from] multibase::Error),
    #[error("invalid length: expected at least 35 bytes, got {0}")]
    InvalidLength(usize),
    #[error("invalid magic byte: expected {0:#x}, got {1:#x}")]
    InvalidMagicByte(u8, u8),
    #[error("invalid blob type: expected {0:#x}, got {1:#x}")]
    InvalidBlobType(u8, u8),
    #[error("invalid multihash type: expected {0:#x}, got {1:#x}")]
    InvalidMultihashType(u8, u8),
}

/// Identifier for a blob in S5.
///
/// A `BlobId` combines the BLAKE3 content hash and the blob size and is
/// encoded as a multibase string according to the S5 v1 spec. It is the
/// human-/URL-facing identifier used to reference content-addressed blobs.
///
/// ```no_run
/// use s5_core::{BlobId, Hash};
///
/// let data = b"hello";
/// let hash = Hash::new(data);
/// let id = BlobId::new(hash, data.len() as u64);
/// println!("blob id: {}", id);
/// ```
#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlobId {
    pub hash: Hash,
    pub size: u64,
}

impl std::fmt::Debug for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobId")
            .field("hash", &self.hash)
            .field("size", &self.size)
            .finish()
    }
}

impl BlobId {
    pub fn new(hash: Hash, size: u64) -> Self {
        Self { hash, size }
    }

    pub fn parse(str: &str) -> Result<Self, BlobIdError> {
        let (_, bytes) = multibase::decode(str)?;

        if bytes.len() < 35 {
            return Err(BlobIdError::InvalidLength(bytes.len()));
        }

        if bytes[0] != S5_MAGIC_BYTE {
            return Err(BlobIdError::InvalidMagicByte(S5_MAGIC_BYTE, bytes[0]));
        }
        if bytes[1] != BLOB_TYPE_DEFAULT {
            return Err(BlobIdError::InvalidBlobType(BLOB_TYPE_DEFAULT, bytes[1]));
        }
        if bytes[2] != MULTIHASH_BLAKE3 {
            return Err(BlobIdError::InvalidMultihashType(
                MULTIHASH_BLAKE3,
                bytes[2],
            ));
        }

        let hash: [u8; 32] = bytes[3..35]
            .try_into()
            .map_err(|_| BlobIdError::InvalidLength(bytes.len()))?;
        let size_slice = &bytes[35..];
        if size_slice.len() > 8 {
            return Err(BlobIdError::InvalidLength(bytes.len()));
        }
        let mut size_bytes = [0u8; 8];
        // Copy available bytes, pad with zeros (little endian)
        let len = size_slice.len();
        size_bytes[..len].copy_from_slice(size_slice);

        let size = u64::from_le_bytes(size_bytes);
        Ok(Self {
            hash: hash.into(),
            size,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let prefix_bytes = vec![
            S5_MAGIC_BYTE,     // S5 Blob Identifier magic byte
            BLOB_TYPE_DEFAULT, // S5 Blob Type Default
            MULTIHASH_BLAKE3,  // multihash blake3
        ];
        let mut size_bytes = self.size.to_le_bytes().to_vec();
        // Trim trailing zero bytes for compact encoding. size=0 encodes as empty.
        if let Some(pos) = size_bytes.iter().rposition(|&x| x != 0) {
            size_bytes.truncate(pos + 1);
        } else {
            size_bytes.clear();
        }

        [prefix_bytes, self.hash.as_bytes().to_vec(), size_bytes].concat()
    }
    pub fn to_base16(&self) -> String {
        multibase::encode(multibase::Base::Base16Lower, self.to_bytes())
    }
    pub fn to_base32(&self) -> String {
        multibase::encode(multibase::Base::Base32Lower, self.to_bytes())
    }
    pub fn to_base58(&self) -> String {
        multibase::encode(multibase::Base::Base58Btc, self.to_bytes())
    }
    pub fn to_base64url(&self) -> String {
        multibase::encode(multibase::Base::Base64Url, self.to_bytes())
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_base32())
    }
}

impl FromStr for BlobId {
    type Err = BlobIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        BlobId::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_id_roundtrip_size_zero() {
        let hash = Hash::new(b"test");
        let id = BlobId::new(hash, 0);
        let bytes = id.to_bytes();
        // Size 0 should encode as empty (no size bytes after the 35-byte prefix)
        assert_eq!(bytes.len(), 35);
        let parsed = BlobId::parse(&id.to_base32()).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_blob_id_roundtrip_size_one() {
        let hash = Hash::new(b"test");
        let id = BlobId::new(hash, 1);
        let bytes = id.to_bytes();
        // Size 1 should encode as single byte
        assert_eq!(bytes.len(), 36);
        let parsed = BlobId::parse(&id.to_base32()).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_blob_id_roundtrip_size_255() {
        let hash = Hash::new(b"test");
        let id = BlobId::new(hash, 255);
        let bytes = id.to_bytes();
        // Size 255 fits in one byte
        assert_eq!(bytes.len(), 36);
        let parsed = BlobId::parse(&id.to_base32()).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_blob_id_roundtrip_size_256() {
        let hash = Hash::new(b"test");
        let id = BlobId::new(hash, 256);
        let bytes = id.to_bytes();
        // Size 256 = 0x0100, needs 2 bytes (little endian: [0x00, 0x01])
        assert_eq!(bytes.len(), 37);
        let parsed = BlobId::parse(&id.to_base32()).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_blob_id_roundtrip_large_size() {
        let hash = Hash::new(b"test");
        let id = BlobId::new(hash, u64::MAX);
        let bytes = id.to_bytes();
        // Max size needs all 8 bytes
        assert_eq!(bytes.len(), 43);
        let parsed = BlobId::parse(&id.to_base32()).unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_blob_id_multibase_formats() {
        let hash = Hash::new(b"hello");
        let id = BlobId::new(hash, 12345);

        // All formats should roundtrip
        let b16 = id.to_base16();
        let b32 = id.to_base32();
        let b58 = id.to_base58();
        let b64 = id.to_base64url();

        assert_eq!(BlobId::parse(&b16).unwrap(), id);
        assert_eq!(BlobId::parse(&b32).unwrap(), id);
        assert_eq!(BlobId::parse(&b58).unwrap(), id);
        assert_eq!(BlobId::parse(&b64).unwrap(), id);
    }

    #[test]
    fn test_blob_id_from_str() {
        let hash = Hash::new(b"test");
        let id = BlobId::new(hash, 100);
        let s = id.to_base32();
        let parsed: BlobId = s.parse().unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn test_blob_id_display() {
        let hash = Hash::new(b"test");
        let id = BlobId::new(hash, 100);
        let display = format!("{}", id);
        // Display uses base32
        assert_eq!(display, id.to_base32());
    }

    #[test]
    fn test_blob_id_debug() {
        let hash = Hash::new(b"test");
        let id = BlobId::new(hash, 100);
        let debug = format!("{:?}", id);
        assert!(debug.contains("BlobId"));
        assert!(debug.contains("hash"));
        assert!(debug.contains("size"));
    }

    #[test]
    fn test_blob_id_error_too_short() {
        let result = BlobId::parse("b"); // too short after decode
        assert!(matches!(result, Err(BlobIdError::InvalidLength(_))));
    }

    #[test]
    fn test_blob_id_error_invalid_magic() {
        // Construct a valid multibase string with wrong magic byte
        let mut bytes = vec![0x00, BLOB_TYPE_DEFAULT, MULTIHASH_BLAKE3];
        bytes.extend_from_slice(&[0u8; 32]);
        let encoded = multibase::encode(multibase::Base::Base32Lower, &bytes);
        let result = BlobId::parse(&encoded);
        assert!(matches!(result, Err(BlobIdError::InvalidMagicByte(_, _))));
    }

    #[test]
    fn test_blob_id_error_invalid_blob_type() {
        let mut bytes = vec![S5_MAGIC_BYTE, 0x00, MULTIHASH_BLAKE3];
        bytes.extend_from_slice(&[0u8; 32]);
        let encoded = multibase::encode(multibase::Base::Base32Lower, &bytes);
        let result = BlobId::parse(&encoded);
        assert!(matches!(result, Err(BlobIdError::InvalidBlobType(_, _))));
    }

    #[test]
    fn test_blob_id_error_invalid_multihash() {
        let mut bytes = vec![S5_MAGIC_BYTE, BLOB_TYPE_DEFAULT, 0x00];
        bytes.extend_from_slice(&[0u8; 32]);
        let encoded = multibase::encode(multibase::Base::Base32Lower, &bytes);
        let result = BlobId::parse(&encoded);
        assert!(matches!(
            result,
            Err(BlobIdError::InvalidMultihashType(_, _))
        ));
    }

    #[test]
    fn test_blob_id_error_size_too_long() {
        // More than 8 bytes for size
        let mut bytes = vec![S5_MAGIC_BYTE, BLOB_TYPE_DEFAULT, MULTIHASH_BLAKE3];
        bytes.extend_from_slice(&[0u8; 32]);
        bytes.extend_from_slice(&[1u8; 9]); // 9 bytes for size
        let encoded = multibase::encode(multibase::Base::Base32Lower, &bytes);
        let result = BlobId::parse(&encoded);
        assert!(matches!(result, Err(BlobIdError::InvalidLength(_))));
    }
}
