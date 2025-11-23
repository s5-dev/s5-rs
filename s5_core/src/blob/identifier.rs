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
        let mut size_bytes = [0u8; 8];
        // Copy available bytes, pad with zeros (little endian)
        let len = size_slice.len().min(8);
        size_bytes[..len].copy_from_slice(&size_slice[..len]);

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
        if let Some(pos) = size_bytes.iter().rposition(|&x| x != 0) {
            size_bytes.truncate(pos + 1);
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
