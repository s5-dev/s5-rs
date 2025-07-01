//! This implementation follows the S5 v1 spec at https://docs.s5.pro/spec/blobs.html

use crate::Hash;
use std::fmt;

#[derive(Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlobId {
    pub hash: Hash,
    pub size: u64,
}

impl BlobId {
    pub fn new(hash: Hash, size: u64) -> Self {
        Self { hash, size }
    }

    pub fn parse(str: &str) -> Self {
        let bytes = multibase::decode(str).unwrap().1;
        // TODO check bytes properly
        let hash: [u8; 32] = bytes[3..35].try_into().expect("invalid hash");
        let size_slice = &bytes[35..];
        let mut size_bytes = [0u8; 8];
        size_bytes[..size_slice.len()].copy_from_slice(size_slice);
        let size = u64::from_le_bytes(size_bytes);
        Self {
            hash: hash.into(),
            size,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let prefix_bytes = vec![
            0x5b, // S5 Blob Identifier magic byte
            0x82, // S5 Blob Type Default
            0x1e, // multihash blake3
        ];
        let mut size_bytes = self.size.to_le_bytes().to_vec();
        if let Some(pos) = size_bytes.iter().rposition(|&x| x != 0) {
            size_bytes.truncate(pos + 1);
        }
        let bytes = [prefix_bytes, self.hash.as_bytes().to_vec(), size_bytes].concat();
        bytes
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
