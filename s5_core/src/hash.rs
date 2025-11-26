//! The hash type used by S5 (blake3, 32 bytes)
//!
//! Implementation from Iroh (MIT OR Apache-2.0)
//! https://github.com/n0-computer/iroh-blobs/blob/main/src/hash.rs

use std::{borrow::Borrow, fmt};

/// Hash type used by S5 (blake3, 32 bytes)
#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub struct Hash(blake3::Hash);

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Hash").field(&self.to_hex()).finish()
    }
}

impl Hash {
    /// The hash for the empty byte range (`b""`).
    pub const EMPTY: Hash = Hash::from_bytes([
        175, 19, 73, 185, 245, 249, 161, 166, 160, 64, 77, 234, 54, 220, 201, 73, 155, 203, 37,
        201, 173, 193, 18, 183, 204, 154, 147, 202, 228, 31, 50, 98,
    ]);

    /// The size of the hash in bytes.
    pub const SIZE: usize = 32;

    /// Calculate the hash of the provided bytes.
    pub fn new(buf: impl AsRef<[u8]>) -> Self {
        let val = blake3::hash(buf.as_ref());
        Hash(val)
    }

    /// Bytes of the hash.
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }

    /// Create a `Hash` from its raw bytes representation.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(blake3::Hash::from_bytes(bytes))
    }

    /// Convert the hash to a hex string.
    pub fn to_hex(&self) -> String {
        self.0.to_hex().to_string()
    }

    /// Convert to a hex string limited to the first 5bytes for a friendly string
    /// representation of the hash.
    pub fn fmt_short(&self) -> String {
        data_encoding::HEXLOWER.encode(&self.as_bytes()[..5])
    }
}

impl AsRef<[u8]> for Hash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Borrow<[u8]> for Hash {
    fn borrow(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Borrow<[u8; 32]> for Hash {
    fn borrow(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }
}

impl From<Hash> for blake3::Hash {
    fn from(value: Hash) -> Self {
        value.0
    }
}

impl From<blake3::Hash> for Hash {
    fn from(value: blake3::Hash) -> Self {
        Hash(value)
    }
}

impl From<[u8; 32]> for Hash {
    fn from(value: [u8; 32]) -> Self {
        Hash(blake3::Hash::from(value))
    }
}

impl From<Hash> for [u8; 32] {
    fn from(value: Hash) -> Self {
        *value.as_bytes()
    }
}

impl From<&[u8; 32]> for Hash {
    fn from(value: &[u8; 32]) -> Self {
        Hash(blake3::Hash::from(*value))
    }
}

impl PartialOrd for Hash {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Hash {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_bytes().cmp(other.0.as_bytes())
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_empty() {
        let hash = Hash::new(b"");
        assert_eq!(hash, Hash::EMPTY);
    }

    #[test]
    fn test_hash_known_value() {
        // BLAKE3 hash of "hello" is well-known
        let hash = Hash::new(b"hello");
        let expected_hex = "ea8f163db38682925e4491c5e58d4bb3506ef8c14eb78a86e908c5624a67200f";
        assert_eq!(hash.to_hex(), expected_hex);
    }

    #[test]
    fn test_hash_roundtrip_bytes() {
        let original = Hash::new(b"test data");
        let bytes: [u8; 32] = original.into();
        let recovered = Hash::from(bytes);
        assert_eq!(original, recovered);
    }

    #[test]
    fn test_hash_from_bytes() {
        let bytes = [0u8; 32];
        let hash = Hash::from_bytes(bytes);
        assert_eq!(hash.as_bytes(), &bytes);
    }

    #[test]
    fn test_hash_fmt_short() {
        let hash = Hash::new(b"hello");
        let short = hash.fmt_short();
        // fmt_short returns first 5 bytes as hex (10 chars)
        assert_eq!(short.len(), 10);
        assert!(hash.to_hex().starts_with(&short));
    }

    #[test]
    fn test_hash_ordering() {
        let h1 = Hash::from_bytes([0u8; 32]);
        let h2 = Hash::from_bytes([1u8; 32]);
        let h3 = Hash::from_bytes([0xff; 32]);
        assert!(h1 < h2);
        assert!(h2 < h3);
    }

    #[test]
    fn test_hash_debug() {
        let hash = Hash::new(b"test");
        let debug = format!("{:?}", hash);
        assert!(debug.starts_with("Hash("));
        assert!(debug.contains(&hash.to_hex()));
    }
}
