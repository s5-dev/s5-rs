//! Pack manifest binary format.
//!
//! One manifest per pack. The manifest is **self-identifying**: the
//! pack body's BLAKE3 hash lives in the header. The filename the
//! manifest is stored under can therefore use any encoding the
//! caller's Store likes — readers don't have to recover the pack
//! hash from the path.
//!
//! Wire layout (little-endian throughout, 16-byte aligned):
//!
//! ```text
//!   offset  size   field
//!     0       8     magic = b"S5PK\x00\x00\x00\x01"
//!                   bytes 0..4 = "S5PK", bytes 4..7 = reserved (0),
//!                   byte 7 = format version (currently 1).
//!     8      32     pack_hash: BLAKE3 of the pack body
//!    40       8     blob_count: u64 LE
//!    48       N×16  member[i]: { hash_prefix: [u8; 12]; length: u32 LE }
//! ```
//!
//! Each member is exactly 16 bytes (12 + 4). Prefix is at the start
//! so binary search on the hash column reads `&bytes[48 + i*16 ..
//! 48 + i*16 + 12]` directly. Members are **sorted ascending by full
//! BLAKE3 hash** before truncation, making the manifest deterministic
//! across devices and binary-searchable.
//!
//! Version 1 implies `prefix_len = 12`. A future schema change picks
//! a new version byte and may change the per-member layout entirely.

use bytes::Bytes;
use s5_core::Hash;
use std::io;

/// Truncated hash bytes stored per member. 12 bytes (96 bits) gives a
/// birthday collision probability of ~2^-37 at 10^9 stored blobs —
/// negligible — while keeping each member 16-byte aligned (4-byte
/// length + 12-byte prefix). Reads verify the full BLAKE3.
pub const HASH_PREFIX_LEN: usize = 12;

/// 8-byte magic. Bytes 0..4 are the ASCII tag, bytes 4..7 are
/// reserved (zero), byte 7 is the format version. Version 1 implies
/// `prefix_len = 12` and the layout described in the module docs.
pub const MAGIC: [u8; 8] = *b"S5PK\x00\x00\x00\x01";

/// Format version embedded in [`MAGIC`].
pub const VERSION: u8 = 1;

/// Bytes consumed by a single member entry: `[u8; 12]` prefix +
/// `u32` length = 16 bytes, naturally aligned. Prefix is laid out
/// first so binary search reads the prefix column directly.
pub const MEMBER_LEN: usize = HASH_PREFIX_LEN + 4;

const HEADER_LEN: usize = 48;

/// One entry per blob in a pack. `length` is bytes within the pack
/// body; offsets are derived by cumulative sum at lookup time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PackMember {
    pub hash_prefix: [u8; HASH_PREFIX_LEN],
    pub length: u32,
}

/// Decoded pack manifest. Self-identifying: the pack body's BLAKE3
/// lives in `pack_hash`, so readers don't depend on the manifest's
/// filename to recover it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackManifest {
    pub pack_hash: Hash,
    pub members: Vec<PackMember>,
}

impl PackManifest {
    /// Encode to the binary layout documented at the module level.
    /// `members` must already be sorted ascending by full BLAKE3 hash.
    pub fn encode(&self) -> Bytes {
        let n = self.members.len();
        let total = HEADER_LEN + n * MEMBER_LEN;
        let mut out = Vec::with_capacity(total);

        // Header (48 bytes): magic + pack_hash + blob_count.
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(self.pack_hash.as_bytes());
        out.extend_from_slice(&(n as u64).to_le_bytes());

        // Members (16 bytes each, prefix then length).
        for m in &self.members {
            out.extend_from_slice(&m.hash_prefix);
            out.extend_from_slice(&m.length.to_le_bytes());
        }

        debug_assert_eq!(out.len(), total);
        Bytes::from(out)
    }

    /// Decode from the binary layout.
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "pack manifest shorter than header",
            ));
        }
        if bytes[0..4] != MAGIC[0..4] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "pack manifest: bad magic tag",
            ));
        }
        if bytes[7] != VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("pack manifest: unsupported version {}", bytes[7]),
            ));
        }
        // bytes[4..7] reserved.

        let mut pack_hash_bytes = [0u8; 32];
        pack_hash_bytes.copy_from_slice(&bytes[8..40]);
        let pack_hash = Hash::from_bytes(pack_hash_bytes);

        let n = u64::from_le_bytes(bytes[40..48].try_into().unwrap()) as usize;
        let expected = HEADER_LEN + n * MEMBER_LEN;
        if bytes.len() != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "pack manifest: length {} doesn't match declared blob_count {n} (expected {expected})",
                    bytes.len()
                ),
            ));
        }

        let mut members = Vec::with_capacity(n);
        for i in 0..n {
            let off = HEADER_LEN + i * MEMBER_LEN;
            let mut hash_prefix = [0u8; HASH_PREFIX_LEN];
            hash_prefix.copy_from_slice(&bytes[off..off + HASH_PREFIX_LEN]);
            let length = u32::from_le_bytes(
                bytes[off + HASH_PREFIX_LEN..off + MEMBER_LEN]
                    .try_into()
                    .unwrap(),
            );
            members.push(PackMember {
                hash_prefix,
                length,
            });
        }
        Ok(PackManifest { pack_hash, members })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(byte: u8) -> [u8; HASH_PREFIX_LEN] {
        [byte; HASH_PREFIX_LEN]
    }

    fn h(byte: u8) -> Hash {
        Hash::from_bytes([byte; 32])
    }

    #[test]
    fn header_is_16_aligned() {
        let m = PackManifest {
            pack_hash: h(0),
            members: Vec::new(),
        };
        let bytes = m.encode();
        assert_eq!(bytes.len(), HEADER_LEN);
        assert_eq!(bytes.len() % 16, 0);
    }

    #[test]
    fn empty_manifest_roundtrip() {
        let m = PackManifest {
            pack_hash: h(0xAA),
            members: Vec::new(),
        };
        let bytes = m.encode();
        let decoded = PackManifest::decode(&bytes).unwrap();
        assert_eq!(decoded, m);
    }

    #[test]
    fn small_manifest_roundtrip() {
        let m = PackManifest {
            pack_hash: h(0x55),
            members: vec![
                PackMember {
                    hash_prefix: p(0x10),
                    length: 100,
                },
                PackMember {
                    hash_prefix: p(0x20),
                    length: 200,
                },
                PackMember {
                    hash_prefix: p(0x30),
                    length: 300,
                },
            ],
        };
        let bytes = m.encode();
        assert_eq!(bytes.len(), HEADER_LEN + 3 * MEMBER_LEN);
        assert_eq!(bytes.len(), 48 + 3 * 16);
        let decoded = PackManifest::decode(&bytes).unwrap();
        assert_eq!(decoded, m);
        assert_eq!(decoded.pack_hash, h(0x55));
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let mut bytes = vec![0u8; HEADER_LEN];
        bytes[0..4].copy_from_slice(b"NOPE");
        assert!(PackManifest::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_wrong_version() {
        let mut bytes = vec![0u8; HEADER_LEN];
        bytes[0..8].copy_from_slice(&MAGIC);
        bytes[7] = 99;
        assert!(PackManifest::decode(&bytes).is_err());
    }
}
