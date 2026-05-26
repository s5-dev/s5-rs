//! Per-blob structural pointer.
//!
//! For every blob a caller `put`s into `IndexdStore`, we persist a
//! tiny fixed-size pointer in the caller-supplied `pointers` Store.
//! The pointer carries exactly the two 32-byte values needed to
//! recover the blob's metadata:
//!
//! - **`object_id`** — `Blake2b256` of the SealedObject's slab metadata
//!   (sia_storage's own identifier). Used as the lookup key when
//!   asking indexd for the SealedObject via `Sdk::object`.
//! - **`metadata_hash`** — `BLAKE3` of the Sia-binary-encoded
//!   SealedObject bytes. Used as the lookup key in the
//!   content-addressed `metadata` BlobsReadWrite.
//!
//! The full SealedObject lives in the `metadata` cache (a
//! `BlobsReadWrite` the caller chose). If that cache misses, the read
//! path falls back to `Sdk::object(object_id)` and write-through
//! populates the cache.
//!
//! ## Wire layout (little-endian, fixed 72 bytes)
//!
//! ```text
//!   offset  size   field
//!     0      8     magic = b"S5IP\x00\x00\x00\x01"
//!                  bytes 0..4 = "S5IP" ("S5 Indexd Pointer"),
//!                  bytes 4..7 = reserved (0),
//!                  byte 7    = format version (currently 1).
//!     8     32     object_id     (Blake2b256 of slab metadata)
//!    40     32     metadata_hash (BLAKE3 of the SealedObject bytes)
//! ```

use std::io;

use sia_storage::Hash256;

/// 8-byte magic + version. Bytes 0..4 are the tag; byte 7 is the
/// version (currently 1).
pub const MAGIC: [u8; 8] = *b"S5IP\x00\x00\x00\x01";

/// Fixed wire size of a pointer record.
pub const POINTER_LEN: usize = 8 + 32 + 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Pointer {
    pub object_id: [u8; 32],
    pub metadata_hash: [u8; 32],
}

impl Pointer {
    pub fn new(object_id: Hash256, metadata_hash: [u8; 32]) -> Self {
        Self {
            object_id: *object_id.as_ref(),
            metadata_hash,
        }
    }

    pub fn object_id(&self) -> Hash256 {
        Hash256::from(self.object_id)
    }

    pub fn encode(&self) -> [u8; POINTER_LEN] {
        let mut buf = [0u8; POINTER_LEN];
        buf[0..8].copy_from_slice(&MAGIC);
        buf[8..40].copy_from_slice(&self.object_id);
        buf[40..72].copy_from_slice(&self.metadata_hash);
        buf
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != POINTER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "indexd pointer: wrong length {} (expected {POINTER_LEN})",
                    bytes.len()
                ),
            ));
        }
        if bytes[0..4] != MAGIC[0..4] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "indexd pointer: bad magic tag",
            ));
        }
        if bytes[7] != MAGIC[7] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("indexd pointer: unsupported version {}", bytes[7]),
            ));
        }
        // bytes[4..7] reserved.
        let mut object_id = [0u8; 32];
        object_id.copy_from_slice(&bytes[8..40]);
        let mut metadata_hash = [0u8; 32];
        metadata_hash.copy_from_slice(&bytes[40..72]);
        Ok(Pointer {
            object_id,
            metadata_hash,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_known_values() {
        let p = Pointer {
            object_id: [0xab; 32],
            metadata_hash: [0xcd; 32],
        };
        let bytes = p.encode();
        assert_eq!(bytes.len(), POINTER_LEN);
        assert_eq!(bytes.len(), 72);
        assert_eq!(&bytes[0..8], &MAGIC);
        assert_eq!(&bytes[8..40], &p.object_id);
        assert_eq!(&bytes[40..72], &p.metadata_hash);
        let decoded = Pointer::decode(&bytes).unwrap();
        assert_eq!(decoded, p);
    }

    #[test]
    fn decode_rejects_bad_magic() {
        let mut bytes = [0u8; POINTER_LEN];
        bytes[0..4].copy_from_slice(b"NOPE");
        bytes[7] = MAGIC[7];
        assert!(Pointer::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_wrong_version() {
        let mut bytes = [0u8; POINTER_LEN];
        bytes[0..8].copy_from_slice(&MAGIC);
        bytes[7] = 99;
        assert!(Pointer::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_wrong_size() {
        assert!(Pointer::decode(&[]).is_err());
        assert!(Pointer::decode(&[0u8; 71]).is_err());
        assert!(Pointer::decode(&[0u8; 73]).is_err());
    }
}
