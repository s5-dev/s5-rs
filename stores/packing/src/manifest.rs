//! Pack header — a **prepended**, self-describing index for a pack body.
//!
//! A pack body is `header ++ data`. The header is at the **front**, so a reader
//! resolves a blob with pure ranged GETs from offset 0 — no pack-size lookup, no
//! tail seek. That's what makes a share-recipient (who holds only a `pack_hash`)
//! and cold recovery cheap: read the header, binary-search the prefix, ranged-read
//! the blob. Putting the index in a trailer would force a `blob_get_size` round
//! trip first; the prefix header removes that dependency, and packs are written
//! once (immutable), so the trailer's only advantage — append-friendliness —
//! doesn't apply.
//!
//! ## Wire layout (little-endian)
//!
//! ```text
//!   0   MAGIC  = "S5.pro" 0x5b 'P'                       8 B
//!   8   reserved (zero); byte 11 = format VERSION        4 B
//!  12   blob_count: u32 LE                               4 B
//!  16   member[i] = hash_prefix[12] | offset: u32 LE     N × 16 B  (sorted by prefix)
//!  …    end_offset: u32 LE  (= total pack size)          4 B
//!  …    zero-pad to the next DATA_ALIGN boundary
//!  <aligned>  data = blob0 ++ blob1 ++ … ++ blobN-1      (same order as members)
//! ```
//!
//! `offset` is **absolute** (byte position in the pack), so a read is a direct
//! ranged GET — no cumulative sum. Blob `i`'s length is `offset[i+1] - offset[i]`;
//! the last runs to `end_offset`. `end_offset` doubles as the pack size, so the
//! header is fully self-describing with no external metadata.
//!
//! `pack_hash` (= `BLAKE3` of the whole body) is **not** stored in the header —
//! it would be circular — it's recovered as the object id the body was fetched
//! by, and committing it via the body hash means trusting the hash trusts the
//! index. The local manifest *cache* ([`PackHeader::to_cache_bytes`]) does carry
//! it, since that's a separate, self-contained local file.

use bytes::Bytes;
use s5_core::Hash;
use std::io;

/// Truncated hash bytes stored per member. 12 bytes (96 bits) → ~2^-37 birthday
/// collision at 10^9 blobs (negligible); reads verify the full BLAKE3. Keeps each
/// member 16-byte aligned (12-byte prefix + 4-byte offset).
pub const HASH_PREFIX_LEN: usize = 12;

/// 8-byte magic at the front of every pack body: `"S5.pro"` + the `0x5b` blob
/// cluster byte + `'P'` (pack). Front-readable, greppable, unmistakably S5.
pub const MAGIC: [u8; 8] = [b'S', b'5', b'.', b'p', b'r', b'o', 0x5b, b'P'];

/// Format version, stored in the reserved word's last byte (index 11).
pub const VERSION: u8 = 1;

/// Bytes per member: 12-byte prefix + `u32` absolute offset = 16, naturally
/// aligned; the prefix is first so binary search reads the prefix column directly.
pub const MEMBER_LEN: usize = HASH_PREFIX_LEN + 4;

/// Fixed prefix before the member table: `MAGIC(8) + reserved(4) + count(4)`.
pub const FIXED_HEADER_LEN: usize = 16;

/// Bytes of the `end_offset: u32` field that follows the member table.
pub const END_OFFSET_LEN: usize = 4;

/// The data region begins on a **4 KiB** boundary (the header is zero-padded up
/// to it). 4 KiB matches the FS5 leaf/node padding block: blobs are densely
/// concatenated after the aligned start and each padded blob is a 4 KiB multiple
/// (pad → length-preserving ChaCha20), so EVERY blob lands on a 4 KiB offset —
/// page-aligned ranged reads, for free. (Emergent, not enforced: holds while
/// blobs stay 4 KiB-padded, the default.) A larger align (e.g. 16 KiB) would
/// only align the *first* blob — the rest follow their 4 KiB-padded sizes — so
/// it would just waste header padding. A speculative ~16 KiB front GET still
/// captures a typical header in one round-trip regardless of this value, and
/// since member offsets are **absolute**, changing it stays backward-compatible
/// with packs written under a different alignment.
pub const DATA_ALIGN: u64 = 4 * 1024;

/// One member: a blob's 12-byte hash prefix and its **absolute** byte offset in
/// the pack body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PackMember {
    pub hash_prefix: [u8; HASH_PREFIX_LEN],
    pub offset: u32,
}

/// A pack's parsed index: members (sorted ascending by prefix, each with an
/// absolute offset) and `end_offset` (= total pack size). `pack_hash` is the
/// body's content address — recovered as the object id, never in the header bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackHeader {
    pub pack_hash: Hash,
    pub members: Vec<PackMember>,
    pub end_offset: u32,
}

impl PackHeader {
    /// Byte length of member `i` — derived from consecutive offsets; the last
    /// member runs to `end_offset`.
    pub fn member_len(&self, i: usize) -> u32 {
        let start = self.members[i].offset;
        let end = self
            .members
            .get(i + 1)
            .map_or(self.end_offset, |m| m.offset);
        end - start
    }

    /// Locate `prefix` by binary search → `(offset, length)`, or `None`.
    pub fn locate(&self, prefix: &[u8; HASH_PREFIX_LEN]) -> Option<(u32, u32)> {
        let i = self
            .members
            .binary_search_by_key(prefix, |m| m.hash_prefix)
            .ok()?;
        Some((self.members[i].offset, self.member_len(i)))
    }

    /// Self-contained encoding for the **local manifest cache** (a separate file,
    /// so it carries `pack_hash` and has no data padding): `pack_hash(32) |
    /// count(u32) | members | end_offset(u32)`.
    pub fn to_cache_bytes(&self) -> Bytes {
        let n = self.members.len();
        let mut out = Vec::with_capacity(32 + 4 + n * MEMBER_LEN + END_OFFSET_LEN);
        out.extend_from_slice(self.pack_hash.as_bytes());
        out.extend_from_slice(&(n as u32).to_le_bytes());
        write_members(&mut out, &self.members);
        out.extend_from_slice(&self.end_offset.to_le_bytes());
        Bytes::from(out)
    }

    /// Inverse of [`to_cache_bytes`](Self::to_cache_bytes).
    pub fn from_cache_bytes(bytes: &[u8]) -> io::Result<PackHeader> {
        if bytes.len() < 36 {
            return Err(bad("manifest cache entry shorter than header"));
        }
        let mut h = [0u8; 32];
        h.copy_from_slice(&bytes[0..32]);
        let pack_hash = Hash::from_bytes(h);
        let n = u32::from_le_bytes(bytes[32..36].try_into().unwrap()) as usize;
        let need = 36 + n * MEMBER_LEN + END_OFFSET_LEN;
        if bytes.len() != need {
            return Err(bad("manifest cache entry length mismatch"));
        }
        let members = read_members(&bytes[36..], n);
        let eo = 36 + n * MEMBER_LEN;
        let end_offset = u32::from_le_bytes(bytes[eo..eo + 4].try_into().unwrap());
        Ok(PackHeader {
            pack_hash,
            members,
            end_offset,
        })
    }
}

/// Byte length of the header region (magic … end_offset, before data padding)
/// for `n` members.
pub fn header_region_len(n: usize) -> usize {
    FIXED_HEADER_LEN + n * MEMBER_LEN + END_OFFSET_LEN
}

/// Absolute byte offset at which the data region begins: the header region
/// rounded up to [`DATA_ALIGN`].
pub fn data_start(n: usize) -> u64 {
    (header_region_len(n) as u64).div_ceil(DATA_ALIGN) * DATA_ALIGN
}

/// Build the **prepended, zero-padded** header for blobs given as
/// `(prefix, length)` pairs **sorted ascending by prefix**. The data blobs, in
/// the same order, are concatenated immediately after the returned bytes to form
/// the pack body. Returns `(header_bytes, end_offset)`; `end_offset` is the total
/// pack size. Offsets are absolute (data starts at the 16 KiB-aligned boundary).
///
/// Errors if the pack would exceed 4 GiB (u32 offsets).
pub fn encode_header(members: &[([u8; HASH_PREFIX_LEN], u32)]) -> io::Result<(Bytes, u32)> {
    let n = members.len();
    let start = data_start(n);
    if start > u32::MAX as u64 {
        return Err(bad("pack header too large for u32 offsets"));
    }
    let mut offset = start as u32;
    let mut offsets = Vec::with_capacity(n);
    for (_, len) in members {
        offsets.push(offset);
        offset = offset
            .checked_add(*len)
            .ok_or_else(|| bad("pack exceeds 4 GiB (u32 offsets)"))?;
    }
    let end_offset = offset;

    let mut out = Vec::with_capacity(start as usize);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&[0, 0, 0, VERSION]); // reserved (3) + version
    out.extend_from_slice(&(n as u32).to_le_bytes());
    for ((prefix, _), off) in members.iter().zip(&offsets) {
        out.extend_from_slice(prefix);
        out.extend_from_slice(&off.to_le_bytes());
    }
    out.extend_from_slice(&end_offset.to_le_bytes());
    out.resize(start as usize, 0); // zero-pad to the aligned data start
    Ok((Bytes::from(out), end_offset))
}

/// Verify the magic + version in the fixed prefix (first [`FIXED_HEADER_LEN`]
/// bytes) and return `blob_count`. Lets a reader learn the header region's size
/// before the second ranged read. `None`-equivalent (an error) for a body that
/// isn't a self-describing S5 pack (foreign / corrupt).
pub fn parse_count(prefix: &[u8]) -> io::Result<u32> {
    if prefix.len() < FIXED_HEADER_LEN {
        return Err(bad("pack body shorter than the fixed header"));
    }
    if prefix[0..8] != MAGIC {
        return Err(bad("pack body: bad magic (not a self-describing S5 pack)"));
    }
    if prefix[11] != VERSION {
        return Err(bad("pack body: unsupported version"));
    }
    Ok(u32::from_le_bytes(prefix[12..16].try_into().unwrap()))
}

/// Decode the full header from the front of a pack body (`bytes` must hold at
/// least `header_region_len(parse_count(bytes))` bytes), given the body's content
/// address `pack_hash`.
pub fn decode_header(bytes: &[u8], pack_hash: Hash) -> io::Result<PackHeader> {
    let n = parse_count(bytes)? as usize;
    let need = header_region_len(n);
    if bytes.len() < need {
        return Err(bad("pack body too short for its declared member count"));
    }
    let members = read_members(&bytes[FIXED_HEADER_LEN..], n);
    let eo = FIXED_HEADER_LEN + n * MEMBER_LEN;
    let end_offset = u32::from_le_bytes(bytes[eo..eo + 4].try_into().unwrap());
    Ok(PackHeader {
        pack_hash,
        members,
        end_offset,
    })
}

fn write_members(out: &mut Vec<u8>, members: &[PackMember]) {
    for m in members {
        out.extend_from_slice(&m.hash_prefix);
        out.extend_from_slice(&m.offset.to_le_bytes());
    }
}

/// Read `n` members from the start of `bytes` (caller validated length).
fn read_members(bytes: &[u8], n: usize) -> Vec<PackMember> {
    let mut members = Vec::with_capacity(n);
    for i in 0..n {
        let off = i * MEMBER_LEN;
        let mut hash_prefix = [0u8; HASH_PREFIX_LEN];
        hash_prefix.copy_from_slice(&bytes[off..off + HASH_PREFIX_LEN]);
        let offset = u32::from_le_bytes(
            bytes[off + HASH_PREFIX_LEN..off + MEMBER_LEN]
                .try_into()
                .unwrap(),
        );
        members.push(PackMember {
            hash_prefix,
            offset,
        });
    }
    members
}

fn bad(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pfx(b: u8) -> [u8; HASH_PREFIX_LEN] {
        [b; HASH_PREFIX_LEN]
    }

    #[test]
    fn header_round_trips_from_front_with_absolute_offsets() {
        let members = [(pfx(1), 100u32), (pfx(2), 4096), (pfx(3), 7)];
        let (bytes, end) = encode_header(&members).unwrap();

        // data starts 16 KiB-aligned; offsets absolute; end = pack size.
        let start = data_start(members.len());
        assert_eq!(bytes.len() as u64, start, "header padded to data start");
        assert_eq!(end as u64, start + 100 + 4096 + 7);

        let h = decode_header(&bytes, Hash::from_bytes([9u8; 32])).unwrap();
        assert_eq!(h.members.len(), 3);
        assert_eq!(h.members[0].offset as u64, start);
        assert_eq!(h.member_len(0), 100);
        assert_eq!(h.member_len(1), 4096);
        assert_eq!(h.member_len(2), 7); // last runs to end_offset
        assert_eq!(h.locate(&pfx(2)), Some((start as u32 + 100, 4096)));
        assert_eq!(h.locate(&pfx(7)), None);
    }

    #[test]
    fn parse_count_rejects_foreign_and_truncated() {
        assert!(parse_count(&[]).is_err());
        assert!(parse_count(b"not-a-pack-xxxxx").is_err());
        let (bytes, _) = encode_header(&[(pfx(1), 10)]).unwrap();
        assert_eq!(parse_count(&bytes).unwrap(), 1);
    }

    #[test]
    fn cache_bytes_round_trip() {
        let (body, _) = encode_header(&[(pfx(5), 11), (pfx(6), 22)]).unwrap();
        let h = decode_header(&body, Hash::from_bytes([3u8; 32])).unwrap();
        let restored = PackHeader::from_cache_bytes(&h.to_cache_bytes()).unwrap();
        assert_eq!(restored, h);
    }
}
