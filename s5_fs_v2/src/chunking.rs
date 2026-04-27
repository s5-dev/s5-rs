//! Content-defined chunking for FS5 leaf blobs.
//!
//! Uses the gearhash rolling-hash CDC algorithm with parameters that are
//! **byte-for-byte compatible with the Huggingface Xet spec**:
//!
//! - `MIN_CHUNK_SIZE = 8 KiB`, `MAX_CHUNK_SIZE = 128 KiB`, `MASK = 0xFFFF_0000_0000_0000`
//! - 16-bit mask → expected boundary every 2^16 = 65 536 bytes (target ~64 KiB)
//! - Same 256-entry `gearhash::DEFAULT_TABLE` (from the `gearhash` crate)
//!
//! This means FS5 and Xet produce identical chunk boundaries for any input,
//! opening the door to future interop (shared chunk stores with Huggingface's
//! dataset ecosystem). Changing any of MIN/MAX/MASK breaks that property.
//!
//! # Why 64 KiB?
//!
//! 64 KiB is an intentional sweet spot for S5's **general-purpose** role as a
//! content-addressed filesystem + sync layer — not just a backup tool:
//!
//! - **~0.1 % metadata overhead.** A 32-byte BLAKE3 hash per chunk means
//!   ~0.05 % per-leaf + ~0.05 % prolly-tree internal-node overhead, totalling
//!   ~0.1 %. Larger chunks would reduce metadata further, but at marginal
//!   absolute savings (tens of MB on a 100 GB vault).
//! - **Random-seek latency for media.** Scrubbing through a large video file
//!   over the network downloads 1–2 chunks per seek — 64–128 KiB with these
//!   defaults (~10 ms on 100 Mbps). 512 KiB chunks would multiply seek
//!   latency 8×; 2 MiB chunks (restic-style) would make interactive
//!   scrubbing painful.
//! - **Chunk-granularity dedup.** Smaller chunks give better cross-file and
//!   cross-version dedup, which matters for document edit workflows.
//! - **Bounded memory in flight.** With up to 8-way parallel chunk processing,
//!   the worst-case working set is 8 × 128 KiB = 1 MiB. Safe on mobile.
//!
//! The conventional larger-chunk backup tools (restic, borg: 1–4 MiB target)
//! optimise for write-once archival throughput. S5 is closer to a live FS.
//!
//! # Xet Spec Reference
//!
//! The canonical spec (constant values, gearhash table source, test vectors):
//!
//! <https://github.com/huggingface/xet-core/blob/main/docs/cdc-spec.md>

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Minimum chunk size. No boundary is ever taken before this offset, which
/// prevents pathologically short chunks when the rolling hash matches too often
/// (e.g. on highly repetitive data).
///
/// **Xet spec requirement**: 8 KiB.
pub const MIN_CHUNK_SIZE: usize = 8 * 1024;

/// Maximum chunk size. A boundary is forced at this offset even if the rolling
/// hash hasn't matched, bounding worst-case chunk size (e.g. on incompressible
/// data where matches are rare).
///
/// **Xet spec requirement**: 128 KiB.
pub const MAX_CHUNK_SIZE: usize = 128 * 1024;

/// Gearhash boundary mask. A byte offset is a chunk boundary when
/// `(rolling_hash & MASK) == 0`. With 16 one-bits in the upper word, boundaries
/// occur with probability `1 / 2^16` per byte, yielding an expected chunk size
/// of ~64 KiB (clamped to `[MIN_CHUNK_SIZE, MAX_CHUNK_SIZE]`).
///
/// **Xet spec requirement**: `0xFFFF_0000_0000_0000`.
pub const MASK: u64 = 0xFFFF_0000_0000_0000;

/// A stream chunker that reads from an `AsyncRead` and yields `Bytes` chunks
/// using the Xet Gearhash CDC algorithm.
pub struct XetChunker<R> {
    stream: R,
    buffer: BytesMut,
    eof: bool,
    hash: u64,
}

impl<R: AsyncRead + std::marker::Unpin> XetChunker<R> {
    pub fn new(stream: R) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(MAX_CHUNK_SIZE * 2),
            eof: false,
            hash: 0,
        }
    }

    /// Read the next content-defined chunk from the stream.
    /// Returns `None` when the stream is fully consumed.
    pub async fn next_chunk(&mut self) -> std::io::Result<Option<Bytes>> {
        loop {
            // 1. If buffer is empty and EOF is reached, we are done.
            if self.eof && self.buffer.is_empty() {
                return Ok(None);
            }

            // 2. Try to find a boundary in the current buffer.
            if let Some(boundary_idx) = self.find_boundary() {
                let chunk = self.buffer.split_to(boundary_idx);
                self.hash = 0; // Reset hash for next chunk
                return Ok(Some(chunk.freeze()));
            }

            // 3. We didn't find a boundary.
            // If we hit EOF, emit whatever is left (it's guaranteed to be < MAX_CHUNK_SIZE
            // because find_boundary would have forced a cut at MAX_CHUNK_SIZE).
            if self.eof {
                let chunk = self.buffer.split();
                self.hash = 0;
                return Ok(Some(chunk.freeze()));
            }

            // 4. We need more data to find a boundary.
            // Read up to 64 KiB at a time to fill the buffer.
            let mut temp = [0u8; 64 * 1024];
            let n = self.stream.read(&mut temp).await?;
            if n == 0 {
                self.eof = true;
            } else {
                self.buffer.extend_from_slice(&temp[..n]);
            }
        }
    }

    /// Scans the current buffer to find the next chunk boundary.
    /// Returns the byte index (exclusive) of the boundary if found, otherwise `None`.
    fn find_boundary(&mut self) -> Option<usize> {
        let len = self.buffer.len();

        // If we have less than MIN_CHUNK_SIZE and we are not at EOF, we cannot boundary yet.
        // We wait for more data.
        if len < MIN_CHUNK_SIZE && !self.eof {
            return None;
        }

        // Fast path: Cut-point skipping optimization.
        // The rolling hash only depends on the last 64 bytes.
        // We can safely skip the first (MIN_CHUNK_SIZE - 64) bytes without hashing.
        // We initialize the hash with the 64 bytes right before MIN_CHUNK_SIZE.
        let skip_len = MIN_CHUNK_SIZE.saturating_sub(64);

        let mut i = skip_len;

        // Ensure we prime the hash up to MIN_CHUNK_SIZE or EOF length
        let prime_end = std::cmp::min(MIN_CHUNK_SIZE.saturating_sub(1), len);
        while i < prime_end {
            let b = self.buffer[i];
            self.hash = (self.hash << 1).wrapping_add(gearhash::DEFAULT_TABLE[b as usize]);
            i += 1;
        }

        // Now we actually check for boundaries
        while i < len {
            let b = self.buffer[i];
            self.hash = (self.hash << 1).wrapping_add(gearhash::DEFAULT_TABLE[b as usize]);

            let size = i + 1;

            if size < MIN_CHUNK_SIZE {
                i += 1;
                continue;
            }

            if size >= MAX_CHUNK_SIZE || (self.hash & MASK) == 0 {
                return Some(size);
            }

            i += 1;
        }

        None
    }
}
