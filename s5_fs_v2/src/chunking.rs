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

/// Target (average) chunk size — the `1 / 2^16`-per-byte boundary probability
/// from [`MASK`] yields this on random data. Not a hard bound (see
/// [`MIN_CHUNK_SIZE`] / [`MAX_CHUNK_SIZE`]); exposed for the default
/// [`crate::node::DataCdcParams`] `avg_size`. **Xet spec target**: 64 KiB.
pub const TARGET_CHUNK_SIZE: usize = 64 * 1024;

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

        // Recompute the rolling hash from scratch every call. `find_boundary`
        // re-scans the whole current buffer from `skip_len`, so it MUST start
        // from a clean hash — otherwise a chunk that spans more than one read
        // (buffer grows, no boundary found on the first pass) would re-prime on
        // top of the previous pass's residual hash and pick DIFFERENT, buffering-
        // dependent boundaries, breaking the "same content → same chunks" CDC
        // invariant. The rescan is cheap (≤2 reads reach MAX_CHUNK_SIZE) and the
        // 64-bit shift means only the last 64 bytes influence the hash, so
        // re-scanning yields the exact same hash as a byte-by-byte pass.
        self.hash = 0;

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

/// Fixed-size stream chunker. Yields `Bytes` of exactly `chunk_size` bytes
/// per call, with the final chunk being whatever remains (no padding).
///
/// Designed for callers whose source files are immutable, content-addressed
/// at the file level, and don't benefit from CDC sub-chunking — append-only
/// log/segment workloads, snapshot-style archival of large mostly-static
/// blobs. Pair with `FileChunkingStrategy::Fixed { chunk_size }` and a
/// `BlobPipeline` that omits padding/encryption when both are unwanted.
///
/// Typical sizing: pick a `chunk_size` that single-blobs the dominant file
/// shape in the workload (so most files = 1 chunk = 1 blob). The "no
/// chunking" case is `chunk_size = u32::MAX` (per `FileChunkingStrategy::None`
/// semantics — files up to 4 GiB become a single blob).
pub struct FixedChunker<R> {
    stream: R,
    chunk_size: usize,
    eof: bool,
}

impl<R: AsyncRead + std::marker::Unpin> FixedChunker<R> {
    pub fn new(stream: R, chunk_size: usize) -> Self {
        assert!(chunk_size > 0, "FixedChunker chunk_size must be > 0");
        Self {
            stream,
            chunk_size,
            eof: false,
        }
    }

    /// Read the next fixed-size chunk from the stream. Returns `None` when
    /// the stream is fully consumed; the last chunk may be smaller than
    /// `chunk_size` if the stream length is not a multiple of `chunk_size`.
    pub async fn next_chunk(&mut self) -> std::io::Result<Option<Bytes>> {
        if self.eof {
            return Ok(None);
        }
        // Grow `buf` to the ACTUAL bytes read via a bounded scratch buffer —
        // NEVER pre-allocate `chunk_size`. For `None` chunking, chunk_size is
        // `u32::MAX` (4 GiB), so the old `resize(chunk_size, 0)` zero-filled a
        // 4 GiB buffer to read a 52-byte file — a ~2.6 s memset at memory
        // bandwidth, INDEPENDENT of the real size. That was the dominant snap
        // cost for the many tiny one-blob sidecars (`.eseg.didx`, `.didbloom`,
        // `.ridx`), measured 2026-06-18 s5 (`import_call_ms≈2600`, prev_get=0,
        // retries=0). The scratch read keeps exact FixedChunker boundary
        // semantics (exactly `chunk_size` per call bar the final partial) with
        // allocation proportional to the data, not the nominal chunk size.
        const SCRATCH: usize = 256 * 1024;
        let mut buf = BytesMut::with_capacity(self.chunk_size.min(SCRATCH));
        let mut scratch = vec![0u8; self.chunk_size.min(SCRATCH)];
        while buf.len() < self.chunk_size {
            let want = (self.chunk_size - buf.len()).min(scratch.len());
            let n = self.stream.read(&mut scratch[..want]).await?;
            if n == 0 {
                self.eof = true;
                break;
            }
            buf.extend_from_slice(&scratch[..n]);
        }
        if buf.is_empty() {
            // Clean EOF on a chunk boundary — nothing more to yield.
            return Ok(None);
        }
        Ok(Some(buf.freeze()))
    }
}

#[cfg(test)]
mod fixed_chunker_tests {
    use super::*;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn empty_stream() {
        let mut c = FixedChunker::new(BufReader::new(&b""[..]), 1024);
        assert!(c.next_chunk().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn shorter_than_chunk_size() {
        let data = vec![0xABu8; 500];
        let mut c = FixedChunker::new(BufReader::new(&data[..]), 1024);
        let chunk = c.next_chunk().await.unwrap().expect("one chunk");
        assert_eq!(chunk.len(), 500);
        assert!(c.next_chunk().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn exact_chunk_boundary() {
        let data = vec![0xCDu8; 2048];
        let mut c = FixedChunker::new(BufReader::new(&data[..]), 1024);
        assert_eq!(c.next_chunk().await.unwrap().unwrap().len(), 1024);
        assert_eq!(c.next_chunk().await.unwrap().unwrap().len(), 1024);
        assert!(c.next_chunk().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn ragged_remainder() {
        let data: Vec<u8> = (0..2500).map(|i| (i % 251) as u8).collect();
        let mut c = FixedChunker::new(BufReader::new(&data[..]), 1024);
        let a = c.next_chunk().await.unwrap().unwrap();
        let b = c.next_chunk().await.unwrap().unwrap();
        let r = c.next_chunk().await.unwrap().unwrap();
        assert_eq!(a.len(), 1024);
        assert_eq!(b.len(), 1024);
        assert_eq!(r.len(), 452);
        assert!(c.next_chunk().await.unwrap().is_none());

        let mut all = Vec::new();
        all.extend_from_slice(&a);
        all.extend_from_slice(&b);
        all.extend_from_slice(&r);
        assert_eq!(all, data);
    }

    /// Regression (2026-06-18 s5): a tiny file with `chunk_size = u32::MAX`
    /// (the `FileChunkingStrategy::None` representation — one blob per file)
    /// must yield the whole file as ONE chunk WITHOUT allocating/zero-filling
    /// `chunk_size`. The old `resize(chunk_size, 0)` zero-filled 4 GiB here
    /// (~2.6 s memset) per tiny sidecar — the dominant publish-snap cost. If
    /// the giant allocation regressed, this test would hang/OOM instead of
    /// finishing instantly.
    #[tokio::test]
    async fn none_chunking_tiny_file_no_giant_alloc() {
        let data = [0x5Au8; 52]; // a 52-byte .eseg.didx-sized sidecar
        let mut c = FixedChunker::new(BufReader::new(&data[..]), u32::MAX as usize);
        let chunk = c.next_chunk().await.unwrap().expect("one chunk");
        assert_eq!(chunk.len(), 52);
        assert_eq!(&chunk[..], &data[..]);
        assert!(c.next_chunk().await.unwrap().is_none());
    }
}

#[cfg(test)]
mod xet_chunker_tests {
    use super::*;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::ReadBuf;

    /// An `AsyncRead` that yields at most `step` bytes per read — forces the
    /// XetChunker to grow its buffer across many `find_boundary` calls per chunk,
    /// exercising the multi-read path where a residual-hash bug would surface.
    struct Trickle {
        data: Vec<u8>,
        pos: usize,
        step: usize,
    }
    impl AsyncRead for Trickle {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<std::io::Result<()>> {
            let remaining = self.data.len() - self.pos;
            let n = remaining.min(self.step).min(buf.remaining());
            if n > 0 {
                let (start, end) = (self.pos, self.pos + n);
                buf.put_slice(&self.data[start..end]);
                self.pos = end;
            }
            Poll::Ready(Ok(()))
        }
    }

    async fn chunk_sizes<R: AsyncRead + Unpin>(mut c: XetChunker<R>) -> Vec<usize> {
        let mut sizes = Vec::new();
        while let Some(ch) = c.next_chunk().await.unwrap() {
            sizes.push(ch.len());
        }
        sizes
    }

    /// Deterministic, well-distributed pseudo-random bytes (so CDC finds natural
    /// boundaries roughly every 64 KiB rather than forcing every chunk to MAX).
    fn pseudo_random(len: usize) -> Vec<u8> {
        (0..len as u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8)
            .collect()
    }

    #[tokio::test]
    async fn boundaries_independent_of_read_buffering() {
        // The core CDC invariant: identical content → identical chunk boundaries,
        // regardless of how the byte stream is delivered. (Regression guard for
        // the residual-hash bug: a trickle reader grows the buffer across many
        // `find_boundary` calls per chunk.)
        let data = pseudo_random(300 * 1024);
        let bulk = chunk_sizes(XetChunker::new(&data[..])).await;
        let trickle = chunk_sizes(XetChunker::new(Trickle {
            data: data.clone(),
            pos: 0,
            step: 97, // tiny, prime-sized reads
        }))
        .await;

        assert_eq!(
            bulk, trickle,
            "chunk boundaries must be independent of read buffering"
        );
        assert!(
            bulk.len() >= 2,
            "300 KiB of random data should yield several chunks"
        );

        // Every chunk but the last is within [MIN, MAX]; the whole thing reassembles.
        for (i, &s) in bulk.iter().enumerate() {
            if i + 1 < bulk.len() {
                assert!(
                    (MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&s),
                    "chunk {i} size {s} out of [{MIN_CHUNK_SIZE}, {MAX_CHUNK_SIZE}]"
                );
            }
        }
        assert_eq!(
            bulk.iter().sum::<usize>(),
            data.len(),
            "chunks cover all bytes"
        );
    }

    /// Xet spec conformance: chunk the *published* reference file and assert our
    /// boundaries match its `.chunks` lengths byte-for-byte — proves S5 ≡ Xet,
    /// not just that we match our own spec paraphrase. `#[ignore]` + env-gated
    /// (the fixture is ~60 MB, not vendored). Fetch from
    /// `huggingface.co/datasets/xet-team/xet-spec-reference-files`, then:
    ///
    /// ```sh
    /// S5_XET_REF_CSV=ref.csv S5_XET_REF_CHUNKS=ref.chunks \
    ///   cargo test -p s5_fs_v2 xet_reference_vectors_match -- --ignored --nocapture
    /// ```
    #[tokio::test]
    #[ignore = "needs the Xet reference fixture; set S5_XET_REF_CSV + S5_XET_REF_CHUNKS"]
    async fn xet_reference_vectors_match() {
        let (Ok(csv), Ok(chunks)) = (
            std::env::var("S5_XET_REF_CSV"),
            std::env::var("S5_XET_REF_CHUNKS"),
        ) else {
            eprintln!("SKIP xet_reference_vectors_match: set S5_XET_REF_CSV + S5_XET_REF_CHUNKS");
            return;
        };
        let data = std::fs::read(&csv).expect("read reference csv");
        // Each `.chunks` line is `<64-hex-hash> <length>`; compare lengths (the
        // spec's sanctioned boundary check).
        let expected: Vec<usize> = std::fs::read_to_string(&chunks)
            .expect("read .chunks")
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                l.split_whitespace()
                    .nth(1)
                    .expect("length column")
                    .parse()
                    .expect("length is a number")
            })
            .collect();

        let got = chunk_sizes(XetChunker::new(&data[..])).await;
        assert_eq!(
            got.len(),
            expected.len(),
            "chunk COUNT differs: got {} vs Xet {}",
            got.len(),
            expected.len()
        );
        assert_eq!(
            got, expected,
            "chunk boundaries diverge from the Xet reference"
        );
        eprintln!(
            "OK: {} chunks match the Xet reference byte-for-byte",
            got.len()
        );
    }
}
