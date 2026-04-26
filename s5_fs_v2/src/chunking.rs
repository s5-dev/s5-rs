use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Xet CDC parameters
pub const MIN_CHUNK_SIZE: usize = 8 * 1024; // 8 KiB
pub const MAX_CHUNK_SIZE: usize = 128 * 1024; // 128 KiB
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
