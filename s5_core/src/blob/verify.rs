//! Full-read BLAKE3 verification — the [`BlobsRead`](super::BlobsRead)
//! integrity contract's shared machinery.
//!
//! Every `BlobsRead` implementation must guarantee that a **full** blob
//! read (`blob_download`, `blob_read`) returns bytes hashing to the
//! requested [`Hash`]. Buffered reads use [`verify_bytes`]; streaming
//! reads wrap their reader in a [`VerifyingReader`], which hashes as
//! bytes flow through and fails the read at EOF on mismatch.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};

use super::BlobResult;
use crate::Hash;

/// Check that `bytes` hash to `hash`; pass them through on success.
pub fn verify_bytes(hash: Hash, bytes: bytes::Bytes) -> BlobResult<bytes::Bytes> {
    let actual = Hash::new(&bytes);
    if actual != hash {
        return Err(anyhow::anyhow!(
            "blob integrity check failed for {hash}: stored bytes hash to {actual}"
        ));
    }
    Ok(bytes)
}

/// An [`AsyncRead`] adapter that BLAKE3-hashes everything read through it
/// and returns `InvalidData` at EOF if the digest doesn't match the
/// expected content address. Bytes surface to the caller before the final
/// verdict — consumers that must not act on unverified data should buffer
/// (or use `blob_download`); the guarantee here is that a full read can
/// never *complete successfully* with wrong bytes.
pub struct VerifyingReader {
    inner: Box<dyn AsyncRead + Send + Unpin>,
    hasher: blake3::Hasher,
    expected: Hash,
    verified: bool,
}

impl VerifyingReader {
    pub fn new(expected: Hash, inner: Box<dyn AsyncRead + Send + Unpin>) -> Self {
        Self {
            inner,
            hasher: blake3::Hasher::new(),
            expected,
            verified: false,
        }
    }
}

impl AsyncRead for VerifyingReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // A zero-capacity buffer reads zero bytes without meaning EOF.
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let pre = buf.filled().len();
        let this = &mut *self;
        match Pin::new(&mut this.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let filled = buf.filled();
                if filled.len() == pre {
                    // EOF — the whole blob has streamed through; verify once.
                    if !this.verified {
                        this.verified = true;
                        let actual: Hash = this.hasher.finalize().into();
                        if actual != this.expected {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!(
                                    "blob integrity check failed for {}: streamed bytes hash to {actual}",
                                    this.expected
                                ),
                            )));
                        }
                    }
                } else {
                    this.hasher.update(&filled[pre..]);
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[test]
    fn verify_bytes_accepts_matching_and_rejects_corrupt() {
        let data = bytes::Bytes::from_static(b"hello blob");
        let hash = Hash::new(&data);
        assert_eq!(verify_bytes(hash, data.clone()).unwrap(), data);

        let wrong = bytes::Bytes::from_static(b"hello blob!");
        let err = verify_bytes(hash, wrong).unwrap_err();
        assert!(err.to_string().contains("integrity check failed"));
    }

    #[tokio::test]
    async fn verifying_reader_passes_good_stream_and_fails_bad() {
        let data = b"some streamed blob contents".to_vec();
        let hash = Hash::new(&data);

        let mut ok = VerifyingReader::new(hash, Box::new(std::io::Cursor::new(data.clone())));
        let mut out = Vec::new();
        ok.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, data);

        let mut corrupt = data;
        corrupt[3] ^= 0xff;
        let mut bad = VerifyingReader::new(hash, Box::new(std::io::Cursor::new(corrupt)));
        let mut out = Vec::new();
        let err = bad.read_to_end(&mut out).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
