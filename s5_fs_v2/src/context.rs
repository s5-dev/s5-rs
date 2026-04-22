//! Stateless crypto and compression helpers for FS5 V2.
//!
//! These are pure functions with no state — they operate on raw bytes
//! with explicit key/config parameters. [`Snapshot`](crate::snapshot::Snapshot)
//! is the runtime type that composes these with a store and on-wire context.
//!
//! # Blob Pipeline
//!
//! Blobs are processed through a three-stage pipeline defined by
//! [`BlobPipeline`](crate::node::BlobPipeline):
//!
//! **Encode (write):** compress → pad → encrypt
//! **Decode (read):**  decrypt → unpad → decompress
//!
//! Each stage is independently optional (controlled by the pipeline config).
//!
//! # Encryption Scheme
//!
//! Uses deterministic ChaCha20 (pure stream cipher, NO Poly1305/AEAD):
//! - **Key derivation**: `Key = blake3::derive_key(kdf_context, master_secret || plaintext_hash)`
//! - **Nonce**: Always zero (each blob has a unique derived key)
//! - **Padding**: Data is padded to block boundary before encryption
//! - **Authentication**: `blake3(ciphertext) == entry.hash` (network) +
//!   `blake3(plaintext) == entry.plaintext_hash` (local)

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::node::{BlobPipeline, CompressionStrategy, EncryptionStrategy};

// ---------------------------------------------------------------------------
// KDF Constants
// ---------------------------------------------------------------------------

/// KDF context for leaf (file content) encryption.
/// Key = blake3::derive_key(KDF_LEAF, master_secret || plaintext_hash)
pub(crate) const KDF_LEAF: &str = "s5/fs/v2/encrypt/leaf";

/// KDF context for metadata (node/tree) encryption.
/// Key = blake3::derive_key(KDF_META, master_secret || plaintext_hash)
pub(crate) const KDF_META: &str = "s5/fs/v2/encrypt/meta";

// ---------------------------------------------------------------------------
// Per-Blob Key Derivation
// ---------------------------------------------------------------------------

/// Derives a per-blob encryption key.
///
/// `Key = blake3::derive_key(context, master_secret || hash_bytes)`
fn derive_blob_key(kdf_context: &str, master_secret: &[u8; 32], hash: &[u8; 32]) -> [u8; 32] {
    let mut input = [0u8; 64];
    input[..32].copy_from_slice(master_secret);
    input[32..].copy_from_slice(hash);
    blake3::derive_key(kdf_context, &input)
}

// ---------------------------------------------------------------------------
// Padding
// ---------------------------------------------------------------------------

/// Pads data to the next `block_size` boundary with zeros.
///
/// If already aligned, no padding is added.
/// `block_size` of 1 is a no-op (every length is aligned to 1).
fn pad_to_boundary(data: &[u8], block_size: u32) -> Vec<u8> {
    let block_size = block_size as usize;
    if block_size <= 1 {
        return data.to_vec();
    }
    let remainder = data.len() % block_size;
    if remainder == 0 {
        return data.to_vec();
    }
    let padded_len = data.len() + (block_size - remainder);
    let mut out = Vec::with_capacity(padded_len);
    out.extend_from_slice(data);
    out.resize(padded_len, 0);
    out
}

/// Returns the stored size in blocks for the given data length and block size.
///
/// This is `ceil(len / block_size)`. With `block_size=1`, returns the exact byte count.
fn stored_blocks(len: usize, block_size: u32) -> u64 {
    let bs = block_size as u64;
    let len = len as u64;
    if bs <= 1 {
        return len;
    }
    len.div_ceil(bs)
}

// ---------------------------------------------------------------------------
// ChaCha20 Encrypt / Decrypt (Pure Stream Cipher)
// ---------------------------------------------------------------------------

/// Applies ChaCha20 keystream to data (encrypt or decrypt — symmetric).
///
/// Uses nonce = 0 because each blob has a unique derived key.
fn apply_chacha20(key: &[u8; 32], data: &mut [u8]) {
    use chacha20::cipher::{KeyIvInit, StreamCipher};
    use chacha20::ChaCha20;

    let nonce = [0u8; 12];
    let mut cipher = ChaCha20::new(key.into(), &nonce.into());
    cipher.apply_keystream(data);
}

// ---------------------------------------------------------------------------
// Pipeline Encode / Decode
// ---------------------------------------------------------------------------

/// Result of encoding a blob through the pipeline.
pub(crate) struct PipelineEncodeResult {
    /// The processed (compressed/padded/encrypted) bytes ready for storage.
    pub bytes: Bytes,
    /// Stored size in padding blocks (= exact byte count when block_size=1).
    pub stored_blocks: u64,
}

/// Encodes plaintext bytes through the blob pipeline: compress → pad → encrypt.
///
/// `plaintext_hash` is the BLAKE3 of the original plaintext, used as KDF input
/// for key derivation when encryption is active.
///
/// `kdf_context` differentiates leaf vs node encryption (e.g. `KDF_LEAF` or `KDF_META`).
///
/// `keys` is the key map from `TraversalContext.keys`.
pub(crate) fn pipeline_encode(
    plaintext: &[u8],
    pipeline: Option<&BlobPipeline>,
    plaintext_hash: &[u8; 32],
    kdf_context: &str,
    keys: Option<&BTreeMap<u8, [u8; 32]>>,
) -> anyhow::Result<PipelineEncodeResult> {
    let Some(pipeline) = pipeline else {
        // No pipeline = no transforms, pass through.
        let len = plaintext.len();
        return Ok(PipelineEncodeResult {
            bytes: Bytes::copy_from_slice(plaintext),
            stored_blocks: len as u64,
        });
    };

    // Stage 1: Compress
    let compressed = compress_bytes(plaintext, &pipeline.compression)?;

    // Stage 2: Pad
    let block_size = pipeline.padding.as_ref().map(|p| p.block_size).unwrap_or(1);
    let padded = pad_to_boundary(&compressed, block_size);
    let blocks = stored_blocks(padded.len(), block_size);

    // Stage 3: Encrypt
    let encrypted = match &pipeline.encryption {
        Some((EncryptionStrategy::DeterministicChaCha20, key_slot)) => {
            let keys = keys.ok_or_else(|| anyhow::anyhow!("encryption requested but no keys"))?;
            let master_secret = keys.get(key_slot).ok_or_else(|| {
                anyhow::anyhow!("encryption key slot 0x{key_slot:02x} not found in key map")
            })?;
            let key = derive_blob_key(kdf_context, master_secret, plaintext_hash);
            let mut data = padded;
            apply_chacha20(&key, &mut data);
            Bytes::from(data)
        }
        Some((EncryptionStrategy::Plaintext, _)) | None => Bytes::from(padded),
    };

    Ok(PipelineEncodeResult {
        bytes: encrypted,
        stored_blocks: blocks,
    })
}

/// Decodes stored bytes through the blob pipeline (reverse): decrypt → truncate padding → decompress.
///
/// `plaintext_hash` is needed as KDF input for decryption key derivation.
/// `plaintext_size` is used to truncate padding zeros before decompression.
///
/// `kdf_context` differentiates leaf vs node encryption (e.g. `KDF_LEAF` or `KDF_META`).
pub(crate) fn pipeline_decode(
    stored: Bytes,
    pipeline: Option<&BlobPipeline>,
    plaintext_hash: Option<&[u8; 32]>,
    plaintext_size: u64,
    kdf_context: &str,
    keys: Option<&BTreeMap<u8, [u8; 32]>>,
) -> anyhow::Result<Bytes> {
    let Some(pipeline) = pipeline else {
        // No pipeline = no transforms, pass through.
        return Ok(stored);
    };

    // Stage 1: Decrypt
    let decrypted = match &pipeline.encryption {
        Some((EncryptionStrategy::DeterministicChaCha20, key_slot)) => {
            let plaintext_hash = plaintext_hash
                .ok_or_else(|| anyhow::anyhow!("decryption requires plaintext_hash for KDF"))?;
            let keys = keys.ok_or_else(|| anyhow::anyhow!("decryption requested but no keys"))?;
            let master_secret = keys.get(key_slot).ok_or_else(|| {
                anyhow::anyhow!("decryption key slot 0x{key_slot:02x} not found in key map")
            })?;
            let key = derive_blob_key(kdf_context, master_secret, plaintext_hash);
            let mut data = stored.to_vec();
            apply_chacha20(&key, &mut data);
            data
        }
        Some((EncryptionStrategy::Plaintext, _)) | None => stored.to_vec(),
    };

    // Stage 2: Truncate padding
    // After decryption, we have compressed data + zero padding.
    // We need to know where the compressed data ends.
    // For compressed data: use plaintext_size as a hint — but the compressed
    // size may differ from plaintext_size. The correct approach depends on
    // the compression format:
    // - Zstd frames are self-delimiting, so zstd will stop at the frame end
    //   and ignore trailing zeros (as long as we use streaming decompression).
    //   Actually, zstd::decode_all does NOT ignore trailing bytes — it may error.
    //   We need to truncate. For uncompressed data, plaintext_size IS the data size.
    //
    // Strategy: if no compression, truncate to plaintext_size. If compressed,
    // the compressed size is unknown from metadata alone, so we must rely on
    // the decompressor handling trailing zeros, OR we store the compressed size.
    // For now, zstd is frame-delimited so we let it parse what it can.
    // TODO: Consider storing compressed size if zstd can't handle trailing zeros.

    // Stage 3: Decompress
    let decompressed = match &pipeline.compression {
        Some(compression) => decompress_bytes_raw(&decrypted, compression)?,
        None => {
            // No compression — truncate to plaintext_size to remove padding.
            let size = plaintext_size as usize;
            if size < decrypted.len() {
                Bytes::from(decrypted[..size].to_vec())
            } else {
                Bytes::from(decrypted)
            }
        }
    };

    Ok(decompressed)
}

// ---------------------------------------------------------------------------
// Compression
// ---------------------------------------------------------------------------

/// Decompresses bytes using the given compression strategy.
///
/// Internal: works on raw byte slices (after decrypt, potentially with trailing padding).
fn decompress_bytes_raw(bytes: &[u8], compression: &CompressionStrategy) -> anyhow::Result<Bytes> {
    match compression {
        CompressionStrategy::Uncompressed => Ok(Bytes::copy_from_slice(bytes)),
        CompressionStrategy::Zstd => {
            // Use streaming Decoder in single_frame mode: reads exactly one
            // zstd frame and stops. Without single_frame(), read_to_end
            // tries to concatenate multiple frames and chokes on trailing
            // zero padding bytes (which aren't a valid frame header).
            use std::io::Read;
            let mut decoder = zstd::Decoder::new(bytes)
                .map_err(|e| anyhow::anyhow!("zstd decoder init failed: {e}"))?
                .single_frame();
            let mut decoded = Vec::new();
            decoder
                .read_to_end(&mut decoded)
                .map_err(|e| anyhow::anyhow!("zstd decompression failed: {e}"))?;
            Ok(Bytes::from(decoded))
        }
        CompressionStrategy::ZstdDictFromPrecedingEntry { .. } => {
            // Without the dictionary, this is just zstd with a dict —
            // the caller must handle dictionary setup externally.
            // For now, bail. The snapshot/persist layers handle dict lookup.
            anyhow::bail!("ZstdDictFromPrecedingEntry requires dictionary setup by caller")
        }
    }
}

/// Compresses bytes using the given compression strategy.
pub(crate) fn compress_bytes(
    bytes: &[u8],
    compression: &Option<CompressionStrategy>,
) -> anyhow::Result<Bytes> {
    match compression {
        Some(CompressionStrategy::Uncompressed) | None => Ok(Bytes::copy_from_slice(bytes)),
        Some(CompressionStrategy::Zstd) => {
            let encoded = zstd::encode_all(bytes, 3)
                .map_err(|e| anyhow::anyhow!("zstd compression failed: {e}"))?;
            Ok(Bytes::from(encoded))
        }
        Some(CompressionStrategy::ZstdDictFromPrecedingEntry { .. }) => {
            // Dictionary-based compression requires external setup.
            anyhow::bail!("ZstdDictFromPrecedingEntry requires dictionary setup by caller")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{EncryptionStrategy, PaddingStrategy};

    #[test]
    fn pipeline_round_trip_encrypted() {
        let plaintext = b"hello world this is some test data for pipeline round trip";
        let plaintext_hash: [u8; 32] = *blake3::hash(plaintext).as_bytes();
        let master_secret = [42u8; 32];
        let mut keys = BTreeMap::new();
        keys.insert(0x0eu8, master_secret);

        let pipeline = BlobPipeline {
            compression: Some(CompressionStrategy::Zstd),
            padding: Some(PaddingStrategy { block_size: 1024 }),
            encryption: Some((EncryptionStrategy::DeterministicChaCha20, 0x0e)),
        };

        // Encode
        let result = pipeline_encode(
            plaintext,
            Some(&pipeline),
            &plaintext_hash,
            KDF_META,
            Some(&keys),
        )
        .unwrap();

        assert_eq!(result.bytes.len(), 1024);

        // Decode
        let decoded = pipeline_decode(
            result.bytes,
            Some(&pipeline),
            Some(&plaintext_hash),
            0, // plaintext_size=0, relying on zstd single_frame
            KDF_META,
            Some(&keys),
        )
        .unwrap();

        assert_eq!(&decoded[..], plaintext);
    }
}
