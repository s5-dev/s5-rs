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
// Compression
// ---------------------------------------------------------------------------

/// Zstd compression level used for all blob encoding.
///
/// Level 1 is chosen as the default because:
///
/// - On already-compressed data (photos, videos, PDFs, zip-family archives,
///   `.deb`/`.rpm` packages) level 1 produces output indistinguishable from
///   higher levels while running ~1.3× faster per core. This equivalence is
///   not coincidence: the padding-aware compression-skip (see
///   [`pipeline_encode`] and `Snapshot::with_skip_unhelpful_compression`)
///   detects that every level is unhelpful on such data and falls back to
///   storing plaintext uniformly. L1 just discards the futile compression
///   attempt faster.
/// - On moderately-redundant data (JSONL records, logs), level 1 is within
///   0–2 % of higher levels on size and often *better* on highly-repetitive
///   logs (zstd's "fast" family catches log patterns efficiently).
/// - On structured text (source code, XML, uncompressed documents) level 1 is
///   4–5 % larger than level 3, which is a real cost but typically small in
///   absolute terms for a mixed backup workload.
/// - Decompression cost is identical across zstd levels, so higher levels only
///   tax the encoder. In a backup + share workload, only the backup creator
///   pays; share recipients don't care which level was used.
///
/// For a typical home backup dominated by media and documents, the weighted
/// storage delta between level 1 and level 3 is under 1 % while the CPU delta
/// is 30–50 % per core — a strongly favourable trade, especially under a
/// daemon that runs frequent incremental backups on battery-powered or mobile
/// hardware.
///
/// See `docs/reference/compression-and-chunking.md` for the full benchmark
/// data this is based on, and for guidance on when a user might want to
/// override this (code-heavy archives, cold-storage vaults with abundant
/// CPU).
pub(crate) const ZSTD_LEVEL: i32 = 1;
// TODO(compression/upload-bound): revisit the level for slow/expensive backends.
// L1 is tuned for local CPU + mixed data (see above), but when the UPLOAD is the
// bottleneck (e.g. ~2.8 MB/s to Sia) a higher level trades cheap local CPU for
// fewer bytes over the wire — often a net-faster snap. Consider a
// backend-/content-adaptive level (higher for remote erasure-coded stores, L1
// for local), set on the vault like the other pipeline defaults.

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
    s5_core::crypto::derive_secret(kdf_context, &input)
}

/// The per-blob ChaCha20 key a `DeterministicChaCha20` **leaf** uses:
/// `derive_blob_key(KDF_LEAF, master, plaintext_hash)`.
///
/// The D21 `copy` mechanism ([`crate::copy`]) inlines *this* value (never
/// the master) so a destination reader can decrypt a reused leaf ciphertext
/// through `ExplicitKeyChaCha20` without ever learning the source master
/// data key.
pub(crate) fn leaf_blob_key(master: &[u8; 32], plaintext_hash: &[u8; 32]) -> [u8; 32] {
    derive_blob_key(KDF_LEAF, master, plaintext_hash)
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

/// Returns the byte length after padding to `block_size` boundary.
///
/// If `block_size <= 1`, returns `len` unchanged.
fn padded_len(len: usize, block_size: u32) -> usize {
    let bs = block_size as usize;
    if bs <= 1 {
        return len;
    }
    let remainder = len % bs;
    if remainder == 0 {
        len
    } else {
        len + (bs - remainder)
    }
}

// ---------------------------------------------------------------------------
// ChaCha20 Encrypt / Decrypt (Pure Stream Cipher)
// ---------------------------------------------------------------------------

/// Applies ChaCha20 keystream to data (encrypt or decrypt — symmetric).
///
/// Uses nonce = 0 because each blob has a unique derived key.
//
// TODO(privacy/opt-in max-privacy vault): nonce=0 + per-blob-derived-key is
// DETERMINISTIC (convergent) — identical plaintext → identical ciphertext within
// a vault, which is exactly what enables cross-device dedup. For a rare
// high-sensitivity vault, offer a per-vault "max privacy" mode: a new
// `EncryptionStrategy` variant with a random nonce, so equal plaintexts encrypt
// differently (no equality leakage) at the cost of convergent dedup. Convergent
// stays the default (right for backups); this is the escape hatch — "powerful
// when needed".
fn apply_chacha20(key: &[u8; 32], data: &mut [u8]) {
    use chacha20::ChaCha20;
    use chacha20::cipher::{KeyIvInit, StreamCipher};

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
    /// True when compression was skipped because the result exceeded the
    /// compression-skip threshold (the blob was stored uncompressed).
    pub compression_skipped: bool,
}

/// Encodes plaintext bytes through the blob pipeline: compress → pad → encrypt.
///
/// `plaintext_hash` is the BLAKE3 of the original plaintext, used as KDF input
/// for key derivation when encryption is active.
///
/// `kdf_context` differentiates leaf vs node encryption (e.g. `KDF_LEAF` or `KDF_META`).
///
/// `keys` is the key map from `TraversalContext.keys`.
///
/// The "skip compression when unhelpful" policy is read from
/// `pipeline.skip_when_unhelpful` — when set, compression is skipped if
/// the compressed+padded size is not smaller than the uncompressed+padded
/// size. The blob is then stored uncompressed and `compression_skipped`
/// is set in the result so the caller can record a per-entry override.
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
            compression_skipped: false,
        });
    };

    // Stage 1: Compress (with optional skip when unhelpful)
    let skip_when_unhelpful = pipeline.skip_when_unhelpful.unwrap_or(false);
    let block_size = pipeline.padding.as_ref().map(|p| p.block_size).unwrap_or(1);
    let (compressed, compression_skipped) = {
        let raw = compress_bytes(plaintext, &pipeline.compression)?;
        if skip_when_unhelpful && !plaintext.is_empty() {
            // Compare padded sizes: only keep compression if it actually
            // reduces the stored (post-padding) size.
            let padded_compressed_len = padded_len(raw.len(), block_size);
            let padded_uncompressed_len = padded_len(plaintext.len(), block_size);
            if padded_compressed_len >= padded_uncompressed_len {
                // Compression unhelpful after padding — fall back to uncompressed.
                (Bytes::copy_from_slice(plaintext), true)
            } else {
                (raw, false)
            }
        } else {
            (raw, false)
        }
    };

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
        Some((EncryptionStrategy::ExplicitKeyChaCha20, key_slot)) => {
            // The slot's 32 bytes ARE the ChaCha20 key — no KDF, no
            // plaintext_hash mixing (see [`crate::copy`]).
            let keys = keys.ok_or_else(|| anyhow::anyhow!("encryption requested but no keys"))?;
            let key = keys.get(key_slot).ok_or_else(|| {
                anyhow::anyhow!("explicit key slot 0x{key_slot:02x} not found in key map")
            })?;
            let mut data = padded;
            apply_chacha20(key, &mut data);
            Bytes::from(data)
        }
        Some((EncryptionStrategy::Plaintext, _)) | None => Bytes::from(padded),
    };

    Ok(PipelineEncodeResult {
        bytes: encrypted,
        stored_blocks: blocks,
        compression_skipped,
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
        Some((EncryptionStrategy::ExplicitKeyChaCha20, key_slot)) => {
            // The slot's 32 bytes ARE the ChaCha20 key. Deliberately does
            // NOT consult `plaintext_hash` (that is the whole point of the
            // `copy` inline: the dest reader has no source master to KDF
            // from). Integrity is still checked one layer up in
            // `export_leaf` via `blake3(plaintext) == plaintext_hash`.
            let keys = keys.ok_or_else(|| anyhow::anyhow!("decryption requested but no keys"))?;
            let key = keys.get(key_slot).ok_or_else(|| {
                anyhow::anyhow!("explicit key slot 0x{key_slot:02x} not found in key map")
            })?;
            let mut data = stored.to_vec();
            apply_chacha20(key, &mut data);
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
    //
    // Callers that don't know the plaintext size pass `plaintext_size == 0`
    // as a sentinel (see Snapshot::load). In those cases we can't truncate
    // padding here, so we return the decrypted bytes unchanged and let the
    // caller handle any trailing zero padding (e.g. zstd ignores it; the
    // plaintext hash check will catch mismatches).
    let decompressed = match &pipeline.compression {
        Some(CompressionStrategy::Uncompressed) => {
            if plaintext_size == 0 {
                Bytes::from(decrypted)
            } else {
                let size = plaintext_size as usize;
                if size < decrypted.len() {
                    Bytes::from(decrypted[..size].to_vec())
                } else {
                    Bytes::from(decrypted)
                }
            }
        }
        Some(compression) => decompress_bytes_raw(&decrypted, compression)?,
        None => {
            // No compression — truncate to plaintext_size to remove padding.
            if plaintext_size == 0 {
                Bytes::from(decrypted)
            } else {
                let size = plaintext_size as usize;
                if size < decrypted.len() {
                    Bytes::from(decrypted[..size].to_vec())
                } else {
                    Bytes::from(decrypted)
                }
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
        // `Zstd` and `ZstdLevel { .. }` decode identically — zstd's
        // decoder reads compression metadata from the frame header, so
        // the encoder-side level is not needed at decode time.
        CompressionStrategy::Zstd | CompressionStrategy::ZstdLevel { .. } => {
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
            let encoded = zstd::encode_all(bytes, ZSTD_LEVEL)
                .map_err(|e| anyhow::anyhow!("zstd compression failed: {e}"))?;
            Ok(Bytes::from(encoded))
        }
        Some(CompressionStrategy::ZstdLevel { level }) => {
            let encoded = zstd::encode_all(bytes, *level as i32)
                .map_err(|e| anyhow::anyhow!("zstd compression failed at level {level}: {e}"))?;
            Ok(Bytes::from(encoded))
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
            padding: Some(PaddingStrategy { block_size: 4096 }),
            encryption: Some((EncryptionStrategy::DeterministicChaCha20, 0x0e)),
            skip_when_unhelpful: None,
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

        assert_eq!(result.bytes.len(), 4096);

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

    /// `ZstdLevel { level }` round-trips, AND a higher level produces a
    /// smaller blob than the default level (sanity check that the level
    /// is actually getting passed to the encoder, not silently ignored).
    #[test]
    fn zstd_level_round_trips_and_compresses_better_at_high_level() {
        // Compressible payload — repeating patterns large enough that
        // L9 has room to find more structure than L1 does.
        let plaintext: Vec<u8> = (0..16 * 1024).map(|i| (i % 251) as u8).collect();
        let plaintext_hash: [u8; 32] = *blake3::hash(&plaintext).as_bytes();

        let pipe_default = BlobPipeline {
            compression: Some(CompressionStrategy::Zstd),
            padding: None,
            encryption: None,
            skip_when_unhelpful: None,
        };
        let pipe_level9 = BlobPipeline {
            compression: Some(CompressionStrategy::ZstdLevel { level: 9 }),
            padding: None,
            encryption: None,
            skip_when_unhelpful: None,
        };

        let r_default = pipeline_encode(
            &plaintext,
            Some(&pipe_default),
            &plaintext_hash,
            KDF_LEAF,
            None,
        )
        .unwrap();
        let r_level9 = pipeline_encode(
            &plaintext,
            Some(&pipe_level9),
            &plaintext_hash,
            KDF_LEAF,
            None,
        )
        .unwrap();

        // Level 9 should be at least as compact as the default (L1).
        assert!(
            r_level9.bytes.len() <= r_default.bytes.len(),
            "L9 produced {} bytes, default produced {} — level isn't reaching the encoder",
            r_level9.bytes.len(),
            r_default.bytes.len(),
        );

        // Round-trip: decoder is level-agnostic, so both pipelines decode
        // each other's bytes.
        let decoded = pipeline_decode(
            r_level9.bytes,
            Some(&pipe_level9),
            Some(&plaintext_hash),
            0,
            KDF_LEAF,
            None,
        )
        .unwrap();
        assert_eq!(&decoded[..], &plaintext[..]);
    }

    /// The `copy` invariant: a leaf encoded with `DeterministicChaCha20`
    /// (key derived from `master ‖ plaintext_hash`) decodes to byte-identical
    /// plaintext when the SAME ciphertext is re-read with
    /// `ExplicitKeyChaCha20` + the recovered per-blob key in the slot — WITHOUT
    /// touching `plaintext_hash`. That equality is exactly what lets a shallow
    /// copy re-home ciphertext into a foreign vault whose master differs.
    #[test]
    fn explicit_key_round_trips() {
        let plaintext = b"the quick brown fox jumps over the lazy dog, repeatedly. ".repeat(9);
        let plaintext_hash: [u8; 32] = *blake3::hash(&plaintext).as_bytes();
        let master = [0x5au8; 32];

        // Encode exactly as the source vault would (KDF_LEAF, slot 0x10).
        let mut src_keys = BTreeMap::new();
        src_keys.insert(0x10u8, master);
        let src_pipe = BlobPipeline {
            compression: Some(CompressionStrategy::Zstd),
            padding: Some(PaddingStrategy { block_size: 4096 }),
            encryption: Some((EncryptionStrategy::DeterministicChaCha20, 0x10)),
            skip_when_unhelpful: None,
        };
        let enc = pipeline_encode(
            &plaintext,
            Some(&src_pipe),
            &plaintext_hash,
            KDF_LEAF,
            Some(&src_keys),
        )
        .unwrap();
        let ciphertext = enc.bytes.clone();

        // Recover the per-blob key the way `copy` does, and decode the SAME
        // ciphertext with ExplicitKeyChaCha20 into slot 0x13 — no master.
        let per_blob_key = leaf_blob_key(&master, &plaintext_hash);
        let mut dst_keys = BTreeMap::new();
        dst_keys.insert(0x13u8, per_blob_key);
        let dst_pipe = BlobPipeline {
            compression: Some(CompressionStrategy::Zstd),
            padding: Some(PaddingStrategy { block_size: 4096 }),
            encryption: Some((EncryptionStrategy::ExplicitKeyChaCha20, 0x13)),
            skip_when_unhelpful: None,
        };
        // Note: pass plaintext_hash = None to prove the explicit path never
        // consults it (a `None` here would make the DeterministicChaCha20 path
        // error out — the explicit path must not care).
        let decoded = pipeline_decode(
            ciphertext,
            Some(&dst_pipe),
            None,
            plaintext.len() as u64,
            KDF_LEAF,
            Some(&dst_keys),
        )
        .unwrap();
        assert_eq!(
            &decoded[..],
            &plaintext[..],
            "explicit-key reuse must be byte-identical"
        );
    }
}
