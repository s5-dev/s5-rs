//! Zstd compression utilities for S5 blobs.
//!
//! Pure functions for compressing, decompressing, and training zstd
//! dictionaries. No state, no I/O beyond the byte slices you pass in.
//!
//! Higher-level concerns (media type classification, dictionary storage,
//! training orchestration) belong in the consuming crate (e.g. `vup_cli`).
//!
//! ## Usage
//!
//! ```rust
//! use s5_compression::{compress, decompress};
//!
//! let data = b"hello world, this is some repeating data to compress. \
//!              hello world, this is some repeating data to compress. ";
//! let compressed = compress(data, None).unwrap();
//! let restored = decompress(&compressed, None).unwrap();
//! assert_eq!(data.as_slice(), restored.as_slice());
//! ```

use anyhow::{Context, Result};

/// Default zstd compression level. Level 3 is a good balance of speed and ratio.
pub const DEFAULT_LEVEL: i32 = 3;

/// Compress a blob with zstd.
///
/// Uses `DEFAULT_LEVEL` (3) for compression. An optional pre-trained
/// dictionary can be supplied for better ratios on small or similar blobs.
pub fn compress(raw: &[u8], dict: Option<&[u8]>) -> Result<Vec<u8>> {
    let mut compressor = match dict {
        Some(d) => zstd::bulk::Compressor::with_dictionary(DEFAULT_LEVEL, d)
            .context("zstd: failed to create compressor with dictionary")?,
        None => zstd::bulk::Compressor::new(DEFAULT_LEVEL)
            .context("zstd: failed to create compressor")?,
    };
    compressor.compress(raw).context("zstd compression failed")
}

/// Compress a blob at a specific zstd level.
pub fn compress_with_level(raw: &[u8], level: i32, dict: Option<&[u8]>) -> Result<Vec<u8>> {
    let mut compressor = match dict {
        Some(d) => zstd::bulk::Compressor::with_dictionary(level, d)
            .context("zstd: failed to create compressor with dictionary")?,
        None => zstd::bulk::Compressor::new(level).context("zstd: failed to create compressor")?,
    };
    compressor.compress(raw).context("zstd compression failed")
}

/// Decompress a zstd-compressed blob.
///
/// The optional dictionary must match the one used during compression.
pub fn decompress(compressed: &[u8], dict: Option<&[u8]>) -> Result<Vec<u8>> {
    match dict {
        Some(d) => {
            let dict = zstd::dict::DecoderDictionary::copy(d);
            let decoder =
                zstd::Decoder::with_prepared_dictionary(std::io::Cursor::new(compressed), &dict)
                    .context("zstd: failed to create decoder with dictionary")?;
            let mut output = Vec::new();
            std::io::Read::read_to_end(&mut { decoder }, &mut output)
                .context("zstd decompression with dictionary failed")?;
            Ok(output)
        }
        None => zstd::stream::decode_all(std::io::Cursor::new(compressed))
            .context("zstd decompression failed"),
    }
}

/// Train a zstd dictionary from a set of sample blobs.
///
/// The resulting dictionary can be passed to [`compress`] / [`decompress`]
/// for improved compression ratios on similar content. `max_size` controls
/// the maximum dictionary size in bytes (112 KB is a good default).
///
/// Requires enough samples to be effective (typically 100+).
pub fn train_dictionary(samples: &[&[u8]], max_size: usize) -> Result<Vec<u8>> {
    zstd::dict::from_samples(samples, max_size).context("zstd dictionary training failed")
}

/// Describes the compression applied to a blob.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CompressionParams {
    /// No compression (stored raw, e.g. already-compressed formats).
    None,

    /// Zstd compression.
    Zstd {
        /// Compression level used.
        level: i32,

        /// Blake3 hash of the dictionary blob, if one was used.
        /// The dictionary itself is stored as a regular blob.
        #[serde(skip_serializing_if = "Option::is_none")]
        dict_hash: Option<[u8; 32]>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_no_dict() {
        let data = b"The quick brown fox jumps over the lazy dog. ".repeat(100);
        let compressed = compress(&data, None).unwrap();
        let restored = decompress(&compressed, None).unwrap();
        assert_eq!(data, restored.as_slice());
        assert!(
            compressed.len() < data.len() / 2,
            "expected >50% compression, got {} -> {}",
            data.len(),
            compressed.len()
        );
    }

    #[test]
    fn round_trip_with_dict() {
        let samples: Vec<Vec<u8>> = (0..100)
            .map(|i| format!("sample document number {i} with some shared structure and vocabulary for testing purposes").into_bytes())
            .collect();
        let sample_refs: Vec<&[u8]> = samples.iter().map(|s| s.as_slice()).collect();
        let dict = train_dictionary(&sample_refs, 16 * 1024).unwrap();

        let data = b"sample document number 999 with some shared structure and vocabulary for testing purposes";
        let compressed = compress(data, Some(&dict)).unwrap();
        let restored = decompress(&compressed, Some(&dict)).unwrap();
        assert_eq!(data.as_slice(), restored.as_slice());
    }

    #[test]
    fn compression_ratio_on_text() {
        let json =
            r#"{"name": "test", "values": [1, 2, 3], "nested": {"key": "value"}}"#.repeat(50);
        let markdown = "# Heading\n\nSome paragraph with **bold** and *italic* text.\n\n- list item 1\n- list item 2\n".repeat(50);
        let code = "fn main() {\n    let x = 42;\n    println!(\"hello {}\", x);\n}\n".repeat(50);

        for (label, data) in [("json", json), ("markdown", markdown), ("code", code)] {
            let compressed = compress(data.as_bytes(), None).unwrap();
            let ratio = 1.0 - (compressed.len() as f64 / data.len() as f64);
            assert!(
                ratio > 0.30,
                "{label}: expected >30% compression, got {:.1}% ({} -> {})",
                ratio * 100.0,
                data.len(),
                compressed.len()
            );
        }
    }

    #[test]
    fn empty_blob() {
        let compressed = compress(b"", None).unwrap();
        let restored = decompress(&compressed, None).unwrap();
        assert!(restored.is_empty());
    }

    #[test]
    fn custom_level() {
        let data = b"some data to compress at different levels".repeat(50);
        let fast = compress_with_level(&data, 1, None).unwrap();
        let slow = compress_with_level(&data, 19, None).unwrap();
        // Both should round-trip
        assert_eq!(data.as_slice(), decompress(&fast, None).unwrap().as_slice());
        assert_eq!(data.as_slice(), decompress(&slow, None).unwrap().as_slice());
        // Higher level should compress at least as well
        assert!(slow.len() <= fast.len());
    }
}
