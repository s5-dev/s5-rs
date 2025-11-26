//! Pure data structures and on-disk (CBOR) schema for FS5 directories.
//!
//! This module defines `DirV1` snapshots and related types. It contains no
//! I/O or async code and is shared across readers/writers.

use anyhow::anyhow;
use bytes::Bytes;
use chacha20poly1305::KeyInit;
use chacha20poly1305::XChaCha20Poly1305;
use chacha20poly1305::aead::{Aead, AeadCore};
use minicbor::{CborLen, Decode, Encode};
use s5_core::Hash;
use s5_core::blob::location::BlobLocation;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::convert::Infallible;

#[derive(Encode, Decode, Serialize, Deserialize, CborLen, Clone, Debug)]
#[cbor(array)]
pub struct DirV1 {
    #[n(0)]
    magic: String,
    #[n(1)]
    pub header: DirHeader,
    #[n(2)]
    pub dirs: BTreeMap<String, DirRef>,
    #[n(3)]
    pub files: BTreeMap<String, FileRef>,
}

impl Default for DirV1 {
    fn default() -> Self {
        Self::new()
    }
}

impl DirV1 {
    /// Creates an empty directory snapshot with default header.
    pub fn new() -> Self {
        Self {
            magic: "S5.pro".to_string(),
            header: DirHeader::new(),
            dirs: BTreeMap::new(),
            files: BTreeMap::new(),
        }
    }
    /// Creates a directory preconfigured for static web apps.
    pub fn new_web_app() -> Self {
        Self {
            magic: "S5.pro/web".to_string(),
            header: DirHeader {
                shard_level: None,
                try_files: Some(vec!["index.html".to_string()]),
                error_pages: None,
                ops_counter: None,
                last_written_by: None,
                shards: None,
            },
            dirs: BTreeMap::new(),
            files: BTreeMap::new(),
        }
    }
    /* pub fn open<P: AsRef<Path>>(path: P) -> io::Result<OpenDirV1> {
        OpenDirV1::open(path)
    } */

    /// Decodes a directory from CBOR bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<DirV1, minicbor::decode::Error> {
        minicbor::decode(bytes)
    }

    /// Encodes this directory to a CBOR `Vec<u8>`.
    pub fn to_vec(&self) -> Result<Vec<u8>, minicbor::encode::Error<Infallible>> {
        minicbor::to_vec(self)
    }
    /// Encodes this directory to CBOR as a `Bytes` buffer.
    pub fn to_bytes(&self) -> Result<Bytes, minicbor::encode::Error<Infallible>> {
        Ok(self.to_vec()?.into())
    }
}

#[derive(Encode, Decode, Serialize, Deserialize, CborLen, Clone, Debug)]
#[cbor(map)]
pub struct DirHeader {
    #[n(0x4)]
    pub shard_level: Option<u8>,
    #[n(0x05)]
    pub shards: Option<BTreeMap<u8, DirRef>>,

    #[n(6)]
    pub try_files: Option<Vec<String>>,
    #[n(14)]
    pub error_pages: Option<BTreeMap<u16, String>>,

    #[n(0x0c)]
    pub ops_counter: Option<u64>,
    #[n(0x0d)] // TODO implement
    pub last_written_by: Option<BTreeMap<[u8; 16], u64>>,
}

impl Default for DirHeader {
    fn default() -> Self {
        Self::new()
    }
}

impl DirHeader {
    /// Creates a default header (no sharding, no hints).
    pub fn new() -> Self {
        Self {
            shard_level: None,
            shards: None,
            error_pages: None,
            try_files: None,
            ops_counter: None,
            last_written_by: None,
        }
    }
}

#[derive(Encode, Decode, Serialize, Deserialize, CborLen, Clone, Debug)]
#[cbor(map)]
pub struct DirRef {
    #[n(0)]
    pub ref_type: Option<DirRefType>,
    #[n(1)]
    #[cbor(with = "minicbor::bytes")]
    pub hash: [u8; 32],
    #[n(3)]
    pub ts_seconds: Option<u32>,
    #[n(4)]
    pub ts_nanos: Option<u32>,
    #[n(0x0c)]
    // TODO serialize these as cbor byte arrays
    pub keys: Option<BTreeMap<u8, [u8; 32]>>,
    #[n(0x0e)]
    pub encryption_type: Option<u8>,
    #[n(0x16)]
    pub extra: Option<()>,
}

pub const ENCRYPTION_TYPE_XCHACHA20_POLY1305: u8 = 0x02;

#[repr(u8)]
#[derive(Encode, Decode, Serialize, Deserialize, CborLen, Clone, Debug)]
#[cbor(index_only)]
pub enum DirRefType {
    #[n(0x03)]
    Blake3Hash = 0x03,
    #[n(0x11)]
    RegistryKey = 0x11,
}

impl DirRef {
    /// Creates a `DirRef` that points to a directory by Blake3 hash.
    pub fn from_hash(hash: Hash) -> Self {
        Self {
            // link: DirLink::FixedHashBlake3(hash),
            ref_type: None,
            hash: hash.into(),
            ts_seconds: None,
            ts_nanos: None,
            extra: None,
            encryption_type: None,
            keys: None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn new_empty() -> Self {
        // let dir = DirV1::new();
        // let hash = blake3::hash(&dir.to_vec().unwrap());
        Self {
            // link: DirLink::FixedHashBlake3(hash),
            ref_type: None,
            hash: [0; 32],
            ts_seconds: None,
            ts_nanos: None,
            extra: None,
            encryption_type: None,
            keys: None,
        }
    }

    pub fn ref_type(&self) -> DirRefType {
        self.ref_type.clone().unwrap_or(DirRefType::Blake3Hash)
    }
}

#[repr(u8)]
#[derive(Encode, Decode, Serialize, Deserialize, CborLen, Clone, Debug, PartialEq, Eq)]
#[cbor(index_only)]
pub enum FileRefType {
    #[n(0x03)]
    Blake3Hash = 0x03,
    #[n(0x11)]
    RegistryKey = 0x11,
    /// Logical deletion marker; current head represents a delete, but
    /// previous versions are retained via `prev`/`first_version`.
    #[n(0x20)]
    Tombstone = 0x20,
}

#[derive(Encode, Decode, Serialize, Deserialize, CborLen, Clone, Debug)]
#[cbor(map)]
pub struct FileRef {
    #[n(0)]
    pub ref_type: Option<FileRefType>,
    #[n(1)]
    #[cbor(with = "minicbor::bytes")]
    pub hash: [u8; 32],
    #[n(2)]
    pub size: u64,
    #[n(3)]
    pub timestamp: Option<u32>,
    #[n(4)]
    pub timestamp_subsec_nanos: Option<u32>,
    #[n(5)]
    pub locations: Option<Vec<BlobLocation>>,
    #[n(6)]
    pub media_type: Option<String>,

    #[n(0x15)]
    pub warc: Option<WebArchiveMetadata>,

    #[n(0x16)]
    pub extra: Option<BTreeMap<String, ()>>,

    #[n(0x17)]
    pub prev: Option<Box<FileRef>>, // Immediate parent (Linked List). only set if not equal to the first_version
    #[n(0x19)]
    pub version_count: Option<u32>, // So UI knows "Version 50" without traversing

    #[n(0x18)]
    // The very first version
    pub first_version: Option<Box<FileRef>>,
}

#[derive(Encode, Decode, Serialize, Deserialize, CborLen, Clone, Debug, Default)]
#[cbor(map)]
pub struct WebArchiveMetadata {
    #[n(0)]
    pub ip_addr: String,
    // ! request
    #[n(1)]
    pub req_http_version: u8,
    #[n(2)]
    pub req_headers: Vec<(String, String)>,
    // ! response
    #[n(3)]
    pub res_http_version: u8,
    #[n(4)]
    pub res_status_code: u16,
    #[n(5)]
    pub res_status_reason: String,
    #[n(6)]
    pub res_headers: Vec<(String, String)>,
}

impl FileRef {
    /// Creates an inline-blob `FileRef` storing data directly in metadata.
    /// Suitable for very small blobs; large blobs should use the blob store.
    ///
    /// TODO: Enforce a max size limit (e.g. 4096 bytes) here to prevent
    /// metadata bloat, as suggested in s5_node/README.md.
    pub fn new_inline_blob(blob: Bytes) -> Self {
        let hash = blake3::hash(&blob);
        Self {
            ref_type: None,
            hash: hash.into(),
            size: blob.len() as u64,
            media_type: None,
            timestamp: None,
            timestamp_subsec_nanos: None,
            locations: Some(vec![BlobLocation::IdentityRawBinary(blob.to_vec())]),
            extra: None,
            prev: None,
            version_count: None,
            warc: None,
            first_version: None,
        }
    }
    /// Creates a hashed `FileRef` referencing content by Blake3 `hash` and `size`.
    pub fn new(hash: Hash, size: u64) -> Self {
        Self {
            ref_type: None,
            hash: *hash.as_bytes(),
            size,
            media_type: None,
            timestamp: None,
            timestamp_subsec_nanos: None,
            locations: None,
            extra: None,
            prev: None,
            version_count: None,
            warc: None,
            first_version: None,
        }
    }

    pub fn ref_type(&self) -> FileRefType {
        self.ref_type.clone().unwrap_or(FileRefType::Blake3Hash)
    }

    /// Returns true if this `FileRef` represents a logical deletion.
    pub fn is_tombstone(&self) -> bool {
        matches!(self.ref_type(), FileRefType::Tombstone)
    }

    /// Creates a tombstone `FileRef` from the last live version.
    ///
    /// - `deleted_at_s` / `deleted_at_ns` indicate when the delete occurred.
    /// - The previous live version is threaded into `prev` / `first_version`
    ///   and `version_count` is incremented if present.
    pub fn from_deleted(previous: FileRef, deleted_at_s: u32, deleted_at_ns: u32) -> Self {
        let first_version = previous
            .first_version
            .clone()
            .unwrap_or_else(|| Box::new(previous.clone()));
        let version_count = previous.version_count.unwrap_or(1).saturating_add(1);

        Self {
            ref_type: Some(FileRefType::Tombstone),
            hash: previous.hash,
            size: previous.size,
            media_type: previous.media_type.clone(),
            timestamp: Some(deleted_at_s),
            timestamp_subsec_nanos: Some(deleted_at_ns),
            locations: None,
            extra: previous.extra.clone(),
            prev: Some(Box::new(previous.clone())),
            version_count: Some(version_count),
            warc: previous.warc.clone(),
            first_version: Some(first_version),
        }
    }
}

impl From<s5_core::BlobId> for FileRef {
    fn from(blob_id: s5_core::BlobId) -> Self {
        Self::new(blob_id.hash, blob_id.size)
    }
}

impl From<FileRef> for s5_core::BlobId {
    fn from(val: FileRef) -> Self {
        s5_core::BlobId::new(
            Hash::from_bytes(
                val.hash[0..32]
                    .try_into()
                    .expect("expected 32-byte Blake3 hash"),
            ),
            val.size,
        )
    }
}

/// Decrypts directory bytes if an encryption key is provided.
pub fn decrypt_dir_bytes(bytes: Bytes, key: Option<&[u8; 32]>) -> anyhow::Result<Bytes> {
    if let Some(key) = key {
        let cipher = XChaCha20Poly1305::new(key.into());

        if bytes.len() < 24 {
            return Err(anyhow!(
                "encrypted directory blob too short for nonce: {} bytes",
                bytes.len()
            ));
        }

        let nonce = &bytes[..24];
        let ciphertext = &bytes[24..];
        let plaintext = cipher
            .decrypt(nonce.into(), ciphertext)
            .map_err(|e| anyhow!("Failed to decrypt directory: {}", e))?;
        Ok(plaintext.into())
    } else {
        Ok(bytes)
    }
}

/// Encrypts directory bytes using XChaCha20Poly1305.
pub fn encrypt_dir_bytes(key: &[u8; 32], plain: &[u8]) -> anyhow::Result<Bytes> {
    let cipher = XChaCha20Poly1305::new(key.into());
    let nonce = XChaCha20Poly1305::generate_nonce(&mut chacha20poly1305::aead::OsRng);
    let ciphertext = cipher
        .encrypt(&nonce, plain)
        .map_err(|e| anyhow!("Failed to encrypt directory: {}", e))?;
    let mut buf = bytes::BytesMut::with_capacity(24 + ciphertext.len());
    use bytes::BufMut;
    buf.put_slice(&nonce);
    buf.put_slice(&ciphertext);
    Ok(buf.into())
}
