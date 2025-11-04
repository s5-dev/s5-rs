use bytes::Bytes;
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
    // TODO: Serialize only when non-empty
    #[n(4)]
    pub shards: BTreeMap<u8, DirRef>,
}

impl DirV1 {
    /// Creates an empty directory snapshot with default header.
    pub fn new() -> Self {
        Self {
            magic: "S5.pro".to_string(),
            header: DirHeader::new(),
            dirs: BTreeMap::new(),
            files: BTreeMap::new(),
            shards: BTreeMap::new(),
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
                random_id: None,
                // extra: Extra::new(),
            },
            dirs: BTreeMap::new(),
            files: BTreeMap::new(),
            shards: BTreeMap::new(),
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

    #[n(6)]
    try_files: Option<Vec<String>>,
    #[n(14)]
    error_pages: Option<BTreeMap<u16, String>>,

    #[n(0xff)]
    #[cbor(with = "minicbor::bytes")]
    pub random_id: Option<[u8; 16]>,
}

impl DirHeader {
    /// Creates a default header (no sharding, no hints).
    pub fn new() -> Self {
        Self {
            shard_level: None,
            error_pages: None,
            try_files: None,
            random_id: None,
        }
    }
}

#[derive(Encode, Decode, Serialize, Deserialize, CborLen, Clone, Debug)]
#[cbor(map)]
pub struct DirRef {
    #[n(0)]
    pub ref_type: DirRefType,
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
            ref_type: DirRefType::Blake3Hash,
            hash: hash.into(),
            ts_seconds: None,
            ts_nanos: None,
            extra: None,
            encryption_type: None,
            keys: None,
        }
    }

    pub(crate) fn new_empty() -> Self {
        // let dir = DirV1::new();
        // let hash = blake3::hash(&dir.to_vec().unwrap());
        Self {
            // link: DirLink::FixedHashBlake3(hash),
            ref_type: DirRefType::Blake3Hash,
            hash: [0; 32],
            ts_seconds: None,
            ts_nanos: None,
            extra: None,
            encryption_type: None,
            keys: None,
        }
    }
}

#[repr(u8)]
#[derive(Encode, Decode, Serialize, Deserialize, CborLen, Clone, Debug)]
#[cbor(index_only)]
pub enum FileRefType {
    #[n(0x03)]
    Blake3Hash = 0x03,
    #[n(0x11)]
    RegistryKey = 0x11,
}

#[derive(Encode, Decode, Serialize, Deserialize, CborLen, Clone, Debug)]
#[cbor(map)]
pub struct FileRef {
    #[n(0)]
    pub ref_type: FileRefType,
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
    pub prev: Option<Box<FileRef>>,
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
    pub fn new_inline_blob(blob: Bytes) -> Self {
        let hash = blake3::hash(&blob);
        Self {
            ref_type: FileRefType::Blake3Hash,
            hash: hash.into(),
            size: blob.len() as u64,
            media_type: None,
            timestamp: None,
            timestamp_subsec_nanos: None,
            locations: Some(vec![BlobLocation::IdentityRawBinary(blob.to_vec())]),
            extra: None,
            prev: None,
            warc: None,
        }
    }
    /// Creates a hashed `FileRef` referencing content by Blake3 `hash` and `size`.
    pub fn new(hash: Hash, size: u64) -> Self {
        Self {
            ref_type: FileRefType::Blake3Hash,
            hash: *hash.as_bytes(),
            size,
            media_type: None,
            timestamp: None,
            timestamp_subsec_nanos: None,
            locations: None,
            extra: None,
            prev: None,
            warc: None,
        }
    }
}

impl From<s5_core::BlobId> for FileRef {
    fn from(blob_id: s5_core::BlobId) -> Self {
        Self::new(blob_id.hash, blob_id.size)
    }
}

impl Into<s5_core::BlobId> for FileRef {
    fn into(self) -> s5_core::BlobId {
        s5_core::BlobId::new(
            Hash::from_bytes(
                self.hash[0..32]
                    .try_into()
                    .expect("expected 32-byte Blake3 hash"),
            ),
            self.size,
        )
    }
}
