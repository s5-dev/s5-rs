use crate::Hash;
use crate::blob::location::BlobLocation;
use bytes::Bytes;
use minicbor::decode::Decoder;
use minicbor::encode::{Encoder, Write};
use minicbor::{CborLen, Decode, Encode};
use std::collections::BTreeMap;

#[cfg(feature = "wasm")]
use wasm_bindgen::prelude::*;

#[cfg_attr(feature = "wasm", wasm_bindgen)]
#[derive(Encode, Decode, CborLen, Clone, Debug)]
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

impl DirV1 {
    pub fn new() -> Self {
        Self {
            magic: "S5.pro".to_string(),
            header: DirHeader::new(),
            dirs: BTreeMap::new(),
            files: BTreeMap::new(),
        }
    }
    pub fn new_web_app() -> Self {
        Self {
            magic: "S5.pro/web".to_string(),
            header: DirHeader {
                try_files: Some(vec!["index.html".to_string()]),
                error_pages: None,
                ops_counter: None,
                last_written_by: None,
                // extra: Extra::new(),
            },
            dirs: BTreeMap::new(),
            files: BTreeMap::new(),
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> DirV1 {
        minicbor::decode(bytes).unwrap()
    }

    fn to_vec(&self) -> Vec<u8> {
        minicbor::to_vec(self).unwrap()
    }
    pub fn to_bytes(&self) -> Bytes {
        self.to_vec().into()
    }

    pub fn merge(mut self, mut other: DirV1) -> Self {
        // TODO implement merge
        // let mut processed_file_paths: HashSet<&String> = HashSet::new();
        for (path, file) in &self.files {
            if let Some(ofile) = other.files.remove(path) {
                // TODO merge ofile into file
            }
        }
        for (opath, ofile) in other.files {
            self.files.insert(opath, ofile);
        }
        self
    }
}

#[derive(Encode, Decode, CborLen, Clone, Debug)]
#[cbor(map)]
pub struct DirHeader {
    #[n(6)]
    try_files: Option<Vec<String>>,
    #[n(14)]
    error_pages: Option<BTreeMap<u16, String>>,

    // TODO implement
    #[n(0x0c)]
    pub ops_counter: Option<u64>,
    #[n(0x0d)] // TODO implement
    pub last_written_by: Option<BTreeMap<[u8; 16], u64>>,
    // #[n(0x16)]
    // #[cbor(with = "Extra")]
    // pub extra: Extra,
}

impl DirHeader {
    pub fn new() -> Self {
        Self {
            error_pages: None,
            // extra: Extra::new(),
            try_files: None,
            ops_counter: None,
            last_written_by: None,
        }
    }
}

#[derive(Encode, Decode, CborLen, Clone, Debug)]
#[cbor(map)]
pub struct DirRef {
    #[n(1)]
    #[cbor(with = "minicbor::bytes")]
    pub hash: [u8; 32],
    #[n(3)]
    pub ts_seconds: Option<u32>,
    #[n(4)]
    pub ts_nanos: Option<u32>,
    #[n(0x16)]
    pub extra: Option<()>,
}

impl DirRef {
    pub fn from_hash(hash: Hash) -> Self {
        Self {
            // link: DirLink::FixedHashBlake3(hash),
            hash: hash.into(),
            ts_seconds: None,
            ts_nanos: None,
            extra: None,
        }
    }
}

#[derive(Encode, Decode, CborLen, Clone, Debug)]
#[cbor(map)]
pub struct FileRef {
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
    #[n(0x16)]
    pub extra: Option<BTreeMap<String, ()>>,
    #[n(0x17)]
    pub prev: Option<Box<FileRef>>,
}

impl FileRef {
    pub fn new(hash: Hash, size: u64) -> Self {
        Self {
            hash: hash.into(),
            size,
            media_type: None,
            timestamp: None,
            timestamp_subsec_nanos: None,
            locations: None,
            extra: None,
            prev: None,
        }
    }
}

impl From<crate::BlobId> for FileRef {
    fn from(blob_id: crate::BlobId) -> Self {
        Self::new(blob_id.hash, blob_id.size)
    }
}

impl Into<crate::BlobId> for FileRef {
    fn into(self) -> crate::BlobId {
        crate::BlobId::new(self.hash.into(), self.size)
    }
}
