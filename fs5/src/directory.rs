use minicbor::decode::Decoder;
use minicbor::encode::{Encoder, Write};
use minicbor::{CborLen, Decode, Encode};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Encode, Decode, CborLen)]
#[cbor(array)]
pub struct DirV1 {
    #[n(0)]
    magic: &'static str,
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
            magic: "S5.pro",
            header: DirHeader::new(),
            dirs: BTreeMap::new(),
            files: BTreeMap::new(),
        }
    }
}

#[derive(Encode, Decode, CborLen)]
#[cbor(map)]
pub struct DirHeader {}

impl DirHeader {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Encode, Decode, CborLen)]
#[cbor(map)]
pub struct DirRef {
    #[n(2)]
    #[cbor(with = "DirLink")]
    pub link: DirLink,
    #[n(7)]
    pub ts_seconds: Option<u32>,
    #[n(8)]
    pub ts_nanos: Option<u32>,
    #[n(0x16)]
    pub extra: Option<()>,
}

#[derive(Encode, Decode, CborLen)]
#[cbor(map)]
pub struct FileRef {
    #[n(3)]
    #[cbor(with = "minicbor::bytes")]
    pub hash: [u8; 32],
    #[n(4)]
    pub size: u64,
    #[n(6)]
    pub media_type: Option<String>,
    #[n(7)]
    pub timestamp: Option<u32>,
    #[n(8)]
    pub timestamp_subsec_nanos: Option<u32>,
    #[n(9)]
    pub locations: Option<Vec<BlobLocation>>,
    #[n(0x13)]
    hash_type: Option<u8>,
    #[n(0x16)]
    pub extra: Option<BTreeMap<String, ()>>,
    #[n(0x17)]
    pub prev: Option<Box<FileRef>>,
}

impl FileRef {
    pub fn new(hash: [u8; 32], size: u64) -> Self {
        Self {
            hash,
            size,
            media_type: None,
            timestamp: None,
            timestamp_subsec_nanos: None,
            locations: None,
            hash_type: None,
            extra: None,
            prev: None,
        }
    }
}

#[derive(Encode, Decode, CborLen)]
#[cbor(flat)]
pub enum BlobLocation {
    #[n(0)]
    IdentityRawBinary(
        #[n(0)]
        #[cbor(with = "minicbor::bytes")]
        Vec<u8>,
    ),

    #[n(1)]
    Http(#[n(0)] String),

    #[n(0x11)]
    MultihashSha1(
        #[n(0)]
        #[cbor(with = "minicbor::bytes")]
        [u8; 20],
    ),

    #[n(0x12)]
    MultihashSha2_256(
        #[n(0)]
        #[cbor(with = "minicbor::bytes")]
        [u8; 32],
    ),

    #[n(0x1e)]
    MultihashBlake3(
        #[n(0)]
        #[cbor(with = "minicbor::bytes")]
        [u8; 32],
    ),

    #[n(0xd5)]
    MultihashMd5(
        #[n(0)]
        #[cbor(with = "minicbor::bytes")]
        [u8; 16],
    ),
}

pub enum DirLink {
    FixedHashBlake3([u8; 32]),
    MutableRegistryEd25519([u8; 32]),
}

impl DirLink {
    pub const SERIALIZED_SIZE: usize = 33;

    fn encode<Ctx, W: Write>(
        v: &DirLink,
        e: &mut Encoder<W>,
        _ctx: &mut Ctx,
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        let mut bytes = [0u8; Self::SERIALIZED_SIZE];
        match v {
            DirLink::FixedHashBlake3(hash) => {
                bytes[0] = 0x1e;
                bytes[1..].copy_from_slice(hash);
            }
            DirLink::MutableRegistryEd25519(pubkey) => {
                bytes[0] = 0xed;
                bytes[1..].copy_from_slice(pubkey);
            }
        }
        e.bytes(&bytes)?.ok()
    }

    fn decode<'b, Ctx>(
        d: &mut Decoder<'b>,
        _ctx: &mut Ctx,
    ) -> Result<DirLink, minicbor::decode::Error> {
        let bytes: &[u8] = d.bytes()?;
        if bytes.len() != Self::SERIALIZED_SIZE {
            return Err(minicbor::decode::Error::custom(
                DirRefLinkDeserializationError::InvalidLength,
            ));
        }
        match bytes[0] {
            0x1e => {
                let mut hash = [0u8; 32];
                hash.copy_from_slice(&bytes[1..]);
                Ok(DirLink::FixedHashBlake3(hash))
            }
            0xed => {
                let mut pubkey = [0u8; 32];
                pubkey.copy_from_slice(&bytes[1..]);
                Ok(DirLink::MutableRegistryEd25519(pubkey))
            }
            _ => Err(minicbor::decode::Error::custom(
                DirRefLinkDeserializationError::InvalidTag,
            )),
        }
    }

    fn cbor_len<Ctx, T>(_val: &T, _ctx: &mut Ctx) -> usize {
        Self::SERIALIZED_SIZE
    }
}

#[derive(Error, Debug)]
enum DirRefLinkDeserializationError {
    #[error("input byte slice has an incorrect length")]
    InvalidLength,
    #[error("tag byte is unknown or invalid")]
    InvalidTag,
}
