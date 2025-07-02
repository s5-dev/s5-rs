use bytes::Bytes;
use minicbor::{CborLen, Decode, Encode, bytes::ByteArray};
use std::collections::BTreeMap;

impl BlobLocation {
    pub fn deserialize(bytes: &[u8]) -> BlobLocation {
        minicbor::decode(bytes).unwrap()
    }
    fn to_vec(&self) -> Vec<u8> {
        minicbor::to_vec(self).unwrap()
    }
    pub fn serialize(&self) -> Bytes {
        self.to_vec().into()
    }
}

#[derive(Encode, Decode, CborLen, Clone, Debug)]
#[cbor(flat)]
pub enum BlobLocation {
    #[n(0)]
    IdentityRawBinary(
        #[n(0)]
        #[cbor(with = "minicbor::bytes")]
        Vec<u8>,
    ),

    #[n(1)]
    #[cbor(array)]
    Url(#[n(0)] String),

    #[n(0x41)]
    SiaFile(#[n(0)] SiaFile),

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

#[derive(Encode, Decode, CborLen, Clone, Debug)]
#[cbor(array)]
pub struct SiaFile {
    #[n(0)]
    pub size: u64,
    #[n(1)]
    pub slab_size: u64,
    #[n(2)]
    pub min_shards: u8,
    #[n(3)]
    pub hosts: BTreeMap<u8, SiaFileHost>,
    #[n(4)]
    #[cbor(with = "minicbor::bytes")]
    pub file_encryption_key: [u8; 32],
    #[n(5)]
    pub slabs: Vec<SiaFileSlab>,
}

#[derive(Encode, Decode, CborLen, Clone, Debug)]
#[cbor(array)]
pub struct SiaFileHost {
    #[n(0)]
    pub hostkey: String,
    #[n(1)]
    pub v2_siamux_addresses: Vec<String>,
    #[n(2)]
    #[cbor(with = "minicbor::bytes")]
    pub ephemeral_account_private_key: [u8; 32],
}

#[derive(Encode, Decode, CborLen, Clone, Debug)]
#[cbor(array)]
pub struct SiaFileSlab {
    #[n(0)]
    #[cbor(with = "minicbor::bytes")]
    pub slab_encryption_key: [u8; 32],

    #[n(1)]
    pub shard_roots: BTreeMap<u8, ByteArray<32>>,
}
