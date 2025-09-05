use std::collections::BTreeSet;

use minicbor::{CborLen, Decode, Encode};
use s5_core::blob::location::BlobLocation;

/// The ALPN string for this protocol
pub const ALPN: &[u8] = b"s5/blobs/0";

#[derive(Encode, Decode, CborLen, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[cbor(flat)]
pub enum Request {
    #[n(0)]
    Query(#[n(0)] Query),
}

#[derive(Encode, Decode, CborLen, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[cbor(flat)]
pub enum Response {
    #[n(0)]
    Announce(#[n(0)] Announce),
    #[n(1)]
    NotFound(#[n(0)] [u8; 32]),
}

/// Query a peer for a blob.
#[derive(Encode, Decode, CborLen, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[cbor(array)]
pub struct Query {
    /// The blake3 hash we want to find blob locations for.
    #[n(0)]
    #[cbor(with = "minicbor::bytes")]
    pub hash: [u8; 32],

    /// The location types we are interested in.
    #[n(1)]
    pub types: BTreeSet<u8>,
}

#[derive(Encode, Decode, CborLen, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[cbor(array)]
pub struct Announce {
    #[n(0)]
    #[cbor(with = "minicbor::bytes")]
    pub hash: [u8; 32],

    #[n(1)]
    pub locations: Vec<BlobLocation>,

    /// The timestamp of the announce in unix seconds.
    #[n(2)]
    pub timestamp: u32,

    #[n(3)]
    #[cbor(default)]
    pub subsec_nanos: u32,
}
