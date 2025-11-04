use bytes::Bytes;
use irpc::channel::{mpsc, oneshot};
use irpc::rpc_requests;
use s5_core::blob::location::BlobLocation;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// The ALPN string for this protocol
pub const ALPN: &[u8] = b"s5/blobs/0";

#[derive(Debug, Serialize, Deserialize)]
#[rpc_requests(message = RpcMessage)]
pub enum RpcProto {
    #[rpc(tx = oneshot::Sender<QueryResponse>)]
    Query(Query),
    // Client streams bytes to server; server replies with a Result.
    #[rpc(tx = oneshot::Sender<Result<(), String>>, rx = mpsc::Receiver<Bytes>)]
    UploadBlob(UploadBlob),
    // Server streams bytes to client for ranged downloads.
    #[rpc(tx = mpsc::Sender<Bytes>)]
    DownloadBlob(DownloadBlob),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UploadBlob {
    pub expected_hash: [u8; 32],
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadBlob {
    pub hash: [u8; 32],
    pub offset: u64,
    pub max_len: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct QueryResponse {
    pub exists: bool,
    pub size: Option<u64>,
    pub locations: Vec<BlobLocation>,
}

/// Query a peer for a blob.
#[derive(Debug, Serialize, Deserialize)]
pub struct Query {
    /// The blake3 hash we want to find blob locations for.
    pub hash: [u8; 32],
    /// The location types we support on the caller side.
    pub location_types: BTreeSet<u8>,
}

/* // TODO maybe just use registry entries for signed announces
#[derive(Debug, Serialize, Deserialize)]
pub struct SignedAnnounce {
    pub node_id: [u8; 32],

    pub hash: [u8; 32],

    pub announce: Announce,

    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Announce {
    pub locations: BTreeMap<AnnounceTarget, Vec<BlobLocation>>,
    /// The timestamp until when the announce should be considered valid in unix seconds.
    pub timestamp: u32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum AnnounceTarget {
    LocationPointer = 0,
    BlobContent = 1,
    Obao6 = 6,
}
*/
