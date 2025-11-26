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
    /// Request that the server unpin this client's reference to
    /// a blob and, if no pins remain, delete it from storage.
    ///
    /// The response is `Ok(true)` if the blob became orphaned
    /// and was deleted, `Ok(false)` if other pins remain, and
    /// `Err(String)` on permission or other server-side errors.
    // TODO this could be a privacy issue, because now any node knows if maybe someone else pinned the same hash on the remote node or not. do we actually need the bool?
    #[rpc(tx = oneshot::Sender<Result<bool, String>>)]
    DeleteBlob(DeleteBlob),
}

/// Delete request identified by the blob's content hash.
#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteBlob {
    /// BLAKE3 hash of the blob to unpin/delete.
    pub hash: [u8; 32],
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UploadBlob {
    pub expected_hash: [u8; 32],
    pub size: u64,
}

#[derive(Debug, Serialize, Deserialize)]
// TODO: Consider extending with a chunk bitmap (RoaringBitmap) or chunk ranges for multi-source piece selection.
pub struct DownloadBlob {
    pub hash: [u8; 32],
    pub offset: u64,
    pub max_len: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
// TODO: Extend discovery responses to carry richer metadata:
// - validity / expiry timestamp for locations (like the old Announce.timestamp).
// - origin metadata for locations (which peer/store provided them).
// - optional chunk availability (e.g. RoaringBitmap) to aid partial downloads.
pub struct QueryResponse {
    pub exists: bool,
    pub size: Option<u64>,
    pub locations: Vec<BlobLocation>,
}

/// Query a peer for a blob.
#[derive(Debug, Serialize, Deserialize)]
// TODO: Implement multi-target queries as in the original design:
// - interpret `location_types` as requested location kinds (e.g. BlobContent, Obao6).
// - have servers filter/populate `QueryResponse.locations` accordingly instead of
//   always returning only "blob content" locations.
pub struct Query {
    /// The blake3 hash we want to find blob locations for.
    pub hash: [u8; 32],
    /// The location types we support on the caller side.
    pub location_types: BTreeSet<u8>,
}

// TODO: Decide on verifiable announces (SignedAnnounce) vs. external registry:
// - keep this crate focused on online queries only, or
// - reintroduce SignedAnnounce + AnnounceTarget (BlobContent, Obao6, etc.) with
//   signatures and expiry, or
// - move long-lived announces entirely into the S5 registry and delete this block.
/*
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
