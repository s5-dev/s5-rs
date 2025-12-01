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
    /// Request that the server pin a blob that already exists.
    ///
    /// The response is `Ok(true)` if the blob was found and pinned,
    /// `Ok(false)` if the blob was not found, and `Err(String)` on error.
    #[rpc(tx = oneshot::Sender<Result<bool, String>>)]
    PinBlob(PinBlob),
}

/// Pin request identified by the blob's content hash.
#[derive(Debug, Serialize, Deserialize)]
pub struct PinBlob {
    /// BLAKE3 hash of the blob to pin.
    pub hash: [u8; 32],
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
    /// If the query was blinded and the blob exists, this contains the actual hash.
    /// This allows the client to learn the real hash only when the server has the blob.
    ///
    /// Note: We cannot use `skip_serializing_if` here because postcard (used by irpc)
    /// is a non-self-describing binary format that requires all fields to be present.
    #[serde(default)]
    pub actual_hash: Option<[u8; 32]>,
}

/// Query a peer for a blob.
#[derive(Debug, Serialize, Deserialize)]
// TODO: Implement multi-target queries as in the original design:
// - interpret `location_types` as requested location kinds (e.g. BlobContent, Obao6).
// - have servers filter/populate `QueryResponse.locations` accordingly instead of
//   always returning only "blob content" locations.
pub struct Query {
    /// The blake3 hash we want to find blob locations for.
    /// If `blinded` is true, this is `blake3(actual_hash)` for privacy.
    pub hash: [u8; 32],
    /// The location types we support on the caller side.
    pub location_types: BTreeSet<u8>,
    /// If true, `hash` is a blinded hash (`blake3(actual_hash)`).
    /// Server will only reveal blob info if it has a blob matching the blinded hash.
    /// This provides zero-knowledge availability checks.
    #[serde(default)]
    pub blinded: bool,
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that QueryResponse serializes/deserializes correctly with postcard.
    /// This verifies the format used by irpc for RPC messages.
    #[test]
    fn test_query_response_postcard_roundtrip() {
        // Empty response
        let response = QueryResponse {
            exists: false,
            size: None,
            locations: vec![],
            actual_hash: None,
        };

        let bytes = postcard::to_allocvec(&response).expect("serialize empty");
        let decoded: QueryResponse = postcard::from_bytes(&bytes).expect("deserialize empty");
        assert_eq!(decoded.exists, false);
        assert_eq!(decoded.size, None);
        assert_eq!(decoded.locations.len(), 0);
        assert_eq!(decoded.actual_hash, None);

        // Response with data
        let response2 = QueryResponse {
            exists: true,
            size: Some(1024),
            locations: vec![BlobLocation::MultihashBlake3([0xab; 32])],
            actual_hash: Some([0x42; 32]),
        };

        let bytes2 = postcard::to_allocvec(&response2).expect("serialize with data");
        let decoded2: QueryResponse = postcard::from_bytes(&bytes2).expect("deserialize with data");
        assert_eq!(decoded2.exists, true);
        assert_eq!(decoded2.size, Some(1024));
        assert_eq!(decoded2.locations.len(), 1);
        assert_eq!(decoded2.actual_hash, Some([0x42; 32]));
    }

    /// Test Query serialization
    #[test]
    fn test_query_postcard_roundtrip() {
        let query = Query {
            hash: [0x42; 32],
            location_types: BTreeSet::new(),
            blinded: false,
        };

        let bytes = postcard::to_allocvec(&query).expect("serialize query");
        let decoded: Query = postcard::from_bytes(&bytes).expect("deserialize query");
        assert_eq!(decoded.hash, [0x42; 32]);
        assert_eq!(decoded.blinded, false);
    }
}
