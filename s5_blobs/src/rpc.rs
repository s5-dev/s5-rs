use bytes::Bytes;
use irpc::channel::{mpsc, oneshot};
use irpc::rpc_requests;
use s5_core::blob::location::BlobLocation;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// ALPN for the **public** blobs protocol — serves only blobs in the
/// node's `public_blob_hashes` set (identity bundles, advertised
/// public-vault content). No challenge handshake; any peer may dial.
pub const ALPN_PUBLIC: &[u8] = b"s5/blobs/public/v1";

/// ALPN for the **ACL-gated** blobs protocol. First exchange must be
/// `AuthChallenge` → `AuthProve` (F02 challenge per
/// `docs/reference/acl-and-revocation.md §3a`). The client signs a
/// server-issued nonce bound to both transport pubkeys with its device
/// ACL key; the server verifies the sig and checks that the ACL pubkey
/// is in some served vault's `authorized_acl_pubkeys`. All subsequent
/// requests are gated by `allow_acl_read(bound_acl_pubkey, hash)`.
pub const ALPN_ACL: &[u8] = b"s5/blobs/acl/v1";

#[derive(Debug, Serialize, Deserialize)]
#[rpc_requests(message = RpcMessage)]
pub enum RpcProto {
    /// **F02 challenge step 1** (ACL ALPN only). The client opens this
    /// RPC as its first message; the server replies with a fresh 32-byte
    /// random nonce held in per-connection state. On the public ALPN,
    /// this is rejected with a permission error.
    #[rpc(tx = oneshot::Sender<AuthChallengeResponse>)]
    AuthChallenge(AuthChallenge),
    /// **F02 challenge step 2** (ACL ALPN only). The client signs
    /// `"s5-blobs-acl-v1-auth:" || binding` (where `binding` is
    /// `blake3.derive_key("s5-blobs-acl-v1-binding", nonce ||
    /// client_iroh_pub || server_iroh_pub)`) with its device ACL key
    /// and presents it here. The server verifies the signature and
    /// checks that `acl_pubkey ∈` some served vault's
    /// `authorized_acl_pubkeys`; on success it records the bound ACL
    /// pubkey for the connection lifetime.
    #[rpc(tx = oneshot::Sender<Result<(), String>>)]
    AuthProve(AuthProve),
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
    //
    // TODO(audit): drop the bool. It tells the caller whether
    // *other* peers also pinned this hash, which is a privacy
    // leak — anyone can probe "is this hash pinned by someone
    // else here?" by sending DeleteBlob and reading the bool.
    // Replace with `Result<(), String>`; clients don't actually
    // consume the bool today.
    #[rpc(tx = oneshot::Sender<Result<bool, String>>)]
    DeleteBlob(DeleteBlob),
    /// Request that the server pin a blob that already exists.
    ///
    /// The response is `Ok(true)` if the blob was found and pinned,
    /// `Ok(false)` if the blob was not found, and `Err(String)` on error.
    #[rpc(tx = oneshot::Sender<Result<bool, String>>)]
    PinBlob(PinBlob),
}

/// First step of the F02 ACL challenge. Client sends a wire-format
/// version tag (currently `1`) so future protocol revisions can be
/// distinguished cleanly; the server generates a fresh 32-byte random
/// nonce and returns it. Client must call this before any other
/// request on the ACL ALPN.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthChallenge {
    pub version: u8,
}

impl Default for AuthChallenge {
    fn default() -> Self {
        Self { version: 1 }
    }
}

/// Response to `AuthChallenge`. The 32-byte `nonce` is the only piece
/// the client needs from the server; combined with both transport
/// pubkeys (known to both sides at QUIC handshake time) it forms the
/// binding the client signs in `AuthProve`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthChallengeResponse {
    pub nonce: [u8; 32],
}

/// Second step of the F02 ACL challenge. `acl_pubkey` is the client's
/// device ACL/read verifying key (must `∈` some served vault's
/// `authorized_acl_pubkeys` for the principal check to pass).
///
/// The signature is ed25519 over `b"s5-blobs-acl-v1-auth:" || binding`,
/// where `binding = blake3.derive_key("s5-blobs-acl-v1-binding",
/// nonce || client_iroh_pubkey || server_iroh_pubkey)`. Split into two
/// 32-byte halves (`sig_r`, `sig_s`) — semantically the R-point and s-
/// scalar of the ed25519 signature; the split exists because serde's
/// derive macros don't support `[u8; 64]` directly.
///
/// The channel-bound binding is the entire MITM/relay defence: a sig
/// minted for connection A can't be replayed on connection B because B's
/// nonce + transport pubkeys produce a different binding. The `nonce`
/// alone prevents time-replay on the same identity.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthProve {
    pub acl_pubkey: [u8; 32],
    pub sig_r: [u8; 32],
    pub sig_s: [u8; 32],
}

impl AuthProve {
    /// Build an `AuthProve` from a contiguous 64-byte ed25519 signature.
    pub fn from_sig(acl_pubkey: [u8; 32], sig: [u8; 64]) -> Self {
        let mut sig_r = [0u8; 32];
        let mut sig_s = [0u8; 32];
        sig_r.copy_from_slice(&sig[..32]);
        sig_s.copy_from_slice(&sig[32..]);
        Self {
            acl_pubkey,
            sig_r,
            sig_s,
        }
    }

    /// Reassemble the 64-byte ed25519 signature.
    pub fn sig_bytes(&self) -> [u8; 64] {
        let mut out = [0u8; 64];
        out[..32].copy_from_slice(&self.sig_r);
        out[32..].copy_from_slice(&self.sig_s);
        out
    }
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
//
// TODO(audit): this struct + `QueryResponse` are the unfinished
// peer-routing design (locations, blinded, actual_hash,
// location_types). In-tree only `exists` and `size` are consumed
// (`BlobsRead::blob_contains` / `blob_get_size`); the rest is
// dead weight on every wire round-trip. Either commit to the
// full peer-discovery design with tests + a real consumer, or
// strip Query/QueryResponse to `{hash} -> {exists, size}` and
// add the richer shape back behind a versioned RPC when there's
// an actual user. See also the original-design TODOs below.
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
        assert!(!decoded.exists);
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
        assert!(decoded2.exists);
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
        assert!(!decoded.blinded);
    }
}
