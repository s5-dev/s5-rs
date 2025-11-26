use std::collections::HashMap;

use bytes::Bytes;
use iroh::{Endpoint, protocol::Router};
use s5_blobs::{ALPN, BlobsServer, Client, PeerConfigBlobs};
use s5_core::{BlobStore, BlobsRead, BlobsWrite};
use s5_store_memory::MemoryStore;

/// Minimal in-process client/server round-trip over iroh.
///
/// This exercises:
/// - Spinning up a `BlobsServer` on an ephemeral `Endpoint`.
/// - Configuring per-peer ACLs that allow both upload and download.
/// - Using `Client` as a `BlobsWrite` and `BlobsRead` implementation
///   to upload a small blob and fetch it back by content hash.
#[tokio::test]
async fn client_server_roundtrip_bytes() {
    // Set up an in-memory blob store and expose it under a single name.
    let store = BlobStore::new(MemoryStore::new());
    let mut stores = HashMap::new();
    stores.insert("mem".to_string(), store);

    // Configure a wildcard peer entry that allows any client to
    // upload into and read from the "mem" store.
    let mut blobs_cfg = PeerConfigBlobs::default();
    blobs_cfg.readable_stores.push("mem".to_string());
    blobs_cfg.store_uploads_in = Some("mem".to_string());

    let mut peer_cfg = HashMap::new();
    peer_cfg.insert("*".to_string(), blobs_cfg);

    // Bind a fresh endpoint for the server and attach the blobs protocol handler.
    let server_endpoint = Endpoint::builder()
        .bind()
        .await
        .expect("bind server endpoint");
    let server = BlobsServer::new(stores, peer_cfg, None);
    let _router = Router::builder(server_endpoint.clone())
        .accept(ALPN, server)
        .spawn();

    // Bind a separate client endpoint and connect it to the server.
    let client_endpoint = Endpoint::builder()
        .bind()
        .await
        .expect("bind client endpoint");
    let addr = server_endpoint.addr();
    let client = Client::connect(client_endpoint.clone(), addr);

    // Upload a small blob and fetch it back.
    let payload = Bytes::from_static(b"hello blobs");
    let blob_id = client
        .blob_upload_bytes(payload.clone())
        .await
        .expect("upload succeeds");

    // Basic query: the server should report the blob as existing
    // with the correct size.
    let contains = client
        .blob_contains(blob_id.hash)
        .await
        .expect("blob_contains call");
    assert!(contains);

    let size = client
        .blob_get_size(blob_id.hash)
        .await
        .expect("blob_get_size call");
    assert_eq!(size, payload.len() as u64);

    // Full download should match the original bytes.
    let downloaded = client
        .blob_download(blob_id.hash)
        .await
        .expect("download succeeds");
    assert_eq!(downloaded, payload);
}
