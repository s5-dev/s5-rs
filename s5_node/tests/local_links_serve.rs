//! Validates that a `BlobsServer` configured with a `LocalLinksStore` as a
//! read source can serve linked files (whole blob and slice) to a remote
//! client. This exercises `BlobsServer::with_read_sources` end-to-end without
//! depending on the encrypted FS layer.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use anyhow::Result;
use iroh::{Endpoint, endpoint::presets};
use s5_blobs::{ALPN as BLOBS_ALPN, BlobsServer, PeerConfigBlobs};
use s5_core::blob::BlobsRead;
use s5_store_local::LocalStore;
use s5_store_local_links::LocalLinksStore;
use tempfile::tempdir;

#[tokio::test]
async fn workflow_local_links_serve() -> Result<()> {
    // 1. Create a temp file to link
    let files_dir = tempdir()?;
    let file_path = files_dir.path().join("video.mp4");
    let file_content = b"fake video content for testing";
    std::fs::write(&file_path, file_content)?;

    // 2. Setup server with both BlobStore and LocalLinksStore
    let server_endpoint = Endpoint::builder(presets::N0).bind().await?;

    // Create local blob store (for regular blobs)
    let store_dir = tempdir()?;
    let local_store = LocalStore::new(store_dir.path());
    let blob_store = local_store.to_blob_store();
    let mut stores = HashMap::new();
    stores.insert("default".to_string(), blob_store.clone());

    // Create local links store
    let links_dir = tempdir()?;
    let links_store = Arc::new(LocalLinksStore::open(links_dir.path())?);

    // Link the file (hash it and register)
    let blob_id = links_store
        .import_file(file_path.clone(), |_| Ok(()))
        .await?;

    // Create read sources with the links store
    let mut read_sources: HashMap<String, Arc<dyn BlobsRead>> = HashMap::new();
    read_sources.insert(
        "links".to_string(),
        links_store.clone() as Arc<dyn BlobsRead>,
    );

    // Configure ACL to allow reading from both stores
    let mut peer_cfg = HashMap::new();
    let acl = PeerConfigBlobs {
        readable_stores: vec!["default".to_string(), "links".to_string()],
        store_uploads_in: Some("default".to_string()),
        ..Default::default()
    };
    peer_cfg.insert("*".to_string(), acl);

    let blobs_server = BlobsServer::with_read_sources(stores, read_sources, peer_cfg, None);

    let router = iroh::protocol::Router::builder(server_endpoint.clone())
        .accept(BLOBS_ALPN, blobs_server)
        .spawn();

    // 3. Setup client and connect to server
    let client_endpoint = Endpoint::builder(presets::N0).bind().await?;
    let client = s5_blobs::Client::connect(client_endpoint.clone(), server_endpoint.addr());

    // 4. Client queries for the linked file
    let query_result = client.query(blob_id.hash, BTreeSet::new()).await?;
    assert!(query_result.exists, "linked file should exist on server");
    assert_eq!(query_result.size, Some(file_content.len() as u64));

    // 5. Client downloads the linked file
    let downloaded = client.blob_download(blob_id.hash).await?;
    assert_eq!(downloaded.as_ref(), file_content);

    // 6. Client downloads a slice
    let slice = client.blob_download_slice(blob_id.hash, 5, Some(5)).await?;
    assert_eq!(slice.as_ref(), &file_content[5..10]);

    router.shutdown().await?;
    Ok(())
}
