//! Validates end-to-end encrypted file system synchronization between two trusted
//! nodes via a third, untrusted node.
//!
//! This test simulates a common user scenario: syncing data securely between two
//! personal devices (e.g., a laptop and a desktop) using a cloud service that
//! should not have access to the plaintext data.
//!
//! The test sets up:
//! 1.  **Two trusted nodes ("laptop", "desktop"):** These represent a user's
//!     personal devices. They share a secret key for encryption and signing.
//! 2.  **One untrusted node ("cloud"):** This represents a cloud storage provider.
//!     It is configured to allow the trusted nodes to upload and read blobs, but
//!     it cannot decrypt the data it stores.
//!
//! The test flow is as follows:
//! 1.  The "laptop" node creates a directory and some files in its local,
//!     plaintext file system.
//! 2.  It then "pushes" a snapshot of this directory to an encrypted, remote-backed
//!     file system. This action encrypts the data and uploads it to the "cloud" node.
//! 3.  To simulate asynchronous syncing (where devices are not online at the same
//!     time), the "laptop" node is shut down.
//! 4.  The "desktop" node then connects to the "cloud" and "pulls" the encrypted
//!     snapshot, decrypting it into its own local, plaintext file system.
//! 5.  Finally, the test verifies that the files and directory structure on the
//!     "desktop" are identical to what the "laptop" originally created. It also
//!     confirms that the data stored on the "cloud" node is indeed encrypted.
//!
//! This setup validates that the untrusted node can successfully store and relay
//! the encrypted data without being able to access the original content, ensuring
//! user privacy and data integrity in a distributed environment.
//!
//! To run this test: `cargo test -p s5_node --test fs_sync_complete`

use std::collections::HashMap;

use anyhow::Result;
use bytes::Bytes;
use iroh::Endpoint;
use s5_blobs::{ALPN as BLOBS_ALPN, BlobsServer, PeerConfigBlobs, RemoteBlobStore};
use s5_core::{BlobStore, RegistryApi};
use s5_fs::dir::FileRef;
use s5_node::{
    REGISTRY_ALPN, RegistryServer, RemoteRegistry, derive_sync_keys,
    sync::{open_encrypted_fs, open_plaintext_fs, pull_snapshot, push_snapshot},
};
use s5_store_local::LocalStore;
use s5_registry_redb::RedbRegistry;
use tempfile::tempdir;

#[tokio::test]
async fn fs_sync_complete() -> Result<()> {
    // Create endpoints for cloud, laptop, and desktop nodes.
    let cloud_endpoint = Endpoint::builder().bind().await?;
    let laptop_endpoint = Endpoint::builder().bind().await?;
    let desktop_endpoint = Endpoint::builder().bind().await?;

    let cloud_addr = cloud_endpoint.addr();

    // Configure the cloud blob store and per-peer ACLs.
    // Use a local, persistent store to simulate pinning.
    let cloud_store_dir = tempdir()?;
    let cloud_blob_store = LocalStore::new(cloud_store_dir.path()).to_blob_store();

    let mut stores = HashMap::new();
    stores.insert("meta".to_string(), cloud_blob_store.clone());

    let mut peer_cfg: HashMap<String, PeerConfigBlobs> = HashMap::new();
    let acl = PeerConfigBlobs {
        readable_stores: vec!["meta".to_string()],
        store_uploads_in: Some("meta".to_string()),
    };
    // Use the EndpointId display string for ACL keys, matching the
    // production server behaviour and config expectations.
    peer_cfg.insert(laptop_endpoint.id().to_string(), acl.clone());
    peer_cfg.insert(desktop_endpoint.id().to_string(), acl);

    let blobs_server = BlobsServer::new(stores, peer_cfg, None);

    // Registry storage on the cloud node.
    let registry_dir = tempdir()?;
    let registry = RedbRegistry::open(registry_dir.path())?;
    let registry_server = RegistryServer::new(registry.clone());

    // Spawn cloud router accepting blob and registry protocols.
    let cloud_router = iroh::protocol::Router::builder(cloud_endpoint.clone())
        .accept(BLOBS_ALPN, blobs_server)
        .accept(REGISTRY_ALPN, registry_server)
        .spawn();

    // Shared secret that both trusted nodes use.
    let shared_secret = b"super-secret-sync";
    let laptop_keys = derive_sync_keys(shared_secret);
    let desktop_keys = derive_sync_keys(shared_secret);
    let stream_key = laptop_keys.stream_key();

    // Prepare laptop plaintext and encrypted file systems.
    let laptop_plain_dir = tempdir()?;
    let laptop_plain = open_plaintext_fs(laptop_plain_dir.path())?;
    let laptop_blob_client = s5_blobs::Client::connect(laptop_endpoint.clone(), cloud_addr.clone());
    let laptop_registry_client =
        RemoteRegistry::connect(laptop_endpoint.clone(), cloud_addr.clone());
    let laptop_encrypted = open_encrypted_fs(
        stream_key,
        &laptop_keys,
        laptop_blob_client.clone(),
        laptop_registry_client.clone(),
    );

    // Prepare desktop plaintext and encrypted file systems.
    let desktop_plain_dir = tempdir()?;
    let desktop_plain = open_plaintext_fs(desktop_plain_dir.path())?;
    let desktop_blob_client =
        s5_blobs::Client::connect(desktop_endpoint.clone(), cloud_addr.clone());
    let desktop_registry_client =
        RemoteRegistry::connect(desktop_endpoint.clone(), cloud_addr.clone());
    // We will open desktop_encrypted later when we need to pull.

    // Create sample data on the laptop plaintext FS.
    laptop_plain.create_dir("docs", false).await?;
    laptop_plain.create_dir("docs/nested", false).await?;
    laptop_plain
        .file_put_sync(
            "docs/readme.txt",
            FileRef::new_inline_blob(Bytes::from_static(b"Hello from the laptop!")),
        )
        .await?;
    laptop_plain
        .file_put_sync(
            "docs/nested/todo.txt",
            FileRef::new_inline_blob(Bytes::from_static(b"1. celebrate successful sync")),
        )
        .await?;
    laptop_plain
        .file_put_sync(
            "root_note.txt",
            FileRef::new_inline_blob(Bytes::from_static(b"Root file content")),
        )
        .await?;
    laptop_plain.save().await?;

    // Push laptop plaintext state into the encrypted FS (publishes to cloud).
    push_snapshot(&laptop_plain, &laptop_encrypted).await?;

    // --- Simulate offline relay: shutdown laptop before desktop pulls ---
    drop(laptop_endpoint);
    drop(laptop_blob_client);
    drop(laptop_registry_client);

    // Pull the encrypted state into desktop plaintext FS via the cloud.
    // Re-open desktop_encrypted to ensure it loads the latest state from the registry.
    let desktop_encrypted = open_encrypted_fs(
        stream_key,
        &desktop_keys,
        desktop_blob_client.clone(),
        desktop_registry_client.clone(),
    );
    pull_snapshot(&desktop_encrypted, &desktop_plain).await?;

    // Validate desktop plaintext matches laptop plaintext.
    let laptop_snapshot = laptop_plain.export_snapshot().await?;
    let desktop_snapshot = desktop_plain.export_snapshot().await?;
    // Compare file paths
    let lp_paths: Vec<_> = laptop_snapshot.files.keys().cloned().collect();
    let dt_paths: Vec<_> = desktop_snapshot.files.keys().cloned().collect();
    assert_eq!(dt_paths, lp_paths);

    // Compare inline file contents
    for (path, lp_ref) in &laptop_snapshot.files {
        let dt_ref = desktop_snapshot
            .files
            .get(path)
            .expect("missing path on desktop");
        use s5_core::blob::location::BlobLocation;
        let lp_bytes = match lp_ref.locations.as_ref().and_then(|v| v.first()) {
            Some(BlobLocation::IdentityRawBinary(b)) => b,
            _ => panic!("expected inline blob for {}", path),
        };
        let dt_bytes = match dt_ref.locations.as_ref().and_then(|v| v.first()) {
            Some(BlobLocation::IdentityRawBinary(b)) => b,
            _ => panic!("expected inline blob for {}", path),
        };
        assert_eq!(dt_bytes, lp_bytes, "content differs for {}", path);
    }

    // Verify the registry entry exists and the stored blob is encrypted.
    let registry_entry = desktop_registry_client
        .get(&stream_key)
        .await?
        .expect("registry entry present");

    // Check that the blob is persisted in the cloud's local store.
    assert!(
        cloud_blob_store.contains(registry_entry.hash).await?,
        "blob should be pinned on the cloud node"
    );

    let remote_blob_store = BlobStore::new(RemoteBlobStore::new(desktop_blob_client.clone()));
    let encrypted_bytes = remote_blob_store
        .read_as_bytes(registry_entry.hash, 0, None)
        .await?;
    let plaintext_bytes = laptop_snapshot.to_bytes()?;
    assert_ne!(encrypted_bytes, plaintext_bytes);
    assert!(
        !std::str::from_utf8(&encrypted_bytes)
            .unwrap_or("")
            .contains("Hello")
    );

    // Shutdown cloud services.
    cloud_router.shutdown().await?;

    Ok(())
}
