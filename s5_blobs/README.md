# s5_blobs

Iroh-based blob transport layer for S5. This crate handles the exchange of content-addressed blobs between peers.

## Components

- **`BlobsServer`**: An Iroh protocol handler that serves blobs from configured `BlobStore`s. It supports access control (ACLs) per peer.
- **`Client`**: An RPC client for querying, uploading, downloading, and deleting blobs on remote peers.
    - Implements `s5_core::BlobsRead` and `s5_core::BlobsWrite`, allowing it to be used generically wherever blob access is needed.
- **`RemoteBlobStore`**: An implementation of `s5_core::Store` that proxies operations to a remote peer via `Client`.

## Protocol

- **ALPN**: `s5/blobs/0`
- **Operations**:
    - `Query(hash)`: Check existence/size.
    - `Download(hash, offset, len)`: Stream blob content.
    - `Upload(hash, size)`: Stream blob content to server.
    - `Delete(hash)`: Unpin/delete blob.

## Usage

```rust
use s5_blobs::{Client, RemoteBlobStore};

// Connect to a peer
let client = Client::connect(endpoint, peer_addr);

// Use as a Store
let store = RemoteBlobStore::new(client);
let bytes = store.open_read_bytes("hash...", 0, None).await?;
```
