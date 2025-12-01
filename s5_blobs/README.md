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

## Status

### Platform Support

| Component | Native | WASM | Feature |
|-----------|--------|------|---------|
| `Client` | Yes | Yes | (always) |
| `RemoteBlobStore` | Yes | Yes | (always) |
| `MultiFetcher` | Yes | Yes | (always) |
| `BlobsServer` | Yes | **No** | `server` |
| `BlobsRead`/`BlobsWrite` on Client | Yes | **No** | `server` |

### Features

- `server` (default): Enables server-side functionality (`BlobsServer`, trait impls). 
  Requires tokio. Not WASM-compatible.

For WASM/browser usage:
```toml
s5_blobs = { version = "...", default-features = false }
```

This gives you `Client`, `RemoteBlobStore`, and `MultiFetcher` without the server components.

### Implementation Notes

- `RemoteBlobStore` implements the `Store` trait by proxying to a remote peer via `Client`
- It interprets store paths as content hashes (e.g., `blob3/aa/bb/cccc...`)
- `put_bytes` and `put_stream` first try to pin (single round-trip optimization), then upload if needed
- `async-trait` is always required (not gated behind `server`) because `Store` trait uses it

## Usage

```rust
use s5_blobs::{Client, RemoteBlobStore};

// Connect to a peer
let client = Client::connect(endpoint, peer_addr);

// Use as a Store
let store = RemoteBlobStore::new(client);
let bytes = store.open_read_bytes("hash...", 0, None).await?;
```
