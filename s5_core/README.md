# s5_core

Core types and traits for the S5 protocol. This crate defines the wire-stable primitives and shared abstractions used across the S5 ecosystem.

## Key Components

### Protocol Types (Wire-Stable)
These types define the on-the-wire format and persistent metadata structure. Changes here are protocol-breaking.

- **`Hash`**: The fundamental content addressing primitive (BLAKE3, 32 bytes).
- **`BlobId`**: The user-facing identifier for a blob. Encoded as a multibase string containing the `Hash` and the blob size (`u64`).
- **`BlobLocation`**: A CBOR-encoded enum describing *how* to retrieve a blob. Supports:
    - **Inline**: Raw bytes embedded directly in the location.
    - **Network**: `Url` (HTTP), `Iroh` (NodeID + Tag), `SiaFile` (renterd metadata).
    - **Transforms**: `Encryption` (XChaCha20-Poly1305), `Compression` (Zstd, Brotli).
- **`StreamMessage`**: The atomic unit of the S5 Registry/Stream layer. Contains a `StreamKey`, revision number, payload, and Ed25519 signature.

### Abstractions (Traits & Helpers)
These are Rust-level abstractions for building S5 applications and nodes.

- **`Store`**: The low-level async storage trait. Implement this to add new backends (e.g., S3, Local FS, Memory).
    - Methods: `put_stream`, `open_read_stream`, `list`, `delete`, `rename`.
- **`BlobStore`**: The high-level facade over a `Store`.
    - Handles `BlobId` verification (hash checking).
    - Manages `BlobLocation` resolution.
    - Implements `BlobsRead` and `BlobsWrite` for ergonomic byte streaming.
- **`RegistryApi`**: Trait for interacting with mutable pointers (S5 Registry).
    - Implementations available in `s5_registry` (MemoryRegistry, TeeRegistry, MultiRegistry) and `s5_registry_redb` (RedbRegistry, native-only).
- **`Pins`**: Trait for preventing garbage collection of blobs.
    - **`RegistryPinner`**: An implementation that pins blobs referenced by local registry entries.

## Usage

```rust,no_run
use s5_core::{BlobId, Hash};

// Create a BlobId from raw data
let data = b"hello world";
let hash = Hash::new(data);
let id = BlobId::new(hash, data.len() as u64);

println!("Blob ID: {}", id);
```
