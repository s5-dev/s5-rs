# s5_core

Core types and traits for the S5 protocol, shared by all S5 Rust crates.

This crate defines the wire formats and abstractions that other S5 components
build on, including content hashes, blob identifiers and locations, storage and
blob facades, stream/registry messages, and pinning primitives.

### Protocol-defining modules (wire formats)

The following modules define on-the-wire formats and persistent metadata
structures. They are intended to have stable wire representations for the
1.0 protocol:

- `hash` – `Hash` (BLAKE3 32-byte hash)
- `blob::identifier` – `BlobId` (S5 blob identifier)
- `blob::location` – `BlobLocation` and related types
- `stream::types` – `StreamKey`, `StreamMessage`, `MessageType`, etc.

Changes to these modules should be treated as protocol changes.

### Convenience implementations (non-wire helpers)

Other items exported by this crate are higher-level helpers built on top of
the protocol types:

- `store::Store`, `StoreFeatures`, and the `BlobStore` facade
- `stream::RegistryApi` and the local `RedbRegistry` implementation
- `pins::Pins`, `PinContext`, and `RegistryPinner`
- `cbor::Value` and related CBOR utilities

These are not themselves part of the on-the-wire protocol. They may evolve
more freely across major versions, or move into separate crates in a future
2.0, without impacting the core wire format.

## Features

- **Content addressing**: `Hash` and `BlobId` types for identifying content.
- **Blob metadata**: `BlobLocation` encodes where and how content can be
  fetched (URLs, Iroh, Sia, multihashes, etc.).
- **Storage abstraction**: `Store` trait for pluggable backends (local FS,
  S3, etc.), plus `BlobStore` as a higher-level blob API on top of a `Store`.
- **Streams & registry**: `StreamMessage` and `StreamKey` define a unified
  message format used by both S5 Streams and the S5 Registry. `RegistryApi`
  describes how to read/write registry entries, and `RedbRegistry` provides a
  simple local implementation.
- **Pinning**: `Pins`, `PinContext`, and `RegistryPinner` provide a small API
  for tracking which nodes or contexts are pinning which blobs.
- **CBOR utilities**: `cbor::Value` offers a token-level CBOR representation
  for diagnostics and tooling.

## Getting started

Add `s5_core` to your `Cargo.toml` with `cargo add s5_core`

Then import the core types you need:

```rust
use s5_core::{
    BlobId, BlobLocation, BlobStore, Hash,
    Store, StoreFeatures,
    StreamKey, StreamMessage, MessageType, RegistryApi, RedbRegistry,
    Pins, PinContext, RegistryPinner,
};
```

### Implementing a `Store`

To make S5 work with your own storage backend, implement the `Store` trait for
that backend and wrap it in a `BlobStore`:

```rust
use async_trait::async_trait;
use bytes::Bytes;
use futures_core::Stream;
use s5_core::{Store, StoreFeatures, StoreResult, BlobStore};

#[derive(Debug)]
struct MyStore { /* fields */ }

#[async_trait]
impl Store for MyStore {
    async fn put_stream(
        &self,
        path: &str,
        stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<()> {
        // write stream to your backend under `path`
        # let _ = (path, stream);
        Ok(())
    }

    fn features(&self) -> StoreFeatures {
        StoreFeatures {
            supports_rename: true,
            case_sensitive: true,
            recommended_max_dir_size: 10_000,
        }
    }

    // implement the remaining methods: exists, put_bytes, open_read_* , etc.
    # async fn exists(&self, _path: &str) -> StoreResult<bool> { Ok(false) }
    # async fn put_bytes(&self, _path: &str, _bytes: Bytes) -> StoreResult<()> { Ok(()) }
    # async fn open_read_stream(
    #     &self,
    #     _path: &str,
    #     _offset: u64,
    #     _max_len: Option<u64>,
    # ) -> StoreResult<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>> {
    #     unimplemented!()
    # }
    # async fn open_read_bytes(
    #     &self,
    #     _path: &str,
    #     _offset: u64,
    #     _max_len: Option<u64>,
    # ) -> StoreResult<Bytes> { unimplemented!() }
    # async fn size(&self, _path: &str) -> StoreResult<u64> { Ok(0) }
    # async fn list(
    #     &self,
    # ) -> StoreResult<Box<dyn Stream<Item = Result<String, std::io::Error>> + Send + Unpin + 'static>> {
    #     unimplemented!()
    # }
    # async fn delete(&self, _path: &str) -> StoreResult<()> { Ok(()) }
    # async fn rename(&self, _old_path: &str, _new_path: &str) -> StoreResult<()> { Ok(()) }
    # async fn provide(&self, _path: &str) -> StoreResult<Vec<s5_core::BlobLocation>> { Ok(vec![]) }
}

fn main() {
    let store = MyStore { /* ... */ };
    let blobs = BlobStore::new(store);
    // use `blobs` to import and read content-addressed blobs
}
```

### Streams and registry

To work with registry entries, implement `RegistryApi` or use `RedbRegistry`:

```rust
use s5_core::{stream::registry::RedbRegistry, StreamKey, StreamMessage, MessageType, Hash};

# fn example() -> anyhow::Result<()> {
let registry = RedbRegistry::open("/path/to/db")?;
let key = StreamKey::Local([0u8; 32]);

// Read the current value
if let Some(msg) = registry.get(&key).await? {
    println!("revision = {}", msg.revision);
}

// Construct and publish a new registry entry (details omitted)
# let msg = StreamMessage::new(
#     MessageType::Registry,
#     key,
#     1,
#     Hash::EMPTY,
#     Box::new([]),
#     None,
# )?;
# registry.set(msg).await?;
# Ok(())
# }
```

## License

Licensed under either of

- Apache License, Version 2.0, or
- MIT license

at your option.

See the root of the workspace for full license texts.
