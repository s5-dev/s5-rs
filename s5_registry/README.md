# s5_registry

Iroh-based registry protocol for S5. This crate implements an RPC protocol to access and modify S5 registry entries over the Iroh network.

## Features

- **`RegistryServer`**: Exposes a local `RedbRegistry` (the default local storage backend from `s5_core`) over an Iroh endpoint.
- **`Client`**: RPC client for the registry protocol.
- **`RemoteRegistry`**: Adapter that implements the `s5_core::RegistryApi` trait using the RPC client. This allows remote registries to be used interchangeably with local ones in application code.

## Protocol

The registry protocol uses `irpc` over Iroh.
- **ALPN**: `s5/registry/0`
- **Operations**:
    - `Get(key_type, key_data)` -> `Option<StreamMessage>`
    - `Set(StreamMessage)` -> `Result<()>`

## Usage

```rust
use s5_registry::{Client, RemoteRegistry};
use iroh::Endpoint;

// Connect to a remote registry
let endpoint = Endpoint::builder().bind().await?;
let client = Client::connect(endpoint, remote_addr);

// Use as a RegistryApi
let registry = RemoteRegistry::connect(endpoint, remote_addr);
let msg = registry.get(&key).await?;
```
