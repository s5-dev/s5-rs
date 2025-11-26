# s5_node

The main S5 node implementation. This crate orchestrates the various components (storage, networking, filesystem, registry) into a running node.

## Features

- **Configuration**: Loads node configuration from TOML files (default: `~/.config/s5/local.toml`).
- **Storage Management**: Initializes and manages configured blob stores (Local, S3, Sia, Memory).
- **Networking**: Sets up the Iroh endpoint and router, registering protocol handlers for Blobs and Registry.
- **Sync**: Runs file synchronization jobs.
- **FUSE**: Spawns FUSE mounts for configured filesystems.

## Usage

This crate is primarily used by the `s5_cli` binary, but can be embedded in other applications.

```rust,no_run
use s5_node::{S5Node, config::S5NodeConfig};

// Load config from a TOML file (S5NodeConfig is serde-deserializable)
let config: S5NodeConfig = toml::from_str(
    &std::fs::read_to_string("config.toml").unwrap()
).unwrap();

// Initialize node with optional local registry
let node = S5Node::new(config, None).await?;

// The node is now running and listening on the Iroh network.
// Keep the process alive...
tokio::signal::ctrl_c().await?;
node.shutdown().await?;
```
