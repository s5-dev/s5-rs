# S5 Node Configuration Reference

This document describes the `toml` configuration file format for S5 nodes.

Default location: `~/.config/s5/local.toml`
Other nodes: `~/.config/s5/nodes/<name>.toml`

## Structure

### Top-level fields

```toml
# Optional human-readable name for the node
name = "my-node"

# Optional path to the registry database directory.
# Defaults to a "registry" directory next to the config file.
registry_path = "/path/to/registry"
```

### `[identity]`

Configures the node's cryptographic identity (Ed25519).

```toml
[identity]
# Path to a file containing the 32-byte secret key.
# After `s5 config init`, this is typically "local.secretkey" (a relative path
# next to the config file). Absolute paths are also supported.
secret_key_file = "local.secretkey"

# OR directly provide the secret key as a hex string (not recommended for production).
# secret_key = "..."
```

### `[store.<name>]`

Defines a blob storage backend. You can define multiple stores.

**Types:** `local`, `s3`, `sia_renterd`, `memory`

#### Local Filesystem
```toml
[store.default]
type = "local"
base_path = "/home/user/.local/share/s5/blobs"
```

#### S3 Compatible
```toml
[store.s3]
type = "s3"
# Required: S3-compatible endpoint URL
endpoint = "https://s3.amazonaws.com"
# Optional: region name; some providers ignore this
region = "us-east-1"
# Bucket name to use for blobs
bucket_name = "my-bucket"
# Access credentials for the bucket
access_key = "..."
secret_key = "..."
```

#### Sia Renterd
```toml
[store.sia]
type = "sia_renterd"
bucket = "s5-blobs"
worker_api_url = "http://localhost:9980/api/worker"
bus_api_url = "http://localhost:9980/api/bus"
password = "renterd-password"
```

### `[peer.<name>]`

Configures a known peer.

```toml
[peer.friend]
# The peer's public Endpoint ID string (required).
id = "2kajba6o6aa3f53szgv5kq1bjbs5d7w6xjbowtafbxfvrmbjb4tq"

# Access Control List (ACL) for blobs (Optional).
# If omitted, this peer has no access to your stores.
[peer.friend.blobs]
# List of store names this peer is allowed to read from.
readable_stores = ["default"]
# Name of the store where uploads from this peer should be saved.
store_uploads_in = "default"
```

### `[sync.<name>]`

Configures a continuous file synchronization job.

```toml
[sync.documents]
# The local directory to sync.
local_path = "/home/user/Documents"

# The peer to route traffic through (must match a [peer.<name>] entry).
via_untrusted = ["friend"]

# Shared secret used to derive encryption and signing keys.
# Must be identical on all devices syncing this folder.
shared_secret = "correct-horse-battery-staple"

# Optional: Interval in seconds for continuous sync.
# If omitted, sync runs once on startup.
interval_secs = 60
```

### `[fuse.<name>]`

Configures a FUSE mount to be started automatically when the node runs.
Mounts are started via the `s5` CLI (`s5 mount` or auto-mounts); you do not need to run any separate FUSE binary directly.

```toml
[fuse.docs]
# The path to the FS5 root directory (contains root.fs5.cbor).
# Usually located in ~/.local/share/s5/roots/<node>.fs5
root_path = "/home/user/.local/share/s5/roots/local.fs5"

# The local mount point.
mount_path = "/mnt/s5-docs"

# Optional: Unmount automatically when the node stops (default: false).
auto_unmount = true

# Optional: Allow root user access (default: false).
allow_root = false
```
