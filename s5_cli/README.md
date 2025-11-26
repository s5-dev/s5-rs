# s5_cli

The command-line interface for S5.

## Installation

```bash
cargo install --git https://github.com/s5-dev/s5-rs s5_cli
```

## Usage

The `s5` command provides several subcommands to interact with S5 nodes, stores, and filesystems.

### Configuration
Manage node configuration (stores, peers, identity).
```bash
# Create or update the default node config
s5 config init

# Then edit ~/.config/s5/local.toml to add stores and peers,
# following docs/reference/configuration.md.
```

### Import
Import data into the default blob store and FS5 root.
```bash
# Import local directory
s5 import local ./my-data

# Import from HTTP
s5 import http https://example.com/
```

### Blobs
Low-level blob operations.
```bash
s5 blobs upload --peer my-peer ./file.txt
s5 blobs download --peer my-peer --out ./file.txt <hash>
s5 blobs delete --peer my-peer <hash>

# Dry-run local GC for the "default" store
s5 blobs gc-local --store default --dry-run

# Apply local GC (after inspecting the dry run)
s5 blobs gc-local --store default

# Verify that all referenced blobs exist in the store
s5 blobs verify-local --store default
```

### Snapshots
Manage FS5 snapshots.
```bash
s5 snapshots list-fs
s5 snapshots create-fs
s5 snapshots restore --peer my-peer --hash <hash> --root ./restore-dir
```

### Mount
Mount the FS5 filesystem via FUSE.
```bash
s5 mount ./mnt
```

### Node
Start the S5 node (usually run in background or via systemd).
```bash
s5 start
```
