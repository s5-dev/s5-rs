# S5 CLI Reference

The `s5` command-line tool manages configuration, data import, and node operations.

## Global Options

*   `--node <NAME>`: Specify which node config to use (default: "local").
*   `-v`, `-vv`: Increase verbosity.

## Commands

### `s5 config init`

Initializes a default configuration file at `~/.config/s5/local.toml` and generates a new identity.

### `s5 start`

Starts the S5 node. This runs:
*   The P2P endpoint.
*   Blob and Registry services.
*   Configured sync jobs.
*   Configured FUSE mounts.

### `s5 import`
 
Imports data into a blob store and records file metadata in the node's primary FS5 root.
 
Subcommands:
- `local <PATH>`: Recursively import files from the local filesystem.
- `http <URL>`: Recursively import content starting from an HTTP/HTTPS URL (following links under the base URL).
 
Common options:
- `--target-store <STORE_NAME>`: Which configured blob store to write content into (default: `default`).
- `--prefix <PATH>`: Import into a scoped FS5 subdirectory. Internally this uses `FS5::subdir(PATH)` so that imported files appear under that logical directory in FS5.
 
`local` specifics:
- Without `--prefix`, imported keys are the absolute file paths (minus any leading `/`).
- With `--prefix some/dir`, keys are made relative to the imported base path and then written under `some/dir` inside FS5 (e.g. `some/dir/subdir/file.txt`).
- By default, directories containing a valid `CACHEDIR.TAG` file (per the [Cache Directory Tagging Specification](https://bford.info/cachedir/)) are ignored.
- Use `--no-ignore-cachedir` to include these directories.
- Use `--ignore-cachedir` to re-enable ignoring them (useful if `--no-ignore` was used).
- Use `--no-ignore` (or `-I`) to disable all ignore rules (including `.gitignore`, `.ignore`, and `CACHEDIR.TAG`).
- Use `--no-ignore-vcs` to disable only VCS ignore files (`.gitignore`, `.git/info/exclude`, etc.).
- Use `--ignore-vcs` to re-enable VCS ignore files after `--no-ignore-vcs`.
- Use `--always-import` to skip metadata checks and always re-import files (useful for forcing a full import).

`http` specifics:

- Without `--prefix`, imported keys use the full URL structure: `scheme/host/path` (e.g. `https/example.com/file.zip`).
- With `--prefix web/mirror`, URLs under the base URL are stored relative to that base and written under `web/mirror` (e.g. `web/mirror/assets/logo.png`).
 
Examples:
```bash
# Import a local directory into the default store, keeping absolute paths as keys
s5 import --target-store default local ./my-data

# Import a local directory into FS5 subdir "projects/my-app" with base-relative keys
s5 import --target-store default local --prefix projects/my-app ./my-data

# Import a single URL and its linked assets under the base URL
s5 import --target-store default http https://example.com/index.html

# Import a site mirror into FS5 subdir "web/mirror", using paths relative to the base URL
s5 import --target-store default http --prefix web/mirror https://example.com/
```


### `s5 blobs`

Low-level operations on blobs.

*   `upload --peer <PEER> <PATH>`: Upload a file as a blob.
*   `download --peer <PEER> --out <PATH> <HASH>`: Download a blob.
*   `delete --peer <PEER> <HASH>`: Delete (unpin) a blob.
*   `gc-local --store <STORE_NAME> [--dry-run]`: Conservatively garbage-collect a local blob store. A blob is only
    eligible for deletion if it has **no pins** in the node registry and is
    **not reachable** from the primary FS5 root (its current head and any
    local snapshots).
*   `verify-local --store <STORE_NAME>`: Verify that all blobs referenced from the primary FS5 root (current
    head and any local snapshots) exist in the given local store. This
    command is read-only and does not modify any data.

Examples:

```bash
# Inspect what would be deleted from the "default" store
s5 blobs gc-local --store default --dry-run

# Actually delete unpinned and unreachable blobs from the "default" store
s5 blobs gc-local --store default

# Sanity-check that the "default" store still contains all referenced blobs
s5 blobs verify-local --store default
```

### `s5 snapshots`

Utilities for managing FS5 snapshots stored in the registry and the local FS5 root.

*   `head --sync <SYNC_NAME>`: Show the current remote snapshot hash and revision for a configured sync job.
*   `download --peer <PEER> --out <PATH> <HASH>`: Download a raw snapshot blob.
*   `restore --root <PATH> --peer <PEER> --hash <HASH>`: Download a snapshot and merge it into a local FS5 root directory.
*   `list-fs`: List snapshots for the node's primary local FS5 root (from `snapshots.fs5.cbor`).
*   `create-fs`: Create a new snapshot for the node's primary local FS5 root, recording it in `snapshots.fs5.cbor` and pinning its hash.
*   `delete-fs --name <NAME>`: Delete a local snapshot entry by name and unpin its hash.

### `s5 tree`
 
Prints a tree view of the node's primary FS5 root (backed by the same FS5 directory used for imports).
 
```bash
# Show entire FS5 root
s5 tree

# Show only a subdirectory inside the FS5 root
s5 tree --path projects/my-app
```
 
### `s5 mount`
 
Mounts an FS5 filesystem using FUSE.
 
```bash
# Mount the node's primary FS5 root
s5 mount /mnt/s5

# Mount only a subdirectory of the FS5 root
s5 mount /mnt/s5 --subdir projects/my-app

# Mount read-only (safe for sharing or inspection)
s5 mount /mnt/s5-ro --read-only
```
 
Options:
*   `--root <PATH>`: Override the FS5 root directory on disk (defaults to the node's standard root).
*   `--subdir <PATH>`: Mount only a logical subdirectory inside the FS5 root.
*   `--read-only`: Mount the filesystem read-only; any attempt to create, modify, or delete files will fail.
*   `--allow-root`: Allow the root user to access the mount.
*   `--auto-unmount`: Automatically unmount on exit.
 
To unmount a running FUSE mount, use:
 
```bash
fusermount3 -u /mnt/s5
```

