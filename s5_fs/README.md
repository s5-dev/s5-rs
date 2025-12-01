# S5 File-system (FS5)

High-level, content-addressed, optionally encrypted directory tree. Everything is an immutable DirV1 snapshot; mutability is simulated through actors that rewrite parent snapshots atomically.

## Quick Start

- Open a local root, put/get a file, create an encrypted subdir, and save changes.

```rust
use s5_fs::{DirContext, FS5, FileRef};
use bytes::Bytes;
use tempfile::tempdir;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let tmp = tempdir()?;
    let ctx = DirContext::open_local_root(tmp.path())?;

    // Default: open without autosave; call save() when ready
    let fs = FS5::open(ctx);

    // Put and get a file
    let blob = Bytes::from("hello fs5");
    let file_ref = FileRef::new_inline_blob(blob.clone());
    fs.file_put("greeting.txt", file_ref).await; // fire-and-forget
    let got = fs.file_get("greeting.txt").await.unwrap();

    // Create encrypted sub-directory
    fs.create_dir("secret", true).await?;
    fs.file_put_sync("secret/plan.txt", FileRef::new_inline_blob(Bytes::from("top secret"))).await?;

    // Work with a scoped subdirectory handle
    let project_fs = fs.subdir("projects/my-app").await?;
    project_fs
        .file_put_sync(
            "config.toml",
            FileRef::new_inline_blob(Bytes::from("name = \"my-app\"")),
        )
        .await?;

    // Batch multiple ops, then persist once
    fs.batch(|fs| async move {
        fs.file_put_sync("a.txt", FileRef::new_inline_blob(Bytes::from("A"))).await?;
        fs.file_put_sync("b.txt", FileRef::new_inline_blob(Bytes::from("B"))).await?;
        fs.file_put_sync("secret/b.txt", FileRef::new_inline_blob(Bytes::from("B"))).await?;
        fs.file_delete("a.txt").await?;
        Ok(())
    }).await?;

    // Persist metadata snapshots
    fs.save().await?;
    Ok(())
}
```

### Scoped subdirectories

- `FS5::subdir("path/to/dir")` returns a new `FS5` handle that is logically rooted at the given subdirectory.
- If the subdirectory (or any of its parents) does not exist yet, it is created automatically.
- When the parent directory is encrypted, newly created subdirectories inherit encryption.

---


## Features
- Content addressed metadata snapshots (`DirV1` via CBOR) with durable persistence.
- Actor-based single-writer per directory for deterministic ordering.
- Optional directory encryption (XChaCha20-Poly1305; keys stored under `0x0e`).
- Registry-backed directories (Ed25519) for decentralized pointers.
- Cursor-based listing over large directories (flat logical view, even when sharded).
- Per-file version chains with tombstone deletes and LWW snapshot merge.

### Reachability and Garbage Collection
- FS5 directory snapshots (`root.fs5.cbor`, `snapshots.fs5.cbor`, and metadata in the FS5 meta store) form the **reachability graph** for content blobs.
- The helper `s5_fs::gc::collect_fs_reachable_hashes` walks these snapshots to produce the set of content hashes that are still live from an FS5 root (including historical versions).
- The helper `s5_fs::gc::gc_store` runs a conservative mark-and-sweep over a blob store: any blob with at least one pin in the node registry or whose hash is reachable from the FS5 root is kept; everything else is a GC candidate.
- The `s5 blobs gc-local` and `s5 blobs verify-local` CLI commands are thin wrappers around these helpers for local stores; higher-level snapshot GC policies are tracked in `s5_fs/TODO.md`.

## Directory Listing (Cursors)
```rust
// First page
let (entries, mut cursor) = fs.list(None, 100).await?;
for (name, kind) in entries {
    println!("{name} {:?}", kind);
}
// Next page (if any)
if let Some(c) = cursor.take() {
    let (more, next) = fs.list(Some(&c), 100).await?;
    // ...
    cursor = next;
}
```

Cursors are base64url-encoded CBOR carrying the last position and kind. For
large directories that have been sharded internally, `list` still presents a
single flat logical namespace aggregated across all shards.

## Versioning & Tombstones
- Each `FileRef` can carry a version chain via `prev`, `first_version`, and
  `version_count`.
- Deleting a file uses tombstones: `FS5::file_delete(path)` creates a
  `FileRefType::Tombstone` head that records when the delete happened and what
  the previous live version was.
- Live reads (`file_get`, `file_exists`) hide tombstones; historical versions
  remain accessible via exported snapshots.
- `merge_from_snapshot` applies last-write-wins (LWW) over timestamps and
  preserves the entire winning version chain, including tombstones.

## Sharding
- Shard metadata lives in the header (`DirHeader.shards: Option<BTreeMap<u8, DirRef>>`).
- Name→bucket routing uses XXH3-64 (fast, non-crypto) for index selection.
- Directories are automatically sharded when their encoded `DirV1` exceeds
  ~64 KiB; shard actors are created and saved behind the scenes.
- Sharding is a storage/layout optimization only: the FS5 API (`file_get`,
  `file_exists`, `list`, `list_at`, `export_snapshot(_at)`) always sees a flat
  logical directory and transparently aggregates data across shards.

## Encryption
- `create_dir(path, enable_encryption = true)` derives/stores per-directory keys and transparently encrypts directory snapshots.
- On load, metadata is decrypted with keys from the context (keys can be inherited/merged from parents).

## Compatibility
- This crate is pre‑v1; on‑disk schema may change between versions.
- Snapshot format: CBOR; see `src/dir.rs` for field indices and types.

## Status

### Platform Support

| Component | Native | WASM |
|-----------|--------|------|
| `DirV1`, `FileRef`, `DirRef` types | Yes | Yes |
| `FS5` API (file ops, listing) | Yes | Yes |
| `DirContext::open_local_root()` | Yes | **No** |
| `DirContextParentLink::LocalFile` | Yes | **No** |
| `DirContextParentLink::RegistryKey` | Yes | Yes |
| `DirContextParentLink::DirHandle` | Yes | Yes |
| `create_snapshot()`, `delete_snapshot()` | Yes | **No** |
| `gc` module | Yes | **No** |
| `snapshots` module | Yes | **No** |

### Parent Link Types

FS5 directories can be rooted in three ways:

1. **`LocalFile`** (native only) - Local filesystem root backed by `root.fs5.cbor`
2. **`RegistryKey`** (all platforms) - Registry-backed directory using Ed25519 public key
3. **`DirHandle`** (all platforms) - Child directory accessed via parent actor handle

For WASM/browser use, use `RegistryKey` with `RemoteRegistry` and `RemoteBlobStore`.

### Known Limitations

- Local file operations (`open_local_root`, snapshots, GC) require native platform
- `current_hash` field in `DirActor` is unused on WASM (no warning suppression)
- Streaming large file uploads not yet optimized for browser

### Key Types

- `FileRef.size` is `u64` (not `Option<u64>`)
- `FileRef.timestamp` is `Option<u32>` (not `ts_seconds`)
- `FileRef.locations` is `Option<Vec<BlobLocation>>` (not `blob_location`)
- `CursorKind::File` and `CursorKind::Directory` (not `Dir`)

## Roadmap
See TODOs and proposed features in `s5_fs/TODO.md`.
