# s5_fuse (FS5 FUSE driver)

A simple, efficient FUSE driver for the FS5 filesystem. It provides read and basic write support by bridging kernel VFS calls to FS5.

## Current Status
- Read: directory listing via `FS5::list_at`, file reads from local blob store; inline blobs served directly from metadata.
- Write: atomic whole-file writes on close (create → sequential `write` → `release` imports blob and updates FS5 metadata). `mkdir` supported.
- Registry-backed subdirs: listing and snapshot loading supported via FS5.

## Missing Features
- Random writes (non-sequential offsets), append, truncate, preallocation.
- `rename`, `unlink`, `rmdir`, `symlink`, `link`.
- `chmod`, `chown`, `utimens`, and other metadata changes.
- `fsync`, `flush`, open flags like `O_TRUNC`/`O_APPEND` (ignored or rejected).
- Proper uid/gid/perms; all attrs are placeholders and not persisted.
- Fine-grained cache invalidation and TTLs; in-memory inode/attr cache is minimal.
- Large directory pagination (readdir uses a fixed limit and ignores cursors).
- Concurrency around the same path: last-writer-wins; no file-level locks.
- Registry-backed writes require a signing key to publish; otherwise subdir state is only visible via the running actor (not published to the registry on disk).

## Suggested Improvements
- Writes
  - Support `O_TRUNC`, append, and random writes via a temp-file writeback layer.
  - Implement `fsync`/`flush` to persist early.
- Directory ops
  - Add `rename`, `unlink`, `rmdir` using FS5 batch mutations.
  - Use real pagination for `readdir` by threading FS5 cursors between FUSE offsets.
- Caching
  - Add LRU for Dir snapshots and attrs; invalidate parent on write.
  - Optional TTLs and a background refresher for registry-backed dirs.
- FS5 API (nice-to-have)
  - `FS5::read_bytes(path, offset, len)` to centralize read IO.
  - `FS5::stat_at(path)` to fetch size/timestamps without loading snapshots.
  - `FS5::list_at` is in place; expose a cursor→offset mapping helper.
- Permissions & IDs
  - Map uid/gid from the running process or a config; optionally read-only mounts.
- Performance
  - Readahead on large reads; simple read cache by blob range.
  - Batch `file_put_sync` calls during heavy writes before `save`.
- Robustness
  - Better error mapping; distinguish IO vs NotFound vs PermissionDenied.
  - Graceful handling of partial writes and ENOSPC conditions.
- Mount UX (rclone-inspired)
  - Add `--daemon` / `--daemon-wait` flags for background mounting with a simple Linux-only re-exec model.
  - Support `--volname` and thread it into `MountOption::FSName` so tools show a meaningful volume label.
  - Expose `--uid`, `--gid`, `--file-perms`, `--dir-perms`, and `--umask` flags to control FUSE attrs for multi-user setups and systemd mounts.
  - Add `--dir-cache-time` / `--attr-timeout` flags to tune FS5/FUSE directory and attribute cache behavior.
  - Provide a signal (e.g. `SIGUSR1`) and/or `s5 fuse flush` helper to drop FUSE caches and re-snapshot FS5.
  - Support `--snapshot <NAME|HASH>` to mount a specific FS5 snapshot as strictly read-only.
  - Document example systemd units (`s5-fuse@.service`, `.mount`/`.automount`) for running `s5 fuse` as a service.
 
## Usage

This driver is normally used indirectly via the `s5` CLI:

```bash
s5 mount /mnt/s5-docs --root /path/to/fs5-root --allow-root --auto-unmount
```

- `--root` points to a directory containing `root.fs5.cbor` and blob data.
- Mount runs async by default for better throughput.

## Notes
- Atomic, whole-file writes: size and metadata update on close (`release`).
- Registry-backed dirs are listed via FS5 and can be read; publishing changes to the registry requires a signing key configured in FS5 context.
- This driver aims for simplicity over completeness; please file issues for missing ops you need.
