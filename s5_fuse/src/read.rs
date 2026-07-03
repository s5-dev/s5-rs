//! Read-only FUSE adapter over an arbitrary [`ReadableLayer`].
//!
//! Implements `fuse3::path::PathFilesystem` so each kernel callback
//! arrives as `(parent, name)` `OsStr` pairs. `fuse3::path::Session`
//! wraps the impl in an `InodePathBridge` that manages inode allocation
//! internally, so no path↔inode table is needed here.
//!
//! Lookups, readdirs, and reads all touch the prolly tree on demand
//! (no eager mount-time walk). See [`crate::path`] for the shared
//! resolution helpers.
//!
//! ## Layered shape
//!
//! The adapter holds two pieces:
//! - `base: Arc<dyn ReadableLayer>` — what to read from. Today's mount
//!   entry points wrap a single [`Snapshot`] in a [`MergedView`] of
//!   length 1; multi-layer composition (peer tips, frozen historical
//!   snapshots) goes through the same slot via
//!   [`ReadOnlyFs::with_layers`].
//! - `pipeline: Arc<Pipeline>` — encryption + per-blob ops. Used by
//!   [`PathFilesystem::read`] to materialise file bytes from the entries
//!   served by `base`. The pipeline carries the on-wire context, so
//!   when merging multiple layers the caller picks one whose context
//!   covers the union of leaves (typically the highest-priority layer).

use std::ffi::{OsStr, OsString};
use std::sync::Arc;

use fuse3::path::prelude::*;
use fuse3::{Errno, Result as FuseResult};
use futures_util::stream;
use s5_fs_v2::layer::ReadableLayer;
use s5_fs_v2::merge::MergedView;
use s5_fs_v2::pipeline::Pipeline;
use s5_fs_v2::snapshot::Snapshot;
use tracing::warn;

use crate::attr::{BLOCK_SIZE, ENTRY_TTL, dir_attr, file_attr};
use crate::path::{
    ResolvedEntry, join, list_children, list_children_with_entries, resolve, snapshot_key,
};

/// Read-only FUSE adapter over a [`ReadableLayer`] + a [`Pipeline`] for
/// materialising file bytes. See module-level docs for the layered shape.
pub struct ReadOnlyFs {
    base: Arc<dyn ReadableLayer>,
    pipeline: Arc<Pipeline>,
}

impl ReadOnlyFs {
    /// Mount a single snapshot. Internally wraps the snapshot in a
    /// length-1 [`MergedView`] so the architectural shape (FUSE over
    /// merged layers) is exercised even in the single-snapshot case.
    pub fn new(snapshot: Snapshot) -> Self {
        let pipeline = Arc::new(snapshot.as_pipeline());
        let layer: Arc<dyn ReadableLayer> = Arc::new(snapshot);
        let base: Arc<dyn ReadableLayer> = Arc::new(MergedView::new(vec![layer]));
        Self { base, pipeline }
    }

    /// Mount an explicit ordered stack of layers (index 0 = highest
    /// priority). The `pipeline` materialises file bytes for every
    /// served entry, regardless of which layer it came from — pick one
    /// whose context covers the union of leaves (typically the
    /// highest-priority layer's pipeline).
    pub fn with_layers(layers: Vec<Arc<dyn ReadableLayer>>, pipeline: Arc<Pipeline>) -> Self {
        let base: Arc<dyn ReadableLayer> = Arc::new(MergedView::new(layers));
        Self { base, pipeline }
    }
}

impl PathFilesystem for ReadOnlyFs {
    async fn init(&self, _req: Request) -> FuseResult<ReplyInit> {
        Ok(ReplyInit {
            // 1 MiB write block — pertinent only when (later) writes are
            // wired; harmless for read-only mounts.
            max_write: std::num::NonZeroU32::new(1024 * 1024).unwrap(),
        })
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, parent: &OsStr, name: &OsStr) -> FuseResult<ReplyEntry> {
        let path = join(parent, name);
        let key = snapshot_key(&path);
        match resolve(self.base.as_ref(), &key).await? {
            ResolvedEntry::File(entry) => Ok(ReplyEntry {
                ttl: ENTRY_TTL,
                attr: file_attr(&entry),
            }),
            ResolvedEntry::Directory => Ok(ReplyEntry {
                ttl: ENTRY_TTL,
                attr: dir_attr(),
            }),
            ResolvedEntry::Tombstone => Err(Errno::from(libc::ENOENT)),
        }
    }

    async fn getattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        _flags: u32,
    ) -> FuseResult<ReplyAttr> {
        let path = path.ok_or_else(|| Errno::from(libc::ENOENT))?;
        let key = snapshot_key(path);
        match resolve(self.base.as_ref(), &key).await? {
            ResolvedEntry::File(entry) => Ok(ReplyAttr {
                ttl: ENTRY_TTL,
                attr: file_attr(&entry),
            }),
            ResolvedEntry::Directory => Ok(ReplyAttr {
                ttl: ENTRY_TTL,
                attr: dir_attr(),
            }),
            ResolvedEntry::Tombstone => Err(Errno::from(libc::ENOENT)),
        }
    }

    /// Read file bytes for `path` over `[offset, offset+size)`.
    //
    // TODO(perf): two kernel-assisted fast paths bypass this callback entirely
    // and are the path to native-speed reads:
    //   • FUSE passthrough (mainline Linux 6.9): for an *unencrypted*,
    //     single-blob leaf (one `LocalStore` file on XFS), hand the kernel that
    //     backing fd at `open` so reads go straight to XFS — never entering the
    //     daemon. Gate strictly on: identity pipeline (no decrypt/decompress)
    //     AND a single `Structural::Leaf` whose blob maps 1:1 to a backing
    //     file. Does not apply to encrypted or multi-chunk content out of the
    //     box (no plaintext backing file to pass through) — but a decrypted
    //     plaintext cache file (materialise once on first open, hand the kernel
    //     *that* fd) extends passthrough to encrypted/chunked content too,
    //     trading disk for native data-path throughput. Blocked on `fuse3`
    //     exposing passthrough.
    //   • Page-cache retention (FOPEN_KEEP_CACHE): content is immutable per
    //     content hash, so the kernel may keep cached pages across opens of the
    //     same inode. Add an `open` impl returning the keep-cache flag for
    //     committed (hash-stable) entries.
    async fn read(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: u64,
        offset: u64,
        size: u32,
    ) -> FuseResult<ReplyData> {
        let path = path.ok_or_else(|| Errno::from(libc::ENOENT))?;
        let key = snapshot_key(path);
        let entry = self
            .base
            .get(&key)
            .await
            .map_err(|err| {
                warn!(key, error = %err, "base.get for read failed");
                Errno::from(libc::EIO)
            })?
            .ok_or_else(|| Errno::from(libc::ENOENT))?;
        // TODO(perf): CRITICAL — `export_bytes` materialises the *entire* file
        // (fetch → decrypt → decompress every chunk) on every `read` call, then
        // discards all but `[offset, offset+size)`. A sequential read of an
        // N-byte file is therefore O(N²): each ~128 KiB kernel read redoes the
        // whole file. Make this range-aware — descend the prolly tree to only
        // the leaf chunks overlapping `[offset, offset+size)` and export those
        // (the CDC chunk index already carries per-chunk offsets/sizes). Pair
        // with a small per-fh decoded-chunk LRU so adjacent sequential reads
        // don't re-fetch the shared boundary chunk. This is the single biggest
        // read-path win and gates any real-world use as a live filesystem.
        //
        // TODO(mount/perf): the ranged primitive ALREADY EXISTS —
        // `Pipeline::export_byte_chunks_at(&entry, &wanted, cache)`
        // (s5_fs_v2/src/snapshot.rs, proven by
        // `export_byte_chunks_at_fetches_only_wanted`). Wiring it here plus
        // the per-fh LRU is the whole fix; no new machinery needed.
        let bytes = self.pipeline.export_bytes(&entry).await.map_err(|err| {
            warn!(key, error = %err, "export_bytes failed");
            Errno::from(libc::EIO)
        })?;
        let start = (offset as usize).min(bytes.len());
        let end = start.saturating_add(size as usize).min(bytes.len());
        Ok(ReplyData {
            data: bytes.slice(start..end),
        })
    }

    async fn opendir(&self, _req: Request, path: &OsStr, _flags: u32) -> FuseResult<ReplyOpen> {
        // Stateless directory I/O — no fh, just verify the path exists.
        let key = snapshot_key(path);
        match resolve(self.base.as_ref(), &key).await? {
            ResolvedEntry::Directory => Ok(ReplyOpen { fh: 0, flags: 0 }),
            _ => Err(Errno::from(libc::ENOTDIR)),
        }
    }

    async fn readdir<'a>(
        &'a self,
        _req: Request,
        path: &'a OsStr,
        _fh: u64,
        offset: i64,
    ) -> FuseResult<
        ReplyDirectory<impl futures_util::Stream<Item = FuseResult<DirectoryEntry>> + Send + 'a>,
    > {
        let key = snapshot_key(path);
        // TODO(perf): this re-scans and re-materialises *all* children on every
        // `readdir`/`readdirplus` batch, then `skip`s `offset`. The kernel pages
        // a large directory in many batches, so listing a dir with D entries is
        // O(D²). Hold a per-fh resumable scan cursor (the prolly-tree scan is
        // already an ordered stream — keep it open across batches keyed by `fh`)
        // so each batch is O(batch), not O(D). readdirplus itself is correctly
        // implemented; this is purely the pagination shape.
        let children = list_children(self.base.as_ref(), &key).await?;

        let mut entries: Vec<DirectoryEntry> = Vec::with_capacity(children.len() + 2);
        entries.push(DirectoryEntry {
            kind: FileType::Directory,
            name: OsString::from("."),
            offset: 1,
        });
        entries.push(DirectoryEntry {
            kind: FileType::Directory,
            name: OsString::from(".."),
            offset: 2,
        });
        for (idx, (child_name, kind)) in children.into_iter().enumerate() {
            entries.push(DirectoryEntry {
                kind,
                name: OsString::from(child_name),
                offset: (idx as i64) + 3,
            });
        }
        let skip = (offset.max(0) as usize).min(entries.len());
        let yielded = entries.into_iter().skip(skip).map(Ok).collect::<Vec<_>>();

        Ok(ReplyDirectory {
            entries: stream::iter(yielded),
        })
    }

    /// Combined `readdir + lookup` — modern Linux kernels (>= 3.9 with
    /// `FUSE_DO_READDIRPLUS`) call this in preference to `readdir` so a
    /// single round trip yields names + attrs. The default trait impl
    /// returns ENOSYS, which the kernel _should_ fall back from to
    /// `readdir` — but in practice some kernels surface the ENOSYS
    /// directly to userspace as `ls: Function not implemented`.
    /// Implementing it explicitly is the safe path.
    async fn readdirplus<'a>(
        &'a self,
        _req: Request,
        path: &'a OsStr,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> FuseResult<
        ReplyDirectoryPlus<
            impl futures_util::Stream<Item = FuseResult<DirectoryEntryPlus>> + Send + 'a,
        >,
    > {
        let key = snapshot_key(path);
        let children = list_children_with_entries(self.base.as_ref(), &key).await?;

        let self_attr = dir_attr();
        let parent_attr = dir_attr();
        let mut entries: Vec<DirectoryEntryPlus> = Vec::with_capacity(children.len() + 2);
        entries.push(DirectoryEntryPlus {
            kind: FileType::Directory,
            name: OsString::from("."),
            offset: 1,
            attr: self_attr,
            entry_ttl: ENTRY_TTL,
            attr_ttl: ENTRY_TTL,
        });
        entries.push(DirectoryEntryPlus {
            kind: FileType::Directory,
            name: OsString::from(".."),
            offset: 2,
            attr: parent_attr,
            entry_ttl: ENTRY_TTL,
            attr_ttl: ENTRY_TTL,
        });
        for (idx, (child_name, kind, entry)) in children.into_iter().enumerate() {
            let attr = match (&kind, entry.as_ref()) {
                (FileType::RegularFile, Some(e)) => file_attr(e),
                _ => dir_attr(),
            };
            entries.push(DirectoryEntryPlus {
                kind,
                name: OsString::from(child_name),
                offset: (idx as i64) + 3,
                attr,
                entry_ttl: ENTRY_TTL,
                attr_ttl: ENTRY_TTL,
            });
        }
        let skip = (offset as usize).min(entries.len());
        let yielded = entries.into_iter().skip(skip).map(Ok).collect::<Vec<_>>();
        Ok(ReplyDirectoryPlus {
            entries: stream::iter(yielded),
        })
    }

    async fn statfs(&self, _req: Request, _path: &OsStr) -> FuseResult<ReplyStatFs> {
        Ok(ReplyStatFs {
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files: 0,
            ffree: 0,
            bsize: BLOCK_SIZE,
            namelen: 4096,
            frsize: BLOCK_SIZE,
        })
    }

    async fn access(&self, _req: Request, _path: &OsStr, _mask: u32) -> FuseResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s5_core::blob::BlobStore;
    use s5_fs_local::backup;
    use s5_fs_local::{BackupConfig, WalkBuilder};
    use s5_fs_v2::node::{
        BlobPipeline, CompressionStrategy, EncryptionStrategy, PaddingStrategy, TraversalContext,
    };
    use s5_fs_v2::snapshot::{KEY_SLOT_LEAF, KEY_SLOT_NODE};
    use s5_store_local::{LocalStore, LocalStoreConfig};
    use std::collections::BTreeMap;

    /// Builds a v2 snapshot containing a small tree, then verifies the
    /// FUSE adapter reports the same shape via the shared path helpers
    /// and `Snapshot::export_bytes` (without ever standing up a real
    /// mountpoint — the trait would invoke the same calls).
    #[tokio::test]
    async fn read_only_serves_snapshot_lazily() -> anyhow::Result<()> {
        let src = tempfile::tempdir()?;
        std::fs::create_dir_all(src.path().join("dir"))?;
        std::fs::write(src.path().join("readme.txt"), b"hello fuse")?;
        std::fs::write(src.path().join("dir/inner.bin"), vec![0xCD; 4096])?;

        let store_dir = tempfile::tempdir()?;
        let store = BlobStore::new(LocalStore::create(LocalStoreConfig {
            base_path: store_dir.path().to_string_lossy().into_owned(),
        }));
        let read_store: Arc<dyn s5_core::BlobsRead> = Arc::new(store.clone());

        let mut keys = BTreeMap::new();
        keys.insert(KEY_SLOT_LEAF, [42u8; 32]);
        keys.insert(KEY_SLOT_NODE, [43u8; 32]);
        let pad = Some(PaddingStrategy { block_size: 4096 });
        let leaf_pipeline = BlobPipeline {
            compression: Some(CompressionStrategy::Zstd),
            padding: pad.clone(),
            encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_LEAF)),
            skip_when_unhelpful: None,
        };
        let node_pipeline = BlobPipeline {
            compression: Some(CompressionStrategy::Zstd),
            padding: pad,
            encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_NODE)),
            skip_when_unhelpful: None,
        };
        let ctx = TraversalContext {
            keys: Some(keys),
            leaf: Some(leaf_pipeline),
            node: Some(node_pipeline),
            chunking: None,
        };
        let snapshot = Snapshot::empty(Arc::clone(&read_store), ctx);
        let result = backup(
            src.path(),
            &snapshot,
            &store,
            &store,
            Arc::clone(&read_store),
            &BackupConfig::default(),
            WalkBuilder::new(src.path()),
            None,
            None,
        )
        .await?;
        let (snapshot, _stats) = result
            .snapshot
            .ok_or_else(|| anyhow::anyhow!("backup produced no snapshot"))?;

        let fs = ReadOnlyFs::new(snapshot);

        let children = list_children(fs.base.as_ref(), "")
            .await
            .map_err(|e| anyhow::anyhow!("list_children: {e:?}"))?;
        let names: Vec<&String> = children.iter().map(|(n, _)| n).collect();
        assert!(
            names.iter().any(|n| n.as_str() == "readme.txt"),
            "got {names:?}"
        );
        assert!(names.iter().any(|n| n.as_str() == "dir"), "got {names:?}");

        let key = snapshot_key(OsStr::new("/readme.txt"));
        match resolve(fs.base.as_ref(), &key)
            .await
            .map_err(|e| anyhow::anyhow!("resolve: {e:?}"))?
        {
            ResolvedEntry::File(entry) => {
                assert_eq!(entry.content.as_ref().unwrap().size, 10);
            }
            _ => panic!("expected ResolvedEntry::File for readme.txt"),
        }

        let key = snapshot_key(OsStr::new("/dir"));
        match resolve(fs.base.as_ref(), &key)
            .await
            .map_err(|e| anyhow::anyhow!("resolve: {e:?}"))?
        {
            ResolvedEntry::Directory => {}
            _ => panic!("expected ResolvedEntry::Directory for `dir`"),
        }

        let children = list_children(fs.base.as_ref(), "dir")
            .await
            .map_err(|e| anyhow::anyhow!("list_children dir: {e:?}"))?;
        let names: Vec<&String> = children.iter().map(|(n, _)| n).collect();
        assert_eq!(names, vec!["inner.bin"], "expected only inner.bin");

        let entry = fs
            .base
            .get("dir/inner.bin")
            .await?
            .expect("inner.bin entry missing");
        let bytes = fs.pipeline.export_bytes(&entry).await?;
        assert_eq!(bytes.len(), 4096);
        assert!(bytes.iter().all(|&b| b == 0xCD));

        Ok(())
    }

    /// Two-snapshot `MergedView` mounted through `ReadOnlyFs::with_layers`:
    /// verifies the union of files is visible and the priority rule
    /// (lower-index layer wins on key collision) holds at the FUSE
    /// adapter boundary. This is the architectural shape behind live
    /// multi-peer mounts (`docs/reference/snapshot-publication.md`).
    ///
    /// Both snapshots share encryption keys + node cache (each is built
    /// over the same `BlobStore`) so the single `Pipeline` we hand to
    /// `with_layers` can decrypt entries from either layer.
    #[tokio::test]
    async fn merged_layers_serve_union_with_priority() -> anyhow::Result<()> {
        // ---- two source trees (overlapping path on purpose) -----------------
        let src_a = tempfile::tempdir()?;
        std::fs::write(src_a.path().join("only_a.txt"), b"alpha-only")?;
        std::fs::write(src_a.path().join("collide.txt"), b"AAA from layer a")?;

        let src_b = tempfile::tempdir()?;
        std::fs::write(src_b.path().join("only_b.txt"), b"bravo-only")?;
        std::fs::write(src_b.path().join("collide.txt"), b"BBB from layer b")?;

        // ---- shared store + matching contexts so one Pipeline serves both ---
        let store_dir = tempfile::tempdir()?;
        let store = BlobStore::new(LocalStore::create(LocalStoreConfig {
            base_path: store_dir.path().to_string_lossy().into_owned(),
        }));
        let read_store: Arc<dyn s5_core::BlobsRead> = Arc::new(store.clone());

        let mut keys = BTreeMap::new();
        keys.insert(KEY_SLOT_LEAF, [42u8; 32]);
        keys.insert(KEY_SLOT_NODE, [43u8; 32]);
        let pad = Some(PaddingStrategy { block_size: 4096 });
        let leaf_pipeline = BlobPipeline {
            compression: Some(CompressionStrategy::Zstd),
            padding: pad.clone(),
            encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_LEAF)),
            skip_when_unhelpful: None,
        };
        let node_pipeline = BlobPipeline {
            compression: Some(CompressionStrategy::Zstd),
            padding: pad,
            encryption: Some((EncryptionStrategy::DeterministicChaCha20, KEY_SLOT_NODE)),
            skip_when_unhelpful: None,
        };
        let ctx = TraversalContext {
            keys: Some(keys),
            leaf: Some(leaf_pipeline),
            node: Some(node_pipeline),
            chunking: None,
        };
        let make_snap = async |src: &std::path::Path| -> anyhow::Result<Snapshot> {
            let snap = Snapshot::empty(Arc::clone(&read_store), ctx.clone());
            let result = backup(
                src,
                &snap,
                &store,
                &store,
                Arc::clone(&read_store),
                &BackupConfig::default(),
                WalkBuilder::new(src),
                None,
                None,
            )
            .await?;
            Ok(result
                .snapshot
                .ok_or_else(|| anyhow::anyhow!("backup produced no snapshot"))?
                .0)
        };
        let snap_a = make_snap(src_a.path()).await?;
        let snap_b = make_snap(src_b.path()).await?;

        // Layer 0 (highest priority) = a; layer 1 = b.
        let pipeline = Arc::new(snap_a.as_pipeline());
        let layers: Vec<Arc<dyn ReadableLayer>> = vec![Arc::new(snap_a), Arc::new(snap_b)];
        let fs = ReadOnlyFs::with_layers(layers, Arc::clone(&pipeline));

        // Union of files at root.
        let children = list_children(fs.base.as_ref(), "")
            .await
            .map_err(|e| anyhow::anyhow!("list_children: {e:?}"))?;
        let names: Vec<String> = children.iter().map(|(n, _)| n.clone()).collect();
        for expected in ["only_a.txt", "only_b.txt", "collide.txt"] {
            assert!(
                names.iter().any(|n| n == expected),
                "expected {expected} in merged readdir, got {names:?}",
            );
        }

        // Layer-a-only file resolves and reads back the right bytes.
        let entry = fs
            .base
            .get("only_a.txt")
            .await?
            .expect("only_a.txt missing");
        let bytes = fs.pipeline.export_bytes(&entry).await?;
        assert_eq!(&bytes[..], b"alpha-only");

        // Layer-b-only file resolves through the fall-through path.
        let entry = fs
            .base
            .get("only_b.txt")
            .await?
            .expect("only_b.txt missing");
        let bytes = fs.pipeline.export_bytes(&entry).await?;
        assert_eq!(&bytes[..], b"bravo-only");

        // On key collision, layer 0 (a) wins.
        let entry = fs
            .base
            .get("collide.txt")
            .await?
            .expect("collide.txt missing");
        let bytes = fs.pipeline.export_bytes(&entry).await?;
        assert_eq!(
            &bytes[..],
            b"AAA from layer a",
            "merge priority broken — layer 1 leaked through"
        );

        Ok(())
    }
}
