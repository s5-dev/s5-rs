use anyhow::{Result, anyhow};
use bytes::Bytes;
use dashmap::DashMap;
use fuser::{FileAttr, FileType, MountOption};
use fuser_async::FilesystemFUSE;
use fuser_async::{DirEntry, Error, Filesystem};
use lasso::{Key, Spur, ThreadedRodeo};
use s5_core::{BlobStore, Hash};
use s5_fs::FS5;
use s5_fs::dir::{DirRef, FileRef};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, UNIX_EPOCH};
use tracing::{debug, info, warn};

use crate::cache::{CachedDir, DirEntryIter, DirEntryMeta, FsCache, OpenFile};

const BLOCK_SIZE: u32 = 512;

pub struct ReadOnlyS5Fs {
    fs: FS5,
    store: BlobStore,
    inodes: ThreadedRodeo<Spur>,
    cache: FsCache,
    open_files: DashMap<u64, OpenFile>,
    next_fh: AtomicU64,
    read_only: bool,
}

impl ReadOnlyS5Fs {
    pub fn new(fs: FS5, store: BlobStore, read_only: bool) -> anyhow::Result<Self> {
        let inodes = ThreadedRodeo::new();
        // Reserve indices so that '/' maps to ino=1
        inodes.get_or_intern("");
        inodes.get_or_intern("/");
        Ok(Self {
            fs,
            store,
            inodes,
            cache: FsCache::new(),
            open_files: DashMap::new(),
            next_fh: AtomicU64::new(1),
            read_only,
        })
    }

    /// Maps a 64-bit inode number to its corresponding FS5 path string.
    ///
    /// This uses the `ThreadedRodeo` interner to resolve the `Spur` (which is cast from the inode).
    /// Returns `None` if the inode is unknown or invalid.
    fn path_for_ino(&self, ino: u64) -> Option<String> {
        let spur = Spur::try_from_usize(ino as usize)?;
        let res = self.inodes.try_resolve(&spur).map(|s| s.to_string());
        if res.is_none() {
            debug!("path_for_ino({}) -> None (spur={:?})", ino, spur);
        }
        res
    }

    /// Maps an FS5 path string to a stable 64-bit inode number.
    ///
    /// This uses the `ThreadedRodeo` interner to get or intern the path string,
    /// casting the resulting `Spur` index to a `u64`.
    ///
    /// Note: This mapping is stable for the lifetime of the process but not across restarts,
    /// as it depends on the interner's state.
    fn ino_for_path(&self, path: &str) -> u64 {
        let spur = self.inodes.get_or_intern(path);
        let idx = spur.into_usize();
        debug!("ino_for_path({}) -> {} (spur={:?})", path, idx, spur);
        idx as u64
    }

    fn child_path(parent_path: &str, name: &str) -> String {
        if parent_path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent_path, name)
        }
    }

    fn fs_path_from_ino(&self, ino: u64) -> Option<String> {
        // Convert inode path like "/a/b" into FS5 path "a/b"
        self.path_for_ino(ino)
            .map(|p| p.trim_start_matches('/').to_string())
    }

    fn root_attr() -> FileAttr {
        FileAttr {
            ino: 1,
            size: 0,
            blocks: 0,
            atime: UNIX_EPOCH,
            mtime: UNIX_EPOCH,
            ctime: UNIX_EPOCH,
            crtime: UNIX_EPOCH,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    }

    fn make_dir_attr(ino: u64, ts: Option<u32>) -> FileAttr {
        let time = UNIX_EPOCH
            .checked_add(Duration::from_secs(ts.unwrap_or(0) as u64))
            .unwrap();
        FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: time,
            mtime: time,
            ctime: time,
            crtime: time,
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    }

    fn make_file_attr(ino: u64, size: u64, ts: Option<u32>) -> FileAttr {
        let time = UNIX_EPOCH
            .checked_add(Duration::from_secs(ts.unwrap_or(0) as u64))
            .unwrap();
        FileAttr {
            ino,
            size,
            blocks: size.div_ceil(BLOCK_SIZE as u64),
            atime: time,
            mtime: time,
            ctime: time,
            crtime: time,
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    }

    async fn load_root_dir(&self) -> Result<CachedDir> {
        let dir = self.fs.export_merged_snapshot().await?;
        Ok(CachedDir {
            dir,
            keys: BTreeMap::new(),
        })
    }

    async fn load_child_dir(
        &self,
        parent: &CachedDir,
        _dir_ref: &DirRef,
        child_fs_path: &str,
    ) -> Result<CachedDir> {
        let fs_path = child_fs_path.trim_start_matches('/');
        let dir = self.fs.export_merged_snapshot_at(fs_path).await?;
        Ok(CachedDir {
            dir,
            keys: parent.keys.clone(),
        })
    }

    /// Ensures that the directory state for a given inode is loaded into the cache.
    ///
    /// This method walks the path components from the root down to the target inode,
    /// loading and caching any missing directory snapshots along the way.
    ///
    /// It populates:
    /// - `self.cache.dirs`: The `DirV1` state for the directory.
    /// - `self.cache.attrs`: The `FileAttr` for the directory and its children.
    /// - `self.cache.parents`: The parent inode mapping for the directory and its children.
    pub async fn ensure_dir_cached(&self, ino: u64) -> Result<()> {
        // Ensure root cached
        if self.cache.dirs.get(&1).is_none() {
            let root = self.load_root_dir().await?;
            self.cache.dirs.insert(1, root);
            self.cache.attrs.insert(1, Self::root_attr());
        }
        if ino == 1 || self.cache.dirs.get(&ino).is_some() {
            return Ok(());
        }
        // Walk from root to target path, caching along the way
        let path = self
            .path_for_ino(ino)
            .ok_or_else(|| anyhow!("unknown inode"))?;
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let mut cur_path = String::from("/");
        let mut cur_ino = 1u64;
        for seg in segments {
            let child_path = Self::child_path(&cur_path, seg);
            let child_ino = self.ino_for_path(&child_path);
            if self.cache.dirs.get(&child_ino).is_none() {
                let parent = self
                    .cache
                    .dirs
                    .get(&cur_ino)
                    .ok_or_else(|| anyhow!("parent not cached"))?;
                let dr = parent
                    .dir
                    .dirs
                    .get(seg)
                    .cloned()
                    .ok_or_else(|| anyhow!("dir not found"))?;
                let child = self.load_child_dir(&parent, &dr, &child_path).await?;
                drop(parent);
                self.cache.dirs.insert(child_ino, child);
                self.cache.parents.insert(child_ino, cur_ino);
                let attr = Self::make_dir_attr(child_ino, dr.ts_seconds);
                self.cache.attrs.insert(child_ino, attr);
            }
            cur_path = child_path;
            cur_ino = child_ino;
        }
        Ok(())
    }

    async fn get_file_ref(&self, ino: u64) -> Result<FileRef> {
        let fs_path = self
            .fs_path_from_ino(ino)
            .ok_or_else(|| anyhow!("unknown inode"))?;
        let fr = self
            .fs
            .file_get(&fs_path)
            .await
            .ok_or_else(|| anyhow!("file not found"))?;
        Ok(fr)
    }

    /// Refreshes the cached state of a directory by re-fetching its snapshot from FS5.
    ///
    /// This is called when a directory is modified (e.g. file creation) or when
    /// `readdir` detects a cache miss. It invalidates the `dir_entries` cache for
    /// this inode to force a rebuild on the next `readdir`.
    async fn refresh_dir_cache(&self, ino: u64) -> Result<()> {
        let path = self
            .path_for_ino(ino)
            .ok_or_else(|| anyhow!("unknown inode"))?;
        debug!("refresh_dir_cache: ino={} path={}", ino, path);
        let fs_path = path.trim_start_matches('/');
        debug!(
            "refresh_dir_cache: calling export_merged_snapshot_at({})",
            fs_path
        );
        let dir = if fs_path.is_empty() {
            self.fs.export_merged_snapshot().await?
        } else {
            self.fs.export_merged_snapshot_at(fs_path).await?
        };
        self.cache.dirs.insert(
            ino,
            CachedDir {
                dir,
                keys: BTreeMap::new(),
            },
        );
        self.cache.dir_entries.remove(&ino);
        Ok(())
    }
}

#[async_trait::async_trait]
impl Filesystem for ReadOnlyS5Fs {
    type Error = Error;

    async fn inodes(&self) -> Result<BTreeSet<u64>, Self::Error> {
        let mut set = BTreeSet::new();
        for i in 1..(self.inodes.len()) {
            set.insert(i as u64);
        }
        Ok(set)
    }

    async fn lookup(&self, parent: u64, name: &std::ffi::OsStr) -> Result<FileAttr, Self::Error> {
        let name = name.to_string_lossy();
        let parent_path = self.path_for_ino(parent).ok_or(Error::NoFileDir)?;
        self.ensure_dir_cached(parent)
            .await
            .map_err(|_| Error::NoFileDir)?;
        let parent_dir = self.cache.dirs.get(&parent).unwrap();

        if let Some(dr) = parent_dir.dir.dirs.get(name.as_ref()) {
            let child_path = Self::child_path(&parent_path, &name);
            let ino = self.ino_for_path(&child_path);
            let attr = Self::make_dir_attr(ino, dr.ts_seconds);
            self.cache.attrs.insert(ino, attr);
            self.cache.parents.insert(ino, parent);
            Ok(attr)
        } else if let Some(fr) = parent_dir.dir.files.get(name.as_ref()) {
            let child_path = Self::child_path(&parent_path, &name);
            let ino = self.ino_for_path(&child_path);
            let attr = Self::make_file_attr(ino, fr.size, fr.timestamp);
            self.cache.attrs.insert(ino, attr);
            self.cache.parents.insert(ino, parent);
            Ok(attr)
        } else {
            Err(Error::NoFileDir)
        }
    }

    async fn getattr(&self, ino: u64) -> Result<FileAttr, Self::Error> {
        if ino == 1 {
            return Ok(Self::root_attr());
        }
        if let Some(a) = self.cache.attrs.get(&ino) {
            return Ok(*a);
        }
        Err(Error::NoFileDir)
    }

    async fn setattr(&mut self, _ino: u64, _size: Option<u64>) -> Result<FileAttr, Self::Error> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        Err(Error::Unimplemented)
    }

    async fn readdir(
        &self,
        ino: u64,
        offset: u64,
    ) -> Result<Box<dyn Iterator<Item = DirEntry> + Send + Sync + '_>, Self::Error> {
        // TODO(perf): when FS5 prefix/range listing APIs are available,
        // adapt readdir to page large directories via those instead of
        // materializing all entries into a Vec first.
        debug!("Reading directory ino={} offset={}", ino, offset);

        let has_cached_dir = self.cache.dirs.contains_key(&ino);
        debug!("readdir: ino={} cache_hit={}", ino, has_cached_dir);

        // Ensure cache is populated
        if !has_cached_dir {
            self.refresh_dir_cache(ino).await.map_err(|e| {
                warn!("Error when reading directory: {}", e);
                Error::NoFileDir
            })?;
        }

        // Ensure we have the directory state cached for this inode.
        self.ensure_dir_cached(ino)
            .await
            .map_err(|_| Error::NoFileDir)?;
        let cached = self.cache.dirs.get(&ino).ok_or(Error::NoFileDir)?;

        let entries_arc = if let Some(entries) = self.cache.dir_entries.get(&ino) {
            entries.clone()
        } else {
            let parent_inode = self.cache.parents.get(&ino).map(|r| *r).unwrap_or(1);
            let path = self.path_for_ino(ino).unwrap_or_else(|| "/".to_string());

            let mut entries: Vec<DirEntryMeta> =
                Vec::with_capacity(2 + cached.dir.dirs.len() + cached.dir.files.len());
            entries.push(DirEntryMeta {
                inode: ino,
                file_type: FileType::Directory,
                name: ".".into(),
            });
            entries.push(DirEntryMeta {
                inode: parent_inode,
                file_type: FileType::Directory,
                name: "..".into(),
            });

            // Add subdirectories from the cached DirV1
            for (name, dref) in &cached.dir.dirs {
                let child = Self::child_path(&path, name);
                let child_ino = self.ino_for_path(&child);
                self.cache.parents.insert(child_ino, ino);
                let attr = Self::make_dir_attr(child_ino, dref.ts_seconds);
                self.cache.attrs.insert(child_ino, attr);
                entries.push(DirEntryMeta {
                    inode: child_ino,
                    file_type: FileType::Directory,
                    name: name.clone(),
                });
            }

            // Add files from the cached DirV1
            for (name, fr) in &cached.dir.files {
                let child = Self::child_path(&path, name);
                let child_ino = self.ino_for_path(&child);
                self.cache.parents.insert(child_ino, ino);
                let attr = Self::make_file_attr(child_ino, fr.size, fr.timestamp);
                self.cache.attrs.insert(child_ino, attr);
                entries.push(DirEntryMeta {
                    inode: child_ino,
                    file_type: FileType::RegularFile,
                    name: name.clone(),
                });
            }

            let entries_arc = Arc::new(entries);
            self.cache.dir_entries.insert(ino, entries_arc.clone());
            entries_arc
        };

        let start_index = offset as usize;
        let iter = DirEntryIter {
            entries: entries_arc,
            index: start_index,
        };

        Ok(Box::new(iter))
    }

    async fn open(&self, _ino: u64, _flags: i32) -> Result<u64, Self::Error> {
        Ok(0)
    }

    async fn release(&self, _ino: u64, fh: u64) -> Result<(), Self::Error> {
        if self.read_only {
            return Ok(());
        }
        if let Some((_k, of)) = self.open_files.remove(&fh) {
            // Import final blob and write metadata atomically
            let size = of.buf.len() as u64;
            let blob = self
                .store
                .import_bytes(Bytes::from(of.buf))
                .await
                .map_err(|_| Error::NoFileDir)?;
            let fr = FileRef::from(blob);
            // Use fs path (no leading slash)
            let fs_path = of.path.trim_start_matches('/');
            self.fs
                .file_put_sync(fs_path, fr)
                .await
                .map_err(|_| Error::NoFileDir)?;
            self.fs.save().await.map_err(|_| Error::NoFileDir)?;
            // Refresh parent dir cache to reflect new file
            let parent_path = if fs_path.is_empty() {
                "".to_string()
            } else {
                fs_path
                    .rsplit_once('/')
                    .map(|(p, _)| p.to_string())
                    .unwrap_or_else(|| "".to_string())
            };
            let parent_ino = if parent_path.is_empty() {
                1
            } else {
                self.ino_for_path(&format!("/{}", parent_path))
            };
            let _ = self.refresh_dir_cache(parent_ino).await;
            // Update file attr cache
            let file_ino = self.ino_for_path(&of.path);
            let attr = Self::make_file_attr(file_ino, size, None);
            self.cache.attrs.insert(file_ino, attr);
        }
        Ok(())
    }

    async fn read(
        &self,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
    ) -> Result<bytes::Bytes, Self::Error> {
        if offset < 0 {
            return Err(Error::InvalidArgument);
        }
        let fr = self.get_file_ref(ino).await.map_err(|_| Error::NoFileDir)?;
        let max_len = Some(size as u64);
        if let Some(locs) = &fr.locations
            && let Some(s5_core::blob::location::BlobLocation::IdentityRawBinary(v)) =
                locs.iter().find(|l| {
                    matches!(
                        l,
                        s5_core::blob::location::BlobLocation::IdentityRawBinary(_)
                    )
                })
        {
            let data = &v[..];
            let start = (offset as usize).min(data.len());
            let end = (start + size as usize).min(data.len());
            return Ok(Bytes::copy_from_slice(&data[start..end]));
        }
        let hash = Hash::from_bytes(fr.hash);
        let bytes = self
            .store
            .read_as_bytes(hash, offset as u64, max_len)
            .await
            .map_err(|_| Error::NoFileDir)?;
        Ok(bytes)
    }

    async fn write(
        &self,
        _ino: u64,
        fh: u64,
        data: bytes::Bytes,
        offset: i64,
    ) -> Result<u32, Self::Error> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        if offset < 0 {
            return Err(Error::InvalidArgument);
        }
        if let Some(mut entry) = self.open_files.get_mut(&fh) {
            let buf = &mut entry.buf;
            if offset as usize != buf.len() {
                return Err(Error::InvalidArgument);
            }
            buf.extend_from_slice(&data);
            return Ok(data.len() as u32);
        }
        Err(Error::NoFileDir)
    }

    async fn create(
        &mut self,
        parent: u64,
        name: std::ffi::OsString,
        _mode: u32,
        _umask: u32,
        _flags: i32,
    ) -> Result<(FileAttr, u64), Self::Error> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        let name = name.to_string_lossy().to_string();
        let parent_path = self.path_for_ino(parent).ok_or(Error::NoFileDir)?;
        self.ensure_dir_cached(parent)
            .await
            .map_err(|_| Error::NoFileDir)?;
        let file_path = if parent_path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent_path, name)
        };
        let ino = self.ino_for_path(&file_path);
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.open_files.insert(
            fh,
            OpenFile {
                path: file_path.clone(),
                buf: Vec::new(),
            },
        );
        // Provisional attr
        let attr = Self::make_file_attr(ino, 0, None);
        self.cache.attrs.insert(ino, attr);
        self.cache.parents.insert(ino, parent);
        Ok((attr, fh))
    }

    async fn mkdir(
        &mut self,
        parent: u64,
        name: std::ffi::OsString,
    ) -> Result<FileAttr, Self::Error> {
        if self.read_only {
            return Err(Error::ReadOnly);
        }
        let name = name.to_string_lossy().to_string();
        let parent_path = self.path_for_ino(parent).ok_or(Error::NoFileDir)?;
        let fs_parent = parent_path.trim_start_matches('/');
        let child_path = if fs_parent.is_empty() {
            name.clone()
        } else {
            format!("{}/{}", fs_parent, name)
        };
        self.fs
            .create_dir(&child_path, false)
            .await
            .map_err(|_| Error::NoFileDir)?;
        self.fs.save().await.map_err(|_| Error::NoFileDir)?;
        // refresh parent cache and compute child's ino/attr
        let _ = self.refresh_dir_cache(parent).await;
        let full_child = if parent_path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent_path, name)
        };
        let ino = self.ino_for_path(&full_child);
        let attr = Self::make_dir_attr(ino, None);
        self.cache.attrs.insert(ino, attr);
        self.cache.parents.insert(ino, parent);
        Ok(attr)
    }
}

pub async fn mount(
    mountpoint: &Path,
    fs5: FS5,
    store: BlobStore,
    read_only: bool,
    allow_root: bool,
    auto_unmount: bool,
) -> anyhow::Result<()> {
    info!(
        "s5_fuse: mounting FS5 at '{}' (allow_root={}, auto_unmount={})",
        mountpoint.display(),
        allow_root,
        auto_unmount,
    );

    // Log a small preview of the FS5 root so users can confirm
    // we are looking at the expected directory state.
    match fs5.list(None, 16).await {
        Ok((entries, _next)) => {
            info!(
                "s5_fuse: FS5 root has {} entries (showing up to 16)",
                entries.len()
            );
            debug!(
                "s5_fuse: FS5 root sample entries: {:?}",
                entries
                    .iter()
                    .map(|(name, kind)| format!("{} ({:?})", name, kind))
                    .collect::<Vec<_>>()
            );
        }
        Err(e) => {
            info!("s5_fuse: failed to list FS5 root entries: {e}");
        }
    }

    let fs = ReadOnlyS5Fs::new(fs5, store, read_only)?;
    // Preload root
    fs.ensure_dir_cached(1).await.ok();

    let mut options = vec![MountOption::FSName("s5".to_string())];
    if allow_root {
        options.push(MountOption::AllowRoot);
    }
    if auto_unmount {
        options.push(MountOption::AutoUnmount);
    }

    let fuse = FilesystemFUSE::new(fs);
    // Always mount async for performance
    options.push(MountOption::Async);

    info!(
        "s5_fuse: calling fuser::spawn_mount2 on '{}'",
        mountpoint.display()
    );
    let mount_result = fuser::spawn_mount2(fuse, mountpoint, &options);

    match mount_result {
        Ok(_mount) => {
            info!("s5_fuse: mount established, waiting for ctrl-c");
            if let Err(e) = tokio::signal::ctrl_c().await {
                info!("s5_fuse: ctrl-c waiter failed: {e}");
            }
            info!("s5_fuse: ctrl-c received, shutting down");
            Ok(())
        }
        Err(e) => {
            info!("s5_fuse: failed to mount FUSE filesystem: {e}");
            Err(e.into())
        }
    }
}
