use anyhow::{Context, anyhow};
use futures::{StreamExt, TryStreamExt};
use ignore::{DirEntry, WalkBuilder};
use s5_core::blob::BlobStore;
use s5_fs::{FS5, FileRef};
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{os::unix::fs::MetadataExt, path::PathBuf};

/// Progress counters for import operations.
#[derive(Default)]
pub struct ImportProgress {
    /// Number of files processed so far
    pub files_processed: AtomicU64,
    /// Number of bytes processed so far
    pub bytes_processed: AtomicU64,
}

/// Import mode determines how files are stored.
#[derive(Clone)]
pub enum ImportMode {
    /// Store file content in blob store (default)
    BlobStore(BlobStore),
    /// Only hash and index files, don't store content
    IndexOnly,
}

/// Imports files and directories from the local file system into an FS5 directory.
pub struct LocalFileSystemImporter {
    /// A semaphore to limit the number of concurrent file hashing operations.
    // rate_limiter: Arc<Semaphore>,
    /// The FS5 directory state where file references will be stored.
    fs: FS5,
    /// Import mode: blob store or index-only
    mode: ImportMode,
    max_concurrent_ops: usize,
    /// When true, keys are relative to the imported base path.
    /// When false, keys use the full absolute path (minus leading slash).
    use_base_relative_keys: bool,
    ignore: bool,
    ignore_vcs: bool,
    check_cachedir_tag: bool,
    /// When true, skip incremental FS5 metadata checks and always import
    /// files. This is useful for first-pass imports or when the caller
    /// knows the target tree is fresh and wants to avoid a `file_get`
    /// round-trip per file.
    always_import: bool,
    /// Optional progress tracking
    progress: Option<Arc<ImportProgress>>,
}

impl LocalFileSystemImporter {
    /// Enables or disables the "always import" fast path.
    ///
    /// When enabled, the importer skips per-file FS5 metadata checks
    /// and unconditionally imports each file it encounters.
    pub fn set_always_import(&mut self, always_import: bool) {
        self.always_import = always_import;
    }

    /// Creates a new `LocalImporter`.
    ///
    /// # Arguments
    ///
    /// * `state_path` - The path to the FS5 directory state file.
    /// * `blob_store` - The blob store for storing file content.
    /// * `base_path` - The root directory to import from. Paths in the FS5 directory
    ///   will be relative to this `base_path` when `use_base_relative_keys` is true.
    /// * `max_concurrent_ops` - The maximum number of files to hash concurrently.
    /// * `use_base_relative_keys` - Whether to strip the imported base path from
    ///   keys before writing into FS5.
    pub fn create(
        fs: FS5,
        blob_store: BlobStore,
        max_concurrent_ops: usize,
        use_base_relative_keys: bool,
        ignore: bool,
        ignore_vcs: bool,
        check_cachedir_tag: bool,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            max_concurrent_ops,
            fs,
            mode: ImportMode::BlobStore(blob_store),
            use_base_relative_keys,
            ignore,
            ignore_vcs,
            check_cachedir_tag,
            always_import: false,
            progress: None,
        })
    }

    /// Creates a new `LocalImporter` in index-only mode.
    ///
    /// Files are hashed and indexed in FS5, but not copied to any blob store.
    /// Useful for cataloging files or finding duplicates without storage overhead.
    pub fn create_index_only(
        fs: FS5,
        max_concurrent_ops: usize,
        use_base_relative_keys: bool,
        ignore: bool,
        ignore_vcs: bool,
        check_cachedir_tag: bool,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            max_concurrent_ops,
            fs,
            mode: ImportMode::IndexOnly,
            use_base_relative_keys,
            ignore,
            ignore_vcs,
            check_cachedir_tag,
            always_import: false,
            progress: None,
        })
    }

    /// Sets a progress tracker for this importer.
    pub fn set_progress(&mut self, progress: Arc<ImportProgress>) {
        self.progress = Some(progress);
    }

    /// Recursively imports files from the configured `base_path`.
    ///
    /// This function walks the directory tree starting from `base_path`, processing
    /// each file concurrently. It checks each file's metadata (size and modification time)
    /// against the stored version in the FS5 directory to determine if an update is needed.
    ///
    pub async fn import_path(&self, path: PathBuf) -> anyhow::Result<()> {
        let path = path
            .canonicalize()
            .with_context(|| format!("Failed to canonicalize base path: {:?}", path))?;

        log::info!("Starting import from {:?}", path);

        let mut builder = WalkBuilder::new(&path);
        // Start with all standard filters disabled, then enable only what we want.
        builder.standard_filters(false);
        // Always respect hidden files and parent ignore discovery by default,
        // similar to fd's behavior where -I only affects ignore files.
        builder.hidden(true).parents(true);

        if self.ignore {
            builder.ignore(true);
            // Support fd-style .fdignore files when ignore rules are enabled.
            builder.add_custom_ignore_filename(".fdignore");
        }

        if self.ignore_vcs {
            builder.git_ignore(true).git_global(true).git_exclude(true);
        }

        if self.check_cachedir_tag {
            builder.filter_entry(|entry| {
                if !entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                    return true;
                }
                let tag_path = entry.path().join("CACHEDIR.TAG");
                if let Ok(mut file) = std::fs::File::open(&tag_path) {
                    let mut buf = [0u8; 43];
                    if std::io::Read::read_exact(&mut file, &mut buf).is_ok()
                        && &buf == b"Signature: 8a477f597d28d172789f06886806bc55"
                    {
                        return false;
                    }
                }
                true
            });
        }

        let walker = builder.build();
        let base_path = path.clone();

        // Use a stream to process directory entries concurrently.
        futures::stream::iter(walker.filter_map(Result::ok))
            .map(move |entry| {
                let base_path = base_path.clone();
                async move {
                    if !entry.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
                        return Ok(());
                    }
                    self.process_entry(entry, &base_path).await
                }
            })
            .buffer_unordered(self.max_concurrent_ops) // Concurrency level
            .try_collect::<()>()
            .await?;

        log::info!("Finished import from {:?}", path);
        Ok(())
    }

    /// Processes a single file entry from the directory walk.
    ///
    /// It checks if the file needs to be updated and, if so, imports it into the
    /// blob store and updates its reference in the FS5 directory.
    async fn process_entry(
        &self,
        entry: DirEntry,
        base_path: &std::path::Path,
    ) -> anyhow::Result<()> {
        let path = entry.path();
        let meta = entry
            .metadata()
            .with_context(|| format!("Failed to get metadata for {:?}", path))?;

        // Compute the key to use in FS5.
        let key = if self.use_base_relative_keys {
            // Path relative to the imported base path.
            let relative_path = path.strip_prefix(base_path).unwrap_or(path);
            relative_path
                .to_str()
                .ok_or_else(|| anyhow!("Path is not valid UTF-8: {:?}", path))?
                .to_string()
        } else {
            // Use the full absolute path, minus any leading slash.
            let path_str = path
                .to_str()
                .ok_or_else(|| anyhow!("Path is not valid UTF-8: {:?}", path))?;
            path_str.trim_start_matches('/').to_string()
        };

        // Optional fast-path: for initial imports or cases where the caller
        // knows the destination tree is fresh, we can skip the per-file
        // metadata comparison and always import.
        // TODO(perf): for incremental imports, consider a cheap
        // directory-level marker or "last successful import" timestamp
        // to skip FS5 lookups when entire subtrees are obviously older
        // than the last snapshot.
        let should_update = if self.always_import {
            log::debug!("Importing (always) file: {}", key);
            true
        } else {
            let current_file_ref = self.fs.file_get(&key).await;

            match current_file_ref {
                Some(current) => {
                    let size_changed = meta.len() != current.size;
                    let time_changed = current.timestamp != Some(meta.mtime().try_into()?)
                        || current.timestamp_subsec_nanos != Some(meta.mtime_nsec().try_into()?);

                    if size_changed {
                        log::debug!("Updating {}: size changed", key);
                    } else if time_changed {
                        log::debug!("Updating {}: last-modified changed", key);
                    }
                    size_changed || time_changed
                }
                None => {
                    log::debug!("Importing new file: {}", key);
                    true
                }
            }
        };

        if !should_update {
            log::trace!("Skipping unchanged file: {}", key);
            return Ok(());
        }

        log::info!("Importing file: {}", key);

        let file_ref = match &self.mode {
            ImportMode::BlobStore(blob_store) => {
                // Normal mode: import file into blob store
                let blob_id = blob_store
                    .import_file(path.to_path_buf(), |_processed| Ok(()))
                    .await
                    .with_context(|| {
                        format!("Failed to import file into blob store: {:?}", path)
                    })?;

                let mut file_ref: FileRef = blob_id.into();
                file_ref.timestamp = Some(meta.mtime().try_into()?);
                file_ref.timestamp_subsec_nanos = Some(meta.mtime_nsec().try_into()?);
                file_ref
            }
            ImportMode::IndexOnly => {
                // Index-only mode: hash file without copying to blob store
                let hash = hash_file(path).await?;

                FileRef {
                    ref_type: None,
                    hash,
                    size: meta.len(),
                    timestamp: Some(meta.mtime().try_into()?),
                    timestamp_subsec_nanos: Some(meta.mtime_nsec().try_into()?),
                    locations: None,
                    media_type: None,
                    extra: None,
                    prev: None,
                    version_count: None,
                    warc: None,
                    first_version: None,
                }
            }
        };

        self.fs.file_put(&key, file_ref.clone()).await?;

        // Update progress
        if let Some(ref progress) = self.progress {
            progress.files_processed.fetch_add(1, Ordering::Relaxed);
            progress
                .bytes_processed
                .fetch_add(file_ref.size, Ordering::Relaxed);
        }

        log::info!("Successfully imported file: {}", key);
        Ok(())
    }
}

/// Hash a file using BLAKE3.
async fn hash_file(path: &std::path::Path) -> anyhow::Result<[u8; 32]> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || -> anyhow::Result<[u8; 32]> {
        let file = std::fs::File::open(&path)
            .with_context(|| format!("Failed to open file: {:?}", path))?;
        let mut reader = std::io::BufReader::new(file);
        let mut hasher = blake3::Hasher::new();
        let mut buf = [0u8; 8192];

        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }

        Ok(hasher.finalize().into())
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::*;
    use s5_core::BlobsRead;
    use s5_fs::DirContext;
    use s5_store_memory::MemoryStore;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_local_import() {
        // 1. Setup source directory with some files
        let source_dir = tempdir().unwrap();
        let file1_path = source_dir.path().join("file1.txt");
        let mut file1 = File::create(&file1_path).unwrap();
        writeln!(file1, "content1").unwrap();

        let subdir = source_dir.path().join("subdir");
        std::fs::create_dir(&subdir).unwrap();
        let file2_path = subdir.join("file2.txt");
        let mut file2 = File::create(&file2_path).unwrap();
        writeln!(file2, "content2").unwrap();

        // 2. Setup FS5 and BlobStore
        let fs_dir = tempdir().unwrap();
        let ctx = DirContext::open_local_root(fs_dir.path()).unwrap();
        let fs = FS5::open(ctx).with_autosave(50).await.unwrap();

        let blob_store = BlobStore::new(MemoryStore::new());

        // 3. Run Importer (default: use absolute paths as keys)
        let importer = LocalFileSystemImporter::create(
            fs.clone(),
            blob_store.clone(),
            4,
            false,
            true,
            true,
            true,
        )
        .unwrap();
        importer
            .import_path(source_dir.path().to_path_buf())
            .await
            .unwrap();

        // 4. Verify FS5 state
        fs.save().await.unwrap();

        // Check file1: key is the absolute path (minus leading slash)
        let file1_path_abs = file1_path.canonicalize().unwrap();
        let file1_key = file1_path_abs.to_str().unwrap().trim_start_matches('/');
        assert!(fs.file_exists(file1_key).await);

        // file2.txt is in subdir
        let file2_path_abs = file2_path.canonicalize().unwrap();
        let file2_key = file2_path_abs.to_str().unwrap().trim_start_matches('/');
        assert!(fs.file_exists(file2_key).await);

        // Verify content in blob store
        let f1_ref = fs.file_get(file1_key).await.unwrap();
        assert!(blob_store.blob_contains(f1_ref.hash.into()).await.unwrap());
    }

    #[tokio::test]
    async fn test_local_import_cachedir_tag() {
        // 1. Setup source directory
        let source_dir = tempdir().unwrap();
        let root = source_dir.path();

        // Normal file
        let file1_path = root.join("file1.txt");
        let mut file1 = File::create(&file1_path).unwrap();
        writeln!(file1, "content1").unwrap();

        // Cached directory
        let cache_dir = root.join("cache_dir");
        std::fs::create_dir(&cache_dir).unwrap();

        // Create CACHEDIR.TAG
        let tag_path = cache_dir.join("CACHEDIR.TAG");
        let mut tag_file = File::create(&tag_path).unwrap();
        write!(tag_file, "Signature: 8a477f597d28d172789f06886806bc55").unwrap();

        // File inside cache dir
        let cache_file_path = cache_dir.join("cached_file.txt");
        let mut cache_file = File::create(&cache_file_path).unwrap();
        writeln!(cache_file, "should be ignored").unwrap();

        // 2. Setup FS5 and BlobStore
        let fs_dir = tempdir().unwrap();
        let ctx = DirContext::open_local_root(fs_dir.path()).unwrap();
        let fs = FS5::open(ctx).with_autosave(50).await.unwrap();
        let blob_store = BlobStore::new(MemoryStore::new());

        // 3. Run Importer with check_cachedir_tag = true
        let importer = LocalFileSystemImporter::create(
            fs.clone(),
            blob_store.clone(),
            4,
            false,
            true,
            true,
            true,
        )
        .unwrap();
        importer
            .import_path(source_dir.path().to_path_buf())
            .await
            .unwrap();

        fs.save().await.unwrap();

        // Check file1 exists
        let file1_path_abs = file1_path.canonicalize().unwrap();
        let file1_key = file1_path_abs.to_str().unwrap().trim_start_matches('/');
        assert!(fs.file_exists(file1_key).await);

        // Check cached file does NOT exist
        let cache_file_path_abs = cache_file_path.canonicalize().unwrap();
        let cache_file_key = cache_file_path_abs
            .to_str()
            .unwrap()
            .trim_start_matches('/');
        assert!(!fs.file_exists(cache_file_key).await);
    }

    #[tokio::test]
    async fn test_index_only_import() {
        // 1. Setup source directory with a file
        let source_dir = tempdir().unwrap();
        let file1_path = source_dir.path().join("hello.txt");
        let mut file1 = File::create(&file1_path).unwrap();
        writeln!(file1, "hello world").unwrap();

        // 2. Setup FS5 (no blob store needed for index-only)
        let fs_dir = tempdir().unwrap();
        let ctx = DirContext::open_local_root(fs_dir.path()).unwrap();
        let fs = FS5::open(ctx).with_autosave(50).await.unwrap();

        // 3. Run Importer in index-only mode with progress tracking
        let progress = Arc::new(ImportProgress::default());
        let mut importer = LocalFileSystemImporter::create_index_only(
            fs.clone(),
            4,
            true, // base-relative keys
            true,
            true,
            true,
        )
        .unwrap();
        importer.set_progress(progress.clone());
        importer
            .import_path(source_dir.path().to_path_buf())
            .await
            .unwrap();

        // 4. Verify FS5 state — file is indexed with correct hash and size
        fs.save().await.unwrap();

        let file_ref = fs.file_get("hello.txt").await.unwrap();
        assert_eq!(file_ref.size, 12); // "hello world\n"
        assert_ne!(file_ref.hash, [0u8; 32]); // hash is populated
        assert!(file_ref.timestamp.is_some());

        // 5. Verify progress counters
        assert_eq!(progress.files_processed.load(Ordering::Relaxed), 1);
        assert_eq!(progress.bytes_processed.load(Ordering::Relaxed), 12);
    }
}
