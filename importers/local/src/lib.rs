use anyhow::{Context, anyhow};
use futures::{StreamExt, TryStreamExt};
use s5_core::BlobStore;
use s5_fs::{FS5, FileRef};
use std::{os::unix::fs::MetadataExt, path::PathBuf};
use walkdir::WalkDir;

/// Imports files and directories from the local file system into an FS5 directory.
pub struct LocalFileSystemImporter {
    /// A semaphore to limit the number of concurrent file hashing operations.
    // rate_limiter: Arc<Semaphore>,
    /// The FS5 directory state where file references will be stored.
    fs: FS5,
    /// The blob store where file content will be stored.
    blob_store: BlobStore,
    max_concurrent_ops: usize,
}

impl LocalFileSystemImporter {
    /// Creates a new `LocalImporter`.
    ///
    /// # Arguments
    ///
    /// * `state_path` - The path to the FS5 directory state file.
    /// * `blob_store` - The blob store for storing file content.
    /// * `base_path` - The root directory to import from. Paths in the FS5 directory
    ///   will be relative to this `base_path`.
    /// * `max_concurrent_ops` - The maximum number of files to hash concurrently.
    pub fn create(
        fs: FS5,
        blob_store: BlobStore,
        max_concurrent_ops: usize,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            max_concurrent_ops,
            fs,
            blob_store,
        })
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

        let walker = WalkDir::new(&path).into_iter();

        // Use a stream to process directory entries concurrently.
        futures::stream::iter(walker.filter_map(Result::ok))
            .map(|entry| async move {
                if !entry.file_type().is_file() {
                    return Ok(());
                }
                self.process_entry(entry).await
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
    async fn process_entry(&self, entry: walkdir::DirEntry) -> anyhow::Result<()> {
        let path = entry.path();
        let meta = entry
            .metadata()
            .with_context(|| format!("Failed to get metadata for {:?}", path))?;

        // Get the utf8 path string to use as the key in the FS5 directory.
        let key = path
            .to_str()
            .ok_or_else(|| anyhow!("Path is not valid UTF-8: {:?}", path))?;

        let current_file_ref = self.fs.file_get(key).await;

        let should_update = match current_file_ref {
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
        };

        if !should_update {
            log::trace!("Skipping unchanged file: {}", key);
            return Ok(());
        }

        log::info!("Importing file: {}", key);

        let (hash, size) = self
            .blob_store
            .import_file(path.to_path_buf())
            .await
            .with_context(|| format!("Failed to import file into blob store: {:?}", path))?;

        let mut file_ref = FileRef::new(hash, size);

        // TODO if hash changed, update prev field
        file_ref.timestamp = Some(meta.mtime().try_into()?);
        file_ref.timestamp_subsec_nanos = Some(meta.mtime_nsec().try_into()?);

        // TODO(perf): do not save for every op, instead save if time X elapsed since last op
        self.fs.file_put(key, file_ref).await?;

        log::info!("Successfully imported file: {}", key);
        Ok(())
    }
}
