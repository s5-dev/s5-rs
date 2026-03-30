//! LocalLinksStore - Reference local files by hash without copying.
//!
//! This store maintains a redb-backed mapping from content hash to filesystem paths.
//! Files are hashed but not copied; the original location is recorded and used to
//! serve blob reads. This enables sync without storage duplication.
//!
//! # Use Case
//!
//! ```bash
//! # Link files instead of copying (no blob storage used)
//! s5 add /media/movies --link
//! ```
//!
//! Files stay on the original disk but:
//! - FS5 knows their hashes
//! - Content is served via `BlobsRead` trait
//! - Remote peers can download linked files (via node ACLs)
//! - FUSE mounts include linked files automatically
//!
//! # Configuration
//!
//! ```toml
//! [store.links]
//! type = "local_links"
//! path = "./links"
//!
//! [peer."*".blobs]
//! readable_stores = ["default", "links"]
//! ```
//!
//! # Limitations
//!
//! - **No BAO outboard**: Large files can't be verified incrementally during transfer.
//!   Future: consider a routing store that generates outboard on-demand or stores it
//!   separately.
//!
//! - **File change detection**: If a file is modified after linking, `resolve()` returns
//!   `None` (size/mtime mismatch). The link must be re-added to update the hash.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use s5_core::blob::BlobsRead;
use s5_core::{BlobId, Hash};
use tokio::io::AsyncRead;

/// Table: hash (32 bytes) -> newline-separated paths
const HASH_TO_PATHS: TableDefinition<&[u8; 32], &[u8]> = TableDefinition::new("hash_to_paths");

/// Table: path string -> hash (32 bytes) for reverse lookups
const PATH_TO_HASH: TableDefinition<&str, &[u8; 32]> = TableDefinition::new("path_to_hash");

/// Table: path string -> metadata (size:u64, mtime_secs:i64, mtime_nanos:u32)
/// Used to detect if file changed since linking
const PATH_TO_META: TableDefinition<&str, &[u8]> = TableDefinition::new("path_to_meta");

/// A store that references local files by hash without copying them.
///
/// Implements `BlobsRead` to serve file contents from their original locations.
#[derive(Clone)]
pub struct LocalLinksStore {
    db: Arc<Database>,
}

impl std::fmt::Debug for LocalLinksStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalLinksStore").finish()
    }
}

impl LocalLinksStore {
    /// Open or create a LocalLinksStore at the given path.
    ///
    /// The database file will be created at `{path}/local_links.redb`.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;
        let db_path = path.join("local_links.redb");
        let db = Database::create(&db_path)
            .with_context(|| format!("Failed to create database at {:?}", db_path))?;

        // Ensure tables exist
        {
            let write_txn = db.begin_write()?;
            {
                let _ = write_txn.open_table(HASH_TO_PATHS)?;
                let _ = write_txn.open_table(PATH_TO_HASH)?;
                let _ = write_txn.open_table(PATH_TO_META)?;
            }
            write_txn.commit()?;
        }

        Ok(Self { db: Arc::new(db) })
    }

    /// Add a path for a given hash.
    ///
    /// Multiple paths can map to the same hash (same content at different locations).
    /// The path is canonicalized before storing. File metadata (size, mtime) is stored
    /// to detect if the file changes after linking.
    pub fn add_link(&self, hash: Hash, path: PathBuf) -> Result<()> {
        let path = path
            .canonicalize()
            .with_context(|| format!("Failed to canonicalize path: {:?}", path))?;
        let path_str = path
            .to_str()
            .ok_or_else(|| anyhow!("Path is not valid UTF-8: {:?}", path))?;

        // Get file metadata for change detection
        let meta = std::fs::metadata(&path)
            .with_context(|| format!("Failed to get metadata for {:?}", path))?;

        let write_txn = self.db.begin_write()?;
        {
            let mut hash_table = write_txn.open_table(HASH_TO_PATHS)?;
            let mut path_table = write_txn.open_table(PATH_TO_HASH)?;
            let mut meta_table = write_txn.open_table(PATH_TO_META)?;

            // Get existing paths for this hash
            let mut paths = self.get_paths_internal(&hash_table, &hash)?;

            // Add path if not already present
            if !paths.contains(&path) {
                paths.push(path.clone());
                let encoded = encode_paths(&paths)?;
                hash_table.insert(hash.as_bytes(), encoded.as_slice())?;
            }

            // Update reverse mapping
            path_table.insert(path_str, hash.as_bytes())?;

            // Store metadata for change detection
            let meta_bytes = encode_metadata(&meta);
            meta_table.insert(path_str, meta_bytes.as_slice())?;
        }
        write_txn.commit()?;

        tracing::debug!(?hash, ?path_str, "Added link");
        Ok(())
    }

    /// Remove a specific path from the store.
    ///
    /// If this was the last path for a hash, the hash entry is also removed.
    pub fn remove_path(&self, path: &Path) -> Result<Option<Hash>> {
        let path = path
            .canonicalize()
            .with_context(|| format!("Failed to canonicalize path: {:?}", path))?;
        let path_str = path
            .to_str()
            .ok_or_else(|| anyhow!("Path is not valid UTF-8: {:?}", path))?;

        let write_txn = self.db.begin_write()?;
        let removed_hash = {
            let mut hash_table = write_txn.open_table(HASH_TO_PATHS)?;
            let mut path_table = write_txn.open_table(PATH_TO_HASH)?;

            // Look up hash for this path
            let hash = match path_table.get(path_str)? {
                Some(guard) => Hash::from(*guard.value()),
                None => return Ok(None),
            };

            // Remove from path table
            path_table.remove(path_str)?;

            // Update hash table
            let mut paths = self.get_paths_internal(&hash_table, &hash)?;
            paths.retain(|p| p != &path);

            if paths.is_empty() {
                hash_table.remove(hash.as_bytes())?;
            } else {
                let encoded = encode_paths(&paths)?;
                hash_table.insert(hash.as_bytes(), encoded.as_slice())?;
            }

            hash
        };
        write_txn.commit()?;

        tracing::debug!(?removed_hash, ?path_str, "Removed link");
        Ok(Some(removed_hash))
    }

    /// Get all paths associated with a hash.
    pub fn get_paths(&self, hash: &Hash) -> Result<Vec<PathBuf>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(HASH_TO_PATHS)?;
        self.get_paths_internal(&table, hash)
    }

    /// Get the hash for a specific path, if known.
    pub fn get_hash(&self, path: &Path) -> Result<Option<Hash>> {
        let path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
        let path_str = match path.to_str() {
            Some(s) => s,
            None => return Ok(None),
        };

        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(PATH_TO_HASH)?;

        match table.get(path_str)? {
            Some(guard) => Ok(Some(Hash::from(*guard.value()))),
            None => Ok(None),
        }
    }

    /// Find the first valid path for a hash.
    ///
    /// Checks each stored path in order and returns the first one that:
    /// 1. Exists on the filesystem
    /// 2. Has matching size and mtime (hasn't changed since linking)
    ///
    /// Returns `None` if no valid path is found.
    pub fn resolve(&self, hash: &Hash) -> Result<Option<PathBuf>> {
        let paths = self.get_paths(hash)?;
        let read_txn = self.db.begin_read()?;
        let meta_table = read_txn.open_table(PATH_TO_META)?;

        for path in paths {
            if !path.exists() {
                continue;
            }

            // Check if file metadata matches what we stored
            let path_str = match path.to_str() {
                Some(s) => s,
                None => continue,
            };

            if let Some(stored_meta) = meta_table.get(path_str)? {
                let current_meta = match std::fs::metadata(&path) {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                if metadata_matches(stored_meta.value(), &current_meta) {
                    return Ok(Some(path));
                } else {
                    tracing::warn!(
                        ?path,
                        "File changed since linking (size/mtime mismatch), skipping"
                    );
                }
            } else {
                // No stored metadata - legacy entry, accept if exists
                return Ok(Some(path));
            }
        }
        Ok(None)
    }

    /// Check if we have any valid path for this hash.
    pub fn contains(&self, hash: &Hash) -> Result<bool> {
        Ok(self.resolve(hash)?.is_some())
    }

    /// List all known hashes.
    pub fn list_hashes(&self) -> Result<Vec<Hash>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(HASH_TO_PATHS)?;

        let mut hashes = Vec::new();
        for result in table.iter()? {
            let (key, _) = result?;
            hashes.push(Hash::from(*key.value()));
        }
        Ok(hashes)
    }

    /// Validate all stored paths and return those that no longer exist.
    pub fn find_missing_paths(&self) -> Result<Vec<(Hash, PathBuf)>> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(HASH_TO_PATHS)?;

        let mut missing = Vec::new();
        for result in table.iter()? {
            let (key, value) = result?;
            let hash = Hash::from(*key.value());
            let paths = decode_paths(value.value())?;
            for path in paths {
                if !path.exists() {
                    missing.push((hash, path));
                }
            }
        }
        Ok(missing)
    }

    /// Remove all paths that no longer exist on the filesystem.
    ///
    /// Returns the number of paths removed.
    pub fn cleanup_missing(&self) -> Result<usize> {
        let missing = self.find_missing_paths()?;
        let count = missing.len();
        for (_, path) in missing {
            self.remove_path(&path)?;
        }
        Ok(count)
    }

    /// Import a file by hashing it and registering the link.
    ///
    /// This is the primary way to add files to the store. The file is hashed
    /// (using BLAKE3) and a link is created from the hash to the file path.
    /// The file is NOT copied - it stays in its original location.
    ///
    /// Returns a `BlobId` with the hash and size, just like `BlobStore::import_file`.
    pub async fn import_file(
        &self,
        path: PathBuf,
        on_progress: impl Fn(u64) -> std::io::Result<()> + Send + Sync + 'static,
    ) -> Result<BlobId> {
        let meta = tokio::fs::metadata(&path).await?;
        let size = meta.len();

        // Hash the file
        let path_clone = path.clone();
        let hash = tokio::task::spawn_blocking(move || -> Result<Hash> {
            let file = std::fs::File::open(&path_clone)
                .with_context(|| format!("Failed to open file: {:?}", path_clone))?;
            let mut reader = std::io::BufReader::new(file);
            let mut hasher = blake3::Hasher::new();
            let mut buf = [0u8; 64 * 1024];
            let mut processed: u64 = 0;

            loop {
                let n = reader.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                hasher.update(&buf[..n]);
                processed += n as u64;
                on_progress(processed)?;
            }

            Ok(hasher.finalize().into())
        })
        .await??;

        // Register the link
        self.add_link(hash, path)?;

        Ok(BlobId { hash, size })
    }

    fn get_paths_internal<T: ReadableTable<&'static [u8; 32], &'static [u8]>>(
        &self,
        table: &T,
        hash: &Hash,
    ) -> Result<Vec<PathBuf>> {
        match table.get(hash.as_bytes())? {
            Some(guard) => decode_paths(guard.value()),
            None => Ok(Vec::new()),
        }
    }
}

/// Encode paths as newline-separated UTF-8 strings.
fn encode_paths(paths: &[PathBuf]) -> Result<Vec<u8>> {
    let strings: Vec<&str> = paths
        .iter()
        .map(|p| p.to_str().ok_or_else(|| anyhow!("Path not UTF-8: {:?}", p)))
        .collect::<Result<_>>()?;
    Ok(strings.join("\n").into_bytes())
}

/// Decode paths from newline-separated UTF-8 strings.
fn decode_paths(data: &[u8]) -> Result<Vec<PathBuf>> {
    let s = std::str::from_utf8(data).context("Invalid UTF-8 in stored paths")?;
    if s.is_empty() {
        return Ok(Vec::new());
    }
    Ok(s.lines().map(PathBuf::from).collect())
}

/// Encode file metadata as bytes: size (8) + mtime_secs (8) + mtime_nanos (4) = 20 bytes
fn encode_metadata(meta: &std::fs::Metadata) -> Vec<u8> {
    use std::os::unix::fs::MetadataExt;
    let mut buf = Vec::with_capacity(20);
    buf.extend_from_slice(&meta.len().to_le_bytes());
    buf.extend_from_slice(&meta.mtime().to_le_bytes());
    buf.extend_from_slice(&(meta.mtime_nsec() as u32).to_le_bytes());
    buf
}

/// Check if current file metadata matches stored metadata
fn metadata_matches(stored: &[u8], current: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    if stored.len() != 20 {
        return false;
    }
    let stored_size = u64::from_le_bytes(stored[0..8].try_into().unwrap());
    let stored_mtime = i64::from_le_bytes(stored[8..16].try_into().unwrap());
    let stored_mtime_ns = u32::from_le_bytes(stored[16..20].try_into().unwrap());

    current.len() == stored_size
        && current.mtime() == stored_mtime
        && (current.mtime_nsec() as u32) == stored_mtime_ns
}

#[async_trait]
impl BlobsRead for LocalLinksStore {
    async fn blob_contains(&self, hash: Hash) -> anyhow::Result<bool> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.contains(&hash))
            .await
            .context("spawn_blocking failed")?
    }

    async fn blob_get_size(&self, hash: Hash) -> anyhow::Result<u64> {
        let path = self
            .resolve(&hash)?
            .ok_or_else(|| anyhow!("No valid path for hash {}", hash))?;
        let meta = tokio::fs::metadata(&path).await?;
        Ok(meta.len())
    }

    async fn blob_download(&self, hash: Hash) -> anyhow::Result<Bytes> {
        let path = self
            .resolve(&hash)?
            .ok_or_else(|| anyhow!("No valid path for hash {}", hash))?;
        let data = tokio::fs::read(&path).await?;
        Ok(data.into())
    }

    async fn blob_download_slice(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> anyhow::Result<Bytes> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};

        let path = self
            .resolve(&hash)?
            .ok_or_else(|| anyhow!("No valid path for hash {}", hash))?;

        let mut file = tokio::fs::File::open(&path).await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;

        let data = match max_len {
            Some(len) => {
                let mut buf = vec![0u8; len as usize];
                let n = file.read(&mut buf).await?;
                buf.truncate(n);
                buf
            }
            None => {
                let mut buf = Vec::new();
                file.read_to_end(&mut buf).await?;
                buf
            }
        };

        Ok(data.into())
    }

    async fn blob_read(&self, hash: Hash) -> anyhow::Result<Box<dyn AsyncRead + Send + Unpin>> {
        let path = self
            .resolve(&hash)?
            .ok_or_else(|| anyhow!("No valid path for hash {}", hash))?;
        let file = tokio::fs::File::open(&path).await?;
        Ok(Box::new(file))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_add_and_resolve() {
        let dir = tempdir().unwrap();
        let store = LocalLinksStore::open(dir.path().join("db")).unwrap();

        // Create a test file
        let file_path = dir.path().join("test.txt");
        let mut file = std::fs::File::create(&file_path).unwrap();
        writeln!(file, "hello world").unwrap();

        let hash = Hash::new(b"hello world\n");

        // Add link
        store.add_link(hash, file_path.clone()).unwrap();

        // Resolve should find it
        let resolved = store.resolve(&hash).unwrap();
        assert!(resolved.is_some());
        assert_eq!(
            resolved.unwrap().canonicalize().unwrap(),
            file_path.canonicalize().unwrap()
        );

        // Contains should return true
        assert!(store.contains(&hash).unwrap());
    }

    #[test]
    fn test_multiple_paths_same_hash() {
        let dir = tempdir().unwrap();
        let store = LocalLinksStore::open(dir.path().join("db")).unwrap();

        // Create two files with same content
        let file1 = dir.path().join("file1.txt");
        let file2 = dir.path().join("file2.txt");
        std::fs::write(&file1, "same content").unwrap();
        std::fs::write(&file2, "same content").unwrap();

        let hash = Hash::new(b"same content");

        store.add_link(hash, file1.clone()).unwrap();
        store.add_link(hash, file2.clone()).unwrap();

        let paths = store.get_paths(&hash).unwrap();
        assert_eq!(paths.len(), 2);
    }

    #[test]
    fn test_remove_path() {
        let dir = tempdir().unwrap();
        let store = LocalLinksStore::open(dir.path().join("db")).unwrap();

        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "content").unwrap();

        let hash = Hash::new(b"content");
        store.add_link(hash, file_path.clone()).unwrap();

        // Remove the path
        let removed = store.remove_path(&file_path).unwrap();
        assert_eq!(removed, Some(hash));

        // Should no longer resolve
        assert!(!store.contains(&hash).unwrap());
    }

    #[test]
    fn test_missing_file_not_resolved() {
        let dir = tempdir().unwrap();
        let store = LocalLinksStore::open(dir.path().join("db")).unwrap();

        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "content").unwrap();

        let hash = Hash::new(b"content");
        store.add_link(hash, file_path.clone()).unwrap();

        // Delete the actual file
        std::fs::remove_file(&file_path).unwrap();

        // resolve() should return None (file doesn't exist)
        assert!(store.resolve(&hash).unwrap().is_none());
        assert!(!store.contains(&hash).unwrap());
    }

    #[test]
    fn test_find_missing_paths() {
        let dir = tempdir().unwrap();
        let store = LocalLinksStore::open(dir.path().join("db")).unwrap();

        let file1 = dir.path().join("exists.txt");
        let file2 = dir.path().join("will_delete.txt");
        std::fs::write(&file1, "content1").unwrap();
        std::fs::write(&file2, "content2").unwrap();

        let hash1 = Hash::new(b"content1");
        let hash2 = Hash::new(b"content2");
        store.add_link(hash1, file1).unwrap();
        store.add_link(hash2, file2.clone()).unwrap();

        // Delete one file
        std::fs::remove_file(&file2).unwrap();

        let missing = store.find_missing_paths().unwrap();
        assert_eq!(missing.len(), 1);
        assert_eq!(missing[0].0, hash2);
    }

    #[tokio::test]
    async fn test_blobs_read_impl() {
        let dir = tempdir().unwrap();
        let store = LocalLinksStore::open(dir.path().join("db")).unwrap();

        let file_path = dir.path().join("test.txt");
        let content = b"hello blob world";
        std::fs::write(&file_path, content).unwrap();

        let hash = Hash::new(content);
        store.add_link(hash, file_path).unwrap();

        // Test blob_contains
        assert!(store.blob_contains(hash).await.unwrap());

        // Test blob_get_size
        assert_eq!(
            store.blob_get_size(hash).await.unwrap(),
            content.len() as u64
        );

        // Test blob_download
        let downloaded = store.blob_download(hash).await.unwrap();
        assert_eq!(downloaded.as_ref(), content);

        // Test blob_download_slice
        let slice = store.blob_download_slice(hash, 6, Some(4)).await.unwrap();
        assert_eq!(slice.as_ref(), b"blob");
    }

    #[tokio::test]
    async fn test_import_file() {
        let dir = tempdir().unwrap();
        let store = LocalLinksStore::open(dir.path().join("db")).unwrap();

        let file_path = dir.path().join("test.txt");
        let content = b"import file test content";
        std::fs::write(&file_path, content).unwrap();

        // Import the file
        let blob_id = store
            .import_file(file_path.clone(), |_| Ok(()))
            .await
            .unwrap();

        // Check returned BlobId
        assert_eq!(blob_id.size, content.len() as u64);
        assert_eq!(blob_id.hash, Hash::new(content));

        // Check that the link was registered
        assert!(store.contains(&blob_id.hash).unwrap());

        // Check that we can read it back via BlobsRead
        let downloaded = store.blob_download(blob_id.hash).await.unwrap();
        assert_eq!(downloaded.as_ref(), content);
    }

    #[test]
    fn test_file_changed_after_link() {
        let dir = tempdir().unwrap();
        let store = LocalLinksStore::open(dir.path().join("db")).unwrap();

        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, "original content").unwrap();

        let hash = Hash::new(b"original content");
        store.add_link(hash, file_path.clone()).unwrap();

        // File should resolve before modification
        assert!(store.resolve(&hash).unwrap().is_some());
        assert!(store.contains(&hash).unwrap());

        // Modify the file (changes mtime and possibly size)
        std::thread::sleep(std::time::Duration::from_millis(10));
        std::fs::write(&file_path, "modified content!").unwrap();

        // resolve() should now return None because metadata changed
        assert!(store.resolve(&hash).unwrap().is_none());
        assert!(!store.contains(&hash).unwrap());
    }
}
