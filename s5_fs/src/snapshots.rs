use std::io;
use std::path::{Path, PathBuf};

use crate::FSResult;
use crate::dir::{DirRef, DirV1};
use chrono::Utc;
use s5_core::Hash;
use tempfile::NamedTempFile;

/// Small helper around the `snapshots.fs5.cbor` index for a local FS5 root.
pub struct SnapshotIndex {
    pub path: PathBuf,
    pub dir: DirV1,
}

impl SnapshotIndex {
    /// Opens (or creates empty) snapshot index at `<fs_root>/snapshots.fs5.cbor`.
    pub fn open<P: AsRef<Path>>(fs_root: P) -> FSResult<Self> {
        let snapshots_path = fs_root.as_ref().join("snapshots.fs5.cbor");
        let dir = match std::fs::read(&snapshots_path) {
            Ok(bytes) => DirV1::from_bytes(&bytes)?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => DirV1::new(),
            Err(e) => return Err(e.into()),
        };
        Ok(SnapshotIndex {
            path: snapshots_path,
            dir,
        })
    }

    /// Returns a view of all snapshot entries as `(name, hash)` pairs.
    pub fn list(&self) -> Vec<(String, Hash)> {
        self.dir
            .dirs
            .iter()
            .map(|(name, dir_ref)| (name.clone(), Hash::from_bytes(dir_ref.hash)))
            .collect()
    }

    /// Inserts a new snapshot pointing at `hash` with a unique name based on
    /// the current timestamp.
    pub fn insert_snapshot(&mut self, hash: Hash) -> (String, Hash) {
        let now = Utc::now();
        let base_name = now.to_rfc3339();
        let mut name = base_name.clone();
        let mut counter: u32 = 1;
        while self.dir.dirs.contains_key(&name) {
            name = format!("{}-{}", base_name, counter);
            counter = counter.saturating_add(1);
        }

        let dir_ref = DirRef {
            ref_type: None,
            hash: hash.into(),
            ts_seconds: Some(now.timestamp() as u32),
            ts_nanos: Some(now.timestamp_subsec_nanos()),
            keys: None,
            encryption_type: None,
            extra: None,
        };

        self.dir.dirs.insert(name.clone(), dir_ref);
        (name, hash)
    }

    /// Deletes a snapshot by name, returning its hash if it existed.
    pub fn remove_snapshot(&mut self, name: &str) -> Option<Hash> {
        self.dir.dirs.remove(name).map(|d| Hash::from_bytes(d.hash))
    }

    /// Atomically persists the index to disk.
    pub fn persist(&self) -> FSResult<()> {
        let bytes = self.dir.to_bytes()?;
        let parent_dir = self.path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "Could not find parent directory for snapshots.fs5.cbor",
            )
        })?;
        let mut temp_file = NamedTempFile::new_in(parent_dir)?;
        use std::io::Write;
        temp_file.write_all(&bytes)?;
        temp_file.as_file().sync_all()?;
        temp_file.persist(&self.path)?;
        Ok(())
    }
}

/// Lists snapshots recorded in `snapshots.fs5.cbor` for a given FS5 root.
///
/// Returns an empty list if the index file does not exist.
pub fn list_snapshots<P: AsRef<Path>>(fs_root: P) -> FSResult<Vec<(String, Hash)>> {
    let index = SnapshotIndex::open(fs_root)?;
    Ok(index.list())
}
