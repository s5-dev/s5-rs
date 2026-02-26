//! Vault state management — opens FS5 root and runs re-indexing.
//!
//! The vault has a single local FS5 tree that tracks all indexed files.
//! During backup, `FileRef.locations` is populated with the full
//! retrieval chain (compress → encrypt → store). Snapshots of this tree
//! are pushed to the remote store for seed-phrase-only restore.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use s5_fs::{DirContext, FS5};
use s5_importer_local::{ImportProgress, LocalFileSystemImporter};

/// Data directory for vault state (FS5 metadata, not user files).
fn data_dir() -> Result<PathBuf> {
    let dirs = directories::ProjectDirs::from("pro", "s5", "vup")
        .context("could not determine data directory")?;
    Ok(dirs.data_dir().to_path_buf())
}

/// Open the local FS5 root at the default location (`~/.local/share/vup/index/`).
///
/// This stores FileRefs for all tracked files (hash, size, mtime).
/// During backup, `FileRef.locations` is populated with retrieval info.
pub fn open_index() -> Result<FS5> {
    let path = data_dir()?.join("index");
    open_index_at(&path)
}

/// Open a local FS5 root at a custom path (used by tests).
pub fn open_index_at(path: &Path) -> Result<FS5> {
    std::fs::create_dir_all(path)?;
    let ctx = DirContext::open_local_root(path)
        .context("failed to open FS5 root")?;
    Ok(FS5::open(ctx))
}

/// Re-index all tracked sources into the local index FS5.
///
/// Walks each source directory, checks mtime+size for changes,
/// hashes changed/new files with blake3, and updates FileRefs.
///
/// Returns (files_processed, bytes_processed).
pub async fn reindex(fs: &FS5, sources: &[PathBuf]) -> Result<(u64, u64)> {
    let progress = Arc::new(ImportProgress::default());

    let mut importer = LocalFileSystemImporter::create_index_only(
        fs.clone(),
        8, // max concurrent hashing ops
        false, // absolute keys (home/user/Photos/img.jpg) — avoids collisions between sources
        true, // respect .gitignore / .ignore
        true, // respect VCS ignore rules
        true, // check CACHEDIR.TAG
    )?;
    importer.set_progress(progress.clone());

    let start = Instant::now();

    for source in sources {
        if !source.exists() {
            tracing::warn!("source does not exist, skipping: {}", source.display());
            continue;
        }
        tracing::info!("indexing {}", source.display());
        importer.import_path(source.clone()).await
            .with_context(|| format!("failed to index {}", source.display()))?;
    }

    fs.save().await?;

    let files = progress.files_processed.load(std::sync::atomic::Ordering::Relaxed);
    let bytes = progress.bytes_processed.load(std::sync::atomic::Ordering::Relaxed);
    let elapsed = start.elapsed();

    tracing::info!(
        "indexed {} files ({}) in {:.1}s",
        files,
        format_bytes(bytes),
        elapsed.as_secs_f64(),
    );

    Ok((files, bytes))
}

/// Format byte count as human-readable string (e.g. "1.2 GiB").
pub fn format_bytes(bytes: u64) -> String {
    humansize::format_size(bytes, humansize::BINARY)
}
