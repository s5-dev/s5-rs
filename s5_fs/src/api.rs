//! Provides the main high-level API for interacting with the S5 file system.

use crate::{
    FSResult,
    actor::{ActorMessage, ActorMessageOp, DirActorHandle},
    context::DirContext,
    dir::{DirV1, FileRef},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as B64_URL};
use chrono::Utc;
use minicbor::{CborLen, Decode, Encode};
use s5_core::Hash;
use tokio::sync::oneshot;

/// The main API for interacting with the S5 file system.
///
/// FS5 offers a simple, async façade for managing a content-addressed,
/// optionally encrypted directory tree backed by immutable snapshots. Writes
/// are applied through an internal single-threaded actor to ensure ordering.
#[derive(Clone)]
pub struct FS5 {
    root: DirActorHandle,
}

#[derive(Encode, Decode, CborLen, Clone, Debug)]
#[cbor(map)]
pub struct CursorData {
    #[n(0)]
    pub position: String,
    #[n(1)]
    pub kind: CursorKind,
    #[n(2)]
    pub timestamp: Option<u32>,
    #[n(3)]
    pub path: Option<Vec<u32>>, // for HAMT, not used in inline
}

#[derive(Encode, Decode, CborLen, Clone, Debug)]
#[cbor(index_only)]
pub enum CursorKind {
    #[n(0)]
    File,
    #[n(1)]
    Directory,
}

pub fn encode_cursor(c: &CursorData) -> String {
    let mut buf = Vec::new();
    minicbor::encode(c, &mut buf).unwrap();
    B64_URL.encode(buf)
}

pub fn decode_cursor(s: &str) -> Option<CursorData> {
    let bytes = B64_URL.decode(s.as_bytes()).ok()?;
    minicbor::decode::<CursorData>(&bytes).ok()
}

impl FS5 {
    /// Opens (or initializes) a file system using the provided [`DirContext`].
    ///
    /// - Autosave is disabled by default; call [`FS5::save`] or use [`FS5::batch`].
    /// - The underlying store and registry are taken from the context.
    ///
    /// Examples
    /// ```rust,no_run
    /// use s5_fs::{DirContext, FS5};
    /// use tempfile::tempdir;
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// let tmp = tempdir()?;
    /// let ctx = DirContext::open_local_root(tmp.path())?;
    /// let fs = FS5::open(ctx);
    /// fs.save().await?;
    /// # Ok(()) }
    /// ```
    pub fn open(context: DirContext) -> Self {
        let root = DirActorHandle::spawn(context, None, None);
        Self { root }
    }

    /// Enables debounced autosave.
    ///
    /// When enabled, mutations will trigger a save after `debounce_ms` milliseconds
    /// of inactivity. This is useful for batching rapid updates (e.g. during imports)
    /// while still persisting state periodically and on shutdown/drop.
    pub async fn with_autosave(self, debounce_ms: u64) -> FSResult<Self> {
        let fs = self;
        fs.root
            .send_msg(ActorMessage::SetAutosave { debounce_ms })
            .await?;
        Ok(fs)
    }

    /// Persists all pending metadata changes to the underlying store.
    ///
    /// Returns when the current directory state (and any dirty children) have
    /// been serialized and stored.
    ///
    /// ```rust,no_run
    /// # use s5_fs::{DirContext, FS5};
    /// # use tempfile::tempdir;
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// # let tmp = tempdir()?;
    /// # let ctx = DirContext::open_local_root(tmp.path())?;
    /// # let fs = FS5::open(ctx);
    /// fs.save().await?;
    /// # Ok(()) }
    /// ```
    pub async fn save(&self) -> FSResult<()> {
        self.root.save_if_dirty().await?;
        Ok(())
    }

    /// Exports the current directory state as an immutable snapshot.
    pub async fn export_snapshot(&self) -> FSResult<DirV1> {
        let (responder, receiver) = oneshot::channel();
        self.root
            .send_msg(ActorMessage::ExportSnapshot { responder })
            .await?;
        receiver.await?
    }

    /// Exports the current directory state as a flat immutable snapshot,
    /// merging any internal shards.
    pub async fn export_merged_snapshot(&self) -> FSResult<DirV1> {
        let (responder, receiver) = oneshot::channel();
        self.root
            .send_msg(ActorMessage::ExportMergedSnapshot { responder })
            .await?;
        receiver.await?
    }

    /// Computes and persists a snapshot blob for the current directory state
    /// and returns its BLAKE3 hash. Works for any FS5 root.
    pub async fn snapshot_hash(&self) -> FSResult<Hash> {
        let (responder, receiver) = oneshot::channel();
        self.root
            .send_msg(ActorMessage::ExportSnapshotHash { responder })
            .await?;
        receiver.await?
    }

    /// Exports the current directory state as an immutable snapshot and
    /// records it in `snapshots.fs5.cbor`, returning the snapshot name and
    /// root hash.
    pub async fn create_snapshot(&self) -> FSResult<(String, Hash)> {
        // Ensure all pending changes are flushed and the head hash is up to date.
        self.save().await?;

        let (responder, receiver) = oneshot::channel();
        self.root
            .send_msg(ActorMessage::CreateSnapshot { responder })
            .await?;
        receiver.await?
    }

    /// Deletes a named snapshot from the local FS5 root's
    /// `snapshots.fs5.cbor` index and unpins its `LocalFsSnapshot`
    /// pin, if present. Deleting an unknown snapshot name is a no-op.
    pub async fn delete_snapshot(&self, name: &str) -> FSResult<()> {
        let (responder, receiver) = oneshot::channel();
        self.root
            .send_msg(ActorMessage::DeleteSnapshot {
                name: name.to_owned(),
                responder,
            })
            .await?;
        receiver.await?
    }

    /// Merges an incoming snapshot into the current directory state, overwriting
    /// files with matching paths.
    pub async fn merge_from_snapshot(&self, snapshot: DirV1) -> FSResult<()> {
        let (responder, receiver) = oneshot::channel();
        self.root
            .send_msg(ActorMessage::MergeSnapshot {
                snapshot,
                responder,
            })
            .await?;
        receiver.await?
    }

    /// Creates a subdirectory at `path`, optionally enabling encryption.
    ///
    /// - Idempotent: creating the same directory again is a no-op.
    /// - If files exist under the `path/` prefix, they are migrated into the new subdir.
    ///
    /// ```rust,no_run
    /// # use s5_fs::{DirContext, FS5};
    /// # use tempfile::tempdir;
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// # let tmp = tempdir()?;
    /// # let ctx = DirContext::open_local_root(tmp.path())?;
    /// # let fs = FS5::open(ctx);
    /// fs.create_dir("secret", true).await?;
    /// fs.save().await?;
    /// # Ok(()) }
    /// ```
    pub async fn create_dir(&self, path: &str, enable_encryption: bool) -> FSResult<()> {
        let (responder, receiver) = oneshot::channel();
        self.root
            .send_msg(ActorMessage::PathOp {
                path: path.to_owned(),
                op: ActorMessageOp::CreateDir {
                    enable_encryption,
                    responder,
                },
            })
            .await?;
        receiver.await?
    }

    /// Inserts or updates a file at `path` (fire-and-forget).
    ///
    /// - Returns immediately after enqueueing; use [`FS5::file_put_sync`] to await application.
    /// - Call [`FS5::save`] to persist metadata when batching multiple writes.
    ///
    /// ```rust,no_run
    /// # use s5_fs::{DirContext, FS5, FileRef};
    /// # use tempfile::tempdir; use bytes::Bytes;
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// # let tmp = tempdir()?; let ctx = DirContext::open_local_root(tmp.path())?; let fs = FS5::open(ctx);
    /// fs.file_put("hello.txt", FileRef::new_inline_blob(Bytes::from_static(b"hi"))).await?;
    /// fs.save().await?;
    /// # Ok(()) }
    /// ```
    pub async fn file_put(&self, path: &str, file_ref: FileRef) -> FSResult<()> {
        if let Err(err) = self
            .root
            .execute_and_forget(path.to_string(), |value| *value = Some(file_ref))
            .await
        {
            tracing::error!("fs5: file_put failed for path {}: {}", path, err);
        }
        Ok(())
    }

    /// Inserts or updates a file at `path` and waits for the mutation to apply.
    ///
    /// Use this for acknowledged writes; pair with [`FS5::save`] for durability.
    ///
    /// ```rust,no_run
    /// # use s5_fs::{DirContext, FS5, FileRef};
    /// # use tempfile::tempdir; use bytes::Bytes;
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// # let tmp = tempdir()?; let ctx = DirContext::open_local_root(tmp.path())?; let fs = FS5::open(ctx);
    /// fs.file_put_sync("hello.txt", FileRef::new_inline_blob(Bytes::from_static(b"hi"))).await?;
    /// fs.save().await?;
    /// # Ok(()) }
    /// ```
    pub async fn file_put_sync(&self, path: &str, file_ref: FileRef) -> FSResult<()> {
        self.root
            .execute(path.to_string(), |value| {
                *value = Some(file_ref);
            })
            .await?;
        Ok(())
    }

    /// Executes multiple operations and persists once at the end.
    ///
    /// The closure receives a clone of `FS5` and can perform async operations.
    /// Errors inside the closure abort the batch and bubble up; `save()` runs only on success.
    ///
    /// ```rust,no_run
    /// # use s5_fs::{DirContext, FS5, FileRef};
    /// # use tempfile::tempdir; use bytes::Bytes;
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// # let tmp = tempdir()?; let ctx = DirContext::open_local_root(tmp.path())?; let fs = FS5::open(ctx);
    /// fs.batch(|fs| async move {
    ///     fs.file_put_sync("a.txt", FileRef::new_inline_blob(Bytes::from_static(b"A"))).await?;
    ///     fs.file_put_sync("b.txt", FileRef::new_inline_blob(Bytes::from_static(b"B"))).await?;
    ///     Ok(())
    /// }).await?;
    /// # Ok(()) }
    /// ```
    pub async fn batch<F, Fut>(&self, f: F) -> FSResult<()>
    where
        F: FnOnce(FS5) -> Fut,
        Fut: std::future::Future<Output = FSResult<()>>,
    {
        f(self.clone()).await?;
        self.save().await
    }

    /// Returns an `FS5` handle scoped to a subdirectory at `path`.
    ///
    /// - The subdirectory is auto-created if it does not yet exist.
    /// - If the parent directory is encrypted, the new subdirectory
    ///   inherits encryption.
    pub async fn subdir(&self, path: &str) -> FSResult<FS5> {
        // Normalize to a logical path relative to this handle.
        let normalized = path.trim_matches('/');
        if normalized.is_empty() {
            return Ok(self.clone());
        }

        let (responder, receiver) = oneshot::channel();
        self.root
            .send_msg(ActorMessage::OpenSubdir {
                path: normalized.to_owned(),
                responder,
            })
            .await?;
        let handle = receiver.await??;
        Ok(FS5 { root: handle })
    }

    /// Retrieves the file reference at `path`, if present.
    ///
    /// ```rust,no_run
    /// # use s5_fs::{DirContext, FS5, FileRef};
    /// # use tempfile::tempdir; use bytes::Bytes;
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// # let tmp = tempdir()?; let ctx = DirContext::open_local_root(tmp.path())?; let fs = FS5::open(ctx);
    /// fs.file_put_sync("x.txt", FileRef::new_inline_blob(Bytes::from_static(b"x"))).await?;
    /// let fr = fs.file_get("x.txt").await;
    /// assert!(fr.is_some());
    /// # Ok(()) }
    /// ```
    pub async fn file_get(&self, path: &str) -> Option<FileRef> {
        use crate::dir::FileRefType;

        self.root
            .execute(path.to_string(), |value| value.clone())
            .await
            .ok()
            .flatten()
            .and_then(|f| match f.ref_type() {
                FileRefType::Tombstone => None,
                _ => Some(f),
            })
    }

    /// Deletes the file at `path`, if present, by creating a tombstone entry.
    ///
    /// - Idempotent: deleting a non-existent path is a no-op.
    /// - Preserves the full version chain in `prev` / `first_version`.
    pub async fn file_delete(&self, path: &str) -> FSResult<()> {
        use crate::dir::FileRef;

        self.root
            .execute(path.to_string(), |value| {
                if let Some(current) = value.take() {
                    let now = Utc::now();
                    let tomb = FileRef::from_deleted(
                        current,
                        now.timestamp() as u32,
                        now.timestamp_subsec_nanos(),
                    );
                    *value = Some(tomb);
                }
            })
            .await?;
        Ok(())
    }

    /// Returns true if a file exists at `path`.
    ///
    /// ```rust,no_run
    /// # use s5_fs::{DirContext, FS5, FileRef};
    /// # use tempfile::tempdir; use bytes::Bytes;
    /// # #[tokio::main]
    /// # async fn main() -> anyhow::Result<()> {
    /// # let tmp = tempdir()?; let ctx = DirContext::open_local_root(tmp.path())?; let fs = FS5::open(ctx);
    /// fs.file_put_sync("x.txt", FileRef::new_inline_blob(Bytes::from_static(b"x"))).await?;
    /// assert!(fs.file_exists("x.txt").await);
    /// # Ok(()) }
    /// ```
    pub async fn file_exists(&self, path: &str) -> bool {
        self.file_get(path).await.is_some()
    }

    /// Lists directory entries with cursor-based pagination (inline layout).
    /// Returns (entries, next_cursor). Cursor is base64url(CBOR(CursorData)).
    pub async fn list(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> FSResult<(Vec<(String, CursorKind)>, Option<String>)> {
        let (responder, receiver) = oneshot::channel();
        self.root
            .send_msg(ActorMessage::List {
                cursor: cursor.map(|s| s.to_string()),
                limit,
                responder,
            })
            .await?;
        receiver.await?
    }

    /// Lists entries at a specific directory path.
    pub async fn list_at(
        &self,
        path: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> FSResult<(Vec<(String, CursorKind)>, Option<String>)> {
        let (responder, receiver) = oneshot::channel();
        self.root
            .send_msg(ActorMessage::ListAt {
                path: path.to_owned(),
                cursor: cursor.map(|s| s.to_string()),
                limit,
                responder,
            })
            .await?;
        receiver.await?
    }

    /// Exports the snapshot of a directory at `path`.
    pub async fn export_snapshot_at(&self, path: &str) -> FSResult<DirV1> {
        let (responder, receiver) = oneshot::channel();
        self.root
            .send_msg(ActorMessage::ExportSnapshotAt {
                path: path.to_owned(),
                responder,
            })
            .await?;
        receiver.await?
    }

    /// Exports the merged snapshot of a directory at `path`.
    pub async fn export_merged_snapshot_at(&self, path: &str) -> FSResult<DirV1> {
        let (responder, receiver) = oneshot::channel();
        self.root
            .send_msg(ActorMessage::ExportMergedSnapshotAt {
                path: path.to_owned(),
                responder,
            })
            .await?;
        receiver.await?
    }

    pub async fn shutdown(&self) -> FSResult<()> {
        self.root.shutdown().await
    }
}
