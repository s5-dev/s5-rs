//! Provides the main high-level API for interacting with the S5 file system.

use crate::{
    FSResult,
    actor::{ActorMessage, ActorMessageOp, DirActorHandle},
    context::DirContext,
    dir::{DirV1, FileRef},
};
use anyhow::Context as _;
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
        let root = DirActorHandle::spawn(context, None, false);
        Self { root }
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

    /// Merges an incoming snapshot into the current directory state, overwriting
    /// files with matching paths.
    pub async fn merge_from_snapshot(&self, snapshot: DirV1) -> FSResult<()> {
        let (responder, receiver) = oneshot::channel();
        self.root
            .send_msg(ActorMessage::MergeSnapshot { snapshot, responder })
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
        let _ = self
            .root
            .execute_and_forget(path.to_string(), |value| *value = Some(file_ref))
            .await;
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
        let _ = self
            .root
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
        self.root
            .execute(path.to_string(), |value| value.clone())
            .await
            .ok()
            .flatten()
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
}
