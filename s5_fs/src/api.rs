//! Provides the main high-level API for interacting with the S5 file system.

use crate::{
    FSResult,
    actor::{ActorMessage, ActorMessageOp, DirActorHandle},
    context::DirContext,
    dir::FileRef,
};
use tokio::sync::oneshot;

/// The main API for interacting with the S5 file system.
#[derive(Clone)]
pub struct FS5 {
    root: DirActorHandle,
}

impl FS5 {
    /// Opens or creates a file system at the given context and returns an API handle.
    pub fn open(context: DirContext) -> Self {
        let root = DirActorHandle::spawn(context, None);
        Self { root }
    }

    /// Transparently splits `path` into a standalone sub-directory and
    /// enables XChaCha20-Poly1305 encryption for it.
    ///
    /// This is **idempotent** – calling it twice has no further effect.
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
            .await;
        receiver.await?
    }

    /// Inserts or updates a file reference at the given path.
    pub async fn file_put(&self, path: &str, file_ref: FileRef) {
        let _ = self
            .root
            .execute(path.to_string(), |value| *value = Some(file_ref))
            .await;
    }

    /// Retrieves a file reference from the given path, if it exists.
    pub async fn file_get(&self, path: &str) -> Option<FileRef> {
        self.root
            .execute(path.to_string(), |value| value.clone())
            .await
            .ok()
            .flatten()
    }

    /// Checks if a file exists at the given path.
    pub async fn file_exists(&self, path: &str) -> bool {
        self.file_get(path).await.is_some()
    }
}
