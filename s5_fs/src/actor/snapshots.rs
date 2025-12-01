use anyhow::anyhow;

#[cfg(not(target_arch = "wasm32"))]
use crate::context::DirContextParentLink;
use crate::{FSResult, dir::DirV1};
#[cfg(not(target_arch = "wasm32"))]
use s5_core::Hash;
#[cfg(not(target_arch = "wasm32"))]
use s5_core::PinContext;

use super::{ActorMessage, DirActor};

impl DirActor {
    pub(super) async fn export_merged_snapshot(&mut self) -> FSResult<DirV1> {
        let mut merged = self.state.clone();

        // If sharded, merge children
        if let Some(shards) = self.state.header.shards.clone() {
            for (index, _dir_ref) in shards {
                let handle = self.open_dir_shard(index, None).await?;
                let (tx, rx) = tokio::sync::oneshot::channel();
                // Recursive call to handle nested sharding
                handle
                    .send_msg(ActorMessage::ExportMergedSnapshot { responder: tx })
                    .await?;
                let shard_snapshot = rx.await??;

                // Merge shard_snapshot into merged
                for (k, v) in shard_snapshot.dirs {
                    merged.dirs.insert(k, v);
                }
                for (k, v) in shard_snapshot.files {
                    merged.files.insert(k, v);
                }
            }
        }

        // Clear sharding info from the merged view so it looks like a flat directory
        merged.header.shard_level = None;
        merged.header.shards = None;

        Ok(merged)
    }

    pub(super) async fn export_snapshot_at(&mut self, path: String) -> FSResult<DirV1> {
        async {
            if path.is_empty() {
                return Ok(self.state.clone());
            }

            if let Some((handle, next_path)) = self.route_to_child(&path).await? {
                let (resp, recv) = tokio::sync::oneshot::channel();
                handle
                    .send_msg(ActorMessage::ExportSnapshotAt {
                        path: next_path,
                        responder: resp,
                    })
                    .await?;
                return recv.await?;
            }

            Err(anyhow!("directory not found"))
        }
        .await
    }

    pub(super) async fn export_merged_snapshot_at(&mut self, path: String) -> FSResult<DirV1> {
        tracing::debug!("ExportMergedSnapshotAt: path={}", path);
        async {
            if path.is_empty() {
                return self.export_merged_snapshot().await;
            }

            if let Some((handle, next_path)) = self.route_to_child(&path).await? {
                let (resp, recv) = tokio::sync::oneshot::channel();
                handle
                    .send_msg(ActorMessage::ExportMergedSnapshotAt {
                        path: next_path,
                        responder: resp,
                    })
                    .await?;
                return recv.await?;
            }

            Err(anyhow!("directory not found"))
        }
        .await
    }

    /// Creates a new snapshot entry in `snapshots.fs5.cbor` and pins the
    /// current root hash as `PinContext::LocalFsSnapshot`.
    ///
    /// This is only available on native platforms as it requires filesystem access.
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn create_snapshot(&mut self) -> FSResult<(String, Hash)> {
        use std::io;

        // Only supported on the local FS5 root.
        let snapshots_path = match &self.context.link {
            DirContextParentLink::LocalFile { path, .. } => {
                let parent = path.parent().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        "Could not find parent directory for snapshots.fs5.cbor",
                    )
                })?;
                parent.join("snapshots.fs5.cbor")
            }
            _ => {
                return Err(anyhow!(
                    "create_snapshot is only supported on local FS5 roots"
                ));
            }
        };

        // Ensure we have a current hash consistent with the meta blob store
        // by asking the actor to export a snapshot hash. This works for any
        // encryption configuration and avoids re-reading from disk.
        let root_hash = {
            let h = self.export_snapshot_hash().await?;
            self.current_hash = Some(h);
            h
        };

        let mut index =
            crate::snapshots::SnapshotIndex::open(snapshots_path.parent().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "Could not find parent directory for snapshots.fs5.cbor",
                )
            })?)?;

        let (name, hash) = index.insert_snapshot(root_hash);
        index.persist()?;

        if let Some(pins) = &self.context.pins {
            pins.pin_hash(
                root_hash,
                PinContext::LocalFsSnapshot {
                    root_hash: root_hash.into(),
                },
            )
            .await?;
        }

        Ok((name, hash))
    }

    /// Deletes a named snapshot from `snapshots.fs5.cbor` and unpins its
    /// `PinContext::LocalFsSnapshot` entry, if present. This is a
    /// best-effort operation: failures to read or decode the snapshots
    /// index are treated as no-ops rather than hard errors.
    ///
    /// This is only available on native platforms as it requires filesystem access.
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn delete_snapshot(&mut self, name: String) -> FSResult<()> {
        use std::io;

        // Only supported on the local FS5 root.
        let snapshots_path = match &self.context.link {
            DirContextParentLink::LocalFile { path, .. } => {
                let parent = path.parent().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        "Could not find parent directory for snapshots.fs5.cbor",
                    )
                })?;
                parent.join("snapshots.fs5.cbor")
            }
            _ => {
                return Err(anyhow!(
                    "delete_snapshot is only supported on local FS5 roots"
                ));
            }
        };

        let mut index = match crate::snapshots::SnapshotIndex::open(
            snapshots_path.parent().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "Could not find parent directory for snapshots.fs5.cbor",
                )
            })?,
        ) {
            Ok(idx) => idx,
            Err(e)
                if e.downcast_ref::<io::Error>()
                    .map(|ioe| ioe.kind() == io::ErrorKind::NotFound)
                    .unwrap_or(false) =>
            {
                return Ok(());
            }
            Err(e) => return Err(e),
        };

        let root_hash = match index.remove_snapshot(&name) {
            Some(h) => h,
            None => return Ok(()),
        };

        index.persist()?;

        // Best-effort unpin of the snapshot root; it's okay if this
        // snapshot was not the last pinner for this hash.
        if let Some(pins) = &self.context.pins {
            let _ = pins
                .unpin_hash(
                    root_hash,
                    PinContext::LocalFsSnapshot {
                        root_hash: *root_hash.as_bytes(),
                    },
                )
                .await;
        }

        Ok(())
    }
}
