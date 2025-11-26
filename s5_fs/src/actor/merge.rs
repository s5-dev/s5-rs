use std::collections::BTreeMap;

use crate::dir::{DirRef, DirV1, FileRef};

use super::sharding::shard_bucket_for;
use super::{ActorMessage, DirActor};

impl DirActor {
    // Implements Last-Write-Wins (LWW) conflict resolution based on timestamps.
    //
    // - `FileRef` version chains (`prev`/`first_version`/`version_count`) are
    //   preserved as-is from the winning side (local vs remote).
    // - Tombstone entries represent deletions while still keeping
    //   historical content in the version chain.
    //
    // If the local directory is sharded, incoming entries are routed to the
    // appropriate shard actors for merge. Merging a sharded remote snapshot
    // into a non-sharded local directory is not fully supported and will
    // log a warning (only top-level entries are merged in that case).
    pub(super) async fn merge_snapshot(&mut self, snapshot: DirV1) -> crate::FSResult<()> {
        let DirV1 {
            header,
            dirs,
            files,
            ..
        } = snapshot;

        let local_sharded = self.state.header.shard_level.is_some();
        let remote_sharded = header.shard_level.is_some();

        // If remote is sharded but local is not, we can only merge top-level
        // entries. This is a limitation - ideally we'd expand remote shards.
        if remote_sharded && !local_sharded {
            tracing::warn!(
                "merge_snapshot: remote is sharded but local is not; \
                 only top-level entries will be merged"
            );
        }

        // If local is sharded, route entries to shard actors.
        if let Some(shard_level) = self.state.header.shard_level {
            return self
                .merge_snapshot_into_shards(shard_level, dirs, files, &header)
                .await;
        }

        // Non-sharded merge: apply LWW directly to local state.
        self.merge_entries_local(dirs, files);

        // Merge header fields selectively.
        self.merge_header_fields(&header);

        self.mark_as_dirty().await;
        Ok(())
    }

    /// Merges entries into a sharded directory by routing to shard actors.
    async fn merge_snapshot_into_shards(
        &mut self,
        shard_level: u8,
        dirs: BTreeMap<String, DirRef>,
        files: BTreeMap<String, FileRef>,
        remote_header: &crate::dir::DirHeader,
    ) -> crate::FSResult<()> {
        // Group entries by shard bucket.
        let mut shard_dirs: BTreeMap<u8, BTreeMap<String, DirRef>> = BTreeMap::new();
        let mut shard_files: BTreeMap<u8, BTreeMap<String, FileRef>> = BTreeMap::new();

        for (name, dir_ref) in dirs {
            let bucket = shard_bucket_for(&name, shard_level);
            shard_dirs.entry(bucket).or_default().insert(name, dir_ref);
        }

        for (name, file_ref) in files {
            let bucket = shard_bucket_for(&name, shard_level);
            shard_files
                .entry(bucket)
                .or_default()
                .insert(name, file_ref);
        }

        // Merge into each shard that has incoming entries.
        let buckets: std::collections::HashSet<u8> = shard_dirs
            .keys()
            .chain(shard_files.keys())
            .copied()
            .collect();

        for bucket in buckets {
            let bucket_dirs = shard_dirs.remove(&bucket).unwrap_or_default();
            let bucket_files = shard_files.remove(&bucket).unwrap_or_default();

            // Build a mini-snapshot for this shard.
            let mut shard_snapshot = DirV1::new();
            shard_snapshot.dirs = bucket_dirs;
            shard_snapshot.files = bucket_files;

            // Get or create shard actor.
            if self
                .state
                .header
                .shards
                .as_ref()
                .is_some_and(|s| s.contains_key(&bucket))
            {
                let handle = self.open_dir_shard(bucket, None).await?;
                let (tx, rx) = tokio::sync::oneshot::channel();
                handle
                    .send_msg(ActorMessage::MergeSnapshot {
                        snapshot: shard_snapshot,
                        responder: tx,
                    })
                    .await?;
                rx.await??;
            } else {
                // Shard doesn't exist yet - this shouldn't normally happen
                // since sharding creates all needed shards, but handle gracefully.
                tracing::warn!(
                    "merge_snapshot: shard bucket {} doesn't exist, entries will be lost",
                    bucket
                );
            }
        }

        // Merge header fields selectively.
        self.merge_header_fields(remote_header);

        self.mark_as_dirty().await;
        Ok(())
    }

    /// Applies LWW merge of entries directly into local state (non-sharded case).
    fn merge_entries_local(
        &mut self,
        dirs: BTreeMap<String, DirRef>,
        files: BTreeMap<String, FileRef>,
    ) {
        // Directories: LWW against local files/dirs.
        for (name, remote_dir) in dirs {
            let remote_ts = dir_ts(&remote_dir);

            // Check conflict with local file
            if let Some(local_file) = self.state.files.get(&name) {
                if remote_ts > file_ts(local_file) {
                    // Remote dir wins over local file
                    self.state.files.remove(&name);
                    self.dir_handles.remove(&name);
                    self.state.dirs.insert(name, remote_dir);
                }
                continue;
            }

            // Check conflict with local dir
            if let Some(local_dir) = self.state.dirs.get(&name) {
                if remote_ts > dir_ts(local_dir) {
                    // Remote dir wins over local dir
                    self.dir_handles.remove(&name);
                    self.state.dirs.insert(name, remote_dir);
                }
                continue;
            }

            // No conflict, insert
            self.state.dirs.insert(name, remote_dir);
        }

        // Files and tombstones
        for (name, remote_file) in files {
            let remote_ts = file_ts(&remote_file);

            // Check conflict with local dir
            if let Some(local_dir) = self.state.dirs.get(&name) {
                if remote_ts > dir_ts(local_dir) {
                    // Remote file (including tombstone) wins over local dir
                    self.dir_handles.remove(&name);
                    self.state.dirs.remove(&name);
                    self.state.files.insert(name.clone(), remote_file);
                }
                continue;
            }

            // Check conflict with local file (including tombstone)
            if let Some(local_file) = self.state.files.get(&name) {
                if remote_ts > file_ts(local_file) {
                    // Remote file wins over local file
                    self.state.files.insert(name.clone(), remote_file);
                }
                continue;
            }

            // No conflict, insert
            self.state.files.insert(name, remote_file);
        }
    }

    /// Merges header fields from remote, preserving local shard structure.
    fn merge_header_fields(&mut self, header: &crate::dir::DirHeader) {
        // Web-serving hints are taken from remote if not set locally.
        if self.state.header.try_files.is_none() {
            self.state.header.try_files = header.try_files.clone();
        }
        if self.state.header.error_pages.is_none() {
            self.state.header.error_pages = header.error_pages.clone();
        }
        // Note: shard_level, shards, ops_counter, last_written_by are intentionally
        // NOT merged - they represent local storage layout and operational state.
    }
}

// Helper to get timestamp from DirRef
fn dir_ts(d: &DirRef) -> u64 {
    let s = d.ts_seconds.unwrap_or(0) as u64;
    let n = d.ts_nanos.unwrap_or(0) as u64;
    s * 1_000_000_000 + n
}

// Helper to get timestamp from FileRef
fn file_ts(f: &FileRef) -> u64 {
    let s = f.timestamp.unwrap_or(0) as u64;
    let n = f.timestamp_subsec_nanos.unwrap_or(0) as u64;
    s * 1_000_000_000 + n
}
