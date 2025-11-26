use std::collections::BTreeMap;

use crate::api::{CursorData, CursorKind, decode_cursor, encode_cursor};
use anyhow::anyhow;
use tokio::sync::oneshot;

use super::{ActorMessage, DirActor, ListResult};

impl DirActor {
    pub(super) async fn list_entries(&mut self, cursor: Option<&str>, limit: usize) -> ListResult {
        use std::ops::Bound::{Excluded, Included, Unbounded};

        // Build a logical view over this directory by using a merged
        // snapshot so that sharded (and nested-sharded) layouts appear
        // as a flat directory to callers.
        // TODO(perf): optionally cache merged shard name sets for read-heavy
        // listing workloads and invalidate on child save, if profiles show
        // `export_merged_snapshot` and key cloning dominate list() costs.
        let snapshot = self.export_merged_snapshot().await?;

        let mut all_dirs: BTreeMap<String, ()> = BTreeMap::new();
        let mut all_files: BTreeMap<String, ()> = BTreeMap::new();

        for name in snapshot.dirs.keys() {
            all_dirs.insert(name.clone(), ());
        }
        for name in snapshot.files.keys() {
            all_files.insert(name.clone(), ());
        }

        let start = cursor.and_then(decode_cursor).map(|c| (c.position, c.kind));
        let (dirs_start, files_start) = match &start {
            None => (Unbounded, Unbounded),
            Some((name, CursorKind::Directory)) => {
                (Excluded(name.as_str()), Included(name.as_str()))
            }
            Some((name, CursorKind::File)) => (Excluded(name.as_str()), Excluded(name.as_str())),
        };

        let mut it_dirs = all_dirs
            .range::<str, _>((dirs_start, Unbounded))
            .map(|(k, _)| (k.as_str(), CursorKind::Directory))
            .peekable();
        let mut it_files = all_files
            .range::<str, _>((files_start, Unbounded))
            .map(|(k, _)| (k.as_str(), CursorKind::File))
            .peekable();

        let mut out: Vec<(String, CursorKind)> = Vec::with_capacity(limit.min(1024));
        while out.len() < limit {
            match (it_dirs.peek(), it_files.peek()) {
                (Some((d, _)), Some((f, _))) => {
                    if d < f {
                        let (name, kind) = it_dirs.next().unwrap();
                        out.push((name.to_string(), kind));
                    } else if f < d {
                        let (name, kind) = it_files.next().unwrap();
                        out.push((name.to_string(), kind));
                    } else {
                        let (dname, dkind) = it_dirs.next().unwrap();
                        out.push((dname.to_string(), dkind));
                        if out.len() == limit {
                            break;
                        }
                        let (_fname, fkind) = it_files.next().unwrap();
                        out.push((dname.to_string(), fkind));
                    }
                }
                (Some(_), None) => {
                    let (name, kind) = it_dirs.next().unwrap();
                    out.push((name.to_string(), kind));
                }
                (None, Some(_)) => {
                    let (name, kind) = it_files.next().unwrap();
                    out.push((name.to_string(), kind));
                }
                (None, None) => break,
            }
        }

        let next = out.last().map(|(name, kind)| {
            encode_cursor(&CursorData {
                position: name.clone(),
                kind: kind.clone(),
                timestamp: None,
                path: None,
            })
        });

        Ok((out, next))
    }

    pub(super) async fn list_at_path(
        &mut self,
        path: String,
        cursor: Option<String>,
        limit: usize,
    ) -> ListResult {
        async {
            if path.is_empty() {
                return self.list_entries(cursor.as_deref(), limit).await;
            }

            if let Some((handle, next_path)) = self.route_to_child(&path).await? {
                let (resp, recv) = oneshot::channel();
                handle
                    .send_msg(ActorMessage::ListAt {
                        path: next_path,
                        cursor,
                        limit,
                        responder: resp,
                    })
                    .await?;
                return recv.await?;
            }

            Err(anyhow!("directory not found"))
        }
        .await
    }
}
