use crate::{
    FS5, FSResult,
    dir::{DirRef, DirRefType, DirV1, FileRef, FileRefType},
};
use std::collections::BTreeMap;

use crate::actor::sharding::shard_bucket_for;

/// Print a debug tree of the FS5 directory structure starting at `path`.
///
/// `path` is a logical path inside the FS5 root ("" or "/" for root).
pub async fn print_tree(fs: &FS5, path: &str) -> FSResult<()> {
    let norm = normalize_path(path);
    let dir = if norm.is_empty() {
        fs.export_snapshot().await?
    } else {
        fs.export_snapshot_at(&norm).await?
    };

    let label = if norm.is_empty() {
        ".".to_string()
    } else {
        format!("./{}", norm)
    };

    print_summary(&dir, &label);
    print_dir_iterative(fs, dir, norm).await
}

/// Print a one-line summary for a DirV1 snapshot, including counts and
/// shard metadata. This is useful when inspecting GC/verify behavior to
/// quickly understand the logical shape of the tree.
fn print_summary(dir: &DirV1, label: &str) {
    let dirs = dir.dirs.len();
    let files = dir.files.len();
    let shard_level = dir.header.shard_level.unwrap_or(0);
    let shard_count = dir.header.shards.as_ref().map(|m| m.len()).unwrap_or(0);
    println!(
        "{} [DirV1 dirs={} files={} shard_level={} shards={}]",
        label, dirs, files, shard_level, shard_count
    );
}

fn normalize_path(path: &str) -> String {
    let trimmed = path.trim_matches('/');
    trimmed.to_string()
}

#[derive(Clone)]
struct Entry {
    name: String,
    kind: EntryKind,
}

#[derive(Clone)]
enum EntryKind {
    Dir(DirRef),
    File(FileRef),
    Shard(u8, DirRef),
}

fn shard_bucket(name: &str, shard_level: u8) -> u8 {
    // Delegate to the canonical implementation in sharding module.
    // Note: shard_bucket_for panics if shard_level > MAX_SHARD_LEVEL (7),
    // but that's an invariant violation that should be caught.
    shard_bucket_for(name, shard_level)
}

fn collect_entries(dir: &DirV1) -> Vec<Entry> {
    let mut entries: Vec<Entry> = Vec::new();

    // If the directory is sharded, prefer to present entries grouped by
    // shard so the physical layout is visible. Non-sharded directories
    // use a simple flat listing.
    if dir.header.shards.is_some() && dir.header.shard_level.is_some() {
        if let Some(shards) = &dir.header.shards {
            let mut shard_indices: Vec<u8> = shards.keys().copied().collect();
            shard_indices.sort_unstable();
            for index in shard_indices {
                if let Some(dir_ref) = shards.get(&index) {
                    entries.push(Entry {
                        name: format!("[shard 0x{:02x}]", index),
                        kind: EntryKind::Shard(index, dir_ref.clone()),
                    });
                }
            }
        }
        return entries;
    }

    for (name, dir_ref) in &dir.dirs {
        entries.push(Entry {
            name: format!("{}/", name),
            kind: EntryKind::Dir(dir_ref.clone()),
        });
    }

    for (name, file_ref) in &dir.files {
        entries.push(Entry {
            name: name.clone(),
            kind: EntryKind::File(file_ref.clone()),
        });
    }

    entries
}

struct Frame {
    dir: DirV1,
    path: String,
    prefix: String,
    entries: Vec<Entry>,
    index: usize,
}

async fn print_dir_iterative(fs: &FS5, root_dir: DirV1, root_path: String) -> FSResult<()> {
    let mut stack: Vec<Frame> = Vec::new();
    stack.push(Frame {
        dir: root_dir,
        path: root_path,
        prefix: String::new(),
        entries: Vec::new(),
        index: 0,
    });

    while let Some(frame) = stack.last_mut() {
        if frame.entries.is_empty() {
            frame.entries = collect_entries(&frame.dir);
        }

        if frame.index >= frame.entries.len() {
            stack.pop();
            continue;
        }

        // Clone the current frame metadata we need so we can safely
        // mutate the stack (push new frames) later without violating
        // Rust's borrowing rules.
        let current_prefix = frame.prefix.clone();
        let current_path = frame.path.clone();
        let current_dir = frame.dir.clone();

        let entry = frame.entries[frame.index].clone();
        let is_last = frame.index + 1 == frame.entries.len();
        frame.index += 1;

        let (branch, child_prefix_piece) = if is_last {
            ("└── ", "    ")
        } else {
            ("├── ", "│   ")
        };

        let line_prefix = format!("{}{}", current_prefix, branch);
        let child_prefix = format!("{}{}", current_prefix, child_prefix_piece);

        print!("{}", line_prefix);

        match entry.kind {
            EntryKind::File(f) => {
                let ty = match f.ref_type() {
                    FileRefType::Blake3Hash => "blob",
                    FileRefType::RegistryKey => "registry",
                    FileRefType::Tombstone => "tombstone",
                };
                let hash_short = short_hash_bytes(&f.hash);
                println!(
                    "{} [FileRef type={} size={} hash={}...]",
                    entry.name, ty, f.size, hash_short
                );
            }
            EntryKind::Dir(dir_ref) => {
                let ty = match dir_ref.ref_type() {
                    DirRefType::Blake3Hash => "blake3",
                    DirRefType::RegistryKey => "registry",
                };
                let hash_short = short_hash_bytes(&dir_ref.hash);
                let enc = if dir_ref.encryption_type.is_some() {
                    "enc"
                } else {
                    "plain"
                };
                println!(
                    "{} [DirRef type={} {} hash={}...]",
                    entry.name, ty, enc, hash_short
                );

                let next_path = if frame.path.is_empty() {
                    entry.name.trim_end_matches('/').to_string()
                } else {
                    format!("{}/{}", frame.path, entry.name.trim_end_matches('/'))
                };

                match fs.export_snapshot_at(&next_path).await {
                    Ok(sub_dir) => {
                        stack.push(Frame {
                            dir: sub_dir,
                            path: next_path,
                            prefix: child_prefix,
                            entries: Vec::new(),
                            index: 0,
                        });
                    }
                    Err(err) => {
                        println!(
                            "{}[BROKEN SUBTREE at {}: failed to load directory state: {}]",
                            child_prefix, next_path, err
                        );
                    }
                };
            }
            EntryKind::Shard(index, dir_ref) => {
                let ty = match dir_ref.ref_type() {
                    DirRefType::Blake3Hash => "blake3",
                    DirRefType::RegistryKey => "registry",
                };
                let hash_short = short_hash_bytes(&dir_ref.hash);
                let enc = if dir_ref.encryption_type.is_some() {
                    "enc"
                } else {
                    "plain"
                };
                println!(
                    "{} [Shard index=0x{:02x} type={} {} hash={}...]",
                    entry.name, index, ty, enc, hash_short
                );

                // Build a synthetic view of the shard contents by grouping the
                // current directory snapshot according to the shard's bucket.
                if let Some(shard_level) = current_dir.header.shard_level {
                    let mut shard_dirs: BTreeMap<String, DirRef> = BTreeMap::new();
                    let mut shard_files: BTreeMap<String, FileRef> = BTreeMap::new();

                    for (name, dref) in &current_dir.dirs {
                        let bucket = shard_bucket(name, shard_level);
                        if bucket == index {
                            shard_dirs.insert(name.clone(), dref.clone());
                        }
                    }

                    for (name, fref) in &current_dir.files {
                        let bucket = shard_bucket(name, shard_level);
                        if bucket == index {
                            shard_files.insert(name.clone(), fref.clone());
                        }
                    }

                    if !shard_dirs.is_empty() || !shard_files.is_empty() {
                        let mut shard_dir = DirV1::new();
                        shard_dir.dirs = shard_dirs;
                        shard_dir.files = shard_files;

                        stack.push(Frame {
                            dir: shard_dir,
                            path: current_path.clone(),
                            prefix: child_prefix,
                            entries: Vec::new(),
                            index: 0,
                        });
                    }
                }
            }
        }
    }

    Ok(())
}

fn short_hash_bytes(bytes: &[u8; 32]) -> String {
    let mut output = Vec::with_capacity(base32_fs::encoded_len(bytes.len()));
    base32_fs::encode(bytes, &mut output);
    let s = String::from_utf8(output).unwrap();
    s.chars().take(8).collect()
}
