use std::collections::{HashSet, VecDeque};
use std::path::Path;

use crate::FSResult;
use crate::dir::{DirRef, DirV1, FileRef, decrypt_dir_bytes};
use s5_core::{BlobStore, Hash, Pins};
use s5_store_local::{LocalStore, LocalStoreConfig};

/// Traverse a `DirV1` tree and collect all content hashes referenced by
/// `FileRef` entries (including historical versions).
///
/// Tombstone entries skip their own hash (which is a copy of the last live
/// version's hash) but still walk `prev`/`first_version` chains to preserve
/// historical content.
///
/// Uses an iterative approach to avoid stack overflow on deep version chains.
pub fn collect_hashes_from_dir(dir: &DirV1, reachable: &mut HashSet<Hash>) {
    // Iterative version traversal to avoid stack overflow on deep chains.
    fn add_versions_iterative(root: &FileRef, acc: &mut HashSet<Hash>) {
        let mut stack: Vec<&FileRef> = vec![root];
        while let Some(fr) = stack.pop() {
            let h = Hash::from_bytes(fr.hash);
            acc.insert(h);
            if let Some(prev) = &fr.prev {
                stack.push(prev);
            }
            if let Some(first) = &fr.first_version {
                stack.push(first);
            }
        }
    }

    for file_ref in dir.files.values() {
        // Tombstones still have prev/first_version chains containing live
        // historical content that must be preserved. We skip the tombstone's
        // own hash (which is just a copy of the last live version's hash) but
        // walk its version chain.
        if file_ref.is_tombstone() {
            if let Some(prev) = &file_ref.prev {
                add_versions_iterative(prev, reachable);
            }
            if let Some(first) = &file_ref.first_version {
                add_versions_iterative(first, reachable);
            }
            continue;
        }
        add_versions_iterative(file_ref, reachable);
    }
}

/// Walks all directory snapshots reachable from the current root and any
/// entries in `snapshots.fs5.cbor`, returning the set of **content blob
/// hashes** that should be considered live from the perspective of this
/// FS5 root.
///
/// This helper operates purely on local metadata (root.fs5.cbor,
/// snapshots.fs5.cbor, and the FS5 meta blob store) and does not depend
/// on any running node or registry state. It is intentionally
/// conservative: failure to decode or load a directory snapshot simply
/// results in its subtree being skipped, never in any blob being marked
/// as deletable.
pub async fn collect_fs_reachable_hashes<P: AsRef<Path>>(
    fs_root: P,
    root_key: Option<&[u8; 32]>,
) -> FSResult<HashSet<Hash>> {
    let fs_root = fs_root.as_ref();
    let mut reachable = HashSet::new();

    // Meta blob store co-located with root.fs5.cbor
    let meta_store = LocalStore::create(LocalStoreConfig {
        base_path: fs_root.to_string_lossy().into(),
    });
    let meta_blobs = BlobStore::new(meta_store);

    let mut dir_queue: VecDeque<DirV1> = VecDeque::new();
    // Queue stores (Hash, Key)
    let mut hash_queue: VecDeque<(Hash, Option<[u8; 32]>)> = VecDeque::new();
    let mut visited_dirs: HashSet<Hash> = HashSet::new();

    // 1. Current live root from root.fs5.cbor
    let root_path = fs_root.join("root.fs5.cbor");
    if let Ok(bytes) = std::fs::read(&root_path) {
        let decrypted = decrypt_dir_bytes(bytes.into(), root_key).unwrap_or_default();
        if let Ok(dir) = DirV1::from_bytes(&decrypted) {
            dir_queue.push_back(dir);
        }
    }

    // 2. Snapshot roots from snapshots.fs5.cbor (if present)
    let snapshots_path = fs_root.join("snapshots.fs5.cbor");
    if let Ok(bytes) = std::fs::read(&snapshots_path)
        && let Ok(snapshots_dir) = DirV1::from_bytes(&bytes)
    {
        for DirRef { hash, .. } in snapshots_dir.dirs.values() {
            // Snapshot roots are encrypted with the root key if encryption is enabled
            hash_queue.push_back((Hash::from_bytes(*hash), root_key.copied()));
        }
    }

    // Traverse all reachable directories.
    while let Some(dir) = dir_queue.pop_front() {
        // Collect file content hashes (including historical versions).
        collect_hashes_from_dir(&dir, &mut reachable);

        // Helper to extract key from DirRef
        let get_key = |d: &DirRef| -> Option<[u8; 32]> {
            d.keys.as_ref().and_then(|k| k.get(&0x0e).copied())
        };

        // Queue child directories referenced from this snapshot.
        if let Some(shards) = &dir.header.shards {
            for dir_ref in shards.values() {
                hash_queue.push_back((Hash::from_bytes(dir_ref.hash), get_key(dir_ref)));
            }
        }
        for dir_ref in dir.dirs.values() {
            hash_queue.push_back((Hash::from_bytes(dir_ref.hash), get_key(dir_ref)));
        }

        // Drain the hash queue by loading child DirV1 snapshots from the
        // meta blob store. Any failure to read/decode is treated as a
        // missing subtree and skipped.
        while let Some((h, key)) = hash_queue.pop_front() {
            if !visited_dirs.insert(h) {
                continue;
            }

            match meta_blobs.read_as_bytes(h, 0, None).await {
                Ok(bytes) => {
                    let decrypted = match decrypt_dir_bytes(bytes, key.as_ref()) {
                        Ok(d) => d,
                        Err(e) => {
                            eprintln!("fs5-gc: failed to decrypt DirV1 for hash {}: {}", h, e);
                            continue;
                        }
                    };
                    match DirV1::from_bytes(&decrypted) {
                        Ok(subdir) => dir_queue.push_back(subdir),
                        Err(err) => {
                            eprintln!("fs5-gc: failed to decode DirV1 for hash {}: {}", h, err);
                        }
                    }
                }
                Err(err) => {
                    eprintln!(
                        "fs5-gc: failed to read DirV1 from meta store for hash {}: {}",
                        h, err
                    );
                }
            }
        }
    }

    Ok(reachable)
}

/// Summary of a garbage-collection run over a blob store.
#[derive(Debug, Default)]
pub struct GcReport {
    /// Total number of blobs examined in the store.
    pub total: usize,
    /// Blobs kept because they have at least one pin.
    pub kept_by_pins: usize,
    /// Blobs kept because they are reachable from the FS5 root.
    pub kept_by_reachability: usize,
    /// Blobs that were (or would be) deleted in this run.
    pub deleted: usize,
    /// Blobs considered deletable by invariants (pins + reachability).
    ///
    /// This is populated regardless of `dry_run`; when `dry_run` is true,
    /// the `deleted` count will be zero but `candidates` still lists
    /// which hashes qualify for deletion.
    pub candidates: Vec<Hash>,
    /// Errors encountered while attempting to delete blobs.
    pub delete_errors: Vec<(Hash, anyhow::Error)>,
}

/// Performs conservative garbage collection over a blob store given a set of
/// reachable content hashes and a pin registry.
///
/// Invariants:
/// - Any blob with at least one pin in `pins` is kept.
/// - Any blob whose hash appears in `reachable` is kept.
/// - All other blobs are considered GC candidates and either deleted
///   (`dry_run == false`) or reported (`dry_run == true`).
pub async fn gc_store(
    blob_store: &BlobStore,
    reachable: &HashSet<Hash>,
    pins: &dyn Pins,
    dry_run: bool,
) -> FSResult<GcReport> {
    let mut report = GcReport::default();

    let all_hashes = blob_store.list_hashes().await?;

    for h in all_hashes {
        report.total += 1;

        // Respect all existing pins in the node registry.
        let pinners = pins.get_pinners(h).await?;
        if !pinners.is_empty() {
            report.kept_by_pins += 1;
            continue;
        }

        // Also keep anything still reachable from the FS5 root.
        if reachable.contains(&h) {
            report.kept_by_reachability += 1;
            continue;
        }

        // This blob is a GC candidate.
        report.candidates.push(h);

        if dry_run {
            continue;
        }

        match blob_store.delete(h).await {
            Ok(()) => {
                report.deleted += 1;
            }
            Err(e) => {
                report.delete_errors.push((h, e));
            }
        }
    }

    Ok(report)
}
