//! D21 `copy` — the one new FS5 mechanism that all sharing composes from.
//!
//! A **shallow** copy re-homes a source subtree into a destination vault
//! WITHOUT re-encrypting the leaf data:
//!
//! * each leaf's ciphertext blob is replicated verbatim (content-addressed,
//!   so a shared store is a no-op — see [`BlobReplicator`]), and
//! * the *per-blob* ChaCha20 key is inlined into the destination entry's
//!   `child_context` under
//!   [`KEY_SLOT_EXPLICIT_LEAF`](crate::snapshot::KEY_SLOT_EXPLICIT_LEAF),
//!   referenced by an
//!   [`ExplicitKeyChaCha20`](crate::node::EncryptionStrategy::ExplicitKeyChaCha20)
//!   leaf pipeline that PINS the source's compression + padding.
//!
//! That `child_context` rides inside the enclosing namespace / byte-stream
//! [`Node`], which is re-encoded under the DESTINATION node keys — so a
//! non-destination reader can't even decode the node, let alone see the
//! inlined key. Crucially the **source master data key is never shared**:
//! only the per-blob key `derive_blob_key(KDF_LEAF, src_master,
//! plaintext_hash)` ever leaves the source vault, and only into nodes the
//! destination reader can already decrypt.
//!
//! Confidentiality holds ONLY if the destination node pipeline is encrypting
//! (otherwise the inlined keys would sit in plaintext nodes). The daemon
//! [`crate::copy`] caller enforces that guard before invoking a shallow copy.
//!
//! A **deep** copy ([`deep_copy_into`]) instead re-imports the source
//! *plaintext* under the destination's own derivation — brand-new ciphertext,
//! brand-new hashes, no inlined keys — which is what makes a share truly
//! future-revocable.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use s5_core::{BlobId, BlobsRead, BlobsWrite, Hash};

use crate::node::{
    BlobPipeline, CompressionStrategy, ContentRef, EncryptionStrategy, Node, NodeEntry, Structural,
    TraversalContext,
};
use crate::persist::MergeStats;
use crate::pipeline::Pipeline;
use crate::snapshot::{KEY_SLOT_EXPLICIT_LEAF, Snapshot, merge_contexts};

// ---------------------------------------------------------------------------
// Blob replication (keeps s5_fs_v2 free of NodeStores)
// ---------------------------------------------------------------------------

/// Copies a source leaf's *ciphertext* blob into the destination store.
///
/// The daemon supplies the concrete download → `blob_contains`-gate → upload
/// implementation; keeping it a trait means `s5_fs_v2` never depends on the
/// node's store map. Contract:
///
/// * **Idempotent** and a **no-op when the stores are shared** (the blob is
///   already present → `blob_contains` short-circuits → zero byte movement).
/// * **MUST NOT re-encrypt.** Ciphertext is reused byte-for-byte, so the
///   content-addressed hash is preserved — that is the whole point.
#[async_trait]
pub trait BlobReplicator: Send + Sync {
    async fn replicate(&self, hash: Hash) -> anyhow::Result<()>;
}

// ---------------------------------------------------------------------------
// Public helpers
// ---------------------------------------------------------------------------

/// The per-blob leaf key a shallow `copy` inlines:
/// `derive_blob_key(KDF_LEAF, master, plaintext_hash)`.
///
/// Exposed so daemon-side tests can assert that the inlined key is the
/// PER-BLOB key and never the source master.
pub fn source_leaf_blob_key(master: &[u8; 32], plaintext_hash: &[u8; 32]) -> [u8; 32] {
    crate::context::leaf_blob_key(master, plaintext_hash)
}

/// Re-root a source walk path into the destination namespace.
///
/// `src_prefix` (if given, without a required trailing slash) filters the
/// walk to that subtree and strips it; `dst_prefix` re-homes the remainder
/// under that path. Returns `None` for paths outside the subtree, or when the
/// result would be empty (e.g. the subtree-root directory entry itself).
pub fn reroot(path: &str, src_prefix: Option<&str>, dst_prefix: Option<&str>) -> Option<String> {
    let rel = match src_prefix {
        None | Some("") => path.to_string(),
        Some(pre) => {
            let pre = pre.trim_end_matches('/');
            if path == pre {
                String::new()
            } else if let Some(r) = path.strip_prefix(&format!("{pre}/")) {
                r.to_string()
            } else {
                return None;
            }
        }
    };
    let out = match dst_prefix {
        None | Some("") => rel,
        Some(dp) => {
            let dp = dp.trim_end_matches('/');
            if rel.is_empty() {
                dp.to_string()
            } else {
                format!("{dp}/{rel}")
            }
        }
    };
    if out.is_empty() { None } else { Some(out) }
}

// ---------------------------------------------------------------------------
// Shallow copy (ciphertext reuse + inlined per-blob keys)
// ---------------------------------------------------------------------------

/// Mirror `src`'s subtree into a set of destination namespace entries,
/// reusing leaf ciphertext and inlining each leaf's per-blob key.
///
/// The returned map (destination path → entry) is what the caller wraps in a
/// [`MapLayer`](crate::layer::MapLayer) and hands to
/// [`Snapshot::merge_and_persist`] on the destination snapshot — leaf data is
/// never touched by that merge; only the destination namespace tree is built.
///
/// `src_leaf_master` is the source vault's leaf master (the key its leaf
/// pipeline references); `None` for a plaintext source. It is used ONLY to
/// derive per-blob keys and is never written into any destination structure.
#[allow(clippy::too_many_arguments)]
pub async fn shallow_copy_into(
    src: &Snapshot,
    src_subtree_prefix: Option<&str>,
    src_leaf_master: Option<&[u8; 32]>,
    dst_ctx: &TraversalContext,
    dst_write: &dyn BlobsWrite,
    dst_read: Arc<dyn BlobsRead>,
    dst_path_prefix: Option<&str>,
    repl: &dyn BlobReplicator,
) -> anyhow::Result<BTreeMap<String, NodeEntry>> {
    let dst_pipe = Pipeline::new(dst_read, dst_ctx.clone());
    let src_pipe = src.as_pipeline();
    let mut stats = MergeStats::default();
    let mut out: BTreeMap<String, NodeEntry> = BTreeMap::new();

    let mut walk = src.walk();
    while let Some(item) = walk.next().await {
        let (path, entry) = item?;
        let Some(dst_path) = reroot(&path, src_subtree_prefix, dst_path_prefix) else {
            continue;
        };

        let Some(content) = entry.content.as_ref() else {
            // Metadata-only entry (directory). Carry its semantic; drop any
            // child_context so no source key material can ride along
            // (break point #3 — rebuilt entries never inherit source keys).
            out.insert(
                dst_path,
                NodeEntry {
                    content: None,
                    semantic: entry.semantic.clone(),
                    child_context: None,
                    tombstone: None,
                },
            );
            continue;
        };

        if entry.is_link() {
            // Chunked file (Link → ByteStream): faithfully mirror the chunk
            // tree, re-encoding every node under the DEST node keys and
            // inlining each chunk's per-blob key.
            let child_src = src_pipe.child_for(&entry);
            let (blob_id, pph) = reencode_subtree(
                &dst_pipe,
                dst_write,
                &child_src,
                content.hash(),
                content.plaintext_hash,
                src_leaf_master,
                repl,
                &mut stats,
            )
            .await?;
            out.insert(
                dst_path,
                NodeEntry {
                    content: Some(ContentRef {
                        structural: Structural::Link,
                        hash: *blob_id.hash.as_bytes(),
                        size: content.size,
                        plaintext_hash: Some(pph),
                        stored_blocks: Some(blob_id.size),
                    }),
                    semantic: entry.semantic.clone(),
                    child_context: None, // inherit DEST node keys
                    tombstone: None,
                },
            );
        } else {
            // Single-leaf file: replicate the ciphertext + inline its key.
            repl.replicate(content.hash()).await?;
            let src_eff = match &entry.child_context {
                Some(cc) => merge_contexts(src.context(), cc),
                None => src.context().clone(),
            };
            let per_blob_key = effective_leaf_key(&src_eff, content, src_leaf_master)?;
            let child_context = inlined_leaf_child_context(src_eff.leaf.as_ref(), per_blob_key);
            out.insert(
                dst_path,
                NodeEntry {
                    content: Some(content.clone()),
                    semantic: entry.semantic.clone(),
                    child_context,
                    tombstone: None,
                },
            );
        }
    }

    Ok(out)
}

/// Faithfully mirror a source byte-stream subtree into the destination,
/// re-encoding each node under the DEST node keys and inlining every chunk
/// leaf's per-blob key. Returns the destination node's `(BlobId,
/// plaintext_hash)`.
///
/// Chunk structure (levels, per-node headers, entry keys) is preserved
/// exactly, so the destination reads back byte-identically. We do NOT use
/// `walk_byte_stream` here — that flattens the tree and would drop the
/// per-chunk keys/pipelines the inline needs.
#[allow(clippy::too_many_arguments)]
async fn reencode_subtree(
    dst_pipe: &Pipeline,
    dst_write: &dyn BlobsWrite,
    src_pipe: &Pipeline,
    node_hash: Hash,
    node_ph: Option<[u8; 32]>,
    src_leaf_master: Option<&[u8; 32]>,
    repl: &dyn BlobReplicator,
    stats: &mut MergeStats,
) -> anyhow::Result<(BlobId, [u8; 32])> {
    let node = src_pipe.load(node_hash, node_ph.as_ref()).await?;

    let mut new = Node::new();
    new.magic = node.magic.clone();
    new.header = node.header.clone();

    for (key, entry) in &node.entries {
        let Some(content) = entry.content.as_ref() else {
            // Tombstone / metadata-only: no content, no key material — clone.
            new.entries.insert(key.clone(), entry.clone());
            continue;
        };

        if entry.is_link() {
            // Internal byte-stream level: descend under the source child
            // context, re-encode under the DEST node keys.
            let child_src = src_pipe.child_for(entry);
            let (blob_id, pph) = Box::pin(reencode_subtree(
                dst_pipe,
                dst_write,
                &child_src,
                content.hash(),
                content.plaintext_hash,
                src_leaf_master,
                repl,
                stats,
            ))
            .await?;
            new.entries.insert(
                key.clone(),
                NodeEntry {
                    content: Some(ContentRef {
                        structural: Structural::Link,
                        hash: *blob_id.hash.as_bytes(),
                        size: content.size,
                        plaintext_hash: Some(pph),
                        stored_blocks: Some(blob_id.size),
                    }),
                    semantic: entry.semantic.clone(),
                    child_context: None, // inherit DEST node keys
                    tombstone: None,
                },
            );
        } else {
            // Leaf chunk: replicate its ciphertext + inline the per-blob key.
            repl.replicate(content.hash()).await?;
            let chunk_eff = match &entry.child_context {
                Some(cc) => merge_contexts(src_pipe.context(), cc),
                None => src_pipe.context().clone(),
            };
            let per_blob_key = effective_leaf_key(&chunk_eff, content, src_leaf_master)?;
            let child_context = inlined_leaf_child_context(chunk_eff.leaf.as_ref(), per_blob_key);
            new.entries.insert(
                key.clone(),
                NodeEntry {
                    content: Some(content.clone()),
                    semantic: entry.semantic.clone(),
                    child_context,
                    tombstone: None,
                },
            );
        }
    }

    dst_pipe
        .write_node_dedup(&new, dst_write, dst_pipe.store().as_ref(), stats)
        .await
}

/// The per-blob ChaCha20 key for a source leaf, or `None` when the leaf is
/// stored without encryption (plaintext leaf, or a leaf whose transforms are
/// compression-only). `Some` requires BOTH a source master AND a
/// `plaintext_hash` (present iff the source applied transforms).
fn per_blob_key_for(src_leaf_master: Option<&[u8; 32]>, content: &ContentRef) -> Option<[u8; 32]> {
    match (src_leaf_master, content.plaintext_hash) {
        (Some(master), Some(ph)) => Some(crate::context::leaf_blob_key(master, &ph)),
        _ => None,
    }
}

/// The per-blob leaf key to INLINE for a reused source leaf, chosen from the
/// source leaf's EFFECTIVE traversal context (`merge_contexts(parent, child)`)
/// rather than blindly re-derived from the source master.
///
/// This is the fix for the transitive / re-shared subtree case: when the
/// source leaf was itself placed by a PRIOR shallow copy, its effective leaf
/// encryption is already [`ExplicitKeyChaCha20`](EncryptionStrategy::ExplicitKeyChaCha20)
/// and the ONLY key that decrypts the (unchanged) ciphertext is the one it
/// already inlines at [`KEY_SLOT_EXPLICIT_LEAF`] — the *first-hop* per-blob key.
/// Re-deriving `leaf_blob_key(this_vault_master, plaintext_hash)` would inline a
/// key that never encrypted the blob, yielding an undecryptable second-hop
/// share. So:
///
/// * `ExplicitKeyChaCha20` source → reuse the existing inlined key verbatim.
/// * `DeterministicChaCha20` source → derive the per-blob key from the source
///   master (never the master itself).
/// * `Plaintext` / no leaf encryption → no key to inline.
fn effective_leaf_key(
    src_eff: &TraversalContext,
    content: &ContentRef,
    src_leaf_master: Option<&[u8; 32]>,
) -> anyhow::Result<Option<[u8; 32]>> {
    match src_eff.leaf.as_ref().and_then(|p| p.encryption.as_ref()) {
        Some((EncryptionStrategy::ExplicitKeyChaCha20, slot)) => src_eff
            .keys
            .as_ref()
            .and_then(|k| k.get(slot))
            .copied()
            .map(Some)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "source leaf uses ExplicitKeyChaCha20 at key slot {slot:#04x} but no key is \
                     present in its effective context — cannot re-home the ciphertext"
                )
            }),
        Some((EncryptionStrategy::DeterministicChaCha20, _)) => {
            Ok(per_blob_key_for(src_leaf_master, content))
        }
        Some((EncryptionStrategy::Plaintext, _)) | None => Ok(None),
    }
}

/// Build the per-entry `child_context` that lets a DEST reader decode a
/// reused SOURCE leaf ciphertext.
///
/// Compression + encryption are always emitted as `Some(..)` so they OVERRIDE
/// the destination leaf pipeline on merge rather than inheriting it — else a
/// reused plaintext blob would be "decrypted" with the dest key, or a Zstd
/// blob decoded as uncompressed. Padding is pinned for fidelity (the read
/// path derives padding from `plaintext_size`, not this field). The inlined
/// key map holds ONLY [`KEY_SLOT_EXPLICIT_LEAF`] (break point #2 — no other
/// slot, and never the source master).
fn inlined_leaf_child_context(
    src_effective_leaf: Option<&BlobPipeline>,
    per_blob_key: Option<[u8; 32]>,
) -> Option<Box<TraversalContext>> {
    let compression = src_effective_leaf
        .and_then(|p| p.compression.clone())
        .unwrap_or(CompressionStrategy::Uncompressed);
    let padding = src_effective_leaf.and_then(|p| p.padding.clone());

    let (keys, encryption) = match per_blob_key {
        Some(key) => {
            let mut keys = BTreeMap::new();
            keys.insert(KEY_SLOT_EXPLICIT_LEAF, key);
            (
                Some(keys),
                Some((
                    EncryptionStrategy::ExplicitKeyChaCha20,
                    KEY_SLOT_EXPLICIT_LEAF,
                )),
            )
        }
        None => (None, Some((EncryptionStrategy::Plaintext, 0u8))),
    };

    Some(Box::new(TraversalContext {
        keys,
        leaf: Some(BlobPipeline {
            compression: Some(compression),
            padding,
            encryption,
            skip_when_unhelpful: None,
        }),
        node: None,
        chunking: None,
    }))
}

// ---------------------------------------------------------------------------
// Deep copy (re-encrypt under the destination's own derivation)
// ---------------------------------------------------------------------------

/// Re-import `src`'s subtree as brand-new, destination-native ciphertext.
///
/// Each source file is exported to plaintext and re-imported through `dst`'s
/// pipeline (destination keys, destination compression/padding). The result
/// shares NO data-blob hashes with the source and inlines NO keys — that is
/// what makes a deep-copied share future-revocable. Directories are carried
/// as metadata-only entries. Structurally this path can never call the inline
/// helpers (break point #4).
pub async fn deep_copy_into(
    src: &Snapshot,
    src_subtree_prefix: Option<&str>,
    dst: &Snapshot,
    dst_write: &dyn BlobsWrite,
    dst_path_prefix: Option<&str>,
) -> anyhow::Result<BTreeMap<String, NodeEntry>> {
    let src_pipe = src.as_pipeline();
    let dst_pipe = dst.as_pipeline();
    let mut out: BTreeMap<String, NodeEntry> = BTreeMap::new();

    let mut walk = src.walk();
    while let Some(item) = walk.next().await {
        let (path, entry) = item?;
        let Some(dst_path) = reroot(&path, src_subtree_prefix, dst_path_prefix) else {
            continue;
        };
        match entry.content.as_ref() {
            None => {
                out.insert(
                    dst_path,
                    NodeEntry {
                        content: None,
                        semantic: entry.semantic.clone(),
                        child_context: None,
                        tombstone: None,
                    },
                );
            }
            Some(_) => {
                let plaintext = src_pipe.export_bytes(&entry).await?;
                let new_entry = dst_pipe
                    .import_bytes(&plaintext, dst_write, entry.semantic.clone())
                    .await?;
                out.insert(dst_path, new_entry);
            }
        }
    }
    Ok(out)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layer::MapLayer;
    use crate::node::{FileChunkingStrategy, PaddingStrategy};
    use s5_core::blob::BlobStore;
    use s5_store_memory::MemoryStore;

    /// A no-op replicator for tests that share one store between src and dst
    /// (the blob is already present, so replicate is genuinely nothing).
    struct SharedStoreReplicator;
    #[async_trait]
    impl BlobReplicator for SharedStoreReplicator {
        async fn replicate(&self, _hash: Hash) -> anyhow::Result<()> {
            Ok(())
        }
    }

    fn rw_store() -> Arc<BlobStore> {
        Arc::new(BlobStore::new(MemoryStore::new()))
    }

    /// The inlined child context pins the SOURCE effective compression +
    /// padding, switches encryption to ExplicitKeyChaCha20 @ slot 0x13, and
    /// carries ONLY the per-blob key (never the source master).
    #[test]
    fn inlined_child_context_pins_source_pipeline() {
        let master = [0x11u8; 32];
        let ph = [0x22u8; 32];
        let per_blob = source_leaf_blob_key(&master, &ph);

        let src_leaf = BlobPipeline {
            compression: Some(CompressionStrategy::Zstd),
            padding: Some(PaddingStrategy { block_size: 4096 }),
            encryption: Some((EncryptionStrategy::DeterministicChaCha20, 0x10)),
            skip_when_unhelpful: Some(true),
        };
        let cc = inlined_leaf_child_context(Some(&src_leaf), Some(per_blob)).unwrap();

        let leaf = cc.leaf.as_ref().unwrap();
        assert_eq!(
            leaf.compression,
            Some(CompressionStrategy::Zstd),
            "pins src compression"
        );
        assert_eq!(
            leaf.padding.as_ref().map(|p| p.block_size),
            Some(4096),
            "pins src padding"
        );
        assert_eq!(
            leaf.encryption,
            Some((
                EncryptionStrategy::ExplicitKeyChaCha20,
                KEY_SLOT_EXPLICIT_LEAF
            )),
            "switches to explicit-key encryption at slot 0x13"
        );

        let keys = cc.keys.as_ref().unwrap();
        assert_eq!(keys.len(), 1, "exactly one inlined key");
        assert_eq!(keys.get(&KEY_SLOT_EXPLICIT_LEAF), Some(&per_blob));
        // The source master must NOT appear anywhere in the inlined map.
        assert!(
            !keys.values().any(|k| k == &master),
            "source master must never be inlined"
        );
    }

    /// A multi-chunk file re-encoded via `reencode_subtree` into a fresh
    /// (encrypted) destination context reads back byte-identically, reuses the
    /// SAME chunk data-blob hashes (ciphertext reuse), and produces DIFFERENT
    /// node hashes (nodes re-encrypted under dest keys).
    #[tokio::test]
    async fn reencode_subtree_preserves_keys_and_reads_back() {
        let store = rw_store();
        let read: Arc<dyn BlobsRead> = store.clone();

        // Source vault: force fixed 4 KiB chunking so the file becomes a
        // Link → ByteStream (exercises the recursive re-encode path).
        let empty = Snapshot::empty_encrypted(store.clone(), [0x30u8; 32]);
        let mut src_ctx = empty.context().clone();
        src_ctx.chunking = Some(FileChunkingStrategy::Fixed { chunk_size: 4096 });
        let src = Snapshot::new(empty.root(), store.clone(), src_ctx, None);

        let plaintext = vec![0x7u8; 40 * 1024]; // ~10 chunks

        // Import through import_stream to build the chunk tree.
        let reader = tokio::io::BufReader::new(&plaintext[..]);
        let file_entry = src
            .import_stream(reader, store.as_ref(), None)
            .await
            .unwrap();
        assert!(file_entry.is_link(), "fixture must be a chunked Link file");

        // The source leaf master = the key its leaf pipeline references.
        let src_leaf_master = leaf_key_of(&src);

        // Destination context: independent encrypted vault.
        let dst_ctx = Snapshot::empty_encrypted(store.clone(), [0x40u8; 32])
            .context()
            .clone();
        let dst_pipe = Pipeline::new(read.clone(), dst_ctx.clone());
        let src_child = src.as_pipeline().child_for(&file_entry);
        let content = file_entry.content.as_ref().unwrap();
        let mut stats = MergeStats::default();
        let (blob_id, pph) = reencode_subtree(
            &dst_pipe,
            store.as_ref(),
            &src_child,
            content.hash(),
            content.plaintext_hash,
            src_leaf_master.as_ref(),
            &SharedStoreReplicator,
            &mut stats,
        )
        .await
        .unwrap();

        // The destination byte-stream root node hash differs from the source
        // (re-encrypted under dest node keys).
        assert_ne!(
            Hash::from(*blob_id.hash.as_bytes()),
            content.hash(),
            "dest node must be re-encrypted (different hash)"
        );

        // Build a dest Link entry and read it back under the DEST context.
        let dst_entry = NodeEntry {
            content: Some(ContentRef {
                structural: Structural::Link,
                hash: *blob_id.hash.as_bytes(),
                size: content.size,
                plaintext_hash: Some(pph),
                stored_blocks: Some(blob_id.size),
            }),
            semantic: None,
            child_context: None,
            tombstone: None,
        };
        let dst_snap = Snapshot::new(src.root(), read.clone(), dst_ctx, None);
        let got = dst_snap
            .as_pipeline()
            .export_bytes(&dst_entry)
            .await
            .unwrap();
        assert_eq!(got.len(), plaintext.len());
        assert_eq!(
            &got[..],
            &plaintext[..],
            "deep read-back must be byte-identical"
        );

        // Ciphertext reuse: the dest chunk data hashes equal the source's.
        let src_chunks = collect_chunk_hashes(&src, &file_entry).await;
        let dst_chunks = collect_chunk_hashes(&dst_snap, &dst_entry).await;
        assert!(!dst_chunks.is_empty());
        assert_eq!(
            src_chunks, dst_chunks,
            "shallow chunk ciphertext must be reused"
        );

        // The source master must NEVER be inlined at ANY re-encoded chunk leaf
        // (the chunked analogue of `source_master_absent_from_dest_entries`;
        // `walk` never surfaces chunk leaves, so scan the byte-stream directly).
        let master = src_leaf_master.expect("encrypted src has a leaf master");
        let dst_content = dst_entry.content.as_ref().unwrap();
        let mut inlined_at_chunks = 0usize;
        let dst_pipe_scan = dst_snap.as_pipeline();
        let mut cs = dst_pipe_scan.walk_byte_stream(dst_content.hash(), dst_content.plaintext_hash);
        while let Some(item) = cs.next().await {
            let chunk = item.unwrap();
            if let Some(keys) = chunk.child_context.as_ref().and_then(|c| c.keys.as_ref()) {
                for key in keys.values() {
                    assert_ne!(
                        key, &master,
                        "source master leaked into a re-encoded chunk leaf"
                    );
                    inlined_at_chunks += 1;
                }
            }
        }
        assert!(
            inlined_at_chunks > 0,
            "chunk leaves must inline per-blob keys — else the scan is vacuous"
        );
    }

    /// `shallow_copy_into` on a single-file source produces a dest entry whose
    /// inlined key map contains the PER-BLOB key and NOT the source master.
    #[tokio::test]
    async fn source_master_absent_from_dest_entries() {
        let store = rw_store();
        let read: Arc<dyn BlobsRead> = store.clone();

        let src = Snapshot::empty_encrypted(store.clone(), [0x55u8; 32]);
        let content = b"a small single-chunk file".to_vec();
        let file_entry = src
            .import_bytes(&content, store.as_ref(), None)
            .await
            .unwrap();
        assert!(file_entry.is_leaf());
        // Persist it into a real one-entry tree so walk() yields it.
        let mut layer_map = BTreeMap::new();
        layer_map.insert("file.txt".to_string(), file_entry.clone());
        let (root, ph, _) = src
            .merge_and_persist(&MapLayer::new(layer_map), store.as_ref())
            .await
            .unwrap()
            .unwrap();
        let src = Snapshot::new(root, read.clone(), src.context().clone(), Some(ph));

        let src_leaf_master = leaf_key_of(&src).expect("encrypted src has a leaf master");
        let dst_ctx = Snapshot::empty_encrypted(store.clone(), [0x66u8; 32])
            .context()
            .clone();

        let entries = shallow_copy_into(
            &src,
            None,
            Some(&src_leaf_master),
            &dst_ctx,
            store.as_ref(),
            read.clone(),
            None,
            &SharedStoreReplicator,
        )
        .await
        .unwrap();

        let dst_entry = entries.get("file.txt").expect("copied file present");
        let cc = dst_entry
            .child_context
            .as_ref()
            .expect("leaf carries inlined ctx");
        let keys = cc.keys.as_ref().expect("inlined key map");
        // The source master must NOT be inlined; the per-blob key MUST be.
        assert!(
            !keys.values().any(|k| k == &src_leaf_master),
            "source master leaked into a dest entry"
        );
        let expected_ph = file_entry.content.as_ref().unwrap().plaintext_hash.unwrap();
        let expected = source_leaf_blob_key(&src_leaf_master, &expected_ph);
        assert_eq!(
            keys.get(&KEY_SLOT_EXPLICIT_LEAF),
            Some(&expected),
            "dest entry must inline the per-blob key"
        );
    }

    /// D21 sharing composes: a shallow copy whose SOURCE leaf was itself placed
    /// by a PRIOR shallow copy (a re-shared / two-hop subtree) must re-home the
    /// SAME ciphertext under the SAME first-hop per-blob key — NOT a key
    /// re-derived from the intermediate vault's own master (which never
    /// encrypted the blob). The second-hop reader must decrypt to byte-identical
    /// plaintext. Regression guard for the transitive-share key-inlining bug.
    #[tokio::test]
    async fn transitive_shallow_copy_reads_back() {
        let store = rw_store();
        let read: Arc<dyn BlobsRead> = store.clone();

        // -- Vault A: the original encrypted source --
        let a0 = Snapshot::empty_encrypted(store.clone(), [0xA1u8; 32]);
        let plaintext = b"a file that gets shared, then re-shared onwards".to_vec();
        let a_file = a0
            .import_bytes(&plaintext, store.as_ref(), None)
            .await
            .unwrap();
        assert!(a_file.is_leaf());
        let ph = a_file.content.as_ref().unwrap().plaintext_hash.unwrap();
        let mut a_map = BTreeMap::new();
        a_map.insert("f.txt".to_string(), a_file);
        let (a_root, a_ph, _) = a0
            .merge_and_persist(&MapLayer::new(a_map), store.as_ref())
            .await
            .unwrap()
            .unwrap();
        let a = Snapshot::new(a_root, read.clone(), a0.context().clone(), Some(a_ph));
        let a_master = leaf_key_of(&a).expect("A is encrypted");

        // -- Hop 1: shallow copy A -> vault B, persisted so B.walk() yields the
        // ExplicitKeyChaCha20 child_context the second hop must re-home. --
        let b0 = Snapshot::empty_encrypted(store.clone(), [0xB2u8; 32]);
        let b_entries = shallow_copy_into(
            &a,
            None,
            Some(&a_master),
            b0.context(),
            store.as_ref(),
            read.clone(),
            None,
            &SharedStoreReplicator,
        )
        .await
        .unwrap();
        let (b_root, b_ph, _) = b0
            .merge_and_persist(&MapLayer::new(b_entries), store.as_ref())
            .await
            .unwrap()
            .unwrap();
        let b = Snapshot::new(b_root, read.clone(), b0.context().clone(), Some(b_ph));
        let b_master = leaf_key_of(&b).expect("B is encrypted");

        // -- Hop 2: shallow copy B -> vault C. `b_master` is B's OWN leaf master
        // (what the daemon passes as `src_leaf_master`); the fix must IGNORE it
        // for the re-shared leaf and reuse A's inlined key. --
        let c0 = Snapshot::empty_encrypted(store.clone(), [0xC3u8; 32]);
        let c_entries = shallow_copy_into(
            &b,
            None,
            Some(&b_master),
            c0.context(),
            store.as_ref(),
            read.clone(),
            None,
            &SharedStoreReplicator,
        )
        .await
        .unwrap();

        let c_entry = c_entries.get("f.txt").expect("re-shared file present");
        let keys = c_entry
            .child_context
            .as_ref()
            .unwrap()
            .keys
            .as_ref()
            .unwrap();
        let inlined = keys.get(&KEY_SLOT_EXPLICIT_LEAF).copied().unwrap();
        let first_hop_key = source_leaf_blob_key(&a_master, &ph);
        let wrong_key = source_leaf_blob_key(&b_master, &ph);
        assert_eq!(
            inlined, first_hop_key,
            "second hop must reuse A's per-blob key"
        );
        assert_ne!(
            inlined, wrong_key,
            "second hop must NOT re-derive from B's own master"
        );

        // Neither master may ever appear in the inlined map.
        assert!(!keys.values().any(|k| k == &a_master || k == &b_master));

        // The second-hop reader (vault C) decrypts to the original plaintext.
        let got = c0.export_bytes(c_entry).await.unwrap();
        assert_eq!(&got[..], &plaintext[..], "second-hop share must read back");
    }

    // -- helpers --

    /// The effective leaf master of an encrypted snapshot: the key its leaf
    /// pipeline references (KEY_SLOT_MASTER for `empty_encrypted`,
    /// KEY_SLOT_LEAF for the split shape).
    fn leaf_key_of(snap: &Snapshot) -> Option<[u8; 32]> {
        let (strat, slot) = snap.context().leaf.as_ref()?.encryption.as_ref()?;
        if *strat != EncryptionStrategy::DeterministicChaCha20 {
            return None;
        }
        snap.context().keys.as_ref()?.get(slot).copied()
    }

    async fn collect_chunk_hashes(snap: &Snapshot, link: &NodeEntry) -> Vec<Hash> {
        let content = link.content.as_ref().unwrap();
        let pipe = snap.as_pipeline();
        let mut stream = pipe.walk_byte_stream(content.hash(), content.plaintext_hash);
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            let e = item.unwrap();
            out.push(e.content.as_ref().unwrap().hash());
        }
        out
    }
}
