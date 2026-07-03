//! Pure data structures and on-disk (CBOR) schema for FS5 directories.
//!
//! This module defines the unified `Node` architecture (V2) for FS5.
//! Everything in FS5—a tiny folder, a 10GB video, an expanded ZIP archive,
//! or a massive Prolly tree—is represented by a single unified `Node`.
//!
//! # Core Principle: Stored Hash is Identity
//!
//! The `hash` field is the CAS address — the BLAKE3 of whatever is stored
//! in the blob store (after compression, padding, encryption). This is
//! what you fetch and what's visible on the network.
//! `plaintext_hash` optionally carries the BLAKE3 of the original plaintext
//! for key derivation (KDF input), local deduplication, and post-decrypt
//! verification. Plaintext hashes never leak to the network.
//!
//! # Structure
//!
//! A `Node` holds a single, uniformly ordered map of all entries.
//! Keys are strictly sorted UTF-8 strings.
//!
//! ```text
//! Node
//! ├── magic: String ("S5.pro")
//! ├── header: NodeHeader (level, kind, build?)
//! └── entries: BTreeMap<String, NodeEntry>
//!     └── NodeEntry
//!         ├── content: Option<ContentRef>
//!         │   ├── structural: Structural (Leaf | Link)
//!         │   ├── hash: [u8; 32] (CAS address — stored blob)
//!         │   ├── size: u64 (plaintext size)
//!         │   ├── plaintext_hash: Option<[u8; 32]> (KDF input)
//!         │   └── stored_blocks: Option<u64> (stored size in blocks)
//!         ├── semantic: Option<SemanticMeta> (timestamps, MIME, etc.)
//!         ├── child_context: Option<TraversalContext>
//!         │   ├── keys: Option<BTreeMap<u8, [u8; 32]>>
//!         │   ├── leaf: Option<BlobPipeline> (compress → pad → encrypt)
//!         │   └── node: Option<BlobPipeline> (compress → pad → encrypt)
//!         └── tombstone: Option<Tombstone> (deletion marker with timestamp)
//!     (content: None without tombstone = metadata-only, e.g. directory)
//! ```
//!
//! # Context Separation: Read vs Build
//!
//! Context is split into two distinct types:
//!
//! - [`TraversalContext`] (read context): flows down the tree via
//!   `NodeEntry.child_context`. Contains only what a reader needs to
//!   decrypt and decompress blobs (keys, encryption/compression strategies).
//!
//! - [`BuildContext`] (write context): stored optionally on [`NodeHeader`].
//!   Contains structural policies for tree construction (prolly chunking
//!   params, CDC file chunking strategy). Only set on the root node (or
//!   where settings change); children inherit. A writer loading an existing
//!   tree uses this to ensure consistent chunking for structural sharing.

use std::collections::BTreeMap;
use std::convert::Infallible;

use bytes::Bytes;
use minicbor::{CborLen, Decode, Encode};
use s5_core::Hash;

pub const NODE_MAGIC: &str = "S5.pro";

/// The unified container for directories and prolly tree nodes.
///
/// This implements the unified Node architecture where everything—
/// files, directories, chunks, and tree nodes—is stored in a single
/// ordered map with consistent structure.
#[derive(Encode, Decode, CborLen, Clone, Debug)]
#[cbor(array)]
pub struct Node {
    #[n(0)]
    pub magic: String,

    /// Node-level metadata: level, kind, and optional build context.
    #[n(1)]
    pub header: NodeHeader,

    /// All entries (files, directories, chunks, links) in a single ordered map.
    /// Keys are strictly sorted UTF-8 strings.
    #[n(2)]
    pub entries: BTreeMap<String, NodeEntry>,
}

impl Default for Node {
    fn default() -> Self {
        Self::new()
    }
}

impl Node {
    /// Creates an empty node with default header.
    pub fn new() -> Self {
        Self {
            magic: NODE_MAGIC.to_string(),
            header: NodeHeader::default(),
            entries: BTreeMap::new(),
        }
    }

    /// Decodes a node from CBOR bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Node, minicbor::decode::Error> {
        minicbor::decode(bytes)
    }

    /// Encodes this node to a CBOR `Vec<u8>`.
    pub fn to_vec(&self) -> Result<Vec<u8>, minicbor::encode::Error<Infallible>> {
        minicbor::to_vec(self)
    }

    /// Encodes this node to CBOR as a `Bytes` buffer.
    pub fn to_bytes(&self) -> Result<Bytes, minicbor::encode::Error<Infallible>> {
        Ok(self.to_vec()?.into())
    }

    /// Computes the BLAKE3 hash of this node's serialized CBOR form.
    ///
    /// This is the hash of the raw serialized bytes, *before* any blob
    /// pipeline processing (compression, padding, encryption). The
    /// pipeline produces the final CAS address stored in `ContentRef.hash`.
    pub fn content_hash(&self) -> Result<Hash, minicbor::encode::Error<Infallible>> {
        let bytes = self.to_bytes()?;
        Ok(Hash::from(*blake3::hash(&bytes).as_bytes()))
    }

    /// Returns the number of entries in this node.
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if this node has no entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Gets an entry by key.
    pub fn get(&self, key: &str) -> Option<&NodeEntry> {
        self.entries.get(key)
    }

    /// Inserts an entry.
    pub fn insert(&mut self, key: String, entry: NodeEntry) -> Option<NodeEntry> {
        self.entries.insert(key, entry)
    }

    /// Removes an entry.
    pub fn remove(&mut self, key: &str) -> Option<NodeEntry> {
        self.entries.remove(key)
    }

    /// Returns an iterator over entries in the given key range.
    pub fn range<K, R>(&self, range: R) -> impl Iterator<Item = (&String, &NodeEntry)>
    where
        K: Ord + ?Sized,
        R: std::ops::RangeBounds<K>,
        String: std::borrow::Borrow<K>,
    {
        self.entries.range(range)
    }

    /// Returns true if this is a leaf node (level 0).
    #[inline]
    pub fn is_leaf_node(&self) -> bool {
        self.header.level == 0
    }

    /// Creates a transparent wrapper node containing a single entry at `""`.
    ///
    /// Used for root pointers (snapshot refs, vault entries).
    pub fn transparent(entry: NodeEntry) -> Self {
        let mut entries = BTreeMap::new();
        entries.insert(String::new(), entry);
        Self {
            magic: NODE_MAGIC.to_string(),
            header: NodeHeader {
                level: 0,
                kind: NodeKind::Transparent,
                build: None,
            },
            entries,
        }
    }

    /// If this is a `Transparent` node, returns the single inner entry.
    pub fn transparent_entry(&self) -> Option<&NodeEntry> {
        if self.header.kind == NodeKind::Transparent {
            self.entries.get("")
        } else {
            None
        }
    }
}

/// What the entries in a `Node` represent.
///
/// Distinguishes directory trees from file content trees, and provides
/// a transparent wrapper for root pointers (snapshot refs, vault entries).
/// The parent `Link` entry doesn't carry this — the reader discovers
/// the kind when it loads the target node.
#[derive(Encode, Decode, CborLen, Clone, Debug, PartialEq, Eq, Default)]
#[cbor(index_only)]
pub enum NodeKind {
    /// Keys are relative paths (directory tree).
    ///
    /// **Convention:** files use their full relative path with no trailing
    /// slash (`"Photos/2024/sunset.jpg"`). Directories use path + trailing
    /// `/` (`"Photos/"`, `"Photos/2024/"`). All intermediate directories
    /// get explicit entries so that metadata is preserved and empty
    /// directories are tracked.
    #[n(0)]
    #[default]
    Namespace,

    /// Keys are chunk indices/offsets (file content tree).
    /// e.g., "000000", "000001", "000002"
    #[n(1)]
    ByteStream,

    /// Single-entry wrapper node. Contains exactly one entry at key `""`.
    /// Used for root pointers (snapshot refs, vault entries) where the
    /// node is just a carrier for a `Link` + `child_context`.
    /// The reader follows through transparently.
    #[n(2)]
    Transparent,
}

/// Node-level metadata for a directory or prolly tree node.
///
/// Since all subtrees are immutable, this header contains only minimal
/// metadata needed for correct traversal and interpretation.
///
/// The optional [`BuildContext`] records how this node was constructed
/// (chunking strategies, etc.). Only set on the root node or where
/// settings change — children inherit. Writers loading an existing tree
/// use this to ensure consistent chunking for structural sharing.
#[derive(Encode, Decode, CborLen, Clone, Default, Debug)]
#[cbor(map)]
pub struct NodeHeader {
    /// Prolly tree level (0 = leaf node, 1+ = internal nodes).
    /// Level 0 indicates this is a leaf node (entries contain actual data).
    /// Level > 0 indicates this is an internal prolly tree node.
    #[n(0)]
    pub level: u8,

    /// What this node's entries represent.
    #[n(1)]
    pub kind: NodeKind,

    /// Structural build policies used to construct this tree.
    ///
    /// Only present on root nodes (or where settings diverge from the
    /// parent). A writer loading an existing snapshot reads this to
    /// ensure identical chunking boundaries for structural sharing.
    #[n(2)]
    pub build: Option<BuildContext>,
}

/// The structural definition of a [`ContentRef`].
///
/// Describes how the blob referenced by `ContentRef.hash` should be
/// interpreted:
/// - `Leaf`: a single blob of raw data
/// - `Link`: a pointer to another serialized `Node`
#[derive(Encode, Decode, CborLen, Clone, Debug, PartialEq, Eq)]
#[cbor(index_only)]
pub enum Structural {
    /// Raw data entry: the hash points to a single blob of content.
    #[n(0)]
    Leaf,

    /// Tree reference: the hash points to another serialized `Node`
    /// (directory, chunked file, prolly tree, virtual archive).
    #[n(1)]
    Link,
}

// =============================================================================
// ContentRef - Storage Identity (returned by import pipeline)
// =============================================================================

/// The storage identity of a blob: what was stored and how to fetch it.
///
/// This is what an import pipeline (e.g. `FileImporter`) returns after
/// processing plaintext bytes through the blob pipeline (compress → pad →
/// encrypt) and optional CDC chunking. The caller combines this with
/// [`SemanticMeta`] to form a complete [`NodeEntry`].
///
/// All fields are intrinsic to the content — they are properties of the
/// blob itself, not context-dependent metadata.
///
/// # Stored vs Plaintext Identity
///
/// `hash` is always the CAS address of the stored blob (after any
/// compression, encryption, and padding). This is what's visible on the
/// network — plaintext hashes never leak.
///
/// `size` is always the plaintext content size.
///
/// When encryption/padding is applied, `plaintext_hash` carries the
/// original content hash (needed as KDF input for key derivation), and
/// `stored_blocks` carries the stored blob size as a block count
/// (actual stored size = `stored_blocks * padding_block_size` from
/// the applicable [`BlobPipeline::padding`]).
///
/// When no transforms are applied, `plaintext_hash` and `stored_blocks`
/// are both `None` — `hash` is already the plaintext hash and `size`
/// is the stored size.
#[derive(Encode, Decode, CborLen, Clone, Debug)]
#[cbor(map)]
pub struct ContentRef {
    /// Is this a direct blob (Leaf) or a pointer to another Node (Link)?
    #[n(0)]
    pub structural: Structural,

    /// The CAS address: BLAKE3 hash of the stored blob (encrypted/compressed).
    /// This is what you fetch from the blob store. Never a plaintext hash
    /// when encryption is active.
    #[n(1)]
    #[cbor(with = "minicbor::bytes")]
    pub hash: [u8; 32],

    /// Plaintext content size in bytes.
    #[n(2)]
    pub size: u64,

    /// BLAKE3 hash of the original plaintext content.
    /// Required before decryption as the KDF input for key derivation.
    /// Also used for local deduplication and post-decrypt verification.
    /// `None` when no transforms were applied (hash == plaintext hash).
    // Invariant: `plaintext_hash.is_some() == stored_blocks.is_some()`
    #[n(3)]
    #[cbor(with = "minicbor::bytes")]
    pub plaintext_hash: Option<[u8; 32]>,

    /// Stored blob size as a number of padding blocks.
    /// Actual stored size = `stored_blocks * padding_block_size`
    /// (block size comes from the applicable [`BlobPipeline::padding`];
    /// defaults to 1 when no padding strategy is set, making
    /// `stored_blocks` the exact byte count).
    /// `None` when no transforms were applied (stored size == `size`).
    // Invariant: `stored_blocks.is_some() == plaintext_hash.is_some()`
    #[n(4)]
    pub stored_blocks: Option<u64>,
}

impl ContentRef {
    /// Returns the CAS address hash (what to fetch from the blob store).
    pub fn hash(&self) -> Hash {
        Hash::from(self.hash)
    }

    /// Returns the plaintext identity hash.
    ///
    /// If transforms were applied, returns `plaintext_hash`.
    /// Otherwise, `hash` is already the plaintext hash.
    pub fn plaintext_hash(&self) -> Hash {
        match self.plaintext_hash {
            Some(h) => Hash::from(h),
            None => Hash::from(self.hash),
        }
    }

    /// Returns the stored blob size in bytes, given the padding block size.
    ///
    /// If no transforms were applied, returns `size` (stored == plaintext).
    /// Pass `1` when no padding strategy is set (stored_blocks is the
    /// exact byte count).
    pub fn stored_size(&self, padding_block_size: u32) -> u64 {
        match self.stored_blocks {
            Some(blocks) => blocks * padding_block_size as u64,
            None => self.size,
        }
    }
}

// =============================================================================
// NodeEntry - Entries in the Node's BTreeMap
// =============================================================================

/// A single entry in a `Node`'s entries map.
///
/// Combines an optional [`ContentRef`] (storage identity, produced by
/// the import pipeline) with optional [`SemanticMeta`] (caller-provided
/// metadata) and an optional [`TraversalContext`] for child traversal.
///
/// # Metadata-Only Entries
///
/// Entries with `content: None` are valid — they represent metadata-only
/// entries such as directory entries that carry permissions/timestamps but
/// no blob content. These are NOT tombstones.
///
/// # Tombstones
///
/// A tombstone (logical deletion marker) is indicated by `tombstone: Some(...)`.
/// The [`Tombstone`] carries the deletion timestamp for LWW (Latest Write
/// Wins) merge resolution. When merging, if timestamps are equal, the entry
/// with the lexicographically larger key wins as a tiebreaker.
///
/// Tombstone entries discard all other fields — `content` and `semantic`
/// are `None`, keeping the wire representation minimal.
#[derive(Encode, Decode, CborLen, Clone, Debug)]
#[cbor(map)]
pub struct NodeEntry {
    /// Storage identity: hash, size, plaintext_hash, structural type.
    /// Produced by the import pipeline (e.g. `FileImporter`).
    ///
    /// `None` for metadata-only entries (e.g. directories) and tombstones.
    #[n(0)]
    pub content: Option<ContentRef>,

    /// Optional semantic metadata: timestamps, MIME type, version info, etc.
    /// Provided by the caller (ingester), not the import pipeline.
    #[n(1)]
    pub semantic: Option<SemanticMeta>,

    /// Pipeline override for this entry's content.
    ///
    /// For **Link** entries: read context for the target subtree (keys,
    /// encryption, compression needed to traverse children).
    ///
    /// For **Leaf** entries: per-blob pipeline override when this entry
    /// was processed differently from the inherited pipeline. For example,
    /// when compression was skipped for an incompressible blob, this carries
    /// `leaf.compression = Some(Uncompressed)` so the decoder knows to
    /// skip decompression.
    ///
    /// Only set when overriding the parent context. `None` = inherit.
    #[n(2)]
    pub child_context: Option<Box<TraversalContext>>,

    /// Tombstone marker. When `Some`, this entry is a deletion marker.
    /// All other fields are ignored (and should be `None`).
    #[n(3)]
    pub tombstone: Option<Tombstone>,
}

/// Tombstone marker for a deleted entry.
///
/// Carries only the deletion timestamp for LWW merge resolution.
/// Minimal on the wire — no semantic metadata or content needed.
#[derive(Encode, Decode, CborLen, Clone, Debug)]
#[cbor(map)]
pub struct Tombstone {
    /// Deletion timestamp (Unix seconds).
    #[n(0)]
    pub timestamp: Option<u32>,

    /// Sub-second precision for deletion timestamp (nanoseconds).
    #[n(1)]
    pub timestamp_subsec_nanos: Option<u32>,
}

impl NodeEntry {
    /// Returns the CAS address hash (what to fetch from the blob store).
    ///
    /// Returns `None` for metadata-only entries and tombstones.
    pub fn hash(&self) -> Option<Hash> {
        self.content.as_ref().map(|c| c.hash())
    }

    /// Returns the plaintext identity hash, if content is present.
    ///
    /// Returns `None` for metadata-only entries and tombstones.
    pub fn plaintext_hash(&self) -> Option<Hash> {
        self.content.as_ref().map(|c| c.plaintext_hash())
    }

    /// Returns true if this is a tombstone (deletion marker).
    pub fn is_tombstone(&self) -> bool {
        self.tombstone.is_some()
    }

    /// Returns true if this is a leaf entry (has content with `Structural::Leaf`).
    pub fn is_leaf(&self) -> bool {
        self.content
            .as_ref()
            .is_some_and(|c| matches!(c.structural, Structural::Leaf))
    }

    /// Returns true if this is a link entry (has content with `Structural::Link`).
    pub fn is_link(&self) -> bool {
        self.content
            .as_ref()
            .is_some_and(|c| matches!(c.structural, Structural::Link))
    }

    /// Creates a tombstone entry with the given deletion timestamp.
    pub fn tombstone(timestamp: u32) -> Self {
        Self {
            content: None,
            semantic: None,
            child_context: None,
            tombstone: Some(Tombstone {
                timestamp: Some(timestamp),
                timestamp_subsec_nanos: None,
            }),
        }
    }
}

// =============================================================================
// SemanticMeta - Optional Metadata
// =============================================================================

/// Optional semantic metadata for a `NodeEntry`.
///
/// This data is not required for storage/retrieval operations but provides
/// useful information for display, sorting, and application logic.
///
/// # Linux Filesystem Support
///
/// Linux filesystem features (file type, permissions, ownership, extended
/// attributes) are packed into `UnixMetadata`. The `FileType` enum provides
/// efficient access to file type without parsing mode bits.
#[derive(Encode, Decode, CborLen, Clone, Debug, Default)]
#[cbor(map)]
pub struct SemanticMeta {
    /// Creation/modification timestamp (Unix seconds).
    #[n(0)]
    pub timestamp: Option<u32>,

    /// Sub-second precision for timestamp (nanoseconds).
    #[n(1)]
    pub timestamp_subsec_nanos: Option<u32>,

    /// MIME media type (e.g., "image/png", "text/html").
    #[n(2)]
    pub media_type: Option<String>,

    /// Unix-style metadata (permissions, ownership, etc.).
    #[n(3)]
    pub unix: Option<UnixMetadata>,

    /// Web Archive (WARC) metadata for HTTP responses.
    #[n(4)]
    pub warc: Option<WebArchiveMetadata>,
    // TODO: Add recursive size fields for Link entries. Candidates:
    // - total_plaintext_size: sum of all ContentRef.size underneath (true content size)
    // - total_stored_size: sum of actual stored blob sizes (disk usage)
    // These belong on ContentRef or a dedicated struct, not here.
}

/// Unix-style file metadata.
#[derive(Encode, Decode, CborLen, Clone, Debug)]
#[cbor(map)]
pub struct UnixMetadata {
    /// File type (regular file, directory, symlink, etc.).
    /// This is stored separately from permissions for efficient access
    /// without parsing mode bits.
    #[n(0)]
    pub file_type: Option<FileType>,

    /// File permissions (Unix mode bits).
    #[n(1)]
    pub permissions: Option<u32>,

    /// User ID (for file ownership).
    #[n(2)]
    pub uid: Option<u32>,

    /// Group ID.
    #[n(3)]
    pub gid: Option<u32>,

    /// Unix ctime (last status change time).
    #[n(4)]
    pub ctime: Option<u64>,

    /// Unix user name.
    #[n(5)]
    pub user: Option<String>,

    /// Unix group name.
    #[n(6)]
    pub group: Option<String>,

    /// Unix inode number.
    #[n(7)]
    pub inode: Option<u64>,

    /// Unix device id.
    #[n(8)]
    pub device_id: Option<u64>,

    /// Number of hardlinks to this node.
    #[n(9)]
    pub nlink: Option<u64>,

    /// Extended attributes of the node.
    #[n(10)]
    pub extended_attributes: Option<Vec<ExtendedAttribute>>,
}

/// Linux filesystem file types.
///
/// Stored explicitly in `UnixMetadata` for efficient access without
/// parsing Unix mode bits.
#[derive(Encode, Decode, CborLen, Clone, Debug, PartialEq, Eq)]
#[cbor(index_only)]
pub enum FileType {
    /// Regular file.
    #[n(0)]
    Regular,

    /// Directory.
    #[n(1)]
    Directory,

    /// Symbolic link.
    #[n(2)]
    Symlink,

    /// Block device.
    #[n(3)]
    BlockDevice,

    /// Character device.
    #[n(4)]
    CharDevice,

    /// Named pipe (FIFO).
    #[n(5)]
    Fifo,

    /// Unix domain socket.
    #[n(6)]
    Socket,
}

/// Extended attribute for Unix metadata.
#[derive(Encode, Decode, CborLen, Clone, Debug)]
#[cbor(map)]
pub struct ExtendedAttribute {
    #[n(0)]
    pub name: String,

    #[n(1)]
    pub value: Option<Vec<u8>>,
}

impl SemanticMeta {
    /// Creates a new SemanticMeta with just a timestamp.
    pub fn with_timestamp(seconds: u32, nanos: Option<u32>) -> Self {
        Self {
            timestamp: Some(seconds),
            timestamp_subsec_nanos: nanos,
            ..Default::default()
        }
    }

    /// Creates a new SemanticMeta with a media type.
    pub fn with_media_type(media_type: impl Into<String>) -> Self {
        Self {
            media_type: Some(media_type.into()),
            ..Default::default()
        }
    }
}

// =============================================================================
// WebArchiveMetadata - WARC HTTP Response Data
// =============================================================================

/// Web Archive (WARC) metadata for HTTP responses.
#[derive(Encode, Decode, CborLen, Clone, Debug, Default)]
#[cbor(map)]
pub struct WebArchiveMetadata {
    /// IP address of the server.
    #[n(0)]
    pub ip_addr: String,

    /// Request HTTP version.
    #[n(1)]
    pub req_http_version: u8,

    /// Request headers (name, value pairs).
    #[n(2)]
    pub req_headers: Vec<(String, String)>,

    /// Response HTTP version.
    #[n(3)]
    pub res_http_version: u8,

    /// Response status code (e.g., 200, 404).
    #[n(4)]
    pub res_status_code: u16,

    /// Response status reason (e.g., "OK", "Not Found").
    #[n(5)]
    pub res_status_reason: String,

    /// Response headers (name, value pairs).
    #[n(6)]
    pub res_headers: Vec<(String, String)>,
}

// =============================================================================
// TraversalContext - Read Context (flows down tree via child_context)
// =============================================================================

/// Read context passed down the tree during traversal.
///
/// Contains only what a reader needs to decrypt and decompress blobs.
/// Flows from parent to child via `NodeEntry.child_context`.
///
/// Separates `keys` (shared key material) from two independent pipelines:
/// - `leaf`: how to process leaf blobs (file content, chunk data)
/// - `node`: how to process node blobs (serialized `Node` metadata)
///
/// These are typically different because metadata and content use
/// different encryption keys (derived from different inputs) and may
/// use different compression strategies.
///
/// See `decisions/fs5-context-passing-pipeline`.
#[derive(Encode, Decode, CborLen, Clone, Debug, Default)]
#[cbor(map)]
pub struct TraversalContext {
    /// Encryption key map: key ID -> key bytes.
    #[n(0)]
    pub keys: Option<BTreeMap<u8, [u8; 32]>>,

    /// Pipeline for leaf blobs (file content, chunk data).
    #[n(1)]
    pub leaf: Option<BlobPipeline>,

    /// Pipeline for node blobs (serialized `Node` metadata).
    #[n(2)]
    pub node: Option<BlobPipeline>,

    /// Strategy for splitting a file stream into leaf-blob chunks.
    /// `None` = inherit from parent / fall back to default (Xet CDC).
    /// Set to `Some(FileChunkingStrategy::Fixed { chunk_size: N })` for
    /// fixed-size slicing — cheap, predictable, no rolling-hash cost,
    /// every file > N bytes becomes ⌈size / N⌉ blobs (last is the
    /// remainder; no padding to N). Set to
    /// `Some(FileChunkingStrategy::None)` for whole-file-as-one-blob
    /// (equivalent to `Fixed { chunk_size: u32::MAX }`).
    //
    // TODO(design): this is a file-structure policy (leaf boundaries) — a
    // different axis from the per-blob `leaf`/`node` encoding pipelines, and
    // spent at the file-import boundary rather than persisting per blob. Hence a
    // flat sibling, not part of `leaf` (a `LeafConfig { chunking, pipeline }`
    // would falsely couple the two axes). Revisit a clearer `file`-scoped
    // framing/name before this is depended on widely.
    #[n(3)]
    pub chunking: Option<FileChunkingStrategy>,
}

/// Processing pipeline for a single blob type.
///
/// Fields are ordered in execution order: compress → pad → encrypt.
///
/// Padding is applied after compression (to reach a block boundary)
/// but before encryption (so the ciphertext hides the padded size).
///
/// # Context Merging
///
/// Each field uses `Option<T>` independently:
/// - `None`: Inherit that stage from the parent's pipeline
/// - `Some(strategy)`: Override with this explicit strategy
///
/// Strategy enums include explicit "off" variants (e.g., `Uncompressed`,
/// `Plaintext`) to allow explicit opt-out as a valid override.
/// [`PaddingStrategy`] uses `block_size: 1` for "no padding".
/// This provides three states per field: inherit, explicit off, explicit on.
#[derive(Encode, Decode, CborLen, Clone, Debug)]
#[cbor(map)]
pub struct BlobPipeline {
    /// Compression strategy.
    #[n(0)]
    pub compression: Option<CompressionStrategy>,

    /// Padding strategy. Stored blobs may be padded to fixed-size blocks
    /// to prevent traffic analysis / size fingerprinting.
    /// Applied after compression, before encryption.
    /// Effective block size defaults to 1 when not set (no padding),
    /// making `ContentRef.stored_blocks` the exact byte count.
    #[n(1)]
    pub padding: Option<PaddingStrategy>,

    /// Encryption strategy with key ID.
    #[n(2)]
    pub encryption: Option<(EncryptionStrategy, u8)>,

    /// When `Some(true)`, the encoder skips compression for this
    /// pipeline if the padded compressed size doesn't beat the padded
    /// uncompressed size — and records a per-entry `Uncompressed`
    /// override in the resulting `NodeEntry.child_context` so the
    /// decoder knows what to do.
    ///
    /// `Option<bool>` (not plain `bool`) because `merge_contexts`
    /// distinguishes three states: `Some(true)` = explicit opt-in,
    /// `Some(false)` = explicit opt-out, `None` = inherit from
    /// parent. With plain `bool` a child entry's default `false`
    /// would silently override a parent's explicit `Some(true)` —
    /// the merge could no longer express "I have no opinion."
    ///
    /// Lives here (rather than as a runtime flag on `Snapshot` /
    /// `Pipeline`) so the policy travels with the rest of the encoding
    /// definition and propagates correctly through `merge_contexts`
    /// when child entries override the parent's leaf pipeline.
    #[n(3)]
    pub skip_when_unhelpful: Option<bool>,
}

// =============================================================================
// BuildContext - Write Context (stored on NodeHeader, inherited by children)
// =============================================================================

/// Structural build policies used to construct a tree.
///
/// Stored optionally on [`NodeHeader`] — only on the root node or where
/// settings diverge from the parent. Children inherit.
///
/// A writer loading an existing snapshot reads this to ensure identical
/// chunking boundaries, preserving structural sharing and efficient diffs.
///
/// This is never needed for reading — a reader only needs
/// [`TraversalContext`] to decrypt and decompress.
#[derive(Encode, Decode, CborLen, Clone, Debug, Default)]
#[cbor(map)]
pub struct BuildContext {
    /// How the structural Prolly Tree (directories/metadata) is chunked.
    #[n(0)]
    pub meta_chunking: Option<MetaChunkingStrategy>,

    /// How file content is chunked (CDC strategy).
    /// This is a semantic/content-level policy: it determines whether
    /// a file becomes a single `Leaf` or a `Link` to a chunk tree.
    #[n(1)]
    pub file_chunking: Option<FileChunkingStrategy>,

    /// Legacy on-wire field from the pre-`dad2135` percentage-threshold
    /// design of compression-skip. Kept for on-wire compatibility but
    /// **not read by the current encode path**.
    ///
    /// Skip-when-unhelpful now lives on the per-pipeline
    /// [`BlobPipeline::skip_when_unhelpful`] field (so the policy
    /// travels with the rest of the encoding definition and propagates
    /// correctly through `merge_contexts`).
    ///
    /// This field may be retired or reused in a future on-wire revision.
    #[n(2)]
    pub compression_skip_threshold: Option<u8>,
}

/// Encryption strategy for data storage.
///
/// Uses explicit variants including `Plaintext` to allow "no encryption"
/// as a valid strategy choice, simplifying context merging semantics.
#[derive(Encode, Decode, CborLen, Clone, Debug, PartialEq, Eq)]
#[cbor(index_only)]
pub enum EncryptionStrategy {
    /// No encryption - data stored as plaintext.
    #[n(0x00)]
    Plaintext,

    /// Deterministic ChaCha20 (pure stream cipher, no Poly1305).
    ///
    /// Key = KDF(master_secret, plaintext_hash), nonce = 0.
    /// Each blob gets a unique key derived from the plaintext BLAKE3 hash,
    /// so nonce reuse is impossible. Authentication is via
    /// `blake3(ciphertext) == ContentRef.hash` (network) and
    /// `blake3(plaintext) == ContentRef.plaintext_hash` (local).
    #[n(0x01)]
    DeterministicChaCha20,

    /// ChaCha20 keyed by an **inlined explicit key** (nonce = 0). The 32
    /// bytes in the referenced key slot ARE the ChaCha20 key directly —
    /// NO KDF, no `plaintext_hash` mixing.
    ///
    /// This is the D21 `copy` mechanism (see [`crate::copy`]). A shallow
    /// copy re-homes a source leaf's ciphertext verbatim and inlines the
    /// source's *per-blob* key (`derive_blob_key(KDF_LEAF, src_master,
    /// plaintext_hash)`) — NOT the source master — into the entry's
    /// `child_context.keys` under
    /// [`KEY_SLOT_EXPLICIT_LEAF`](crate::snapshot::KEY_SLOT_EXPLICIT_LEAF).
    /// The destination reader then reproduces the identical keystream from
    /// that one slot without ever learning the source master data key.
    /// Integrity is still enforced one layer up by the
    /// `blake3(plaintext) == plaintext_hash` check in
    /// [`Pipeline::export_leaf`](crate::pipeline::Pipeline::export_leaf).
    #[n(0x02)]
    ExplicitKeyChaCha20,
}

/// Compression strategy for data storage.
///
/// Uses explicit variants including `Uncompressed` to allow "no compression"
/// as a valid strategy choice, simplifying context merging semantics.
#[derive(Encode, Decode, CborLen, Clone, Debug, PartialEq, Eq)]
#[cbor(map)]
pub enum CompressionStrategy {
    /// No compression - data stored uncompressed.
    #[n(0x00)]
    Uncompressed,

    /// Standard Zstd compression (no dictionary).
    #[n(0x01)]
    Zstd,

    // Tag `0x02` is permanently RESERVED — formerly
    // `ZstdDictFromPrecedingEntry` (zstd dictionary / "D-chunk"
    // chaining). The strategy was purged (almost never won its
    // complexity; see git history). Never reuse `0x02`: a legacy node
    // carrying it must decode to a clean `Err` (verified by
    // `node::dchunk_purge_guard`), not mis-map onto a new variant.
    /// Zstd at an explicit compression level.
    ///
    /// Use [`CompressionStrategy::Zstd`] for "default level" — this
    /// variant exists so per-pipeline overrides can tune the level
    /// (e.g. `level: 9` for highly-redundant logs where the
    /// L1 → L9 ratio bump is worth the extra encode CPU).
    ///
    /// `level` follows the standard zstd convention: 1–22 for normal
    /// compression, negative values for fast mode (`zstd --fast=N`).
    /// Decoder is level-agnostic — the level is only consulted at
    /// encode time and is recorded here purely so writers know what to
    /// pass `zstd::encode_all`.
    ///
    /// Back-compat: added at a fresh minicbor tag (`0x03`); existing
    /// vaults whose entries carry the unit `Zstd` variant decode
    /// unchanged.
    #[n(0x03)]
    ZstdLevel {
        /// Compression level passed verbatim to the zstd encoder.
        #[n(0)]
        level: i8,
    },
}

/// Padding strategy for stored blobs.
///
/// Blobs are padded to multiples of `block_size` after compression and
/// before encryption, preventing size fingerprinting. The stored blob
/// size is `ContentRef.stored_blocks * block_size`.
///
/// A `block_size` of 1 means no padding (exact byte count).
/// A `block_size` of 4096 pads to 4KiB boundaries (default).
/// A `block_size` of 4096 pads to 4KiB boundaries.
///
/// Uses `Option<PaddingStrategy>` in [`BlobPipeline`] where `None`
/// means inherit from parent context.
#[derive(Encode, Decode, CborLen, Clone, Debug, PartialEq, Eq)]
#[cbor(map)]
pub struct PaddingStrategy {
    /// Block size in bytes. Blobs are padded to multiples of this value.
    /// Use 1 for no padding (stored_blocks = exact byte count).
    #[n(0)]
    pub block_size: u32,
}

/// Meta chunking strategy for directory serialization.
#[derive(Encode, Decode, CborLen, Clone, Debug, PartialEq, Eq)]
#[cbor(map)]
pub enum MetaChunkingStrategy {
    /// Item-driven probability boundary when (blake3(key)[0] & mask) == 0.
    #[n(0x01)]
    ProllyBlake3 {
        /// Expected number of entries per node.
        #[n(0)]
        expected_entries_per_node: u32,
    },
}

/// File chunking strategy for serialization.
///
/// The default is **Gearhash CDC (Huggingface Xet spec)** — content-defined
/// ~64 KiB chunks bounded to `[8 KiB, 128 KiB]` (see [`crate::chunking`]). It is
/// set *explicitly* on every vault at creation, next to the encryption config
/// (see `snapshot::encrypted_split_context`), rather than left to an implicit
/// fallback. `Fixed`/`None` are deliberate opt-outs for blobs that are
/// content-addressed at the file level (append-only logs, immutable segments).
#[derive(Encode, Decode, CborLen, Clone, Debug, PartialEq, Eq)]
#[cbor(map)]
pub enum FileChunkingStrategy {
    /// No chunking - store as single blob.
    #[n(0x00)]
    None,

    /// Strictly fixed-size blocks (e.g., 4MB chunks).
    #[n(0x01)]
    Fixed {
        /// Block size in bytes.
        #[n(0)]
        chunk_size: u32,
    },

    /// Content-Defined Chunking (Gearhash / Xet spec) — the default (see
    /// [`FileChunkingStrategy::default`]).
    #[n(0x02)]
    DataCdc {
        /// CDC parameters. Defaults to the Xet spec constants; the current
        /// [`crate::chunking::XetChunker`] always applies those constants (the
        /// only [`CdcAlgorithm`]), so the params are descriptive metadata today.
        #[n(0)]
        params: DataCdcParams,
    },
}

impl Default for FileChunkingStrategy {
    fn default() -> Self {
        Self::DataCdc {
            params: DataCdcParams::default(),
        }
    }
}

/// Parameters for Content-Defined Chunking.
#[derive(Encode, Decode, CborLen, Clone, Debug, PartialEq, Eq)]
#[cbor(map)]
pub struct DataCdcParams {
    /// Algorithm used for CDC boundary detection.
    #[n(0)]
    pub algorithm: CdcAlgorithm,

    /// Minimum chunk size in bytes.
    #[n(1)]
    pub min_size: u32,

    /// Average/target chunk size in bytes.
    #[n(2)]
    pub avg_size: u32,

    /// Maximum chunk size in bytes.
    #[n(3)]
    pub max_size: u32,
}

/// Algorithm for Content-Defined Chunking.
#[derive(Encode, Decode, CborLen, Clone, Debug, PartialEq, Eq, Default)]
#[cbor(index_only)]
pub enum CdcAlgorithm {
    /// Gearhash: Xet chunking specification using Gear hash.
    #[n(0x00)]
    #[default]
    Gearhash,
}

impl Default for DataCdcParams {
    /// The canonical Huggingface Xet spec constants (see [`crate::chunking`]):
    /// Gearhash, 8 KiB min / 64 KiB target / 128 KiB max.
    fn default() -> Self {
        Self {
            algorithm: CdcAlgorithm::Gearhash,
            min_size: crate::chunking::MIN_CHUNK_SIZE as u32,
            avg_size: crate::chunking::TARGET_CHUNK_SIZE as u32,
            max_size: crate::chunking::MAX_CHUNK_SIZE as u32,
        }
    }
}

#[cfg(test)]
mod dchunk_purge_guard {
    //! Pre-purge verification. Removing the `#[n(0x02)]`
    //! `ZstdDictFromPrecedingEntry` variant must yield a clean `Decode`
    //! error for any legacy tag-0x02 node — never a panic or a mis-map
    //! to another variant. This guard proves that holds *before* the
    //! deletion lands, deciding the commit shape (pure delete + reserved
    //! tag vs. an explicit skip-variant). Post-purge, the second test
    //! flips to assert the captured legacy bytes now decode to `Err`.
    use super::CompressionStrategy;

    /// minicbor `#[cbor(map)]` enums encode a variant as `{ tag => … }`.
    /// A tag absent from `CompressionStrategy` exercises the exact
    /// unknown-discriminant path that removing `0x02` will trigger.
    #[derive(minicbor::Encode)]
    #[cbor(map)]
    enum ForeignTag {
        #[n(0x42)]
        Ghost {
            #[n(0)]
            mask: u8,
        },
    }

    #[test]
    fn unknown_variant_tag_decodes_to_clean_err() {
        let bytes = minicbor::to_vec(ForeignTag::Ghost { mask: 0x07 }).unwrap();
        let decoded: Result<CompressionStrategy, _> = minicbor::decode(&bytes);
        assert!(
            decoded.is_err(),
            "unknown #[cbor(map)] variant tag must Decode to Err (got {decoded:?}); \
             pure purge + reserved tag 0x02 is safe only if this holds"
        );
    }

    /// RED while the variant exists; GREEN once purged. Pins the exact
    /// legacy wire form (captured from minicbor pre-purge:
    /// `ZstdDictFromPrecedingEntry { mask: 0x07 }` →
    /// CBOR `[array(2): tag 0x02, {0: 7}]`) and asserts a legacy
    /// tag-0x02 node decodes to a clean `Err` — the reserved-tag
    /// guarantee. No constructor reference, so it keeps compiling after
    /// the variant is gone.
    #[test]
    fn legacy_0x02_node_decodes_to_err() {
        const LEGACY_DCHUNK_0X02: &[u8] = &[130, 2, 161, 0, 7];
        let decoded: Result<CompressionStrategy, _> = minicbor::decode(LEGACY_DCHUNK_0X02);
        assert!(
            decoded.is_err(),
            "reserved tag 0x02 (formerly ZstdDictFromPrecedingEntry) must \
             decode to Err post-purge, got {decoded:?}"
        );
    }
}
