//! s5_fs_v2: Unified Node filesystem for S5 (V2)
//!
//! This crate defines the unified `Node` architecture for FS5, where
//! everything — files, directories, chunks, and tree nodes — is represented
//! by a single unified `Node` type stored in CBOR format.
//!
//! # Architecture
//!
//! - **Data model** (`node`): `Node`, `NodeEntry`, `TraversalContext` (on-wire CBOR types)
//! - **Crypto helpers** (`context`): Stateless encrypt/decrypt/compress/decompress functions
//! - **Layer trait** (`layer`): `ReadableLayer` — async read interface for snapshots and overlays
//! - **Snapshot** (`snapshot`): The main runtime type — immutable prolly tree with
//!   node loading, context derivation, file import/export, and recursive walk
//! - **Overlay** (`overlay`): `WritableOverlay` — mutable layer on top of snapshots
//! - **Merge** (`merge`): `MergedView` — k-way priority merge over layers
//! - **Persist** (`persist`): `Snapshot::merge_and_persist()` — diff-aware prolly tree builder with dedup

pub mod layer;
pub mod node;
pub mod overlay;

pub mod chunking;
pub(crate) mod context;
pub mod merge;
pub mod persist;
pub mod snapshot;
