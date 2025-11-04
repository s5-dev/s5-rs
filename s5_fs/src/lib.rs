//! # S5 File-system (FS5)
//!
//! High-level, *content addressed*, optionally *encrypted* directory tree
//! Everything is an immutable [`DirV1`] snapshot; mutability is simulated
//! through **actors** that rewrite parent snapshots atomically.
//!
//! ## Layers
//! 1. `dir`   – pure data structures (CBOR encoded).  
//! 2. `actor` – single-threaded state machine that owns one directory
//!    snapshot and processes commands sequentially.  
//! 3. `api`   – ergonomic façade (`FS5`) that applications use.  
//! 4. `context` – wiring (blob-store, registry, encryption keys …).  

mod actor;
mod api;
mod context;
pub mod dir;

pub use api::FS5;
pub use context::{DirContext, DirContextParentLink, SigningKey};
pub use dir::FileRef;

/// Crate-wide result alias that bubbles up [`anyhow::Error`].
pub type FSResult<T> = anyhow::Result<T>;
