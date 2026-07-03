//! FUSE adapter for s5 vaults.
//!
//! Sits on top of [`s5_fs_v2`] and exposes a vault as a normal POSIX
//! filesystem via [`fuse3`]. The implementation is split into:
//!
//! - [`attr`] — file/directory attribute builders (TTLs, perms, size).
//! - [`path`] — snapshot-key resolution helpers shared across read and
//!   write adapters.
//! - [`read`] — [`read::ReadOnlyFs`] (the read-only adapter).
//! - [`mount`] — mount entry points (currently [`mount::mount`] for the
//!   read-only path; writable mount lands next).
//!
//! Public surface kept narrow: callers normally just need
//! [`mount::mount`] (re-exported as [`crate::mount`]) and, when
//! testing, [`read::ReadOnlyFs`].
//!
//! TODO(mount/test): there is NO end-to-end mount test —
//! nothing mounts a real vault and reads/writes through the kernel. Add
//! an env-gated E2E (needs /dev/fuse): mount → hash-verify the tree →
//! write via --rw → unmount → verify the published snapshot.
//! TODO(mount/limits): document-as-beta-limitations rather
//! than silently EIO: no rename, no random-offset writes (whole-file
//! replace only), no chmod/chown persistence beyond size-truncate.

mod attr;
mod path;
pub mod read;
pub mod write;

pub mod debounce;
pub mod mount;

pub use mount::{mount, mount_rw, preflight};
pub use read::ReadOnlyFs;
pub use write::WritableFs;
