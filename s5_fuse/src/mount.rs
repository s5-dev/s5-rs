//! Mount entry points.
//!
//! Each entry point builds the appropriate `PathFilesystem` impl,
//! configures `fuse3::path::Session`, and blocks until either the
//! kernel drops the handle or the caller-supplied `until` future
//! resolves. The `until` parameter is the only cancellation source —
//! callers wire it up to whatever fits their lifecycle:
//!
//! - A CLI process listens on `tokio::signal::ctrl_c()`.
//! - A daemon's mount manager wires a `oneshot::Receiver<()>` it can
//!   trigger from an unmount RPC.
//! - A test passes `std::future::pending()` for a mount that runs
//!   until the kernel ejects it.
//!
//! The function returns when `until` resolves (the `MountHandle`
//! drops, which performs the actual unmount) or when the handle drops
//! on its own (kernel-side ejection).

use std::path::Path;

use anyhow::anyhow;
use fuse3::MountOptions;
use fuse3::path::Session;
use fuse3::raw::MountHandle;
use s5_core::blob::BlobStore;
use s5_fs_v2::snapshot::Snapshot;
use tracing::info;

use crate::read::ReadOnlyFs;
use crate::write::WritableFs;

/// Sanity-check the local FUSE surface before handing a `Session` to
/// the kernel. Catches the failure modes that would otherwise surface
/// as the opaque `fusermount run failed`:
///
/// - `/dev/fuse` missing (containerised env without `--device /dev/fuse`,
///   or `fuse` kernel module not loaded on the host).
/// - mount point doesn't exist on disk.
///
/// Public so the caller (CLI banner, daemon RPC handler) can run it
/// before announcing the mount, so any error appears above the
/// announcement rather than below it.
///
/// `fusermount3`'s own "not found" error is already legible, so we
/// don't shadow it here.
pub fn preflight(mountpoint: &Path) -> anyhow::Result<()> {
    if !std::path::Path::new("/dev/fuse").exists() {
        return Err(anyhow!(
            "/dev/fuse is not present — the FUSE kernel device node is missing.\n\
             • In a container: re-launch with `--device /dev/fuse --cap-add SYS_ADMIN` \
               (and `--security-opt apparmor:unconfined` on AppArmor hosts).\n\
             • Bare metal: load the module with `sudo modprobe fuse` (the \
             `fuse` / `fuse3` package on Debian/Ubuntu)."
        ));
    }
    if !mountpoint.exists() {
        return Err(anyhow!(
            "mount point '{}' does not exist — create it first \
             (`mkdir -p {}`)",
            mountpoint.display(),
            mountpoint.display(),
        ));
    }
    Ok(())
}

/// Mount the snapshot at `mountpoint` as read-only. Blocks until
/// either `until` resolves or the kernel drops the mount.
///
/// `allow_root` adds the FUSE `allow_root` option (lets root access the
/// mount alongside the mounting user). `auto_unmount` selects the
/// unprivileged mount path (via `fusermount3`), which auto-unmounts on
/// process exit; the privileged path requires manual `umount`.
pub async fn mount<F>(
    mountpoint: &Path,
    snapshot: Snapshot,
    allow_root: bool,
    auto_unmount: bool,
    until: F,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = ()>,
{
    preflight(mountpoint)?;
    info!(
        mountpoint = %mountpoint.display(),
        allow_root,
        auto_unmount,
        "s5_fuse: mounting (read-only)"
    );

    let fs = ReadOnlyFs::new(snapshot);

    let mut options = MountOptions::default();
    options.fs_name("s5");
    options.read_only(true);
    options.allow_root(allow_root);
    options.uid(0);
    options.gid(0);

    let session = Session::new(options);
    let handle: MountHandle = if auto_unmount {
        session.mount_with_unprivileged(fs, mountpoint).await?
    } else {
        session.mount(fs, mountpoint).await?
    };

    info!("s5_fuse: mounted, waiting for unmount or cancel");
    tokio::select! {
        _ = handle => {}
        _ = until => {
            info!("s5_fuse: cancel signalled, unmounting");
        }
    }
    Ok(())
}

/// Mount the snapshot at `mountpoint` as read-write. Blocks until
/// either `until` resolves or the kernel drops the mount.
///
/// Writes accumulate in an in-memory overlay over the snapshot; the
/// caller drives persistence via [`WritableFs::flush_overlay`] (typical
/// shape: a debounce timer). The `on_mount` callback fires once the
/// FS is constructed (before the kernel sees it), giving the caller a
/// `WritableFs` clone to keep alive for flush triggering — both
/// clones share the same internal `Arc`s, so writes via the kernel
/// and reads via the caller see the same state.
///
/// `auto_unmount` selects the unprivileged mount path (`fusermount3`,
/// auto-unmounts on process exit); the privileged path requires manual
/// `umount`. `allow_root` opts root in to mount visibility.
pub async fn mount_rw<F, U>(
    mountpoint: &Path,
    snapshot: Snapshot,
    store: BlobStore,
    allow_root: bool,
    auto_unmount: bool,
    on_mount: F,
    until: U,
) -> anyhow::Result<()>
where
    F: FnOnce(WritableFs),
    U: std::future::Future<Output = ()>,
{
    preflight(mountpoint)?;
    info!(
        mountpoint = %mountpoint.display(),
        allow_root,
        auto_unmount,
        "s5_fuse: mounting (writable)"
    );

    let fs = WritableFs::new(snapshot, store);
    on_mount(fs.clone());

    let mut options = MountOptions::default();
    options.fs_name("s5");
    options.allow_root(allow_root);
    options.uid(0);
    options.gid(0);

    let session = Session::new(options);
    let handle: MountHandle = if auto_unmount {
        session.mount_with_unprivileged(fs, mountpoint).await?
    } else {
        session.mount(fs, mountpoint).await?
    };

    info!("s5_fuse: writable mount up, waiting for unmount or cancel");
    tokio::select! {
        _ = handle => {}
        _ = until => {
            info!("s5_fuse: cancel signalled, unmounting");
        }
    }
    Ok(())
}
