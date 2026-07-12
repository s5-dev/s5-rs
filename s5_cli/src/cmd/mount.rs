use std::path::PathBuf;

use anyhow::{Result, anyhow};
use directories::ProjectDirs;
use s5_fs::FS5;
use s5_node::config::S5NodeConfig;

#[allow(dead_code)] // accepted by run_mount but the v2 adapter ignores them
pub struct MountOptions {
    pub mount_point: PathBuf,
    pub root: Option<PathBuf>,
    pub subdir: Option<String>,
    pub read_only: bool,
    pub allow_root: bool,
    pub auto_unmount: bool,
}

/// FUSE mount via the legacy `s5` CLI is no longer wired up — the FUSE
/// adapter only speaks `s5_fs_v2::Snapshot` (the new vault-shaped fs),
/// not the v1 `FS5` this command targeted. The `vup mount <vault>:`
/// path covers the v2 case end-to-end; this stub stays only so the
/// `s5` binary still builds while the legacy CLI is being phased out.
pub async fn run_mount(
    _dirs: &ProjectDirs,
    _cli_node: &str,
    _config: &S5NodeConfig,
    _fs: FS5,
    _options: MountOptions,
) -> Result<()> {
    Err(anyhow!(
        "`s5 mount` is not supported on the v2 FUSE adapter — \
         use `vup mount <vault>: <path>` instead"
    ))
}
