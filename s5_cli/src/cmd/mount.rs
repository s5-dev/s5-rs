use std::path::PathBuf;

use anyhow::Result;
use directories::ProjectDirs;
use s5_core::BlobStore;
use s5_fs::FS5;
use s5_node::config::S5NodeConfig;

pub struct MountOptions {
    pub mount_point: PathBuf,
    pub root: Option<PathBuf>,
    pub subdir: Option<String>,
    pub read_only: bool,
    pub allow_root: bool,
    pub auto_unmount: bool,
}

pub async fn run_mount(
    dirs: &ProjectDirs,
    cli_node: &str,
    config: &S5NodeConfig,
    fs: FS5,
    options: MountOptions,
) -> Result<()> {
    let fs_root = options.root.unwrap_or_else(|| {
        dirs.data_dir()
            .join("roots")
            .join(format!("{}.fs5", cli_node))
    });
    std::fs::create_dir_all(&fs_root)?;
    std::fs::create_dir_all(&options.mount_point)?;

    // Optionally scope the FS5 view to a subdirectory
    let fs = if let Some(path) = &options.subdir {
        fs.subdir(path).await?
    } else {
        fs
    };

    // Resolve the default store path if possible
    let store_path = if let Some(store_cfg) = config.store.get("default") {
        match store_cfg {
            s5_node::config::NodeConfigStore::Local(cfg) => {
                std::path::PathBuf::from(&cfg.base_path)
            }
            _ => fs_root.clone(),
        }
    } else {
        fs_root.clone()
    };

    // Local blob store for reading file contents and metadata blobs
    let local = s5_store_local::LocalStore::create(s5_store_local::LocalStoreConfig {
        base_path: store_path.to_string_lossy().to_string(),
    });
    let store = BlobStore::new(local);

    s5_fuse::mount(
        &options.mount_point,
        fs,
        store,
        options.read_only,
        options.allow_root,
        options.auto_unmount,
    )
    .await?;
    Ok(())
}
