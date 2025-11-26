use std::path::PathBuf;

use anyhow::Result;
use directories::ProjectDirs;
use s5_fs::{DirContext, FS5};
use s5_node::config::S5NodeConfig;

mod blobs;
mod import;
mod mount;
mod snapshots;
mod tree;
mod util;

pub use blobs::run_blobs;
pub use import::run_import;
pub use mount::run_mount;
pub use snapshots::run_snapshots;
pub use tree::run_tree;

pub async fn run_command(
    dirs: &ProjectDirs,
    cli_node: &str,
    node_config_file: PathBuf,
    local_data_dir: &std::path::Path,
    cmd: crate::Commands,
) -> Result<()> {
    match cmd {
        crate::Commands::Config { cmd } => {
            cmd.run(node_config_file, local_data_dir)?;
            Ok(())
        }
        crate::Commands::Start => {
            let toml_content = std::fs::read_to_string(&node_config_file)?;
            let config: S5NodeConfig = toml::from_str(&toml_content)?;
            s5_node::run_node(node_config_file, config).await?;
            Ok(())
        }
        _ => {
            let toml_content = std::fs::read_to_string(&node_config_file)?;
            let config: S5NodeConfig = toml::from_str(&toml_content)?;

            // TODO support using custom fs meta path
            let fs_root = dirs
                .data_dir()
                .join("roots")
                .join(format!("{}.fs5", cli_node));
            let context = DirContext::open_local_root(&fs_root)?;
            let fs = FS5::open(context).with_autosave(1000).await?;
            let fs_handle = fs.clone();

            match cmd {
                crate::Commands::Import { cmd, target_store } => {
                    run_import(cmd, target_store, &config, &fs, &fs_handle, &fs_root).await
                }
                crate::Commands::Blobs { cmd } => {
                    run_blobs(cmd, &config, &node_config_file, &fs_root).await
                }
                crate::Commands::Snapshots { cmd } => {
                    run_snapshots(cmd, &config, &fs, &fs_handle, &fs_root).await
                }
                crate::Commands::Mount {
                    mount_point,
                    root,
                    subdir,
                    read_only,
                    allow_root,
                    auto_unmount,
                } => {
                    run_mount(
                        dirs,
                        cli_node,
                        &config,
                        fs,
                        mount::MountOptions {
                            mount_point,
                            root,
                            subdir,
                            read_only,
                            allow_root,
                            auto_unmount,
                        },
                    )
                    .await
                }
                crate::Commands::Tree { path } => run_tree(fs, fs_handle, path).await,
                crate::Commands::Config { .. } | crate::Commands::Start => unreachable!(),
            }
        }
    }
}
