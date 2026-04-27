use std::path::PathBuf;

use anyhow::Result;
use s5_fs::FS5;
use s5_node::config::S5NodeConfig;

use crate::SnapshotsCmd;

pub async fn run_snapshots(
    cmd: SnapshotsCmd,
    _config: &S5NodeConfig,
    _node_config_file: &std::path::Path,
    fs: &FS5,
    fs_handle: &FS5,
    fs_root: &PathBuf,
) -> Result<()> {
    match cmd {
        SnapshotsCmd::ListFs => {
            let snapshots = s5_fs::snapshots::list_snapshots(fs_root)?;
            if snapshots.is_empty() {
                println!("no snapshots");
            } else {
                for (name, hash) in snapshots {
                    println!("{}\t{}", name, hash);
                }
            }
            fs_handle.shutdown().await?;
        }
        SnapshotsCmd::CreateFs => {
            let (name, hash) = fs.create_snapshot().await?;
            println!("created snapshot\t{}\t{}", name, hash);
            fs_handle.save().await?;
            fs_handle.shutdown().await?;
        }
        SnapshotsCmd::DeleteFs { name } => {
            fs.delete_snapshot(&name).await?;
            println!("deleted snapshot\t{}", name);
            fs_handle.save().await?;
            fs_handle.shutdown().await?;
        }
    }

    Ok(())
}
