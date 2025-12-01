use std::path::PathBuf;

use anyhow::{Context, Result};
use s5_blobs::Client as BlobsClient;
use s5_core::{BlobsRead, RegistryApi};
use s5_fs::{DirContext, FS5, dir::DirV1};
use s5_node::{RemoteRegistry, config::S5NodeConfig, derive_sync_keys};

use crate::SnapshotsCmd;
use crate::helpers::{build_endpoint, parse_hash_hex, peer_endpoint_addr};

pub async fn run_snapshots(
    cmd: SnapshotsCmd,
    config: &S5NodeConfig,
    node_config_file: &std::path::Path,
    fs: &FS5,
    fs_handle: &FS5,
    fs_root: &PathBuf,
) -> Result<()> {
    let config_dir = node_config_file.parent();
    match cmd {
        SnapshotsCmd::Head { sync } => {
            let sync_cfg = config
                .sync
                .get(&sync)
                .with_context(|| format!("sync.{sync} not found in node config"))?;
            let first = sync_cfg
                .via_untrusted
                .first()
                .with_context(|| format!("sync.{sync} has empty via_untrusted list"))?;
            let _peer_cfg = config
                .peer
                .get(first)
                .with_context(|| format!("via_untrusted peer '{first}' not found"))?;
            let endpoint = build_endpoint(&config.identity, config_dir).await?;
            let peer_addr = peer_endpoint_addr(config, first)?;
            let keys = derive_sync_keys(&sync_cfg.shared_secret);
            let stream_key = keys.stream_key();
            let registry = RemoteRegistry::connect(endpoint, peer_addr);
            if let Some(msg) = registry.get(&stream_key).await? {
                println!(
                    "sync.{sync} head: hash={} revision={}",
                    msg.hash, msg.revision
                );
            } else {
                println!("sync.{sync} has no remote snapshot yet");
            }
        }
        SnapshotsCmd::Download { peer, hash, out } => {
            let endpoint = build_endpoint(&config.identity, config_dir).await?;
            let peer_addr = peer_endpoint_addr(config, &peer)?;
            let client = BlobsClient::connect(endpoint, peer_addr);
            let hash = parse_hash_hex(&hash)?;
            let bytes = client
                .blob_download(hash)
                .await
                .context("failed to download snapshot blob")?;
            tokio::fs::write(&out, &bytes)
                .await
                .with_context(|| format!("failed to write to {}", out.display()))?;
            println!(
                "downloaded snapshot {} bytes to {}",
                bytes.len(),
                out.display()
            );
        }
        SnapshotsCmd::Restore { root, peer, hash } => {
            let endpoint = build_endpoint(&config.identity, config_dir).await?;
            let peer_addr = peer_endpoint_addr(config, &peer)?;
            let client = BlobsClient::connect(endpoint, peer_addr);
            let hash = parse_hash_hex(&hash)?;
            let bytes = client
                .blob_download(hash)
                .await
                .context("failed to download snapshot blob")?;

            let snapshot =
                DirV1::from_bytes(&bytes).context("failed to decode directory snapshot")?;
            std::fs::create_dir_all(&root)?;
            let ctx = DirContext::open_local_root(&root)?;
            let fs_local = FS5::open(ctx);
            fs_local
                .merge_from_snapshot(snapshot)
                .await
                .context("failed to merge snapshot into local FS5 root")?;
            fs_local
                .save()
                .await
                .context("failed to save restored FS5 root")?;
            println!("restored snapshot into FS5 root at {}", root.display());
        }
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
