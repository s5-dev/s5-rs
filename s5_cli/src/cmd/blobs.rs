use std::path::PathBuf;

use anyhow::{Context, Result, anyhow, bail};
use s5_blobs::Client as BlobsClient;
use s5_core::{BlobsRead, BlobsWrite, RedbRegistry, RegistryPinner};
use s5_node::config::S5NodeConfig;

use super::util::{open_store, registry_path};
use crate::BlobsCmd;
use crate::helpers::{build_endpoint, parse_hash_hex, peer_endpoint_addr};

pub async fn run_blobs(
    cmd: BlobsCmd,
    config: &S5NodeConfig,
    node_config_file: &std::path::Path,
    fs_root: &PathBuf,
) -> Result<()> {
    match cmd {
        BlobsCmd::Upload { peer, path } => {
            let endpoint = build_endpoint(&config.identity).await?;
            let peer_addr = peer_endpoint_addr(config, &peer)?;
            let client = BlobsClient::connect(endpoint, peer_addr);
            let blob = client
                .blob_upload_file(path.clone())
                .await
                .context("failed to upload blob")?;
            println!("uploaded blob: hash={} size={}", blob.hash, blob.size);
        }
        BlobsCmd::Download { peer, hash, out } => {
            let endpoint = build_endpoint(&config.identity).await?;
            let peer_addr = peer_endpoint_addr(config, &peer)?;
            let client = BlobsClient::connect(endpoint, peer_addr);
            let hash = parse_hash_hex(&hash)?;
            let bytes = client
                .blob_download(hash)
                .await
                .context("failed to download blob")?;
            tokio::fs::write(&out, &bytes)
                .await
                .with_context(|| format!("failed to write to {}", out.display()))?;
            println!("downloaded {} bytes to {}", bytes.len(), out.display());
        }
        BlobsCmd::Delete { peer, hash } => {
            let endpoint = build_endpoint(&config.identity).await?;
            let peer_addr = peer_endpoint_addr(config, &peer)?;
            let client = BlobsClient::connect(endpoint, peer_addr);
            let hash = parse_hash_hex(&hash)?;
            let res = client.delete_blob(hash).await.map_err(|e| anyhow!(e))?;
            match res {
                Ok(true) => {
                    println!("deleted blob: it became orphaned and was removed from storage")
                }
                Ok(false) => {
                    println!("un-pinned blob for this node; other pins still reference it")
                }
                Err(msg) => {
                    bail!("remote error while deleting blob: {}", msg);
                }
            }
        }
        BlobsCmd::GcLocal { store, dry_run } => {
            // Compute registry path exactly as the node would, so we
            // see the same pin metadata used by the running node.
            let registry_path = registry_path(node_config_file, config);
            std::fs::create_dir_all(&registry_path)?;
            let registry = RedbRegistry::open(&registry_path)?;
            let pinner = RegistryPinner::new(registry);

            // Open the configured store locally, using the same
            // configuration schema as the node.
            let blob_store = open_store(config, &store).await?;

            // Collect all content hashes reachable from this node's
            // primary FS5 root (current head + snapshots).
            let root_key = config.keys.get(&0x0e);
            let reachable = s5_fs::gc::collect_fs_reachable_hashes(fs_root, root_key).await?;

            let report = s5_fs::gc::gc_store(&blob_store, &reachable, &pinner, dry_run).await?;

            if dry_run {
                println!(
                    "{} blobs would be deleted from local store '{}'",
                    report.candidates.len(),
                    store
                );
                for h in &report.candidates {
                    println!("{}", h);
                }
            } else {
                for (h, err) in &report.delete_errors {
                    eprintln!("failed to delete {}: {err}", h);
                }
                println!(
                    "deleted {} blobs from local store '{}'",
                    report.deleted, store
                );
            }
        }
        BlobsCmd::VerifyLocal { store } => {
            // Open the configured store locally, using the same
            // configuration schema as the node.
            let blob_store = open_store(config, &store).await?;

            // Collect all content hashes reachable from this node's
            // primary FS5 root (current head + snapshots).
            let root_key = config.keys.get(&0x0e);
            let reachable = s5_fs::gc::collect_fs_reachable_hashes(fs_root, root_key).await?;

            let mut missing = Vec::new();
            for h in &reachable {
                if !blob_store.contains(*h).await? {
                    missing.push(*h);
                }
            }

            if missing.is_empty() {
                println!(
                    "all {} referenced blobs are present in store '{}'",
                    reachable.len(),
                    store
                );
            } else {
                eprintln!(
                    "{} referenced blobs are MISSING in store '{}'",
                    missing.len(),
                    store
                );
                for h in missing {
                    eprintln!("{}", h);
                }
            }
        }
    }

    Ok(())
}
