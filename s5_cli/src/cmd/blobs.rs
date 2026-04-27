use std::path::PathBuf;

use anyhow::Result;
use s5_core::{BlobsRead, RegistryPinner};
use s5_node::config::S5NodeConfig;
use s5_registry_redb::RedbRegistry;

use super::util::{open_store, registry_path};
use crate::BlobsCmd;

pub async fn run_blobs(
    cmd: BlobsCmd,
    config: &S5NodeConfig,
    node_config_file: &std::path::Path,
    fs_root: &PathBuf,
) -> Result<()> {
    match cmd {
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
            let root_key = None;
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
            let root_key = None;
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
