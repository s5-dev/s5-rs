use std::process::Command;

use anyhow::Result;

use crate::{
    S5Node,
    config::{NodeConfigFuseMount, NodeConfigStore},
};

/// Spawns configured FUSE mounts for a node (best-effort).
pub async fn spawn_fuse_mounts(node: &S5Node) -> Result<()> {
    // Snapshot FUSE configs so we can move them into async tasks.
    let fuse_cfgs: Vec<(String, NodeConfigFuseMount)> = node
        .config
        .fuse
        .iter()
        .map(|(name, cfg)| (name.clone(), cfg.clone()))
        .collect();

    // Resolve default store path for FUSE
    let default_store_path =
        if let Some(NodeConfigStore::Local(cfg)) = node.config.store.get("default") {
            Some(cfg.base_path.clone())
        } else {
            None
        };

    for (
        name,
        NodeConfigFuseMount {
            root_path,
            mount_path,
            auto_unmount,
            allow_root,
        },
    ) in fuse_cfgs
    {
        let root_path = root_path.clone();
        let mount_path = mount_path.clone();
        let store_path = default_store_path.clone();

        tracing::info!("fuse.{name}: mounting {root_path} -> {mount_path}");
        tokio::spawn(async move {
            let mut cmd = Command::new("fs5-fuse");
            cmd.arg("--root").arg(&root_path);
            if let Some(sp) = store_path {
                cmd.arg("--store-path").arg(sp);
            }
            if auto_unmount {
                cmd.arg("--auto-unmount");
            }
            if allow_root {
                cmd.arg("--allow-root");
            }
            cmd.arg(&mount_path);
            match cmd.spawn() {
                Ok(_) => {
                    tracing::info!("fuse.{name}: spawned fs5-fuse for {}", mount_path);
                }
                Err(err) => {
                    tracing::warn!(
                        "fuse.{name}: failed to spawn fs5-fuse for {}: {err}",
                        mount_path
                    );
                }
            }
        });
    }

    Ok(())
}
