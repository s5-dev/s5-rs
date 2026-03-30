use anyhow::Result;

use crate::S5Node;

/// Spawns configured FUSE mounts for a node (best-effort).
///
/// NOTE: The old `[fuse.*]` config section has been removed. FUSE mounts
/// will be re-introduced through the vault model in a future iteration.
/// This function is a no-op stub.
pub async fn spawn_fuse_mounts(_node: &S5Node) -> Result<()> {
    tracing::debug!("spawn_fuse_mounts: no-op (fuse config removed, will use vaults)");
    Ok(())
}
