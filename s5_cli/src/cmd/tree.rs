use anyhow::Result;
use s5_fs::FS5;

pub async fn run_tree(fs: FS5, fs_handle: FS5, path: Option<String>) -> Result<()> {
    let start = path.unwrap_or_default();
    s5_fs::debug::print_tree(&fs, &start).await?;
    fs_handle.shutdown().await?;
    Ok(())
}
