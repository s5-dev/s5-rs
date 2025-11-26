use std::path::PathBuf;

use anyhow::Result;
use bytes::Bytes;
use s5_fs::{DirContext, FS5, FileRef};

#[tokio::main]
async fn main() -> Result<()> {
    let root: PathBuf = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().expect("current dir"));

    let file_count: usize = std::env::var("FS5_LOAD_FILES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);

    let ctx = DirContext::open_local_root(&root)?;
    let fs = FS5::open(ctx);

    fs.batch(|fs| async move {
        let data = Bytes::from_static(b"x");
        for i in 0..file_count {
            let path = format!("dir_{}/file_{}.bin", i / 1_000, i);
            fs.file_put_sync(&path, FileRef::new_inline_blob(data.clone()))
                .await?;
        }
        Ok(())
    })
    .await?;

    fs.save().await?;

    Ok(())
}
