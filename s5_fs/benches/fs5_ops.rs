use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use s5_fs::{DirContext, FS5, FileRef};
use tempfile::tempdir;

async fn create_fs_with_files(file_count: usize) -> anyhow::Result<FS5> {
    let tmp = tempdir()?;
    let ctx = DirContext::open_local_root(tmp.path())?;
    let fs = FS5::open(ctx);

    fs.batch(|fs| async move {
        for i in 0..file_count {
            let path = format!("dir_{}/file_{}.bin", i / 1000, i);
            let data = Bytes::from_static(b"x");
            fs.file_put_sync(&path, FileRef::new_inline_blob(data.clone()))
                .await?;
        }
        Ok(())
    })
    .await?;

    Ok(fs)
}

fn bench_fs5_batch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let rt_handle = &rt;

    c.bench_function("fs5_batch_1k_files", |b| {
        b.iter(|| {
            rt_handle.block_on(async {
                let _ = create_fs_with_files(1_000).await.unwrap();
            });
        });
    });
}

fn bench_fs5_list(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let rt_handle = &rt;

    // Pre-populate a filesystem and then benchmark listing it.
    let fs = rt_handle.block_on(async { create_fs_with_files(5_000).await.unwrap() });

    c.bench_function("fs5_list_root_5k_entries", |b| {
        let fs = fs.clone();
        b.iter(|| {
            let fs = fs.clone();
            rt_handle.block_on(async move {
                let (_entries, _cursor) = fs.list(None, 1_000).await.unwrap();
            });
        });
    });
}

fn bench_fs5_file_get(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let rt_handle = &rt;

    // Pre-populate a filesystem and then benchmark file_get on a hot path.
    let fs = rt_handle.block_on(async { create_fs_with_files(5_000).await.unwrap() });

    c.bench_function("fs5_file_get_hot", |b| {
        let fs = fs.clone();
        b.iter(|| {
            let fs = fs.clone();
            rt_handle.block_on(async move {
                let key = "dir_0/file_0.bin";
                let _ = fs.file_get(key).await;
            });
        });
    });
}

criterion_group!(fs5, bench_fs5_batch, bench_fs5_list, bench_fs5_file_get);
criterion_main!(fs5);
