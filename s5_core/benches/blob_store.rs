use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use s5_core::blob::store::BlobStore;
use s5_store_memory::MemoryStore;

fn bench_import_bytes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let rt_handle = &rt;
    let store = BlobStore::new(MemoryStore::default());
    let small = Bytes::from(vec![0u8; 4 * 1024]);
    let medium = Bytes::from(vec![0u8; 1024 * 1024]);

    let mut group = c.benchmark_group("blob_store_import_bytes");

    group.bench_function("import_bytes_4k", |b| {
        let store = store.clone();
        let data = small.clone();
        b.iter(|| {
            let store = store.clone();
            let data = data.clone();
            rt_handle.block_on(async move {
                let _ = store.import_bytes(data).await.unwrap();
            });
        });
    });

    group.bench_function("import_bytes_1mb", |b| {
        let store = store.clone();
        let data = medium.clone();
        b.iter(|| {
            let store = store.clone();
            let data = data.clone();
            rt_handle.block_on(async move {
                let _ = store.import_bytes(data).await.unwrap();
            });
        });
    });

    group.finish();
}

fn bench_read_bytes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let rt_handle = &rt;
    let store = BlobStore::new(MemoryStore::default());

    // Preload one blob and reuse its hash in the benchmark loop.
    let hash = rt_handle.block_on(async {
        let data = Bytes::from(vec![0u8; 1024 * 1024]);
        let id = store.import_bytes(data).await.unwrap();
        id.hash
    });

    c.bench_function("read_bytes_1mb", |b| {
        let store = store.clone();
        b.iter(|| {
            let store = store.clone();
            rt_handle.block_on(async move {
                let _ = store.read_as_bytes(hash, 0, None).await.unwrap();
            });
        });
    });
}

criterion_group!(blob_store, bench_import_bytes, bench_read_bytes);
criterion_main!(blob_store);
