use crate::{
    Hash,
    blob::location::BlobLocation,
    store::{Store, StoreResult},
};
use bytes::Bytes;
use std::sync::Arc;
use tokio_util::io::StreamReader;

use super::paths::{blob_path_for_hash, obao6_path_for_hash};

pub async fn read_as_bytes(
    store: &Arc<dyn Store>,
    hash: Hash,
    offset: u64,
    max_len: Option<u64>,
) -> StoreResult<Bytes> {
    store
        .open_read_bytes(
            &blob_path_for_hash(hash, &store.features()),
            offset,
            max_len,
        )
        .await
}

pub async fn read_stream(
    store: &Arc<dyn Store>,
    hash: Hash,
) -> StoreResult<Box<dyn tokio::io::AsyncRead + Send + Unpin>> {
    let stream = store
        .open_read_stream(&blob_path_for_hash(hash, &store.features()), 0, None)
        .await?;
    Ok(Box::new(StreamReader::new(stream)))
}

pub async fn provide(store: &Arc<dyn Store>, hash: Hash) -> StoreResult<Vec<BlobLocation>> {
    store
        .provide(&blob_path_for_hash(hash, &store.features()))
        .await
}

pub async fn provide_obao6(
    outboard_store: &Option<Arc<dyn Store>>,
    hash: Hash,
) -> StoreResult<Vec<BlobLocation>> {
    if let Some(obao_store) = outboard_store {
        obao_store
            .provide(&obao6_path_for_hash(hash, &obao_store.features()))
            .await
    } else {
        Ok(vec![])
    }
}

pub async fn contains(store: &Arc<dyn Store>, hash: Hash) -> StoreResult<bool> {
    store
        .exists(&blob_path_for_hash(hash, &store.features()))
        .await
}

pub async fn contains_obao6(
    outboard_store: &Option<Arc<dyn Store>>,
    hash: Hash,
) -> StoreResult<bool> {
    if let Some(obao_store) = outboard_store {
        obao_store
            .exists(&obao6_path_for_hash(hash, &obao_store.features()))
            .await
    } else {
        Ok(false)
    }
}

pub async fn size(store: &Arc<dyn Store>, hash: Hash) -> StoreResult<u64> {
    store
        .size(&blob_path_for_hash(hash, &store.features()))
        .await
}
