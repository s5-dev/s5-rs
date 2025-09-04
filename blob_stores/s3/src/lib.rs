use bytes::Bytes;
use futures::Stream;
use s3::{Bucket, Region, creds::Credentials};
use s5_core::{
    blob::location::BlobLocation,
    store::{StoreFeatures, StoreResult},
};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct S3StoreConfig {}

#[derive(Debug, Clone)]
pub struct S3Store {
    bucket: Box<Bucket>,
}

impl S3Store {
    pub fn create(config: S3StoreConfig) -> Self {
        let bucket = Bucket::new(
            "test-rust-s3",
            Region::Custom {
                endpoint: "http://127.0.0.1:8080".to_string(),
                region: "".to_string(),
            },
            Credentials::new(
                Some("ACCESS_KEY_ID"),
                Some("SECRET_ACCESS_KEY"),
                None,
                None,
                None,
            )
            .unwrap(),
        )
        .unwrap()
        .with_path_style();
        Self { bucket }
    }
}

#[async_trait::async_trait]
impl s5_core::store::Store for S3Store {
    async fn put_stream(
        &self,
        path: &str,
        stream: Box<dyn Stream<Item = Bytes> + Send + Unpin + 'static>,
    ) -> StoreResult<()> {
        todo!()
    }

    fn features(&self) -> StoreFeatures {
        todo!()
    }

    async fn exists(&self, path: &str) -> StoreResult<bool> {
        todo!()
    }

    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<()> {
        todo!()
    }

    async fn open_read_stream(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Box<dyn Stream<Item = Bytes> + Send + Unpin + 'static>> {
        todo!()
    }

    async fn open_read_bytes(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        todo!()
    }

    async fn delete(&self, path: &str) -> StoreResult<()> {
        todo!()
    }

    async fn rename(&self, old_path: &str, new_path: &str) -> StoreResult<()> {
        todo!()
    }

    async fn provide(&self, path: &str) -> StoreResult<Vec<BlobLocation>> {
        todo!()
    }
}
