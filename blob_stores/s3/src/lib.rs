use anyhow::anyhow;
use bytes::Bytes;
use futures::Stream;
use s3::{Bucket, Region, creds::Credentials, error::S3Error};
use s5_core::{
    blob::location::BlobLocation,
    store::{StoreFeatures, StoreResult},
};
use std::u64;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct S3StoreConfig {
    endpoint: String,
    #[serde(default)]
    region: String,
    bucket_name: String,
    access_key: String,
    secret_key: String,
}

#[derive(Debug, Clone)]
pub struct S3Store {
    bucket: Box<Bucket>,
}

impl S3Store {
    pub fn create(config: S3StoreConfig) -> Self {
        let bucket = Bucket::new(
            &config.bucket_name,
            Region::Custom {
                endpoint: config.endpoint,
                region: config.region,
            },
            Credentials::new(
                Some(&config.access_key),
                Some(&config.secret_key),
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
        todo!("implement")
    }

    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<()> {
        todo!("implement")
    }

    fn features(&self) -> StoreFeatures {
        StoreFeatures {
            supports_rename: false,
            case_sensitive: true,
            recommended_max_dir_size: u64::MAX,
        }
    }

    async fn exists(&self, path: &str) -> StoreResult<bool> {
        match self.bucket.head_object(path).await {
            Ok((_, 200)) => Ok(true),
            Ok((_, 404)) => Ok(false),
            Ok((_, code)) => Err(anyhow!("unexpected http status code {code}")),
            Err(e) => Err(e.into()),
        }
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
        self.bucket.delete_object(path).await?;
        Ok(())
    }

    async fn rename(&self, old_path: &str, new_path: &str) -> StoreResult<()> {
        panic!("not supported by this store")
    }

    async fn provide(&self, path: &str) -> StoreResult<Vec<BlobLocation>> {
        let res = self.bucket.presign_get(path, 86400, None).await?;
        Ok(vec![BlobLocation::Url(res)])
    }
}
