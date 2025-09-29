use anyhow::anyhow;
use bytes::Bytes;
use futures::Stream;
use s3::{Bucket, Region, creds::Credentials};
use s5_core::{
    blob::location::BlobLocation,
    store::{StoreFeatures, StoreResult},
};
use std::u64;
use tokio_util::io::{ReaderStream, StreamReader};

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
        s3::set_retries(5);
        Self { bucket }
    }
}

#[async_trait::async_trait]
impl s5_core::store::Store for S3Store {
    async fn put_stream(
        &self,
        path: &str,
        stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<()> {
        let mut reader = StreamReader::new(stream);
        self.bucket.put_object_stream(&mut reader, path).await?;
        Ok(())
    }

    async fn put_bytes(&self, path: &str, bytes: Bytes) -> StoreResult<()> {
        self.bucket.put_object(path, &bytes).await?;
        Ok(())
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
    ) -> StoreResult<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>>
    {
        let (reader, mut writer) = tokio::io::duplex(64 * 1024);

        let bucket = self.bucket.clone();
        let path = path.to_owned();
        tokio::spawn(async move {
            let _ = bucket
                .get_object_range_to_writer(
                    path,
                    offset,
                    max_len.map(|len| len - offset - 1),
                    &mut writer,
                )
                .await;
        });

        Ok(Box::new(ReaderStream::new(reader)))
    }

    async fn open_read_bytes(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        let res = self
            .bucket
            .get_object_range(path, offset, max_len.map(|len| len - offset - 1))
            .await?;
        Ok(res.into_bytes())
    }

    async fn delete(&self, path: &str) -> StoreResult<()> {
        self.bucket.delete_object(path).await?;
        Ok(())
    }

    async fn rename(&self, _: &str, _: &str) -> StoreResult<()> {
        panic!("not supported by this store")
    }

    async fn provide(&self, path: &str) -> StoreResult<Vec<BlobLocation>> {
        let res = self.bucket.presign_get(path, 86400, None).await?;
        Ok(vec![BlobLocation::Url(res)])
    }
}
