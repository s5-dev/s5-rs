use anyhow::anyhow;
use bytes::Bytes;
use futures::Stream;
use s3::{Bucket, Region, creds::Credentials};
use s5_core::{
    blob::location::BlobLocation,
    store::{StoreFeatures, StoreResult},
};
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
        let mut bucket = *self.bucket.clone();

        let range_val = if let Some(len) = max_len {
            format!("bytes={}-{}", offset, offset + len - 1)
        } else {
            format!("bytes={}-", offset)
        };
        bucket.add_header("Range", &range_val);

        let response_data = bucket.get_object_stream(path).await?;
        let stream = ReaderStream::new(response_data);

        Ok(Box::new(stream))
    }

    async fn open_read_bytes(
        &self,
        path: &str,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        let end = max_len.map(|len| offset + len - 1);
        let res = self.bucket.get_object_range(path, offset, end).await?;
        Ok(res.into_bytes())
    }

    async fn delete(&self, path: &str) -> StoreResult<()> {
        self.bucket.delete_object(path).await?;
        Ok(())
    }

    async fn rename(&self, _: &str, _: &str) -> StoreResult<()> {
        Err(anyhow!("rename not supported by S3Store"))
    }

    async fn provide(&self, path: &str) -> StoreResult<Vec<BlobLocation>> {
        let res = self.bucket.presign_get(path, 86400, None).await?;
        Ok(vec![BlobLocation::Url(res)])
    }

    async fn size(&self, path: &str) -> StoreResult<u64> {
        let (head, code) = self.bucket.head_object(path).await?;
        if code != 200 {
            return Err(anyhow!("unexpected http status code {code}"));
        }
        let len = head
            .content_length
            .ok_or_else(|| anyhow!("missing content-length"))?;
        Ok(len.try_into()?)
    }

    async fn list(
        &self,
    ) -> StoreResult<Box<dyn Stream<Item = Result<String, std::io::Error>> + Send + Unpin + 'static>>
    {
        let results = self.bucket.list("".to_string(), None).await?;
        let paths: Vec<String> = results
            .into_iter()
            .flat_map(|res| res.contents)
            .map(|obj| obj.key)
            .collect();

        let stream = futures::stream::iter(paths.into_iter().map(Ok));
        Ok(Box::new(stream))
    }
}

#[cfg(test)]
mod tests {
    // S3 tests require a running S3-compatible server (e.g., MinIO)
    // They are ignored by default
    #[allow(unused_imports)]
    use super::*;
    #[allow(unused_imports)]
    use s5_core::testutil::StoreTests;

    #[tokio::test]
    #[ignore = "requires S3-compatible server"]
    async fn test_s3_store() {
        let config = S3StoreConfig {
            endpoint: "http://localhost:9000".to_string(),
            region: "us-east-1".to_string(),
            bucket_name: "test-bucket".to_string(),
            access_key: "minioadmin".to_string(),
            secret_key: "minioadmin".to_string(),
        };
        let store = S3Store::create(config);
        StoreTests::new(&store).run_all().await.unwrap();
    }
}
