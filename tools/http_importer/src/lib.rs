use chrono::DateTime;
use reqwest::header::LAST_MODIFIED;
use s5_core::{BlobStore, DirV1, FileRef, OpenDirV1};
use scraper::{Html, Selector};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Semaphore;
use url::Url;

pub struct HttpImporter {
    http_client: reqwest::Client,
    rate_limiter: Arc<Semaphore>,
    dir: OpenDirV1,
    store: BlobStore,
}

impl HttpImporter {
    pub fn new(state_path: PathBuf, store: BlobStore, max_concurrent_blob_imports: usize) -> Self {
        let dir = DirV1::open(state_path).unwrap();

        Self {
            http_client: reqwest::Client::new(),
            rate_limiter: Arc::new(Semaphore::new(max_concurrent_blob_imports)),
            dir,
            store,
        }
    }

    pub async fn import_url(&self, url: Url) -> anyhow::Result<()> {
        if self.dir.file_exists(url.as_str()).await {
            return Ok(());
        }

        let handle = self.rate_limiter.acquire().await;
        log::info!("import_url {url}");

        let res = self.http_client.get(url.clone()).send().await?;
        let content_type = res
            .headers()
            .get("content-type")
            .map(|h| h.to_str().unwrap_or_default())
            .unwrap_or_default();

        let last_modified = res
            .headers()
            .get(LAST_MODIFIED)
            .map(|v| DateTime::parse_from_rfc2822(v.to_str().unwrap()).ok())
            .flatten();

        // TODO use etag header if present

        if content_type.starts_with("text/html") {
            let urls: Vec<Url> = {
                let doc = Html::parse_document(&res.text().await?);
                drop(handle);

                let link_selector = Selector::parse("a").unwrap();
                let base_url = url.as_str();
                doc.select(&link_selector)
                    .filter_map(|element| element.attr("href"))
                    .filter_map(|href| {
                        Url::parse(href)
                            .or_else(|_| url.join(href))
                            .ok()
                            .filter(|parsed_url| parsed_url.as_str().starts_with(base_url))
                    })
                    .collect()
            };

            let tasks: Vec<_> = urls.into_iter().map(|url| self.import_url(url)).collect();
            futures::future::join_all(tasks).await;
        } else {
            // TODO stream response instead of buffering in memory
            /* self.store
            .import_stream(res.bytes_stream().map(Result::unwrap))
            .await?; */
            /* self.indexing_state
            .files
            .insert(url.to_string(), FileRef::new()); */

            let bytes = res.bytes().await?;
            let len = bytes.len() as u64;
            // TODO proper error handling
            let hash = self.store.import_bytes(bytes).await.unwrap();

            let mut file_ref = FileRef::new(hash.into(), len);

            if let Some(lm) = last_modified {
                file_ref.timestamp = Some(lm.timestamp() as u32);
            }

            self.dir.file_put(url.as_str(), file_ref).await?;
        }
        Ok(())
    }
}
