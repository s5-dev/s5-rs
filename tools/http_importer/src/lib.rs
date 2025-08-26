use chrono::DateTime;
use reqwest::header::LAST_MODIFIED;
use s5_core::{BlobStore, DirV1, FileRef};
use scraper::{Html, Selector};
use std::{fs::File, path::PathBuf, sync::Arc};
use tokio::sync::{RwLock, Semaphore};
use url::Url;

pub struct HttpImporter<T: BlobStore> {
    http_client: reqwest::Client,
    rate_limiter: Arc<Semaphore>,
    indexing_state: Arc<RwLock<DirV1>>,
    store: T,
    indexing_state_dir_path: PathBuf,
    lock_file: File,
}

impl<T: BlobStore> HttpImporter<T> {
    pub fn new(state_path: PathBuf, store: T, max_concurrent_blob_imports: usize) -> Self {
        std::fs::create_dir_all(state_path.parent().unwrap()).unwrap();

        let lock_file = File::create(state_path.with_extension("lock")).unwrap();
        lock_file.lock().unwrap();

        let indexing_state = if std::fs::exists(&state_path).unwrap() {
            DirV1::from_bytes(&std::fs::read(&state_path).unwrap())
        } else {
            DirV1::new()
        };

        Self {
            lock_file,
            http_client: reqwest::Client::new(),
            rate_limiter: Arc::new(Semaphore::new(max_concurrent_blob_imports)),
            indexing_state: Arc::new(RwLock::new(indexing_state)),
            store,
            indexing_state_dir_path: state_path,
        }
    }

    pub async fn import_url(&self, url: Url) -> anyhow::Result<()> {
        {
            if self
                .indexing_state
                .read()
                .await
                .files
                .contains_key(url.as_str())
            {
                return Ok(());
            }
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

            let mut dir = self.indexing_state.write().await;

            let mut file_ref = FileRef::new(hash.into(), len);

            if let Some(lm) = last_modified {
                file_ref.timestamp = Some(lm.timestamp() as u32);
            }

            dir.files.insert(url.to_string(), file_ref);
            std::fs::write(&self.indexing_state_dir_path, dir.to_bytes())?;
        }
        Ok(())
    }
}
