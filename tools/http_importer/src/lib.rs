use anyhow::anyhow;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use reqwest::header::{CONTENT_LENGTH, LAST_MODIFIED};
use s5_core::BlobStore;
use s5_fs::{FS5, FileRef};
use scraper::{Html, Selector};
use std::sync::Arc;
use tokio::sync::Semaphore;
use url::Url;

pub struct HttpImporter {
    http_client: reqwest::Client,
    rate_limiter: Arc<Semaphore>,
    fs: FS5,
    blob_store: BlobStore,
}

impl HttpImporter {
    pub fn create(
        fs: FS5,
        blob_store: BlobStore,
        max_concurrent_requests: usize,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            http_client: reqwest::Client::new(),
            rate_limiter: Arc::new(Semaphore::new(max_concurrent_requests)),
            fs,
            blob_store,
        })
    }

    /// Recursively imports content from a given URL.
    ///
    /// This function first checks if the content needs updating by sending a `HEAD`
    /// request and comparing `Content-Length` and `Last-Modified` headers with
    /// the locally stored version.
    ///
    /// If the URL points to an HTML page and needs processing, it is parsed for
    /// links, and this function is called recursively on them.
    ///
    /// If the URL points to a file and needs updating, it is downloaded and
    /// added to the `BlobStore`.
    pub async fn import_url(&self, url: Url) -> anyhow::Result<()> {
        let _permit = self.rate_limiter.acquire().await?;

        // Get the current state of the file from our directory.
        let current_file_ref = self.fs.file_get(url.as_str()).await;

        // Decide if we need to download the file.
        let should_update = match current_file_ref {
            Some(current) => {
                // Send a HEAD request to get metadata without downloading the body.
                let head_res = self.http_client.head(url.clone()).send().await?;

                if !head_res.status().is_success() {
                    // If the file doesn't exist remotely, we can't import it.
                    // We could optionally mark it as deleted in our state here.
                    log::warn!(
                        "HEAD request for {} failed with status: {}",
                        url,
                        head_res.status()
                    );
                    return Ok(());
                }

                // Extract metadata from HEAD response.
                let remote_size = head_res
                    .headers()
                    .get(CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok());

                let remote_last_modified = head_res
                    .headers()
                    .get(LAST_MODIFIED)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| DateTime::parse_from_rfc2822(s).ok());

                let size_changed = remote_size.map_or(true, |s| s != current.size);
                let time_changed = remote_last_modified.map_or(true, |r_lm| {
                    let remote_ts = r_lm.timestamp() as u32;
                    let remote_ts_nano = r_lm.timestamp_subsec_nanos();
                    current.timestamp != Some(remote_ts)
                        || current.timestamp_subsec_nanos != Some(remote_ts_nano)
                });

                if size_changed {
                    log::debug!("Updating {}: size changed", url);
                } else if time_changed {
                    log::debug!("Updating {}: last-modified changed", url);
                }

                size_changed || time_changed
            }
            None => {
                log::debug!("Importing new URL: {}", url);
                true // File is new, so we must import it.
            }
        };

        if !should_update {
            log::debug!("Skipping unchanged URL: {}", url);
            return Ok(());
        }

        // If we need to update, perform the full GET request.
        log::info!("Importing URL: {}", url);
        let res = self.http_client.get(url.clone()).send().await?;

        if !res.status().is_success() {
            return Err(anyhow!(
                "HTTP GET request for {} failed with status: {}",
                url,
                res.status()
            ));
        }

        let content_type = res
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .unwrap_or_default();

        let last_modified = res
            .headers()
            .get(LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| DateTime::parse_from_rfc2822(s).ok());

        // TODO: Also extract and use the ETag header for more robust change detection.
        // let etag = res.headers().get(ETAG).and_then(|v| v.to_str().ok());

        if content_type.starts_with("text/html") {
            drop(_permit);
            self.handle_html_page(url, res).await
        } else {
            self.handle_file(url, res, last_modified).await
        }
    }

    /// Parses an HTML page, extracts links, and triggers further imports.
    async fn handle_html_page(&self, base_url: Url, res: reqwest::Response) -> anyhow::Result<()> {
        let text = res.text().await?;
        let doc = Html::parse_document(&text);
        let link_selector = Selector::parse("a").unwrap();

        let urls_to_visit: Vec<Url> = doc
            .select(&link_selector)
            .filter_map(|element| element.attr("href"))
            // Ignore links that are clearly not files or subdirectories
            .filter(|href| !href.starts_with(['?', '#', '.']))
            .filter_map(|href| base_url.join(href).ok())
            // Ensure we don't crawl outside the original directory path.
            .filter(|parsed_url| parsed_url.as_str().starts_with(base_url.as_str()))
            // Avoid recursion into the same page (e.g. links to "/")
            .filter(|parsed_url| *parsed_url != base_url)
            .collect();

        log::debug!("Found {} links on page {}", urls_to_visit.len(), base_url);

        let tasks: Vec<_> = urls_to_visit
            .into_iter()
            .map(|url| self.import_url(url))
            .collect();

        futures::future::try_join_all(tasks).await?;

        Ok(())
    }

    /// Imports a file into the BlobStore and adds a reference to the DirV1.
    async fn handle_file(
        &self,
        url: Url,
        res: reqwest::Response,
        last_modified: Option<DateTime<chrono::FixedOffset>>,
    ) -> anyhow::Result<()> {
        let blob_id = self
            .blob_store
            .import_stream(Box::new(
                res.bytes_stream()
                    .map(|c| c.map_err(|e| std::io::Error::other(e))),
            ))
            .await?;

        let mut file_ref: FileRef = blob_id.into();

        // Use the file's modification time if available, otherwise use the current time.
        let ts = last_modified
            .map(|lm| (lm.timestamp(), lm.timestamp_subsec_nanos()))
            .unwrap_or_else(|| {
                let now = Utc::now();
                (now.timestamp(), now.timestamp_subsec_nanos())
            });

        file_ref.timestamp = Some(ts.0 as u32);
        file_ref.timestamp_subsec_nanos = Some(ts.1 as u32);

        self.fs.file_put(url.as_str(), file_ref).await;

        log::info!("Successfully imported file: {}", url);
        Ok(())
    }
}
