# s5_importer_http

Imports content from HTTP URLs into an S5 filesystem (`FS5`).

## Features

- **Recursive Crawling**: Parses HTML to find links (`<a href="...">`) and recursively imports them (within the base URL scope).
- **Incremental**: Uses `HEAD` requests to check `Content-Length` and `Last-Modified` before downloading.
- **Concurrency**: Parallel processing of URLs.

## Usage

```rust
use s5_importer_http::HttpImporter;
use url::Url;

let base_url = Url::parse("https://example.com/docs/")?;
let importer = HttpImporter::create(
    fs,
    blob_store,
    4, // concurrency
    base_url.clone(),
    true, // use_base_relative_keys
)?;

importer.import_url(base_url).await?;
```
