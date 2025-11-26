# s5_store_local

Local filesystem implementation of the `s5_core::Store` trait.

## Overview

- **Backend**: `tokio::fs`
- **Features**: Supports rename, case-insensitive (on some OSs), standard file IO.
- **Configuration**: `LocalStoreConfig` (base path).

## Usage

```rust,no_run
use bytes::Bytes;
use s5_store_local::LocalStore;
use s5_core::Store;

let store = LocalStore::new("/tmp/s5-blobs");
store.put_bytes("foo.txt", Bytes::from("content")).await?;
```
