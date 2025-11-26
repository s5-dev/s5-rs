# s5_store_sia

Sia network storage implementation of the `s5_core::Store` trait, using `renterd`.

## Overview

- **Backend**: `renterd` API (Sia).
- **Features**: Supports rename, high durability/availability via Sia network.
- **Direct Downloads**: Implements `provide` to return `BlobLocation::SiaFile`, enabling clients to download directly from Sia hosts without proxying through the S5 node.

## Configuration

Requires a running `renterd` instance. Config includes worker/bus API URLs and password.

## Usage

```rust
use s5_store_sia::{SiaStore, SiaStoreConfig};

let config = SiaStoreConfig {
    bucket: "default".into(),
    worker_api_url: "http://localhost:9980/api/worker".into(),
    // ...
};
let store = SiaStore::create(config).await?;
```
