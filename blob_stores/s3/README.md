# s5_store_s3

S3-compatible object storage implementation of the `s5_core::Store` trait.

## Overview

- **Backend**: `rust-s3` crate.
- **Features**: No rename support, case-sensitive.
- **Configuration**: Endpoint, region, bucket, access key, secret key.

## Usage

`S3StoreConfig` is typically loaded from a TOML configuration file (see `docs/reference/configuration.md`).

```rust,no_run
use s5_store_s3::{S3Store, S3StoreConfig};

// Load config from TOML (or construct via serde deserialization)
let config: S3StoreConfig = toml::from_str(r#"
    endpoint = "https://s3.amazonaws.com"
    region = "us-east-1"
    bucket_name = "my-bucket"
    access_key = "AKIAIOSFODNN7EXAMPLE"
    secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
"#).unwrap();

let store = S3Store::create(config);
```
