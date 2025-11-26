# s5_store_memory

In-memory implementation of the `s5_core::Store` trait.

## Overview

- **Backend**: `DashMap<String, Bytes>`
- **Features**: Supports rename, case-sensitive, unlimited directory size.
- **Use Case**: Testing, temporary storage, caching.

## Usage

```rust,no_run
use bytes::Bytes;
use s5_store_memory::MemoryStore;
use s5_core::Store;

let store = MemoryStore::new();
store.put_bytes("foo", Bytes::from("bar")).await?;
```
