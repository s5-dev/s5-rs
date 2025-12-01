//! Test utilities for `Store` implementations.
//!
//! This module provides a comprehensive test suite that can be run against any
//! `Store` implementation to verify correctness.
//!
//! # Usage
//!
//! In your store crate's `Cargo.toml`:
//!
//! ```toml
//! [dev-dependencies]
//! s5_core = { workspace = true, features = ["testutil"] }
//! ```
//!
//! In your test file:
//!
//! ```ignore
//! use s5_core::testutil::StoreTests;
//!
//! #[tokio::test]
//! async fn test_my_store() {
//!     let store = MyStore::new(...);
//!     StoreTests::new(store).run_all().await;
//! }
//! ```

use crate::store::{Store, StoreResult};
use bytes::Bytes;
use futures::StreamExt;
use rand::Rng;
use std::collections::HashSet;

/// Test suite for `Store` implementations.
///
/// Runs a comprehensive set of tests to verify that a store implementation
/// behaves correctly according to the `Store` trait contract.
pub struct StoreTests<'a, S> {
    store: &'a S,
    /// Prefix for test files to avoid conflicts
    prefix: String,
}

impl<'a, S: Store> StoreTests<'a, S> {
    /// Create a new test suite for the given store.
    pub fn new(store: &'a S) -> Self {
        let prefix = format!("_test_{}/", rand::rng().random::<u32>());
        Self { store, prefix }
    }

    /// Create a new test suite with a custom prefix.
    pub fn with_prefix(store: &'a S, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into(),
        }
    }

    fn path(&self, name: &str) -> String {
        format!("{}{}", self.prefix, name)
    }

    /// Run all tests.
    pub async fn run_all(&self) -> StoreResult<()> {
        self.test_put_get_bytes().await?;
        self.test_put_get_stream().await?;
        self.test_exists().await?;
        self.test_size().await?;
        self.test_delete().await?;
        self.test_list().await?;
        self.test_partial_read().await?;
        self.test_overwrite().await?;

        if self.store.features().supports_rename {
            self.test_rename().await?;
        }

        // Cleanup
        self.cleanup().await?;

        Ok(())
    }

    /// Test basic put and get with bytes.
    pub async fn test_put_get_bytes(&self) -> StoreResult<()> {
        let path = self.path("bytes_test.bin");
        let data = Bytes::from_static(b"hello, world!");

        self.store.put_bytes(&path, data.clone()).await?;

        let retrieved = self.store.open_read_bytes(&path, 0, None).await?;
        assert_eq!(retrieved, data, "retrieved data should match original");

        Ok(())
    }

    /// Test put and get with streams.
    pub async fn test_put_get_stream(&self) -> StoreResult<()> {
        let path = self.path("stream_test.bin");
        let data = Bytes::from(vec![0u8; 1024 * 10]); // 10KB

        // Put via stream
        let stream = futures::stream::iter(vec![Ok::<_, std::io::Error>(data.clone())]);
        self.store.put_stream(&path, Box::new(stream)).await?;

        // Get via stream
        let mut stream = self.store.open_read_stream(&path, 0, None).await?;
        let mut retrieved = Vec::new();
        while let Some(chunk) = stream.next().await {
            retrieved.extend_from_slice(&chunk?);
        }

        assert_eq!(
            retrieved.len(),
            data.len(),
            "stream data length should match"
        );
        assert_eq!(retrieved, data.as_ref(), "stream data should match");

        Ok(())
    }

    /// Test exists check.
    pub async fn test_exists(&self) -> StoreResult<()> {
        let path = self.path("exists_test.bin");

        assert!(
            !self.store.exists(&path).await?,
            "file should not exist before creation"
        );

        self.store
            .put_bytes(&path, Bytes::from_static(b"test"))
            .await?;

        assert!(
            self.store.exists(&path).await?,
            "file should exist after creation"
        );

        Ok(())
    }

    /// Test size retrieval.
    pub async fn test_size(&self) -> StoreResult<()> {
        let path = self.path("size_test.bin");
        let data = Bytes::from(vec![42u8; 12345]);

        self.store.put_bytes(&path, data.clone()).await?;

        let size = self.store.size(&path).await?;
        assert_eq!(size, 12345, "size should match data length");

        Ok(())
    }

    /// Test file deletion.
    pub async fn test_delete(&self) -> StoreResult<()> {
        let path = self.path("delete_test.bin");

        self.store
            .put_bytes(&path, Bytes::from_static(b"to be deleted"))
            .await?;

        assert!(
            self.store.exists(&path).await?,
            "file should exist before delete"
        );

        self.store.delete(&path).await?;

        assert!(
            !self.store.exists(&path).await?,
            "file should not exist after delete"
        );

        Ok(())
    }

    /// Test file listing.
    pub async fn test_list(&self) -> StoreResult<()> {
        let files = ["list_a.bin", "list_b.bin", "subdir/list_c.bin"];

        for file in &files {
            let path = self.path(file);
            self.store
                .put_bytes(&path, Bytes::from_static(b"list test"))
                .await?;
        }

        let mut stream = self.store.list().await?;
        let mut found: HashSet<String> = HashSet::new();

        while let Some(result) = stream.next().await {
            let path = result?;
            if path.starts_with(&self.prefix) {
                found.insert(path);
            }
        }

        for file in &files {
            let path = self.path(file);
            assert!(found.contains(&path), "list should contain {}", path);
        }

        Ok(())
    }

    /// Test partial/range reads.
    pub async fn test_partial_read(&self) -> StoreResult<()> {
        let path = self.path("partial_test.bin");
        let data = Bytes::from_static(b"0123456789abcdef");

        self.store.put_bytes(&path, data.clone()).await?;

        // Read from offset
        let partial = self.store.open_read_bytes(&path, 5, None).await?;
        assert_eq!(partial.as_ref(), b"56789abcdef", "offset read should work");

        // Read with length limit
        let partial = self.store.open_read_bytes(&path, 0, Some(5)).await?;
        assert_eq!(
            partial.as_ref(),
            b"01234",
            "length-limited read should work"
        );

        // Read with offset and length
        let partial = self.store.open_read_bytes(&path, 4, Some(4)).await?;
        assert_eq!(
            partial.as_ref(),
            b"4567",
            "offset + length read should work"
        );

        Ok(())
    }

    /// Test overwriting existing files.
    pub async fn test_overwrite(&self) -> StoreResult<()> {
        let path = self.path("overwrite_test.bin");

        self.store
            .put_bytes(&path, Bytes::from_static(b"original content"))
            .await?;

        self.store
            .put_bytes(&path, Bytes::from_static(b"new content"))
            .await?;

        let retrieved = self.store.open_read_bytes(&path, 0, None).await?;
        assert_eq!(
            retrieved.as_ref(),
            b"new content",
            "overwritten content should be new"
        );

        Ok(())
    }

    /// Test rename (only run if supported).
    pub async fn test_rename(&self) -> StoreResult<()> {
        let old_path = self.path("rename_old.bin");
        let new_path = self.path("rename_new.bin");

        self.store
            .put_bytes(&old_path, Bytes::from_static(b"rename me"))
            .await?;

        self.store.rename(&old_path, &new_path).await?;

        assert!(
            !self.store.exists(&old_path).await?,
            "old path should not exist after rename"
        );
        assert!(
            self.store.exists(&new_path).await?,
            "new path should exist after rename"
        );

        let content = self.store.open_read_bytes(&new_path, 0, None).await?;
        assert_eq!(
            content.as_ref(),
            b"rename me",
            "content should be preserved after rename"
        );

        Ok(())
    }

    /// Clean up test files.
    pub async fn cleanup(&self) -> StoreResult<()> {
        let mut stream = self.store.list().await?;

        while let Some(result) = stream.next().await {
            let path = result?;
            if path.starts_with(&self.prefix) {
                let _ = self.store.delete(&path).await;
            }
        }

        Ok(())
    }
}

/// Generate random bytes for testing.
pub fn random_bytes(len: usize) -> Bytes {
    let mut data = vec![0u8; len];
    rand::rng().fill(&mut data[..]);
    Bytes::from(data)
}

/// Assert that two byte slices are equal with a descriptive message.
#[macro_export]
macro_rules! assert_bytes_eq {
    ($left:expr, $right:expr) => {
        assert_eq!(
            $left.as_ref() as &[u8],
            $right.as_ref() as &[u8],
            "byte content mismatch"
        );
    };
    ($left:expr, $right:expr, $($arg:tt)+) => {
        assert_eq!(
            $left.as_ref() as &[u8],
            $right.as_ref() as &[u8],
            $($arg)+
        );
    };
}
