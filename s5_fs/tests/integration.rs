//! Comprehensive integration test for the S5 File System
//!
//! It ensures that the end-to-end encryption, directory management,
//! and core file operations are working correctly together.
//!
//! It validates:
//! 1.  **Core API Functionality:** `file_put`, `file_get`, `file_exists` in a standard, unencrypted context.
//! 2.  **End-to-End Encryption:** Creation of an encrypted subdirectory using `create_dir`.
//! 3.  **Encrypted Operations:** Writing to and reading from an encrypted subdirectory, verifying
//!     that the metadata is decrypted transparently for the user.
//! 4.  **Directory Migration:** The ability to create a directory at a path where files already
//!     exist, correctly moving those files into the new subdirectory's state.
//! 5.  **Idempotency:** Ensuring that creating the same directory multiple times does not cause errors.
//! 6.  **Path Handling:** Correctly routing operations to nested actors (e.g., "secret/file.txt").
//! 7.  **Dependency Integration:** Implicitly tests the integration with `s5_core` (BlobId, Hash)
//!     and `s5_store_local` (for the underlying blob and registry storage).

use bytes::Bytes;
use s5_fs::{DirContext, FS5, FileRef};
use tempfile::tempdir;

#[tokio::test]
async fn full_test() {
    // --- SETUP ---
    // Create a temporary directory to act as the local storage backend for this test.
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let temp_dir_path = temp_dir.path().to_path_buf();
    let fs_context =
        DirContext::open_local_root(&temp_dir_path).expect("Failed to open local FS root");
    let fs = FS5::open(fs_context);

    let file_content = b"This is a test file for the S5 file system.";
    let file_hash = blake3::hash(file_content);
    let file_ref = FileRef::new(file_hash.into(), file_content.len() as u64);

    let another_file_content = b"This file will be migrated.";
    let another_file_hash = blake3::hash(another_file_content);
    let another_file_ref =
        FileRef::new(another_file_hash.into(), another_file_content.len() as u64);

    // --- PHASE 1: Core Unencrypted File Operations ---

    // Test file_exists on a non-existent file
    assert!(
        !fs.file_exists("root_file.txt").await,
        "file_exists should be false for a non-existent file"
    );

    // Test file_put to add a file to the root directory
    fs.file_put("root_file.txt", file_ref.clone())
        .await
        .unwrap();

    // Test file_exists on an existing file
    assert!(
        fs.file_exists("root_file.txt").await,
        "file_exists should be true after putting a file"
    );

    // Test file_get to retrieve the file and verify its metadata
    let retrieved_file = fs
        .file_get("root_file.txt")
        .await
        .expect("file_get should return the file we just put");

    assert_eq!(
        retrieved_file.hash.as_slice(),
        file_hash.as_bytes(),
        "Retrieved file hash does not match original"
    );
    assert_eq!(
        retrieved_file.size,
        file_content.len() as u64,
        "Retrieved file size does not match original"
    );

    // --- PHASE 2: End-to-End Encryption ---

    // Create a new subdirectory with encryption enabled.
    fs.create_dir("secret", true)
        .await
        .expect("Failed to create encrypted directory");

    // Put a file inside the new encrypted directory.
    fs.file_put("secret/secret_file.txt", file_ref.clone())
        .await
        .unwrap();

    // Retrieve the file from the encrypted directory. The decryption should be transparent.
    let retrieved_secret_file = fs
        .file_get("secret/secret_file.txt")
        .await
        .expect("Should be able to get file from encrypted directory");

    // Verify the metadata of the decrypted file.
    assert_eq!(
        retrieved_secret_file.hash.as_slice(),
        file_hash.as_bytes(),
        "Decrypted file hash does not match original"
    );
    assert_eq!(
        retrieved_secret_file.size,
        file_content.len() as u64,
        "Decrypted file size does not match original"
    );

    // --- PHASE 3: Directory Creation with File Migration ---

    // First, place a file at a path that will later become a directory.
    fs.file_put("to_be_migrated/another_file.txt", another_file_ref.clone())
        .await
        .unwrap();
    assert!(
        fs.file_exists("to_be_migrated/another_file.txt").await,
        "File should exist before directory creation"
    );

    // Now, create a directory at the parent path. This should "capture" the existing file.
    fs.create_dir("to_be_migrated", true)
        .await
        .expect("Failed to create directory over existing file path");

    // Verify the file is still accessible through its full path, now managed by the new subdirectory actor.
    let migrated_file = fs
        .file_get("to_be_migrated/another_file.txt")
        .await
        .expect("File should be accessible after migration into new directory");

    assert_eq!(
        migrated_file.hash.as_slice(),
        another_file_hash.as_bytes(),
        "Migrated file hash does not match"
    );

    // --- PHASE 4: Idempotency of create_dir ---

    // Calling create_dir on an already existing directory should not fail.
    let result = fs.create_dir("secret", true).await;
    assert!(
        result.is_ok(),
        "Calling create_dir again should be idempotent and not return an error"
    );

    // --- FINAL VERIFICATION ---
    // Final check to ensure all files are where they are expected to be.
    assert!(fs.file_get("root_file.txt").await.is_some());
    assert!(fs.file_get("secret/secret_file.txt").await.is_some());
    assert!(
        fs.file_get("to_be_migrated/another_file.txt")
            .await
            .is_some()
    );
    assert!(fs.file_get("non_existent_file.txt").await.is_none());
}

#[tokio::test]
async fn sharding_basic_persists() {
    let temp_dir = tempdir().expect("tmp");
    let ctx = DirContext::open_local_root(temp_dir.path()).expect("ctx");
    let fs = FS5::open(ctx);

    // Create many small files to grow metadata
    for i in 0..3000u32 {
        let name = format!("bulk/{}.txt", i);
        let fr = FileRef::new_inline_blob(Bytes::from_static(b"x"));
        fs.file_put(&name, fr).await.unwrap();
    }
    fs.save().await.unwrap();

    // Spot check a few entries
    assert!(fs.file_exists("bulk/0.txt").await);
    assert!(fs.file_exists("bulk/1024.txt").await);
    assert!(fs.file_exists("bulk/2999.txt").await);
}

#[tokio::test]
#[ignore]
async fn encrypted_round_trip() {
    let temp_dir = tempdir().expect("tmp");
    let base = temp_dir.path().to_path_buf();

    {
        let ctx = DirContext::open_local_root(&base).expect("ctx");
        let fs = FS5::open(ctx);
        fs.create_dir("enc", true).await.unwrap();
        fs.file_put_sync(
            "enc/one.txt",
            FileRef::new_inline_blob(Bytes::from_static(b"1")),
        )
        .await
        .unwrap();
        fs.save().await.unwrap();
    }

    // Re-open and read back
    let ctx2 = DirContext::open_local_root(&base).expect("ctx2");
    let fs2 = FS5::open(ctx2);
    assert!(fs2.file_exists("enc/one.txt").await);
}

#[tokio::test]
async fn concurrency_ordering_smoke() {
    let temp_dir = tempdir().expect("tmp");
    let ctx = DirContext::open_local_root(temp_dir.path()).expect("ctx");
    let fs = FS5::open(ctx);

    let mut handles = vec![];
    for i in 0..20u32 {
        let fs_i = fs.clone();
        handles.push(tokio::spawn(async move {
            let path = format!("c/t{}.txt", i);
            fs_i.file_put_sync(&path, FileRef::new_inline_blob(Bytes::from_static(b"v")))
                .await
                .unwrap();
            fs_i.file_get(&path).await.is_some()
        }));
    }
    for h in handles {
        assert!(h.await.unwrap());
    }
}
