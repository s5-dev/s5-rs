//! Tests for GC behavior with tombstones and version chains.
//!
//! These tests verify that:
//! 1. Tombstones' version chains (prev/first_version) are preserved by GC
//! 2. Only unreferenced blobs are collected
//! 3. Historical versions remain accessible after GC

use s5_core::Hash;
use s5_fs::dir::{DirV1, FileRef, FileRefType};
use s5_fs::gc::collect_hashes_from_dir;
use std::collections::HashSet;

/// Helper to create a FileRef with a specific hash
fn file_ref_with_hash(hash_byte: u8, size: u64) -> FileRef {
    let mut hash = [0u8; 32];
    hash[0] = hash_byte;
    FileRef {
        ref_type: None,
        hash,
        size,
        media_type: None,
        timestamp: Some(1000),
        timestamp_subsec_nanos: None,
        locations: None,
        extra: None,
        prev: None,
        version_count: None,
        warc: None,
        first_version: None,
    }
}

/// Helper to create a tombstone from a previous FileRef
fn tombstone_from(previous: FileRef) -> FileRef {
    FileRef::from_deleted(previous, 2000, 0)
}

// We need to expose collect_hashes_from_dir for testing, so let's test via
// the public API by building DirV1 snapshots and checking what's collected.

#[test]
fn test_gc_collects_live_file_hash() {
    let mut dir = DirV1::new();
    let file = file_ref_with_hash(0x01, 100);
    dir.files.insert("file.txt".to_string(), file);

    let mut reachable = HashSet::new();
    collect_hashes_from_dir(&dir, &mut reachable);

    assert_eq!(reachable.len(), 1);
    let expected_hash = {
        let mut h = [0u8; 32];
        h[0] = 0x01;
        Hash::from_bytes(h)
    };
    assert!(reachable.contains(&expected_hash));
}

#[test]
fn test_gc_collects_version_chain_hashes() {
    // Create a file with multiple versions:
    // v1 (hash 0x01) -> v2 (hash 0x02) -> v3 (hash 0x03, current)
    let v1 = file_ref_with_hash(0x01, 100);
    let mut v2 = file_ref_with_hash(0x02, 200);
    v2.prev = Some(Box::new(v1.clone()));
    v2.first_version = Some(Box::new(v1.clone()));
    v2.version_count = Some(2);

    let mut v3 = file_ref_with_hash(0x03, 300);
    v3.prev = Some(Box::new(v2.clone()));
    v3.first_version = Some(Box::new(v1.clone()));
    v3.version_count = Some(3);

    let mut dir = DirV1::new();
    dir.files.insert("file.txt".to_string(), v3);

    let mut reachable = HashSet::new();
    collect_hashes_from_dir(&dir, &mut reachable);

    // All three versions should be reachable
    assert_eq!(reachable.len(), 3, "All version hashes should be collected");

    for hash_byte in [0x01, 0x02, 0x03] {
        let mut h = [0u8; 32];
        h[0] = hash_byte;
        assert!(
            reachable.contains(&Hash::from_bytes(h)),
            "Hash 0x{:02x} should be reachable",
            hash_byte
        );
    }
}

#[test]
fn test_gc_preserves_tombstone_version_chain() {
    // Create a file, then delete it (creating a tombstone)
    // The tombstone's prev/first_version chains should still be preserved
    let v1 = file_ref_with_hash(0x01, 100);
    let mut v2 = file_ref_with_hash(0x02, 200);
    v2.prev = Some(Box::new(v1.clone()));
    v2.first_version = Some(Box::new(v1.clone()));
    v2.version_count = Some(2);

    // Delete creates a tombstone
    let tombstone = tombstone_from(v2.clone());

    // Verify it's actually a tombstone
    assert!(tombstone.is_tombstone());
    assert_eq!(tombstone.ref_type(), FileRefType::Tombstone);

    let mut dir = DirV1::new();
    dir.files.insert("deleted.txt".to_string(), tombstone);

    let mut reachable = HashSet::new();
    collect_hashes_from_dir(&dir, &mut reachable);

    // Both v1 and v2 hashes should be preserved (tombstone's own hash is skipped
    // since it's just a copy of v2's hash, but v2 is in prev chain)
    assert!(
        reachable.len() >= 2,
        "At least v1 and v2 hashes should be preserved, got {}",
        reachable.len()
    );

    let h1 = {
        let mut h = [0u8; 32];
        h[0] = 0x01;
        Hash::from_bytes(h)
    };
    let h2 = {
        let mut h = [0u8; 32];
        h[0] = 0x02;
        Hash::from_bytes(h)
    };

    assert!(
        reachable.contains(&h1),
        "First version hash should be preserved"
    );
    assert!(
        reachable.contains(&h2),
        "Previous version hash should be preserved"
    );
}

#[test]
fn test_gc_tombstone_without_version_chain() {
    // Edge case: a tombstone with no prev/first_version (shouldn't happen in
    // normal usage, but GC should handle it gracefully)
    let mut tombstone = file_ref_with_hash(0x99, 100);
    tombstone.ref_type = Some(FileRefType::Tombstone);

    let mut dir = DirV1::new();
    dir.files
        .insert("orphan_tombstone.txt".to_string(), tombstone);

    let mut reachable = HashSet::new();
    collect_hashes_from_dir(&dir, &mut reachable);

    // Tombstone's hash is skipped, and there's no chain, so nothing collected
    assert_eq!(
        reachable.len(),
        0,
        "Tombstone with no version chain should not add any hashes"
    );
}

#[test]
fn test_gc_multiple_files_with_tombstones() {
    let mut dir = DirV1::new();

    // Live file
    let live_file = file_ref_with_hash(0x10, 100);
    dir.files.insert("live.txt".to_string(), live_file);

    // Deleted file with history
    let deleted_v1 = file_ref_with_hash(0x20, 200);
    let mut deleted_v2 = file_ref_with_hash(0x21, 210);
    deleted_v2.prev = Some(Box::new(deleted_v1.clone()));
    deleted_v2.first_version = Some(Box::new(deleted_v1.clone()));
    let tombstone = tombstone_from(deleted_v2);
    dir.files.insert("deleted.txt".to_string(), tombstone);

    // Another live file with versions
    let other_v1 = file_ref_with_hash(0x30, 300);
    let mut other_v2 = file_ref_with_hash(0x31, 310);
    other_v2.prev = Some(Box::new(other_v1.clone()));
    other_v2.first_version = Some(Box::new(other_v1.clone()));
    dir.files.insert("other.txt".to_string(), other_v2);

    let mut reachable = HashSet::new();
    collect_hashes_from_dir(&dir, &mut reachable);

    // Expected hashes:
    // - 0x10 (live.txt)
    // - 0x20, 0x21 (deleted.txt history)
    // - 0x30, 0x31 (other.txt versions)
    let expected: Vec<u8> = vec![0x10, 0x20, 0x21, 0x30, 0x31];
    assert_eq!(
        reachable.len(),
        expected.len(),
        "Expected {} hashes, got {}",
        expected.len(),
        reachable.len()
    );

    for hash_byte in expected {
        let mut h = [0u8; 32];
        h[0] = hash_byte;
        assert!(
            reachable.contains(&Hash::from_bytes(h)),
            "Hash 0x{:02x} should be reachable",
            hash_byte
        );
    }
}

#[test]
fn test_gc_deep_version_chain() {
    // Test a longer version chain to ensure recursive traversal works
    let mut current = file_ref_with_hash(0x01, 100);
    let first = current.clone();

    for i in 2u8..=10 {
        let mut next = file_ref_with_hash(i, (i as u64) * 100);
        next.prev = Some(Box::new(current.clone()));
        next.first_version = Some(Box::new(first.clone()));
        next.version_count = Some(i as u32);
        current = next;
    }

    let mut dir = DirV1::new();
    dir.files.insert("versioned.txt".to_string(), current);

    let mut reachable = HashSet::new();
    collect_hashes_from_dir(&dir, &mut reachable);

    // All 10 versions should be reachable
    assert_eq!(
        reachable.len(),
        10,
        "All 10 version hashes should be collected"
    );

    for i in 1u8..=10 {
        let mut h = [0u8; 32];
        h[0] = i;
        assert!(
            reachable.contains(&Hash::from_bytes(h)),
            "Hash 0x{:02x} should be reachable",
            i
        );
    }
}

#[test]
fn test_gc_tombstone_deep_version_chain() {
    // Same as above but with tombstone at the end
    let mut current = file_ref_with_hash(0x01, 100);
    let first = current.clone();

    for i in 2u8..=5 {
        let mut next = file_ref_with_hash(i, (i as u64) * 100);
        next.prev = Some(Box::new(current.clone()));
        next.first_version = Some(Box::new(first.clone()));
        next.version_count = Some(i as u32);
        current = next;
    }

    let tombstone = tombstone_from(current);

    let mut dir = DirV1::new();
    dir.files
        .insert("deleted_versioned.txt".to_string(), tombstone);

    let mut reachable = HashSet::new();
    collect_hashes_from_dir(&dir, &mut reachable);

    // All 5 historical versions should be preserved
    assert!(
        reachable.len() >= 5,
        "All historical versions should be preserved, got {}",
        reachable.len()
    );

    for i in 1u8..=5 {
        let mut h = [0u8; 32];
        h[0] = i;
        assert!(
            reachable.contains(&Hash::from_bytes(h)),
            "Hash 0x{:02x} should be reachable",
            i
        );
    }
}
