//! Milestone 1 validation: config persistence, indexing, and compression.

use std::collections::BTreeMap;
use std::path::Path;
use tempfile::TempDir;
use vup_cli::config::{LocalStoreConfig, Source, VaultConfig, VaultSettings};
use vup_cli::vault;

fn write_file(dir: &Path, name: &str, content: &[u8]) {
    let p = dir.join(name);
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(p, content).unwrap();
}

#[test]
fn config_persistence() {
    let tmp = TempDir::new().unwrap();
    let path = tmp.path().join("config.toml");

    let mut targets = BTreeMap::new();
    targets.insert("local".into(), LocalStoreConfig { base_path: "/tmp/backup".into() });

    let cfg = VaultConfig {
        vault: VaultSettings {
            seed_phrase: Some("abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about".into()),
        },
        sources: vec![Source { path: "/home/user/Docs".into() }],
        targets,
    };

    cfg.save(&path).unwrap();
    let loaded = VaultConfig::load(&path).unwrap();

    assert_eq!(loaded.vault.seed_phrase.as_deref(), cfg.vault.seed_phrase.as_deref());
    assert_eq!(loaded.sources.len(), 1);
    assert_eq!(loaded.targets["local"], LocalStoreConfig { base_path: "/tmp/backup".into() });

    // Missing file returns defaults
    let missing = VaultConfig::load(&tmp.path().join("nope.toml")).unwrap();
    assert!(missing.vault.seed_phrase.is_none());
    assert!(missing.sources.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn index_and_reindex() {
    let src = TempDir::new().unwrap();
    let idx = TempDir::new().unwrap();

    write_file(src.path(), "hello.txt", b"hello world");
    write_file(src.path(), "sub/nested.txt", b"nested content");

    let fs = vault::open_index_at(idx.path()).unwrap();
    let (files, bytes) = vault::reindex(&fs, &[src.path().to_path_buf()]).await.unwrap();
    assert_eq!(files, 2);
    assert_eq!(bytes, 25); // 11 + 14

    let prefix = src.path().to_str().unwrap().trim_start_matches('/');
    let hello = fs.file_get(&format!("{prefix}/hello.txt")).await.unwrap();
    assert_eq!(hello.hash, *blake3::hash(b"hello world").as_bytes());
    assert!(hello.locations.is_none() || hello.locations.as_ref().unwrap().is_empty());

    // Modify + add, then reindex
    std::thread::sleep(std::time::Duration::from_millis(50));
    write_file(src.path(), "hello.txt", b"modified");
    write_file(src.path(), "new.txt", b"new file");

    let (files2, _) = vault::reindex(&fs, &[src.path().to_path_buf()]).await.unwrap();
    assert!(files2 >= 2);

    let hello2 = fs.file_get(&format!("{prefix}/hello.txt")).await.unwrap();
    assert_ne!(hello.hash, hello2.hash);
    assert_eq!(hello2.hash, *blake3::hash(b"modified").as_bytes());
    assert!(fs.file_get(&format!("{prefix}/new.txt")).await.is_some());

    fs.shutdown().await.unwrap();
}

#[test]
fn compression_over_30_percent_on_mixed_corpus() {
    let corpus: Vec<(&str, Vec<u8>)> = vec![
        ("json", r#"{"users":[{"name":"Alice","email":"alice@example.com","roles":["admin"]}],"meta":{"v":"1.0"}}"#.repeat(20).into()),
        ("markdown", "# Docs\n\nLong documentation with repeated structure and patterns.\n\n## Section\n\nMore content here with details.\n".repeat(30).into()),
        ("code", "fn process(entries: &[Entry]) -> Result<Vec<Blob>> {\n    entries.iter().map(|e| compress(&e.data)).collect()\n}\n".repeat(30).into()),
        ("logs", "[2026-02-25T14:30:00Z INFO vup::vault] indexed 843 files (1.2 GB) in 1.2s\n".repeat(50).into()),
        ("toml", "[vault]\nseed_phrase = \"abandon abandon abandon\"\n\n[[sources]]\npath = \"/home/user/Docs\"\n".repeat(30).into()),
    ];

    for (label, data) in &corpus {
        let compressed = s5_compression::compress(data, None).unwrap();
        let ratio = 1.0 - (compressed.len() as f64 / data.len() as f64);
        assert!(ratio > 0.30, "{label}: {:.1}% < 30%", ratio * 100.0);

        let restored = s5_compression::decompress(&compressed, None).unwrap();
        assert_eq!(data.as_slice(), restored.as_slice(), "{label}: round-trip failed");
    }
}
