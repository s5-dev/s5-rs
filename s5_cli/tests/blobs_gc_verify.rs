use bytes::Bytes;
use s5_core::RegistryPinner;
use s5_core::{BlobStore, PinContext, Pins};
use s5_registry_redb::RedbRegistry;
use s5_store_local::{LocalStore, LocalStoreConfig};
use std::fs;
use std::path::PathBuf;
use std::process::Command;

#[tokio::test]
async fn blobs_gc_and_verify_local_comprehensive() -> anyhow::Result<()> {
    // 1. Setup
    let mut base = std::env::temp_dir();
    let unique = format!(
        "s5_cli_gc_verify_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    base.push(unique);
    fs::create_dir_all(&base)?;

    let mut config_root = PathBuf::from(&base);
    config_root.push("s5");
    fs::create_dir_all(&config_root)?;
    let data_root = config_root.clone();

    let node_config = config_root.join("local.toml");
    let store_path = data_root.join("stores").join("default_store");
    fs::create_dir_all(&store_path)?;

    let config_contents = format!(
        r#"[identity]
secret_key_file = ""

[store.default]
type = "local"
base_path = "{}"

[peer]

[sync]
"#,
        store_path.to_string_lossy(),
    );
    fs::write(&node_config, config_contents)?;

    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has a parent")
        .to_path_buf();

    // Helper to run CLI commands
    let run_cli = |args: &[&str]| -> std::process::Output {
        Command::new("cargo")
            .arg("run")
            .arg("-p")
            .arg("s5_cli")
            .arg("--quiet")
            .arg("--")
            .arg("--node")
            .arg("local")
            .args(args)
            .env("XDG_CONFIG_HOME", &base)
            .env("XDG_DATA_HOME", &base)
            // Use a separate target directory to avoid locking conflicts with the running test
            .env(
                "CARGO_TARGET_DIR",
                workspace_root.join("target").join("test_cli"),
            )
            .current_dir(&workspace_root)
            .output()
            .expect("failed to execute process")
    };

    // Open the store directly to manipulate it
    let local_store = LocalStore::create(LocalStoreConfig {
        base_path: store_path.to_string_lossy().into(),
    });
    let blob_store = BlobStore::new(local_store);

    // 2. Create an orphan blob
    let orphan_data = b"orphan blob data";
    let orphan_id = blob_store
        .import_bytes(Bytes::from(&orphan_data[..]))
        .await?;
    let orphan_hash = orphan_id.hash;

    assert!(blob_store.contains(orphan_hash).await?);

    // 3. Run GC dry-run
    let output = run_cli(&["blobs", "gc-local", "--store", "default", "--dry-run"]);
    if !output.status.success() {
        eprintln!(
            "GC dry-run failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout.contains(&orphan_hash.to_string()),
        "Dry run should list orphan hash"
    );
    assert!(
        blob_store.contains(orphan_hash).await?,
        "Dry run should not delete blob"
    );

    // 4. Run GC
    let output = run_cli(&["blobs", "gc-local", "--store", "default"]);
    if !output.status.success() {
        eprintln!(
            "GC (orphan) failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(output.status.success());
    assert!(
        !blob_store.contains(orphan_hash).await?,
        "GC should delete orphan blob"
    );

    // 5. Create a pinned blob
    let pinned_data = b"pinned blob data";
    let pinned_id = blob_store
        .import_bytes(Bytes::from(&pinned_data[..]))
        .await?;
    let pinned_hash = pinned_id.hash;

    // Open registry and pin it
    {
        let registry_dir = config_root.join("registry");
        fs::create_dir_all(&registry_dir)?;
        let registry = RedbRegistry::open(&registry_dir)?;
        let pinner = RegistryPinner::new(registry);
        pinner
            .pin_hash(pinned_hash, PinContext::LocalFsHead)
            .await?;
    }

    // 6. Run GC
    let output = run_cli(&["blobs", "gc-local", "--store", "default"]);
    if !output.status.success() {
        eprintln!(
            "GC (pinned) failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(output.status.success());
    assert!(
        blob_store.contains(pinned_hash).await?,
        "GC should keep pinned blob"
    );

    // 7. Create a reachable blob (via import)
    let import_file_path = base.join("test_file.txt");
    fs::write(&import_file_path, "reachable blob data")?;

    let output = run_cli(&[
        "import",
        "--target-store",
        "default",
        "local",
        import_file_path.to_str().unwrap(),
    ]);
    if !output.status.success() {
        eprintln!("Import failed: {}", String::from_utf8_lossy(&output.stderr));
    }
    assert!(output.status.success());

    // We need to find the hash of the imported file.
    // We can calculate it manually since we know the content.
    let reachable_hash = blake3::hash(b"reachable blob data").into();

    assert!(
        blob_store.contains(reachable_hash).await?,
        "Import should store the blob"
    );

    // 8. Run GC
    let output = run_cli(&["blobs", "gc-local", "--store", "default"]);
    if !output.status.success() {
        eprintln!(
            "GC (reachable) failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(output.status.success());
    assert!(
        blob_store.contains(reachable_hash).await?,
        "GC should keep reachable blob"
    );

    // 9. Verify-local should pass
    let output = run_cli(&["blobs", "verify-local", "--store", "default"]);
    if !output.status.success() {
        eprintln!("Verify failed: {}", String::from_utf8_lossy(&output.stderr));
    }
    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout)?;
    assert!(
        stdout.contains("all"),
        "Verify should report all blobs present"
    );

    Ok(())
}
