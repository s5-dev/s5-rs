# s5_importer_local

Imports files from the local filesystem into an S5 filesystem (`FS5`).

## Features

- **Recursive Import**: Walks directory trees.
- **Incremental**: Checks file size and modification time to skip unchanged files.
- **Concurrency**: Parallel processing of files.
- **Filtering**: Supports `.gitignore`, `.fdignore`, and `CACHEDIR.TAG`.

## Usage

```rust
use s5_importer_local::LocalFileSystemImporter;
use s5_fs::FS5;
use s5_core::BlobStore;

let importer = LocalFileSystemImporter::create(
    fs,
    blob_store,
    4, // concurrency
    false, // use_base_relative_keys
    true, // ignore
    true, // ignore_vcs
    true, // check_cachedir_tag
)?;

importer.import_path("/path/to/source".into()).await?;
```
