use std::path::PathBuf;

use anyhow::Result;
use s5_fs::FS5;
use s5_importer_http::HttpImporter;
use s5_importer_local::LocalFileSystemImporter;
use s5_node::config::S5NodeConfig;
use url::Url;

use super::util::open_store;
use crate::ImportCmd;

pub async fn run_import(
    cmd: ImportCmd,
    target_store_name: String,
    config: &S5NodeConfig,
    fs: &FS5,
    fs_handle: &FS5,
    _fs_root: &PathBuf,
) -> Result<()> {
    let target_store = open_store(config, &target_store_name).await?;

    match cmd {
        ImportCmd::Http {
            url,
            concurrency,
            prefix,
        } => {
            let url_parsed: Url = url.parse()?;
            let scoped_fs = if let Some(ref p) = prefix {
                fs.subdir(p).await?
            } else {
                fs.clone()
            };

            // When a custom prefix is provided, use keys
            // relative to the base URL so that final paths
            // are `<prefix>/<relative>`.
            let use_base_relative_keys = prefix.is_some();

            let http_importer = HttpImporter::create(
                scoped_fs,
                target_store,
                concurrency,
                url_parsed.clone(),
                use_base_relative_keys,
            )?;
            http_importer.import_url(url_parsed).await?;
        }

        ImportCmd::Local {
            path,
            concurrency,
            prefix,
            no_ignore,
            no_ignore_vcs,
            no_ignore_cachedir,
            ignore,
            ignore_vcs,
            ignore_cachedir,
            always_import,
        } => {
            // imported_local
            // imported_http
            // let root_dir = DirV1::open(fs).context("Failed to open FS5 directory state")?;

            // let dir = root_dir.consume();

            let mut use_ignore = true;
            let mut use_ignore_vcs = true;
            let mut use_check_cachedir_tag = true;

            if no_ignore {
                use_ignore = false;
                use_ignore_vcs = false;
                use_check_cachedir_tag = false;
            }
            if no_ignore_vcs {
                use_ignore_vcs = false;
            }
            if no_ignore_cachedir {
                use_check_cachedir_tag = false;
            }
            if ignore {
                use_ignore = true;
                use_ignore_vcs = true;
                use_check_cachedir_tag = true;
            }
            if ignore_vcs {
                use_ignore_vcs = true;
            }
            if ignore_cachedir {
                use_check_cachedir_tag = true;
            }

            let scoped_fs = if let Some(ref p) = prefix {
                fs.subdir(p).await?
            } else {
                fs.clone()
            };

            // When a custom prefix is provided, use keys
            // relative to the imported base path so that
            // final paths are `<prefix>/<relative>`.
            let use_base_relative_keys = prefix.is_some();

            let mut importer = LocalFileSystemImporter::create(
                scoped_fs,
                target_store,
                concurrency,
                use_base_relative_keys,
                use_ignore,
                use_ignore_vcs,
                use_check_cachedir_tag,
            )?;

            if always_import || std::env::var("IMPORT_ALWAYS").as_deref() == Ok("1") {
                importer.set_always_import(true);
            }

            importer.import_path(path).await?;
        }
    }

    fs_handle.save().await?;
    fs_handle.shutdown().await?;
    Ok(())
}
