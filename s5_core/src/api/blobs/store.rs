use base64::Engine;
use bytes::Bytes;
use futures_core::Stream;
use std::{fmt::Debug, path::PathBuf, sync::Arc};
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead};

use crate::{
    BlobId, Hash,
    bao::outboard::compute_outboard,
    blob::location::BlobLocation,
    store::{Store, StoreFeatures, StoreResult},
};

#[derive(Debug, Clone)]
pub struct BlobStore {
    store: Arc<Box<dyn Store + 'static>>,
    outboard_store: Option<Arc<Box<dyn Store + 'static>>>,
}

impl BlobStore {
    pub fn new<S>(store: S) -> Self
    where
        S: Store + 'static,
    {
        let store = Arc::new(Box::new(store) as Box<dyn Store>);
        Self {
            store: store.clone(),
            outboard_store: Some(store),
        }
    }
    pub fn new_boxed(store: Box<dyn Store + 'static>) -> Self {
        let store = Arc::new(store);
        Self {
            store: store.clone(),
            outboard_store: Some(store),
        }
    }
    fn path_for_hash(&self, hash: Hash, features: &StoreFeatures) -> String {
        let hash_str = if features.case_sensitive {
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash)
        } else {
            let mut output = Vec::with_capacity(base32_fs::encoded_len(hash.as_bytes().len()));
            base32_fs::encode(hash.as_bytes(), &mut output);
            String::from_utf8(output).unwrap()
        };

        if features.recommended_max_dir_size < 10000 {
            if features.case_sensitive {
                format!("{}/{}/{}", &hash_str[0..2], &hash_str[2..4], &hash_str[4..],)
            } else {
                format!(
                    "{}/{}/{}/{}",
                    &hash_str[0..2],
                    &hash_str[2..4],
                    &hash_str[4..6],
                    &hash_str[6..]
                )
            }
        } else {
            hash_str
        }
    }

    fn blob_path_for_hash(&self, hash: Hash) -> String {
        format!("blob3/{}", self.path_for_hash(hash, &self.store.features()))
    }

    /// returns path for storing the bao outboard metadata for a specific blob hash
    fn obao6_path_for_hash(&self, hash: Hash) -> String {
        format!(
            "obao6/{}",
            self.path_for_hash(hash, &self.outboard_store.as_ref().unwrap().features())
        )
    }

    /// Deletes a blob and its associated outboard data from the store.
    pub async fn delete(&self, hash: Hash) -> StoreResult<()> {
        // Delete the main blob data.
        self.store.delete(&self.blob_path_for_hash(hash)).await?;

        // If an outboard store is configured, delete the outboard data as well.
        if let Some(obao_store) = &self.outboard_store {
            // It's okay if the outboard data doesn't exist, so we can ignore a NotFound error.
            match obao_store.delete(&self.obao6_path_for_hash(hash)).await {
                Ok(_) => {}
                Err(e) => {
                    // A proper error type would allow matching on "NotFound".
                    // For now, we assume any error here is likely due to the file not existing,
                    // which is acceptable. A more robust solution would be to have typed errors.
                    // TODO tracing::warn!("Failed to delete outboard data for {}: {:?}", hash, e);
                }
            }
        }
        Ok(())
    }

    pub async fn size(&self, hash: Hash) -> StoreResult<u64> {
        self.store.size(&self.blob_path_for_hash(hash)).await
    }

    pub async fn contains(&self, hash: Hash) -> StoreResult<bool> {
        self.store.exists(&self.blob_path_for_hash(hash)).await
    }

    pub async fn contains_obao6(&self, hash: Hash) -> StoreResult<bool> {
        if let Some(obao_store) = &self.outboard_store {
            obao_store.exists(&&self.obao6_path_for_hash(hash)).await
        } else {
            Ok(false)
        }
    }

    pub async fn provide(&self, hash: Hash) -> StoreResult<Vec<BlobLocation>> {
        self.store.provide(&self.blob_path_for_hash(hash)).await
    }

    pub async fn provide_obao6(&self, hash: Hash) -> StoreResult<Vec<BlobLocation>> {
        if let Some(obao_store) = &self.outboard_store {
            obao_store.provide(&&self.obao6_path_for_hash(hash)).await
        } else {
            Ok(vec![])
        }
    }

    pub async fn read_as_bytes(
        &self,
        hash: Hash,
        offset: u64,
        max_len: Option<u64>,
    ) -> StoreResult<Bytes> {
        self.store
            .open_read_bytes(&self.blob_path_for_hash(hash), offset, max_len)
            .await
    }

    /// Insert an in-memory blob of bytes to the blob store
    pub async fn import_bytes(&self, bytes: bytes::Bytes) -> StoreResult<BlobId> {
        let size = bytes.len() as u64;
        let hash = if self.outboard_store.is_some() {
            let obao = compute_outboard(bytes.as_ref(), bytes.len() as u64, |_| Ok(()))?;
            if let Some(outboard) = obao.1 {
                self.outboard_store
                    .as_ref()
                    .unwrap()
                    .put_bytes(&self.obao6_path_for_hash(obao.0), outboard.into())
                    .await?;
            }
            obao.0
        } else {
            blake3::hash(&bytes).into()
        };

        // TODO maybe first check if store already contains hash?
        self.store
            .put_bytes(&self.blob_path_for_hash(hash), bytes)
            .await?;

        Ok(BlobId { hash, size })
    }

    /// Import a blob from a stream of bytes.
    ///
    /// This method consumes the stream exactly once. It simultaneously calculates the
    /// hash (and outboard data, if configured) while streaming the content to a
    /// temporary location in the underlying store.
    ///
    /// Upon successful completion of the stream, the temporary file is atomically
    /// renamed to its final content-addressed path. If any part of the process fails,
    /// the temporary file is cleaned up.
    pub async fn import_stream(
        &self,
        mut stream: Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Unpin + 'static>,
    ) -> StoreResult<(Hash, u64)> {
        let temp_path: PathBuf = std::env::temp_dir()
            .join("s5_import")
            .join(uuid::Uuid::new_v4().to_string());
        std::fs::create_dir_all(temp_path.parent().unwrap())?;
        let mut writer = tokio::fs::File::create(&temp_path).await?;
        while let Some(chunk) = stream.next().await {
            writer.write_all(&chunk?).await?;
        }
        writer.flush().await?;
        drop(writer);
        let res = self.import_file(temp_path.clone()).await;
        std::fs::remove_file(temp_path)?;
        res
    }

    //type Error: Sized + Debug + Send + Sync + 'static;

    // fn init();
    // fn blobs(&self) -> impl Future<Output = io::Result<DbIter<Hash>>> + Send;

    /// This trait method imports a file from a local path.
    ///
    /// `data` is the path to the file.
    /// `mode` is a hint how the file should be imported.
    /// `progress` is a sender that provides a way for the importer to send progress messages
    /// when importing large files. This also serves as a way to cancel the import. If the
    /// consumer of the progress messages is dropped, subsequent attempts to send progress
    /// will fail.
    ///
    /// Returns the hash of the imported file. The reason to have this method is that some database
    /// implementations might be able to import a file without copying it.
    pub async fn import_file(
        &self,
        path: PathBuf,
        // TODO progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> StoreResult<(Hash, u64)> {
        let meta = std::fs::metadata(&path)?;
        let size = meta.len();

        let hash = if self.outboard_store.is_some() {
            let file = std::fs::File::open(&path)?;
            let (hash, obao) = compute_outboard(file, size, move |_| Ok(()))?;
            if let Some(outboard) = obao {
                self.outboard_store
                    .as_ref()
                    .unwrap()
                    .put_bytes(&self.obao6_path_for_hash(hash), outboard.into())
                    .await?;
            }
            hash
        } else {
            let mut hasher = blake3::Hasher::new();
            hasher.update_mmap_rayon(&path)?;
            hasher.finalize().into()
        };

        if self.store.exists(&self.blob_path_for_hash(hash)).await? {
            return Ok((hash, size));
        }

        let stream = FramedRead::new(tokio::fs::File::open(path).await?, BytesCodec::new())
            .map(|result| result.map(|b| b.into()));

        self.store
            .put_stream(&self.blob_path_for_hash(hash), Box::new(stream))
            .await?;

        Ok((hash, size))
    }

    // progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,

    // Import data from an async byte reader.
    /*     fn import_reader(
        &self,
        data: impl AsyncRead + Send + Unpin + 'static,
        progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> impl Future<Output = io::Result<(Hash, u64)>> + Send {
        let stream = tokio_util::io::ReaderStream::new(data);
        self.import_stream(stream, format, progress)
    } */
}
