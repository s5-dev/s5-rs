//! Public API for flutter_rust_bridge.
//!
//! All public functions and types in this module will be exposed to Dart.
//! This API mirrors s5_wasm for feature parity.

use std::sync::Arc;

use bytes::Bytes;
use iroh::{Endpoint, RelayMode, SecretKey};
use s5_blobs::RemoteBlobStore;
use s5_client::DerivedKeys;
use s5_core::{blob::location::BlobLocation, BlobStore, Hash, StreamKey};
use s5_fs::{CursorKind, DirContext, FileRef, SigningKey, FS5};
use s5_registry::RemoteRegistry;
use tokio::sync::RwLock;

pub use flutter_rust_bridge::frb;

/// Maximum size for inline blobs (stored directly in directory metadata).
const INLINE_BLOB_THRESHOLD: usize = 4096;

/// Block size for chunked encryption of large files.
const ENCRYPTION_BLOCK_SIZE: u64 = 256 * 1024; // 256 KiB

// ============================================================================
// Error Types
// ============================================================================

/// Error type for S5 operations.
#[derive(Debug, thiserror::Error)]
pub enum S5Error {
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("Connection error: {0}")]
    ConnectionError(String),
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("File not found: {0}")]
    FileNotFound(String),
    #[error("Crypto error: {0}")]
    CryptoError(String),
    #[error("Internal error: {0}")]
    InternalError(String),
}

impl From<s5_client::keys::KeyError> for S5Error {
    fn from(e: s5_client::keys::KeyError) -> Self {
        S5Error::InvalidInput(e.to_string())
    }
}

impl From<anyhow::Error> for S5Error {
    fn from(e: anyhow::Error) -> Self {
        S5Error::InternalError(e.to_string())
    }
}

// ============================================================================
// Seed Phrase / Key Management
// ============================================================================

/// Generate a new 12-word BIP39 seed phrase.
#[frb(sync)]
pub fn generate_seed_phrase() -> Result<String, S5Error> {
    s5_client::keys::generate_seed_phrase().map_err(|e| S5Error::InvalidInput(e.to_string()))
}

/// Validate a BIP39 seed phrase.
#[frb(sync)]
pub fn validate_seed_phrase(phrase: String) -> bool {
    s5_client::keys::validate_seed_phrase(&phrase)
}

/// Derive all keys from a seed phrase.
#[frb(sync)]
pub fn derive_keys(phrase: String) -> Result<S5Keys, S5Error> {
    let keys = DerivedKeys::from_seed_phrase(&phrase)?;
    Ok(S5Keys {
        root_secret_hex: keys.root_secret_hex(),
        public_key_hex: keys.public_key_hex(),
        encryption_key_hex: hex::encode(keys.sync_keys.encryption_key),
        signing_key_hex: hex::encode(keys.sync_keys.signing_key_bytes),
        iroh_secret_key_hex: keys.iroh_secret_key_hex(),
    })
}

/// All cryptographic keys derived from a seed phrase.
#[frb(dart_metadata = ("freezed"))]
pub struct S5Keys {
    /// Root secret (32 bytes as hex)
    pub root_secret_hex: String,
    /// Public key / user identity (32 bytes as hex)
    pub public_key_hex: String,
    /// Encryption key for FS5 (32 bytes as hex)
    pub encryption_key_hex: String,
    /// Signing key (32 bytes as hex)
    pub signing_key_hex: String,
    /// Iroh node secret key (32 bytes as hex)
    pub iroh_secret_key_hex: String,
}

// ============================================================================
// Crypto Functions
// ============================================================================

/// Compute BLAKE3 hash of data.
#[frb(sync)]
pub fn hash_blake3(data: Vec<u8>) -> Vec<u8> {
    s5_client::crypto::hash_blake3(&data).to_vec()
}

/// Encrypt data with XChaCha20-Poly1305.
/// Returns: nonce (24 bytes) || ciphertext
#[frb(sync)]
pub fn encrypt_xchacha20poly1305(key: Vec<u8>, plaintext: Vec<u8>) -> Result<Vec<u8>, S5Error> {
    s5_client::crypto::encrypt_xchacha20poly1305(&key, &plaintext)
        .map_err(|e| S5Error::CryptoError(e.to_string()))
}

/// Decrypt data with XChaCha20-Poly1305.
/// Input: nonce (24 bytes) || ciphertext
#[frb(sync)]
pub fn decrypt_xchacha20poly1305(key: Vec<u8>, data: Vec<u8>) -> Result<Vec<u8>, S5Error> {
    s5_client::crypto::decrypt_xchacha20poly1305(&key, &data)
        .map_err(|e| S5Error::CryptoError(e.to_string()))
}

/// Decrypt a chunk with XChaCha20-Poly1305 using chunk index as nonce.
#[frb(sync)]
pub fn decrypt_chunk_xchacha20poly1305(
    key: Vec<u8>,
    chunk_index: u64,
    ciphertext: Vec<u8>,
) -> Result<Vec<u8>, S5Error> {
    s5_client::crypto::decrypt_chunk(&key, chunk_index, &ciphertext)
        .map_err(|e| S5Error::CryptoError(e.to_string()))
}

// ============================================================================
// Directory Listing
// ============================================================================

/// A file entry in a directory listing.
#[frb(dart_metadata = ("freezed"))]
pub struct FileEntry {
    pub name: String,
    /// FileRef as JSON string
    pub file_ref_json: String,
    /// File size in bytes
    pub size: u64,
    /// MIME type if known
    pub media_type: Option<String>,
    /// Timestamp (seconds since epoch)
    pub timestamp: Option<u32>,
}

/// Directory listing result.
#[frb(dart_metadata = ("freezed"))]
pub struct DirectoryListing {
    pub files: Vec<FileEntry>,
    pub directories: Vec<String>,
}

// ============================================================================
// S5 Client
// ============================================================================

struct S5ClientInner {
    #[allow(dead_code)]
    keys: DerivedKeys,
    endpoint: Endpoint,
    blobs_client: s5_blobs::Client,
    fs: FS5,
}

/// S5 Client for Flutter - E2EE cloud storage.
///
/// This client operates directly against a remote S5 node:
/// - All blobs are stored encrypted on the remote node
/// - Directory metadata is stored in the remote registry
/// - The remote node only sees encrypted data
#[frb(opaque)]
pub struct S5Client {
    inner: Arc<RwLock<Option<S5ClientInner>>>,
    public_key_hex: String,
    node_id: String,
}

impl S5Client {
    /// Create and connect a new S5 client.
    ///
    /// # Arguments
    /// * `seed_phrase` - 12-word BIP39 mnemonic
    /// * `remote_node_id` - Iroh node ID of the remote storage node
    pub async fn connect(seed_phrase: String, remote_node_id: String) -> Result<S5Client, S5Error> {
        let keys = DerivedKeys::from_seed_phrase(&seed_phrase)?;
        let public_key_hex = keys.public_key_hex();

        // Create iroh SecretKey from derived key
        let secret_key = SecretKey::from_bytes(&keys.iroh_secret_key);
        let node_id = secret_key.public().to_string();

        // Build endpoint
        let endpoint = Endpoint::builder()
            .secret_key(secret_key)
            .alpns(vec![s5_blobs::ALPN.to_vec(), s5_registry::ALPN.to_vec()])
            .relay_mode(RelayMode::Default)
            .bind()
            .await
            .map_err(|e| S5Error::ConnectionError(format!("Failed to bind endpoint: {}", e)))?;

        // Wait for relay connection
        endpoint.online().await;

        // Parse remote node ID
        let remote_id: iroh::EndpointId = remote_node_id
            .parse()
            .map_err(|e| S5Error::InvalidInput(format!("Invalid remote node ID: {}", e)))?;
        let remote_addr = iroh::EndpointAddr::from(remote_id);

        // Create blob client and stores
        let blobs_client = s5_blobs::Client::connect(endpoint.clone(), remote_addr.clone());
        let remote_blob_store = RemoteBlobStore::new(blobs_client.clone());
        let content_blob_store = BlobStore::new(remote_blob_store);

        // Create remote registry
        let remote_registry = RemoteRegistry::connect(endpoint.clone(), remote_addr);
        let registry: Arc<dyn s5_core::RegistryApi + Send + Sync> = Arc::new(remote_registry);

        // Create FS5 context with encryption
        let stream_key = StreamKey::PublicKeyEd25519(keys.sync_keys.public_key);
        let signing_key = SigningKey::new(keys.sync_keys.signing_key_bytes);

        let ctx = DirContext::new_encrypted_registry(
            stream_key,
            signing_key,
            keys.sync_keys.encryption_key,
            content_blob_store,
            registry,
        );

        let fs = FS5::open(ctx);

        Ok(S5Client {
            inner: Arc::new(RwLock::new(Some(S5ClientInner {
                keys,
                endpoint,
                blobs_client,
                fs,
            }))),
            public_key_hex,
            node_id,
        })
    }

    /// Check if the client is connected.
    pub async fn is_connected(&self) -> bool {
        self.inner.read().await.is_some()
    }

    /// Get the user's public key (identity).
    #[frb(sync, getter)]
    pub fn public_key(&self) -> String {
        self.public_key_hex.clone()
    }

    /// Get the iroh node ID for this client.
    #[frb(sync, getter)]
    pub fn node_id(&self) -> String {
        self.node_id.clone()
    }

    /// Test the connection by making a simple query.
    pub async fn test_connection(&self) -> Result<String, S5Error> {
        let guard = self.inner.read().await;
        let inner = guard
            .as_ref()
            .ok_or_else(|| S5Error::ConnectionError("Not connected".to_string()))?;

        let test_hash = Hash::from_bytes([0u8; 32]);
        match inner
            .blobs_client
            .query(test_hash, std::collections::BTreeSet::new())
            .await
        {
            Ok(resp) => Ok(format!(
                "Connection OK - {} locations",
                resp.locations.len()
            )),
            Err(e) => Err(S5Error::ConnectionError(format!("Query failed: {:?}", e))),
        }
    }

    /// List contents of a directory.
    pub async fn list_directory(&self, path: String) -> Result<DirectoryListing, S5Error> {
        let guard = self.inner.read().await;
        let inner = guard
            .as_ref()
            .ok_or_else(|| S5Error::ConnectionError("Not connected".to_string()))?;

        let (entries, _cursor) = if path.is_empty() || path == "/" {
            inner.fs.list(None, 1000).await
        } else {
            inner.fs.list_at(&path, None, 1000).await
        }
        .map_err(|e| S5Error::StorageError(format!("Failed to list directory: {}", e)))?;

        let mut files = Vec::new();
        let mut directories = Vec::new();

        for (name, kind) in entries {
            match kind {
                CursorKind::File => {
                    let full_path = if path.is_empty() || path == "/" {
                        name.clone()
                    } else {
                        format!("{}/{}", path, name)
                    };
                    if let Some(file_ref) = inner.fs.file_get(&full_path).await {
                        let file_ref_json =
                            serde_json::to_string(&file_ref).unwrap_or_else(|_| "{}".to_string());
                        files.push(FileEntry {
                            name,
                            file_ref_json,
                            size: file_ref.size,
                            media_type: file_ref.media_type.clone(),
                            timestamp: file_ref.timestamp,
                        });
                    }
                }
                CursorKind::Directory => {
                    directories.push(name);
                }
            }
        }

        Ok(DirectoryListing { files, directories })
    }

    /// Get a file's metadata as JSON.
    pub async fn file_get(&self, path: String) -> Result<Option<String>, S5Error> {
        let guard = self.inner.read().await;
        let inner = guard
            .as_ref()
            .ok_or_else(|| S5Error::ConnectionError("Not connected".to_string()))?;

        match inner.fs.file_get(&path).await {
            Some(file_ref) => {
                let json = serde_json::to_string(&file_ref)
                    .map_err(|e| S5Error::InternalError(format!("Serialization failed: {}", e)))?;
                Ok(Some(json))
            }
            None => Ok(None),
        }
    }

    /// Check if a file exists.
    pub async fn file_exists(&self, path: String) -> Result<bool, S5Error> {
        let guard = self.inner.read().await;
        let inner = guard
            .as_ref()
            .ok_or_else(|| S5Error::ConnectionError("Not connected".to_string()))?;

        Ok(inner.fs.file_exists(&path).await)
    }

    /// Create a new directory.
    pub async fn create_directory(&self, path: String) -> Result<(), S5Error> {
        let guard = self.inner.read().await;
        let inner = guard
            .as_ref()
            .ok_or_else(|| S5Error::ConnectionError("Not connected".to_string()))?;

        inner
            .fs
            .create_dir(&path, true)
            .await
            .map_err(|e| S5Error::StorageError(format!("Failed to create directory: {}", e)))?;

        inner
            .fs
            .save()
            .await
            .map_err(|e| S5Error::StorageError(format!("Failed to save: {}", e)))?;

        Ok(())
    }

    /// Upload a file with encryption.
    ///
    /// Small files (< 4KB) are stored inline in directory metadata.
    /// Larger files are encrypted with XChaCha20-Poly1305 and stored separately.
    /// Returns the FileRef as JSON.
    pub async fn upload_file(
        &self,
        path: String,
        filename: String,
        content: Vec<u8>,
        media_type: String,
    ) -> Result<String, S5Error> {
        let guard = self.inner.read().await;
        let inner = guard
            .as_ref()
            .ok_or_else(|| S5Error::ConnectionError("Not connected".to_string()))?;

        let full_path = if path.is_empty() || path == "/" {
            filename.clone()
        } else {
            format!("{}/{}", path, filename)
        };

        // Get current timestamp
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let now_secs = now.as_secs() as u32;
        let now_nanos = now.subsec_nanos();

        let mut file_ref = if content.len() <= INLINE_BLOB_THRESHOLD {
            // Small file: store inline
            let mut fr = FileRef::new_inline_blob(Bytes::from(content));
            fr.media_type = Some(media_type);
            fr
        } else {
            // Large file: encrypt and upload
            Self::upload_encrypted_blob_inner(inner, &content, &media_type).await?
        };

        file_ref.timestamp = Some(now_secs);
        file_ref.timestamp_subsec_nanos = Some(now_nanos);

        inner
            .fs
            .file_put_sync(&full_path, file_ref.clone())
            .await
            .map_err(|e| S5Error::StorageError(format!("Failed to put file: {}", e)))?;

        inner
            .fs
            .save()
            .await
            .map_err(|e| S5Error::StorageError(format!("Failed to save: {}", e)))?;

        serde_json::to_string(&file_ref)
            .map_err(|e| S5Error::InternalError(format!("Serialization failed: {}", e)))
    }

    async fn upload_encrypted_blob_inner(
        inner: &S5ClientInner,
        content: &[u8],
        media_type: &str,
    ) -> Result<FileRef, S5Error> {
        use s5_core::blob::location::EncryptionXChaCha20Poly1305Location;

        // Generate random encryption key
        let key = s5_client::crypto::generate_key()
            .map_err(|e| S5Error::CryptoError(format!("Key generation failed: {}", e)))?;

        let block_size = ENCRYPTION_BLOCK_SIZE as usize;
        let overhead = 16; // Poly1305 tag

        // Calculate encrypted size
        let num_full_blocks = content.len() / block_size;
        let last_block_size = content.len() % block_size;
        let encrypted_size = if last_block_size > 0 {
            (num_full_blocks * (block_size + overhead)) + (last_block_size + overhead)
        } else if num_full_blocks > 0 {
            num_full_blocks * (block_size + overhead)
        } else {
            0
        };

        // Encrypt all chunks
        let mut all_encrypted: Vec<u8> = Vec::with_capacity(encrypted_size);
        let mut encrypted_chunks: Vec<Bytes> = Vec::new();

        for (chunk_index, chunk) in content.chunks(block_size).enumerate() {
            let encrypted_chunk = s5_client::crypto::encrypt_chunk(&key, chunk_index as u64, chunk)
                .map_err(|e| S5Error::CryptoError(format!("Encryption failed: {}", e)))?;

            all_encrypted.extend_from_slice(&encrypted_chunk);
            encrypted_chunks.push(Bytes::from(encrypted_chunk));
        }

        // Compute hash of encrypted content
        let encrypted_hash = Hash::new(&all_encrypted);
        drop(all_encrypted);

        // Stream upload
        let (tx, rx) = inner
            .blobs_client
            .upload_begin(encrypted_hash, encrypted_size as u64, 8)
            .await
            .map_err(|e| S5Error::StorageError(format!("Upload begin failed: {}", e)))?;

        for (i, chunk) in encrypted_chunks.into_iter().enumerate() {
            tx.send(chunk)
                .await
                .map_err(|e| S5Error::StorageError(format!("Failed to send chunk {}: {}", i, e)))?;
        }
        drop(tx);

        match rx.await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(S5Error::StorageError(format!("Upload rejected: {}", err))),
            Err(e) => return Err(S5Error::StorageError(format!("Upload failed: {}", e))),
        }

        // Create FileRef with encryption location
        let plaintext_hash = Hash::new(content);
        let mut file_ref = FileRef::new(plaintext_hash, content.len() as u64);
        file_ref.media_type = Some(media_type.to_string());

        let enc_location =
            BlobLocation::EncryptionXChaCha20Poly1305(EncryptionXChaCha20Poly1305Location {
                inner: Box::new(BlobLocation::MultihashBlake3(*encrypted_hash.as_bytes())),
                key,
                block_size: ENCRYPTION_BLOCK_SIZE,
            });
        file_ref.locations = Some(vec![enc_location]);

        Ok(file_ref)
    }

    /// Download a file by path (with decryption if encrypted).
    pub async fn download_file(&self, path: String) -> Result<Vec<u8>, S5Error> {
        let guard = self.inner.read().await;
        let inner = guard
            .as_ref()
            .ok_or_else(|| S5Error::ConnectionError("Not connected".to_string()))?;

        let file_ref = inner
            .fs
            .file_get(&path)
            .await
            .ok_or_else(|| S5Error::FileNotFound(path.clone()))?;

        // Check locations
        if let Some(locations) = &file_ref.locations {
            for loc in locations {
                match loc {
                    BlobLocation::IdentityRawBinary(data) => {
                        return Ok(data.clone());
                    }
                    BlobLocation::EncryptionXChaCha20Poly1305(enc_loc) => {
                        return Self::download_encrypted_blob_inner(inner, enc_loc).await;
                    }
                    _ => {}
                }
            }
        }

        // Fallback: download by hash (unencrypted)
        let hash = Hash::from_bytes(file_ref.hash);
        let bytes = inner
            .blobs_client
            .download_bytes(hash, 0, None)
            .await
            .map_err(|e| S5Error::StorageError(format!("Download failed: {}", e)))?;

        Ok(bytes.to_vec())
    }

    async fn download_encrypted_blob_inner(
        inner: &S5ClientInner,
        enc_loc: &s5_core::blob::location::EncryptionXChaCha20Poly1305Location,
    ) -> Result<Vec<u8>, S5Error> {
        let hash = Self::extract_hash_from_location(&enc_loc.inner)?;

        let encrypted_bytes = inner
            .blobs_client
            .download_bytes(hash, 0, None)
            .await
            .map_err(|e| S5Error::StorageError(format!("Download failed: {}", e)))?;

        let block_size = enc_loc.block_size as usize;
        if block_size == 0 {
            return Err(S5Error::CryptoError("Invalid block size: 0".to_string()));
        }

        let overhead = 16;
        let encrypted_block_size = block_size + overhead;

        let mut plaintext = Vec::new();
        let mut chunk_index: u64 = 0;
        let mut offset = 0;

        while offset < encrypted_bytes.len() {
            let end = std::cmp::min(offset + encrypted_block_size, encrypted_bytes.len());
            let chunk = &encrypted_bytes[offset..end];

            let decrypted = s5_client::crypto::decrypt_chunk(&enc_loc.key, chunk_index, chunk)
                .map_err(|e| {
                    S5Error::CryptoError(format!(
                        "Decryption failed at chunk {}: {}",
                        chunk_index, e
                    ))
                })?;

            plaintext.extend_from_slice(&decrypted);
            offset = end;
            chunk_index += 1;
        }

        Ok(plaintext)
    }

    fn extract_hash_from_location(loc: &BlobLocation) -> Result<Hash, S5Error> {
        match loc {
            BlobLocation::MultihashBlake3(hash) => Ok(Hash::from_bytes(*hash)),
            BlobLocation::EncryptionXChaCha20Poly1305(inner) => {
                Self::extract_hash_from_location(&inner.inner)
            }
            _ => Err(S5Error::InternalError(format!(
                "Unsupported location type: {:?}",
                loc.location_type()
            ))),
        }
    }

    /// Download a raw blob by hash (no decryption).
    pub async fn download_blob(&self, hash_hex: String) -> Result<Vec<u8>, S5Error> {
        let guard = self.inner.read().await;
        let inner = guard
            .as_ref()
            .ok_or_else(|| S5Error::ConnectionError("Not connected".to_string()))?;

        let hash_bytes = hex::decode(&hash_hex)
            .map_err(|e| S5Error::InvalidInput(format!("Invalid hash hex: {}", e)))?;
        if hash_bytes.len() != 32 {
            return Err(S5Error::InvalidInput("Hash must be 32 bytes".to_string()));
        }

        let mut arr = [0u8; 32];
        arr.copy_from_slice(&hash_bytes);
        let hash = Hash::from_bytes(arr);

        let bytes = inner
            .blobs_client
            .download_bytes(hash, 0, None)
            .await
            .map_err(|e| S5Error::StorageError(format!("Download failed: {}", e)))?;

        Ok(bytes.to_vec())
    }

    /// Delete a file.
    pub async fn delete_file(&self, path: String) -> Result<(), S5Error> {
        let guard = self.inner.read().await;
        let inner = guard
            .as_ref()
            .ok_or_else(|| S5Error::ConnectionError("Not connected".to_string()))?;

        inner
            .fs
            .file_delete(&path)
            .await
            .map_err(|e| S5Error::StorageError(format!("Failed to delete file: {}", e)))?;

        inner
            .fs
            .save()
            .await
            .map_err(|e| S5Error::StorageError(format!("Failed to save: {}", e)))?;

        Ok(())
    }

    /// Disconnect from the remote node.
    pub async fn disconnect(&self) -> Result<(), S5Error> {
        let mut guard = self.inner.write().await;
        if let Some(inner) = guard.take() {
            // Save pending changes
            let _ = inner.fs.save().await;
            // Close endpoint
            inner.endpoint.close().await;
        }
        Ok(())
    }
}
