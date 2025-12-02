//! S5 Client for browser WASM.
//!
//! Connects to a remote S5 node via iroh and provides filesystem operations.
//!
//! ## Architecture
//!
//! This client uses a simplified architecture where all operations go directly
//! to a remote S5 node:
//!
//! - **Blobs**: Stored on the remote node via `RemoteBlobStore`
//! - **Registry**: Stored on the remote node via `RemoteRegistry`
//! - **FS5**: Uses `DirContextParentLink::RegistryKey` backed by the remote stores
//! - **Encryption**: All content is encrypted client-side before upload
//!
//! The remote node is **untrusted** - it only sees encrypted blobs and signed
//! registry entries. All cryptographic operations happen in the browser.
//!
//! ## Type Exposure
//!
//! This client exposes s5_fs types (FileRef, DirRef) directly as JSON rather than
//! creating wrapper types. This keeps the API consistent with the Rust types and
//! avoids duplication.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use iroh::{Endpoint, RelayMode, SecretKey};
use s5_blobs::RemoteBlobStore;
use s5_client::DerivedKeys;
use s5_core::{BlobStore, Hash, StreamKey, blob::location::BlobLocation};
use s5_fs::{CursorKind, DirContext, FS5, FileRef, SigningKey};
use s5_registry::RemoteRegistry;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

macro_rules! console_log {
    ($($t:tt)*) => (log(&format_args!($($t)*).to_string()))
}

/// Maximum size for inline blobs (stored directly in directory metadata).
/// Larger files are encrypted and stored separately in the blob store.
const INLINE_BLOB_THRESHOLD: usize = 4096;

/// Block size for chunked encryption of large files.
const ENCRYPTION_BLOCK_SIZE: u64 = 256 * 1024; // 256 KiB

/// Derived keys from a seed phrase (WASM wrapper)
#[wasm_bindgen]
pub struct WasmDerivedKeys {
    inner: DerivedKeys,
}

#[wasm_bindgen]
impl WasmDerivedKeys {
    /// Get the root secret as hex string
    #[wasm_bindgen(getter)]
    pub fn root_secret_hex(&self) -> String {
        self.inner.root_secret_hex()
    }

    /// Get the public key as hex string (this is the "user id" / stream key)
    #[wasm_bindgen(getter)]
    pub fn public_key_hex(&self) -> String {
        self.inner.public_key_hex()
    }

    /// Get the iroh node secret key as hex
    #[wasm_bindgen(getter)]
    pub fn iroh_secret_key_hex(&self) -> String {
        self.inner.iroh_secret_key_hex()
    }
}

/// Derive all cryptographic keys from a BIP39 seed phrase
#[wasm_bindgen]
pub fn derive_keys_from_seed_phrase(phrase: &str) -> Result<WasmDerivedKeys, JsError> {
    let inner =
        DerivedKeys::from_seed_phrase(phrase).map_err(|e| JsError::new(&format!("{}", e)))?;
    Ok(WasmDerivedKeys { inner })
}

/// Directory listing result - contains file and directory names with their references
#[wasm_bindgen]
pub struct DirectoryListing {
    /// Map of filename -> FileRef
    files_map: BTreeMap<String, FileRef>,
    /// Directory names (we don't have full DirRef without loading them)
    dir_names: Vec<String>,
}

#[wasm_bindgen]
impl DirectoryListing {
    /// Get all files as JSON object: { "filename": FileRef, ... }
    #[wasm_bindgen(getter)]
    pub fn files(&self) -> Result<JsValue, JsError> {
        serde_wasm_bindgen::to_value(&self.files_map)
            .map_err(|e| JsError::new(&format!("Failed to serialize files: {}", e)))
    }

    /// Get directory names as JSON array
    #[wasm_bindgen(getter)]
    pub fn directories(&self) -> Result<JsValue, JsError> {
        serde_wasm_bindgen::to_value(&self.dir_names)
            .map_err(|e| JsError::new(&format!("Failed to serialize directories: {}", e)))
    }

    /// Get number of files
    #[wasm_bindgen(getter)]
    pub fn file_count(&self) -> usize {
        self.files_map.len()
    }

    /// Get number of directories
    #[wasm_bindgen(getter)]
    pub fn directory_count(&self) -> usize {
        self.dir_names.len()
    }

    /// Get a specific file's FileRef as JSON, or null if not found
    #[wasm_bindgen]
    pub fn get_file(&self, name: &str) -> Result<JsValue, JsError> {
        match self.files_map.get(name) {
            Some(file_ref) => serde_wasm_bindgen::to_value(file_ref)
                .map_err(|e| JsError::new(&format!("Failed to serialize file: {}", e))),
            None => Ok(JsValue::NULL),
        }
    }
}

/// S5 Client for browser-based E2EE cloud storage
///
/// This client operates directly against a remote S5 node:
/// - All blobs are stored encrypted on the remote node
/// - Directory metadata is stored in the remote registry
/// - The remote node only sees encrypted data and signed messages
#[wasm_bindgen]
pub struct S5Client {
    /// Derived cryptographic keys
    keys: DerivedKeys,
    /// Remote node ID to connect to
    remote_node_id: String,
    /// iroh endpoint (created on connect)
    endpoint: Option<Endpoint>,
    /// S5 blobs client for blob operations
    blobs_client: Option<s5_blobs::Client>,
    /// FS5 instance for filesystem operations
    fs: Option<FS5>,
}

#[wasm_bindgen]
impl S5Client {
    /// Create a new S5 client from a seed phrase
    ///
    /// This will:
    /// 1. Derive all keys from the seed phrase
    /// 2. Set up the iroh endpoint with derived node identity
    /// 3. Connect to the remote S5 node
    #[wasm_bindgen(constructor)]
    pub fn new(seed_phrase: &str, remote_node_id: String) -> Result<S5Client, JsError> {
        let keys = DerivedKeys::from_seed_phrase(seed_phrase)
            .map_err(|e| JsError::new(&format!("{}", e)))?;

        console_log!(
            "S5Client created with public key: {}",
            keys.public_key_hex()
        );
        console_log!("Will connect to remote node: {}", remote_node_id);

        Ok(S5Client {
            keys,
            remote_node_id,
            endpoint: None,
            blobs_client: None,
            fs: None,
        })
    }

    /// Initialize the connection to the remote node
    ///
    /// This must be called before any filesystem operations.
    /// In WASM/browser, connections go through relay servers.
    #[wasm_bindgen]
    pub async fn connect(&mut self) -> Result<(), JsError> {
        console_log!("Connecting to remote node: {}", self.remote_node_id);

        // Create iroh SecretKey from our derived key
        let secret_key = SecretKey::from_bytes(&self.keys.iroh_secret_key);

        console_log!(
            "Creating iroh endpoint with node ID: {}",
            secret_key.public().fmt_short()
        );

        // Build the endpoint with our secret key and default relays
        let endpoint = Endpoint::builder()
            .secret_key(secret_key)
            .alpns(vec![s5_blobs::ALPN.to_vec(), s5_registry::ALPN.to_vec()])
            .relay_mode(RelayMode::Default)
            .bind()
            .await
            .map_err(|e| JsError::new(&format!("Failed to bind endpoint: {}", e)))?;

        console_log!("Endpoint bound, waiting for relay connection...");

        // Wait for the endpoint to be online (connected to relay)
        endpoint.online().await;

        console_log!(
            "Endpoint online. Our address: {}",
            endpoint.id().fmt_short()
        );

        // Parse the remote node ID (EndpointId is the node's public key)
        let remote_id: iroh::EndpointId = self
            .remote_node_id
            .parse()
            .map_err(|e| JsError::new(&format!("Invalid remote node ID: {}", e)))?;
        // EndpointAddr wraps the EndpointId with optional relay/direct addresses
        let remote_addr = iroh::EndpointAddr::from(remote_id);

        console_log!("Setting up remote stores and FS5...");

        // Create the s5_blobs client
        let blobs_client = s5_blobs::Client::connect(endpoint.clone(), remote_addr.clone());

        // Create remote blob store (for content blobs)
        let remote_blob_store = RemoteBlobStore::new(blobs_client.clone());
        let content_blob_store = BlobStore::new(remote_blob_store.clone());

        // Create remote registry
        let remote_registry = RemoteRegistry::connect(endpoint.clone(), remote_addr);
        let registry: Arc<dyn s5_core::RegistryApi + Send + Sync> = Arc::new(remote_registry);

        // Create FS5 context with RegistryKey link
        // The public key from our sync keys becomes the stream key for the FS5 root
        let stream_key = StreamKey::PublicKeyEd25519(self.keys.sync_keys.public_key);
        let signing_key = SigningKey::new(self.keys.sync_keys.signing_key_bytes);

        // Create encrypted context backed by remote stores
        let ctx = DirContext::new_encrypted_registry(
            stream_key,
            signing_key,
            self.keys.sync_keys.encryption_key,
            content_blob_store,
            registry,
        );

        // Open FS5 instance
        let fs = FS5::open(ctx);

        console_log!("FS5 initialized with remote-only backend!");

        self.endpoint = Some(endpoint);
        self.blobs_client = Some(blobs_client.clone());
        self.fs = Some(fs);

        // Test the connection with a simple query
        console_log!("Testing connection with blob query...");
        let test_hash = s5_core::Hash::from_bytes([0u8; 32]);
        match blobs_client
            .query(test_hash, std::collections::BTreeSet::new())
            .await
        {
            Ok(resp) => console_log!(
                "Connection test PASSED - got {} locations",
                resp.locations.len()
            ),
            Err(e) => console_log!("Connection test FAILED: {:?}", e),
        }

        Ok(())
    }

    /// Test the connection by making a simple registry query
    #[wasm_bindgen]
    pub async fn test_connection(&self) -> Result<String, JsError> {
        let blobs_client = self
            .blobs_client
            .as_ref()
            .ok_or_else(|| JsError::new("Not connected"))?;

        console_log!("Testing connection with a simple blob query...");

        // Query for a non-existent hash - this should return quickly with "not found"
        let test_hash = s5_core::Hash::from_bytes([0u8; 32]);

        console_log!("Sending query request...");
        let result = blobs_client
            .query(test_hash, std::collections::BTreeSet::new())
            .await;

        match result {
            Ok(response) => {
                console_log!("Query succeeded! Response: {:?}", response.locations.len());
                Ok(format!(
                    "Connection OK - {} locations",
                    response.locations.len()
                ))
            }
            Err(e) => {
                console_log!("Query failed: {:?}", e);
                Err(JsError::new(&format!("Query failed: {:?}", e)))
            }
        }
    }

    /// Check if the client is connected
    #[wasm_bindgen(getter)]
    pub fn is_connected(&self) -> bool {
        self.fs.is_some()
    }

    /// Get the user's public key (used as identity)
    #[wasm_bindgen(getter)]
    pub fn public_key(&self) -> String {
        self.keys.public_key_hex()
    }

    /// Get the iroh node ID for this client (public key)
    #[wasm_bindgen(getter)]
    pub fn node_id(&self) -> String {
        let secret_key = SecretKey::from_bytes(&self.keys.iroh_secret_key);
        secret_key.public().to_string()
    }

    /// List contents of a directory
    ///
    /// Returns a DirectoryListing with files and directories as JSON objects
    /// using the native s5_fs FileRef and DirRef types.
    #[wasm_bindgen]
    pub async fn list_directory(&self, path: &str) -> Result<DirectoryListing, JsError> {
        let fs = self
            .fs
            .as_ref()
            .ok_or_else(|| JsError::new("Not connected. Call connect() first."))?;

        console_log!("Listing directory: {}", path);

        // Use list_at for nested paths, list for root
        let (entries, _cursor) = if path.is_empty() || path == "/" {
            fs.list(None, 1000)
                .await
                .map_err(|e| JsError::new(&format!("Failed to list directory: {}", e)))?
        } else {
            fs.list_at(path, None, 1000)
                .await
                .map_err(|e| JsError::new(&format!("Failed to list directory: {}", e)))?
        };

        let mut files_map = BTreeMap::new();
        let mut dir_names = Vec::new();

        for (name, kind) in entries {
            match kind {
                CursorKind::File => {
                    let full_path = if path.is_empty() || path == "/" {
                        name.clone()
                    } else {
                        format!("{}/{}", path, name)
                    };
                    if let Some(file_ref) = fs.file_get(&full_path).await {
                        files_map.insert(name, file_ref);
                    }
                }
                CursorKind::Directory => {
                    dir_names.push(name);
                }
            }
        }

        Ok(DirectoryListing {
            files_map,
            dir_names,
        })
    }

    /// Get a file's metadata (FileRef) as JSON
    #[wasm_bindgen]
    pub async fn file_get(&self, path: &str) -> Result<JsValue, JsError> {
        let fs = self
            .fs
            .as_ref()
            .ok_or_else(|| JsError::new("Not connected. Call connect() first."))?;

        match fs.file_get(path).await {
            Some(file_ref) => serde_wasm_bindgen::to_value(&file_ref)
                .map_err(|e| JsError::new(&format!("Failed to serialize FileRef: {}", e))),
            None => Ok(JsValue::NULL),
        }
    }

    /// Create a new directory
    #[wasm_bindgen]
    pub async fn create_directory(&self, path: &str) -> Result<(), JsError> {
        let fs = self
            .fs
            .as_ref()
            .ok_or_else(|| JsError::new("Not connected. Call connect() first."))?;

        console_log!("Creating directory: {}", path);

        // Create with encryption enabled (inherits from parent)
        console_log!("Calling fs.create_dir...");
        fs.create_dir(path, true)
            .await
            .map_err(|e| JsError::new(&format!("Failed to create directory: {}", e)))?;

        console_log!("Directory created locally, now saving to remote...");
        // Save changes to remote
        fs.save()
            .await
            .map_err(|e| JsError::new(&format!("Failed to save: {}", e)))?;

        console_log!("Save completed successfully!");

        Ok(())
    }

    /// Upload a file with encryption
    ///
    /// Small files (< 4KB) are stored inline in directory metadata.
    /// Larger files are encrypted with XChaCha20-Poly1305 and stored separately.
    /// Returns the FileRef as JSON.
    #[wasm_bindgen]
    pub async fn upload_file(
        &self,
        path: &str,
        filename: &str,
        content: &[u8],
        media_type: &str,
    ) -> Result<JsValue, JsError> {
        let fs = self
            .fs
            .as_ref()
            .ok_or_else(|| JsError::new("Not connected. Call connect() first."))?;

        console_log!(
            "Uploading file: {}/{} ({} bytes)",
            path,
            filename,
            content.len()
        );

        // Build the full path
        let full_path = if path.is_empty() || path == "/" {
            filename.to_string()
        } else {
            format!("{}/{}", path, filename)
        };

        // Get current timestamp (seconds since epoch)
        let now_ms = js_sys::Date::now() as u64;
        let now_secs = (now_ms / 1000) as u32;
        let now_nanos = ((now_ms % 1000) * 1_000_000) as u32;

        let mut file_ref = if content.len() <= INLINE_BLOB_THRESHOLD {
            // Small file: store inline (encrypted with directory)
            console_log!("Storing as inline blob ({} bytes)", content.len());
            let mut fr = FileRef::new_inline_blob(Bytes::copy_from_slice(content));
            fr.media_type = Some(media_type.to_string());
            fr
        } else {
            // Large file: encrypt and upload separately
            console_log!(
                "Encrypting and uploading large file ({} bytes)",
                content.len()
            );
            self.upload_encrypted_blob(content, media_type).await?
        };

        // Set timestamp
        file_ref.timestamp = Some(now_secs);
        file_ref.timestamp_subsec_nanos = Some(now_nanos);

        // Put the file in FS5
        fs.file_put_sync(&full_path, file_ref.clone())
            .await
            .map_err(|e| JsError::new(&format!("Failed to put file: {}", e)))?;

        // Save changes to remote
        fs.save()
            .await
            .map_err(|e| JsError::new(&format!("Failed to save: {}", e)))?;

        console_log!(
            "Uploaded file: {} ({} bytes)",
            hex::encode(file_ref.hash),
            content.len()
        );

        // Return the FileRef as JSON
        serde_wasm_bindgen::to_value(&file_ref)
            .map_err(|e| JsError::new(&format!("Failed to serialize FileRef: {}", e)))
    }

    /// Encrypt and upload a blob, returning a FileRef with encryption location
    ///
    /// Uses streaming upload to handle large files without hitting message size limits.
    async fn upload_encrypted_blob(
        &self,
        content: &[u8],
        media_type: &str,
    ) -> Result<FileRef, JsError> {
        use s5_core::blob::location::EncryptionXChaCha20Poly1305Location;

        let client = self
            .blobs_client
            .as_ref()
            .ok_or_else(|| JsError::new("Not connected"))?;

        // Generate a random encryption key for this file
        let mut key = [0u8; 32];
        getrandom::getrandom(&mut key)
            .map_err(|e| JsError::new(&format!("Failed to generate key: {}", e)))?;

        let block_size = ENCRYPTION_BLOCK_SIZE as usize;
        let overhead = 16; // Poly1305 tag size

        // Calculate encrypted size and hash
        let num_full_blocks = content.len() / block_size;
        let last_block_size = content.len() % block_size;
        let encrypted_size = if last_block_size > 0 {
            (num_full_blocks * (block_size + overhead)) + (last_block_size + overhead)
        } else if num_full_blocks > 0 {
            num_full_blocks * (block_size + overhead)
        } else {
            0
        };

        // Encrypt all chunks and collect them, computing the hash as we go
        let mut all_encrypted: Vec<u8> = Vec::with_capacity(encrypted_size);
        let mut encrypted_chunks: Vec<Bytes> = Vec::new();

        for (chunk_index, chunk) in content.chunks(block_size).enumerate() {
            let encrypted_chunk = s5_client::crypto::encrypt_chunk(&key, chunk_index as u64, chunk)
                .map_err(|e| JsError::new(&format!("Encryption failed: {}", e)))?;

            all_encrypted.extend_from_slice(&encrypted_chunk);
            encrypted_chunks.push(Bytes::from(encrypted_chunk));
        }

        // Compute hash of all encrypted content
        let encrypted_hash = Hash::new(&all_encrypted);
        drop(all_encrypted); // Free memory

        console_log!(
            "Encrypted {} bytes -> {} bytes ({} chunks)",
            content.len(),
            encrypted_size,
            encrypted_chunks.len()
        );

        // Stream upload the encrypted chunks
        let (tx, rx) = client
            .upload_begin(encrypted_hash, encrypted_size as u64, 8)
            .await
            .map_err(|e| JsError::new(&format!("Upload begin failed: {}", e)))?;

        // Send each encrypted chunk
        for (i, chunk) in encrypted_chunks.into_iter().enumerate() {
            tx.send(chunk)
                .await
                .map_err(|e| JsError::new(&format!("Failed to send chunk {}: {}", i, e)))?;
        }
        drop(tx); // Signal end of upload

        // Wait for upload confirmation
        match rx
            .await
            .map_err(|e| JsError::new(&format!("Upload response failed: {}", e)))?
        {
            Ok(()) => {}
            Err(err) => return Err(JsError::new(&format!("Upload rejected: {}", err))),
        }

        console_log!(
            "Uploaded encrypted blob: {}",
            hex::encode(encrypted_hash.as_bytes())
        );

        // Create a FileRef with encryption location
        let plaintext_hash = Hash::new(content);
        let mut file_ref = FileRef::new(plaintext_hash, content.len() as u64);
        file_ref.media_type = Some(media_type.to_string());

        // Set the location to point to the encrypted blob with decryption info
        let enc_location =
            BlobLocation::EncryptionXChaCha20Poly1305(EncryptionXChaCha20Poly1305Location {
                inner: Box::new(BlobLocation::MultihashBlake3(*encrypted_hash.as_bytes())),
                key,
                block_size: ENCRYPTION_BLOCK_SIZE,
            });
        file_ref.locations = Some(vec![enc_location]);

        Ok(file_ref)
    }

    /// Download a file by hash (raw blob, no decryption)
    #[wasm_bindgen]
    pub async fn download_blob(&self, hash_hex: &str) -> Result<Vec<u8>, JsError> {
        let client = self
            .blobs_client
            .as_ref()
            .ok_or_else(|| JsError::new("Not connected. Call connect() first."))?;

        console_log!("Downloading blob: {}", hash_hex);

        // Parse hash from hex
        let hash_bytes =
            hex::decode(hash_hex).map_err(|e| JsError::new(&format!("Invalid hash hex: {}", e)))?;
        if hash_bytes.len() != 32 {
            return Err(JsError::new("Hash must be 32 bytes"));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&hash_bytes);
        let hash = s5_core::Hash::from_bytes(arr);

        let bytes = client
            .download_bytes(hash, 0, None)
            .await
            .map_err(|e| JsError::new(&format!("Download failed: {}", e)))?;

        console_log!("Downloaded {} bytes", bytes.len());

        Ok(bytes.to_vec())
    }

    /// Download a file by path (from FS5)
    ///
    /// The file will be fetched and decrypted if encrypted.
    #[wasm_bindgen]
    pub async fn download_file(&self, path: &str) -> Result<Vec<u8>, JsError> {
        let fs = self
            .fs
            .as_ref()
            .ok_or_else(|| JsError::new("Not connected. Call connect() first."))?;

        console_log!("Downloading file: {}", path);

        // Get the file reference from FS5
        let file_ref = fs
            .file_get(path)
            .await
            .ok_or_else(|| JsError::new(&format!("File not found: {}", path)))?;

        // Check locations for the blob data
        if let Some(locations) = &file_ref.locations {
            for loc in locations {
                match loc {
                    // Inline blob - data is directly in the location
                    BlobLocation::IdentityRawBinary(data) => {
                        return Ok(data.clone());
                    }
                    // Encrypted blob - decrypt after download
                    BlobLocation::EncryptionXChaCha20Poly1305(enc_loc) => {
                        return self.download_encrypted_blob(enc_loc).await;
                    }
                    // Other location types - download by hash
                    _ => {}
                }
            }
        }

        // Fall back to downloading by hash (unencrypted)
        let hash = Hash::from_bytes(file_ref.hash);
        let client = self.blobs_client.as_ref().unwrap();
        let bytes = client
            .download_bytes(hash, 0, None)
            .await
            .map_err(|e| JsError::new(&format!("Download failed: {}", e)))?;

        Ok(bytes.to_vec())
    }

    /// Download and decrypt an encrypted blob
    async fn download_encrypted_blob(
        &self,
        enc_loc: &s5_core::blob::location::EncryptionXChaCha20Poly1305Location,
    ) -> Result<Vec<u8>, JsError> {
        let client = self
            .blobs_client
            .as_ref()
            .ok_or_else(|| JsError::new("Not connected"))?;

        // Get the hash from the inner location
        let hash = self.get_hash_from_location(&enc_loc.inner)?;

        console_log!(
            "Downloading encrypted blob: {}",
            hex::encode(hash.as_bytes())
        );

        // Download the encrypted blob
        let encrypted_bytes = client
            .download_bytes(hash, 0, None)
            .await
            .map_err(|e| JsError::new(&format!("Download failed: {}", e)))?;

        console_log!(
            "Downloaded {} encrypted bytes, decrypting...",
            encrypted_bytes.len()
        );

        // Decrypt using XChaCha20-Poly1305
        let block_size = enc_loc.block_size as usize;

        if block_size == 0 {
            return Err(JsError::new("Invalid block size: 0"));
        }

        let overhead = 16; // Poly1305 tag size
        let encrypted_block_size = block_size + overhead;

        let mut plaintext = Vec::new();
        let mut chunk_index: u64 = 0;

        let mut offset = 0;
        while offset < encrypted_bytes.len() {
            let end = std::cmp::min(offset + encrypted_block_size, encrypted_bytes.len());
            let chunk = &encrypted_bytes[offset..end];

            let decrypted = s5_client::crypto::decrypt_chunk(&enc_loc.key, chunk_index, chunk)
                .map_err(|e| {
                    JsError::new(&format!(
                        "Decryption failed at chunk {}: {}",
                        chunk_index, e
                    ))
                })?;

            plaintext.extend_from_slice(&decrypted);
            offset = end;
            chunk_index += 1;
        }

        console_log!("Decrypted {} bytes", plaintext.len());

        Ok(plaintext)
    }

    /// Extract hash from a BlobLocation
    fn get_hash_from_location(&self, loc: &BlobLocation) -> Result<Hash, JsError> {
        Self::extract_hash_from_location(loc)
    }

    /// Extract hash from a BlobLocation (static helper)
    fn extract_hash_from_location(loc: &BlobLocation) -> Result<Hash, JsError> {
        match loc {
            BlobLocation::MultihashBlake3(hash) => Ok(Hash::from_bytes(*hash)),
            BlobLocation::Iroh(_) => Err(JsError::new(
                "Iroh location without hash - not yet supported",
            )),
            BlobLocation::EncryptionXChaCha20Poly1305(inner) => {
                Self::extract_hash_from_location(&inner.inner)
            }
            _ => Err(JsError::new(&format!(
                "Unsupported inner location type: {:?}",
                loc.location_type()
            ))),
        }
    }

    /// Delete a file
    #[wasm_bindgen]
    pub async fn delete_file(&self, path: &str) -> Result<(), JsError> {
        let fs = self
            .fs
            .as_ref()
            .ok_or_else(|| JsError::new("Not connected. Call connect() first."))?;

        console_log!("Deleting file: {}", path);

        fs.file_delete(path)
            .await
            .map_err(|e| JsError::new(&format!("Failed to delete file: {}", e)))?;

        // Save changes to remote
        fs.save()
            .await
            .map_err(|e| JsError::new(&format!("Failed to save: {}", e)))?;

        Ok(())
    }

    /// Check if a file exists
    #[wasm_bindgen]
    pub async fn file_exists(&self, path: &str) -> Result<bool, JsError> {
        let fs = self
            .fs
            .as_ref()
            .ok_or_else(|| JsError::new("Not connected. Call connect() first."))?;

        Ok(fs.file_exists(path).await)
    }

    /// Disconnect from the remote node
    #[wasm_bindgen]
    pub async fn disconnect(&mut self) -> Result<(), JsError> {
        console_log!("Disconnecting...");

        // Save any pending changes before disconnecting
        if let Some(fs) = &self.fs {
            let _ = fs.save().await;
        }

        // Drop FS5
        self.fs = None;

        // Drop the blobs client
        self.blobs_client = None;

        // Close the endpoint
        if let Some(endpoint) = self.endpoint.take() {
            endpoint.close().await;
        }

        Ok(())
    }
}

/// Static methods for S5Client
#[wasm_bindgen]
impl S5Client {
    /// Generate a new seed phrase
    #[wasm_bindgen]
    pub fn generate_seed_phrase() -> Result<String, JsError> {
        crate::generate_seed_phrase()
    }

    /// Validate a seed phrase
    #[wasm_bindgen]
    pub fn validate_seed_phrase(phrase: &str) -> bool {
        crate::validate_seed_phrase(phrase)
    }
}
