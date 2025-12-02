# @redsolver/s5-wasm

S5 WebAssembly client for browser-based end-to-end encrypted (E2EE) cloud storage.

This package provides TypeScript/JavaScript bindings for the S5 Rust client, enabling browser applications to:

- Generate and manage BIP39 seed phrases for secure key derivation
- Connect to S5 nodes via iroh's relay network
- Upload and download encrypted files
- Manage encrypted filesystem directories

## Installation

```bash
npm install @redsolver/s5-wasm
# or
yarn add @redsolver/s5-wasm
# or
bun add @redsolver/s5-wasm
```

## Quick Start

```typescript
import init, { 
  S5Client, 
  generate_seed_phrase, 
  validate_seed_phrase 
} from '@redsolver/s5-wasm';

// Initialize WASM module (required before any other calls)
await init();

// Generate a new seed phrase for a new user
const seedPhrase = generate_seed_phrase();
console.log('Your recovery phrase:', seedPhrase);

// Create and connect the client
const client = new S5Client(seedPhrase, 'remote-node-id...');
await client.connect();

// Create a directory
await client.create_directory('documents');

// Upload a file
const content = new TextEncoder().encode('Hello, S5!');
const fileRef = await client.upload_file('documents', 'hello.txt', content, 'text/plain');
console.log('Uploaded file');

// List directory contents
const listing = await client.list_directory('documents');
console.log('Files:', listing.files);  // Object with FileRef values
console.log('Directories:', listing.directories);  // Array of directory names

// Download a file
const data = await client.download_file('documents/hello.txt');
console.log('Downloaded:', new TextDecoder().decode(data));

// Clean up
await client.disconnect();
```

## API Reference

### Module Initialization

#### `init()`

Initialize the WASM module. **Must be called before using any other functions.**

```typescript
import init from '@redsolver/s5-wasm';
await init();
```

You can optionally pass a URL or `WebAssembly.Module` to load from a custom location:

```typescript
await init('/path/to/s5_wasm_bg.wasm');
```

---

### Seed Phrase Functions

These functions handle BIP39 mnemonic seed phrases for deterministic key derivation.

#### `generate_seed_phrase(): string`

Generate a new cryptographically secure 12-word BIP39 seed phrase.

```typescript
const seedPhrase = generate_seed_phrase();
// Example: "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about"
```

**Security Note:** Store this phrase securely. It is the only way to recover access to encrypted data.

#### `validate_seed_phrase(phrase: string): boolean`

Check if a seed phrase is valid BIP39 English mnemonic.

```typescript
if (validate_seed_phrase(userInput)) {
  console.log('Valid seed phrase');
} else {
  console.log('Invalid seed phrase');
}
```

#### `derive_keys_from_seed_phrase(phrase: string): WasmDerivedKeys`

Derive all cryptographic keys from a seed phrase. Returns a `WasmDerivedKeys` object.

```typescript
const keys = derive_keys_from_seed_phrase(seedPhrase);
console.log('Public key:', keys.public_key_hex);  // User identity
console.log('Root secret:', keys.root_secret_hex);  // For storage/recovery
console.log('Iroh node key:', keys.iroh_secret_key_hex);  // Network identity
```

---

### Cryptographic Functions

Low-level cryptographic primitives exposed for advanced use cases.

#### `hash_blake3(data: Uint8Array): Uint8Array`

Compute a 32-byte BLAKE3 hash of the input data.

```typescript
const data = new TextEncoder().encode('Hello, world!');
const hash = hash_blake3(data);  // Uint8Array(32)
```

#### `encrypt_xchacha20poly1305(key: Uint8Array, plaintext: Uint8Array): Uint8Array`

Encrypt data using XChaCha20-Poly1305 with a random nonce.

**Parameters:**
- `key` - 32-byte encryption key
- `plaintext` - Data to encrypt

**Returns:** `nonce (24 bytes) || ciphertext`

#### `decrypt_xchacha20poly1305(key: Uint8Array, data: Uint8Array): Uint8Array`

Decrypt data encrypted with `encrypt_xchacha20poly1305`.

**Parameters:**
- `key` - 32-byte encryption key
- `data` - `nonce (24 bytes) || ciphertext`

**Returns:** Decrypted plaintext

#### `decrypt_chunk_xchacha20poly1305(key: Uint8Array, chunk_index: number, ciphertext: Uint8Array): Uint8Array`

Decrypt an FS5 encrypted chunk using chunk-index-based nonce derivation.

This is used for streaming decryption of large files where:
- Each chunk is encrypted separately
- The nonce is derived from the chunk index (0, 1, 2, ...)
- No nonce is prepended to the ciphertext

---

### S5Client Class

The main client class for interacting with S5 storage nodes.

#### Constructor

```typescript
new S5Client(seed_phrase: string, remote_node_id: string): S5Client
```

Create a new S5 client from a seed phrase.

**Parameters:**
- `seed_phrase` - 12-word BIP39 mnemonic
- `remote_node_id` - Iroh node ID of the remote storage node

```typescript
const client = new S5Client(seedPhrase, 'nodeId...');
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `is_connected` | `boolean` | Whether the client is connected to a remote node |
| `public_key` | `string` | User's public key (hex), used as identity |
| `node_id` | `string` | Iroh node ID for this client |

#### Methods

##### `connect(): Promise<void>`

Connect to the remote S5 node. Must be called before file operations.

```typescript
await client.connect();
console.log('Connected! Node ID:', client.node_id);
```

##### `disconnect(): Promise<void>`

Disconnect from the remote node and clean up resources.

```typescript
await client.disconnect();
```

##### `create_directory(path: string): Promise<void>`

Create a new directory.

```typescript
await client.create_directory('documents/projects');
```

##### `list_directory(path: string): Promise<DirectoryListing>`

List contents of a directory.

**Returns:** `DirectoryListing` object with:
- `files` - Object mapping filename to FileRef (as JsValue)
- `directories` - Array of directory names
- `file_count` - Number of files
- `directory_count` - Number of directories

```typescript
const listing = await client.list_directory('documents');
console.log(`${listing.file_count} files, ${listing.directory_count} directories`);

// Access files (returns JsValue, use as object)
const files = listing.files;  // { "hello.txt": FileRef, ... }

// Access directory names
const dirs = listing.directories;  // ["subdir1", "subdir2"]
```

##### `upload_file(path: string, filename: string, content: Uint8Array, media_type: string): Promise<JsValue>`

Upload a file to the storage node.

**Parameters:**
- `path` - Directory path (e.g., `"documents"` or `""` for root)
- `filename` - File name (e.g., `"report.pdf"`)
- `content` - File content as `Uint8Array`
- `media_type` - MIME type (e.g., `"application/pdf"`)

**Returns:** FileRef as JsValue (JSON object with hash, size, locations, etc.)

```typescript
const content = new Uint8Array([...fileBytes]);
const fileRef = await client.upload_file(
  'photos',
  'vacation.jpg',
  content,
  'image/jpeg'
);
```

##### `download_file(path: string): Promise<Uint8Array>`

Download and decrypt a file by its filesystem path.

```typescript
const data = await client.download_file('documents/hello.txt');
const text = new TextDecoder().decode(data);
```

##### `download_blob(hash_hex: string): Promise<Uint8Array>`

Download a raw blob by its BLAKE3 hash (no decryption).

```typescript
const data = await client.download_blob('abc123...');
```

##### `file_get(path: string): Promise<JsValue | null>`

Get a file's metadata (FileRef) without downloading content.

```typescript
const fileRef = await client.file_get('documents/report.pdf');
if (fileRef) {
  console.log('File size:', fileRef.size);
}
```

##### `file_exists(path: string): Promise<boolean>`

Check if a file exists at the given path.

```typescript
if (await client.file_exists('config.json')) {
  const config = await client.download_file('config.json');
}
```

##### `delete_file(path: string): Promise<void>`

Delete a file at the given path.

```typescript
await client.delete_file('documents/old-file.txt');
```

##### Static Methods

```typescript
S5Client.generate_seed_phrase(): string
S5Client.validate_seed_phrase(phrase: string): boolean
```

Convenience wrappers for the module-level seed phrase functions.

---

### Type Definitions

#### `WasmDerivedKeys`

Keys derived from a seed phrase.

```typescript
interface WasmDerivedKeys {
  readonly root_secret_hex: string;      // 32-byte root secret (hex)
  readonly public_key_hex: string;       // Ed25519 public key (hex) - user identity
  readonly iroh_secret_key_hex: string;  // Iroh node secret key (hex)
}
```

#### `DirectoryListing`

Result of listing a directory.

```typescript
interface DirectoryListing {
  readonly files: object;           // Map of filename -> FileRef
  readonly directories: string[];   // Array of directory names
  readonly file_count: number;
  readonly directory_count: number;
}
```

#### FileRef

Files are represented using the native `s5_fs::FileRef` type, serialized as JSON. Key fields:

- `hash` - BLAKE3 hash of plaintext content (32 bytes)
- `size` - Size in bytes
- `media_type` - MIME type (optional)
- `timestamp` - Unix timestamp in seconds (optional)
- `locations` - Array of BlobLocation for retrieval/decryption

---

## Key Derivation

S5 uses deterministic key derivation from BIP39 seed phrases:

```
seed_phrase (12 words BIP39)
    |
    v
mnemonic.to_seed("") -> 64 bytes
    |
    v
blake3::derive_key("s5/root", seed) -> root_secret [32 bytes]
    |
    +-- blake3::derive_key("s5/fs/root", root_secret) -> fs_root_secret
    |       |
    |       +-- "s5/fs/sync/xchacha20" -> encryption_key
    |       +-- "s5/fs/sync/ed25519" -> signing_key -> public_key
    |
    +-- blake3::derive_key("s5/iroh/node", root_secret) -> iroh_secret_key
```

The `public_key` derived from the signing key serves as the user's identity (stream key) in the S5 network.

---

## Architecture

The WASM client uses a **remote-only** architecture:

```
Browser (s5_wasm)
    |
    +-- DerivedKeys (from seed phrase)
    |       +-- encryption_key (XChaCha20-Poly1305)
    |       +-- signing_key (Ed25519)
    |       +-- public_key (user identity / StreamKey)
    |       +-- iroh_node_key
    |
    +-- FS5 instance
    |       +-- DirContextParentLink::RegistryKey (public_key)
    |       +-- RemoteBlobStore (content storage)
    |       +-- RemoteRegistry (directory metadata)
    |
    +-- Iroh Endpoint (relay-based networking)
            |
            v
      Remote S5 Node (untrusted)
            +-- Encrypted blobs only
            +-- Signed registry entries only
```

The remote node never sees plaintext content or directory structure.

---

## Security Considerations

1. **Seed Phrase Storage**: Never store seed phrases in localStorage in production. Use secure storage mechanisms appropriate for your platform.

2. **Key Derivation**: All cryptographic keys are derived deterministically from the seed phrase. Anyone with the seed phrase has full access to all encrypted data.

3. **End-to-End Encryption**: Files are encrypted client-side before upload. The storage node never sees plaintext content.

4. **Memory Safety**: WASM provides memory isolation. Cryptographic operations happen in the WASM sandbox.

---

## Dependencies

- `s5_client` - Key derivation, crypto primitives
- `s5_core` - Protocol types (`Hash`, `StreamKey`, `BlobStore`)
- `s5_fs` - Filesystem abstraction (`FS5`, `DirContext`, `FileRef`)
- `s5_blobs` - Blob client (`Client`, `RemoteBlobStore`)
- `s5_registry` - Registry client (`RemoteRegistry`)

---

## License

MIT OR Apache-2.0
