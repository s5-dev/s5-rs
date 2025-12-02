# s5_flutter

Flutter bindings for S5 - encrypted storage on the decentralized web.

This package provides Dart/Flutter bindings to the S5 Rust client via flutter_rust_bridge, enabling:

- BIP39 seed phrase generation and validation
- Deterministic key derivation for E2EE storage
- Connection to remote S5 nodes via iroh relay
- Encrypted file upload/download
- Encrypted filesystem directory management

## Installation

```yaml
dependencies:
  s5_flutter: ^1.0.0-beta.1
```

## Quick Start

```dart
import 'package:s5_flutter/s5_flutter.dart';

// Initialize the Rust library
await RustLib.init();

// Generate a new seed phrase
final seedPhrase = generateSeedPhrase();

// Connect to a remote S5 node
final client = await S5Client.connect(
  seedPhrase: seedPhrase,
  remoteNodeId: 'your-node-id...',
);

// Create a directory
await client.createDirectory(path: 'documents');

// Upload a file (automatically encrypted)
final content = utf8.encode('Hello, S5!');
await client.uploadFile(
  path: 'documents',
  filename: 'hello.txt',
  content: content,
  mediaType: 'text/plain',
);

// List directory contents
final listing = await client.listDirectory(path: 'documents');
print('Files: ${listing.files.length}');
print('Directories: ${listing.directories.length}');

// Download a file (automatically decrypted)
final data = await client.downloadFile(path: 'documents/hello.txt');
print(utf8.decode(data));

// Disconnect when done
await client.disconnect();
```

## API Reference

### Seed Phrase Functions

```dart
// Generate a new 12-word BIP39 seed phrase
String generateSeedPhrase();

// Validate a seed phrase
bool validateSeedPhrase({required String phrase});

// Derive all keys from a seed phrase
S5Keys deriveKeys({required String phrase});
```

### Crypto Functions

```dart
// BLAKE3 hash
Uint8List hashBlake3({required List<int> data});

// XChaCha20-Poly1305 encryption/decryption
Uint8List encryptXchacha20Poly1305({required List<int> key, required List<int> plaintext});
Uint8List decryptXchacha20Poly1305({required List<int> key, required List<int> data});
```

### S5Client

```dart
// Connect to a remote node
static Future<S5Client> connect({
  required String seedPhrase,
  required String remoteNodeId,
});

// Properties
String get publicKey;  // User identity (hex)
String get nodeId;     // Iroh node ID

// Directory operations
Future<void> createDirectory({required String path});
Future<DirectoryListing> listDirectory({required String path});

// File operations
Future<String> uploadFile({
  required String path,
  required String filename,
  required List<int> content,
  required String mediaType,
});
Future<Uint8List> downloadFile({required String path});
Future<Uint8List> downloadBlob({required String hashHex});
Future<void> deleteFile({required String path});
Future<bool> fileExists({required String path});
Future<String?> fileGet({required String path});

// Connection management
Future<bool> isConnected();
Future<String> testConnection();
Future<void> disconnect();
```

## Architecture

The client operates against a remote S5 node:

- All blobs are stored **encrypted** on the remote node
- Directory metadata is stored in the remote registry
- The remote node only sees encrypted data and signed messages
- All cryptographic operations happen client-side

```
Flutter App
    |
    +-- S5Client
    |       +-- DerivedKeys (from seed phrase)
    |       +-- FS5 (encrypted filesystem)
    |       +-- Iroh Endpoint (relay networking)
    |
    v
Remote S5 Node (untrusted)
    +-- Encrypted blobs only
    +-- Signed registry entries only
```

## Platform Support

- Android
- iOS
- Linux
- macOS
- Windows

## License

MIT OR Apache-2.0
