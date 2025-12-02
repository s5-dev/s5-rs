#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
// EXTRA BEGIN
typedef struct DartCObject *WireSyncRust2DartDco;
typedef struct WireSyncRust2DartSse {
  uint8_t *ptr;
  int32_t len;
} WireSyncRust2DartSse;

typedef int64_t DartPort;
typedef bool (*DartPostCObjectFnType)(DartPort port_id, void *message);
void store_dart_post_cobject(DartPostCObjectFnType ptr);
// EXTRA END
typedef struct _Dart_Handle* Dart_Handle;

typedef struct wire_cst_list_prim_u_8_strict {
  uint8_t *ptr;
  int32_t len;
} wire_cst_list_prim_u_8_strict;

typedef struct wire_cst_list_prim_u_8_loose {
  uint8_t *ptr;
  int32_t len;
} wire_cst_list_prim_u_8_loose;

typedef struct wire_cst_list_String {
  struct wire_cst_list_prim_u_8_strict **ptr;
  int32_t len;
} wire_cst_list_String;

typedef struct wire_cst_file_entry {
  struct wire_cst_list_prim_u_8_strict *name;
  struct wire_cst_list_prim_u_8_strict *file_ref_json;
  uint64_t size;
  struct wire_cst_list_prim_u_8_strict *media_type;
  uint32_t *timestamp;
} wire_cst_file_entry;

typedef struct wire_cst_list_file_entry {
  struct wire_cst_file_entry *ptr;
  int32_t len;
} wire_cst_list_file_entry;

typedef struct wire_cst_directory_listing {
  struct wire_cst_list_file_entry *files;
  struct wire_cst_list_String *directories;
} wire_cst_directory_listing;

typedef struct wire_cst_S5Error_InvalidInput {
  struct wire_cst_list_prim_u_8_strict *field0;
} wire_cst_S5Error_InvalidInput;

typedef struct wire_cst_S5Error_ConnectionError {
  struct wire_cst_list_prim_u_8_strict *field0;
} wire_cst_S5Error_ConnectionError;

typedef struct wire_cst_S5Error_StorageError {
  struct wire_cst_list_prim_u_8_strict *field0;
} wire_cst_S5Error_StorageError;

typedef struct wire_cst_S5Error_FileNotFound {
  struct wire_cst_list_prim_u_8_strict *field0;
} wire_cst_S5Error_FileNotFound;

typedef struct wire_cst_S5Error_CryptoError {
  struct wire_cst_list_prim_u_8_strict *field0;
} wire_cst_S5Error_CryptoError;

typedef struct wire_cst_S5Error_InternalError {
  struct wire_cst_list_prim_u_8_strict *field0;
} wire_cst_S5Error_InternalError;

typedef union S5ErrorKind {
  struct wire_cst_S5Error_InvalidInput InvalidInput;
  struct wire_cst_S5Error_ConnectionError ConnectionError;
  struct wire_cst_S5Error_StorageError StorageError;
  struct wire_cst_S5Error_FileNotFound FileNotFound;
  struct wire_cst_S5Error_CryptoError CryptoError;
  struct wire_cst_S5Error_InternalError InternalError;
} S5ErrorKind;

typedef struct wire_cst_s_5_error {
  int32_t tag;
  union S5ErrorKind kind;
} wire_cst_s_5_error;

typedef struct wire_cst_s_5_keys {
  struct wire_cst_list_prim_u_8_strict *root_secret_hex;
  struct wire_cst_list_prim_u_8_strict *public_key_hex;
  struct wire_cst_list_prim_u_8_strict *encryption_key_hex;
  struct wire_cst_list_prim_u_8_strict *signing_key_hex;
  struct wire_cst_list_prim_u_8_strict *iroh_secret_key_hex;
} wire_cst_s_5_keys;

void frbgen_s5_flutter_wire__crate__api__S5Client_connect(int64_t port_,
                                                          struct wire_cst_list_prim_u_8_strict *seed_phrase,
                                                          struct wire_cst_list_prim_u_8_strict *remote_node_id);

void frbgen_s5_flutter_wire__crate__api__S5Client_create_directory(int64_t port_,
                                                                   uintptr_t that,
                                                                   struct wire_cst_list_prim_u_8_strict *path);

void frbgen_s5_flutter_wire__crate__api__S5Client_delete_file(int64_t port_,
                                                              uintptr_t that,
                                                              struct wire_cst_list_prim_u_8_strict *path);

void frbgen_s5_flutter_wire__crate__api__S5Client_disconnect(int64_t port_, uintptr_t that);

void frbgen_s5_flutter_wire__crate__api__S5Client_download_blob(int64_t port_,
                                                                uintptr_t that,
                                                                struct wire_cst_list_prim_u_8_strict *hash_hex);

void frbgen_s5_flutter_wire__crate__api__S5Client_download_file(int64_t port_,
                                                                uintptr_t that,
                                                                struct wire_cst_list_prim_u_8_strict *path);

void frbgen_s5_flutter_wire__crate__api__S5Client_file_exists(int64_t port_,
                                                              uintptr_t that,
                                                              struct wire_cst_list_prim_u_8_strict *path);

void frbgen_s5_flutter_wire__crate__api__S5Client_file_get(int64_t port_,
                                                           uintptr_t that,
                                                           struct wire_cst_list_prim_u_8_strict *path);

void frbgen_s5_flutter_wire__crate__api__S5Client_is_connected(int64_t port_, uintptr_t that);

void frbgen_s5_flutter_wire__crate__api__S5Client_list_directory(int64_t port_,
                                                                 uintptr_t that,
                                                                 struct wire_cst_list_prim_u_8_strict *path);

WireSyncRust2DartDco frbgen_s5_flutter_wire__crate__api__S5Client_node_id(uintptr_t that);

WireSyncRust2DartDco frbgen_s5_flutter_wire__crate__api__S5Client_public_key(uintptr_t that);

void frbgen_s5_flutter_wire__crate__api__S5Client_test_connection(int64_t port_, uintptr_t that);

void frbgen_s5_flutter_wire__crate__api__S5Client_upload_file(int64_t port_,
                                                              uintptr_t that,
                                                              struct wire_cst_list_prim_u_8_strict *path,
                                                              struct wire_cst_list_prim_u_8_strict *filename,
                                                              struct wire_cst_list_prim_u_8_loose *content,
                                                              struct wire_cst_list_prim_u_8_strict *media_type);

WireSyncRust2DartDco frbgen_s5_flutter_wire__crate__api__decrypt_chunk_xchacha20poly1305(struct wire_cst_list_prim_u_8_loose *key,
                                                                                         uint64_t chunk_index,
                                                                                         struct wire_cst_list_prim_u_8_loose *ciphertext);

WireSyncRust2DartDco frbgen_s5_flutter_wire__crate__api__decrypt_xchacha20poly1305(struct wire_cst_list_prim_u_8_loose *key,
                                                                                   struct wire_cst_list_prim_u_8_loose *data);

WireSyncRust2DartDco frbgen_s5_flutter_wire__crate__api__derive_keys(struct wire_cst_list_prim_u_8_strict *phrase);

WireSyncRust2DartDco frbgen_s5_flutter_wire__crate__api__encrypt_xchacha20poly1305(struct wire_cst_list_prim_u_8_loose *key,
                                                                                   struct wire_cst_list_prim_u_8_loose *plaintext);

WireSyncRust2DartDco frbgen_s5_flutter_wire__crate__api__generate_seed_phrase(void);

WireSyncRust2DartDco frbgen_s5_flutter_wire__crate__api__hash_blake3(struct wire_cst_list_prim_u_8_loose *data);

WireSyncRust2DartDco frbgen_s5_flutter_wire__crate__api__validate_seed_phrase(struct wire_cst_list_prim_u_8_strict *phrase);

void frbgen_s5_flutter_rust_arc_increment_strong_count_RustOpaque_flutter_rust_bridgefor_generatedRustAutoOpaqueInnerS5Client(const void *ptr);

void frbgen_s5_flutter_rust_arc_decrement_strong_count_RustOpaque_flutter_rust_bridgefor_generatedRustAutoOpaqueInnerS5Client(const void *ptr);

uint32_t *frbgen_s5_flutter_cst_new_box_autoadd_u_32(uint32_t value);

struct wire_cst_list_String *frbgen_s5_flutter_cst_new_list_String(int32_t len);

struct wire_cst_list_file_entry *frbgen_s5_flutter_cst_new_list_file_entry(int32_t len);

struct wire_cst_list_prim_u_8_loose *frbgen_s5_flutter_cst_new_list_prim_u_8_loose(int32_t len);

struct wire_cst_list_prim_u_8_strict *frbgen_s5_flutter_cst_new_list_prim_u_8_strict(int32_t len);
static int64_t dummy_method_to_enforce_bundling(void) {
    int64_t dummy_var = 0;
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_cst_new_box_autoadd_u_32);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_cst_new_list_String);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_cst_new_list_file_entry);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_cst_new_list_prim_u_8_loose);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_cst_new_list_prim_u_8_strict);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_rust_arc_decrement_strong_count_RustOpaque_flutter_rust_bridgefor_generatedRustAutoOpaqueInnerS5Client);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_rust_arc_increment_strong_count_RustOpaque_flutter_rust_bridgefor_generatedRustAutoOpaqueInnerS5Client);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__S5Client_connect);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__S5Client_create_directory);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__S5Client_delete_file);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__S5Client_disconnect);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__S5Client_download_blob);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__S5Client_download_file);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__S5Client_file_exists);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__S5Client_file_get);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__S5Client_is_connected);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__S5Client_list_directory);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__S5Client_node_id);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__S5Client_public_key);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__S5Client_test_connection);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__S5Client_upload_file);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__decrypt_chunk_xchacha20poly1305);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__decrypt_xchacha20poly1305);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__derive_keys);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__encrypt_xchacha20poly1305);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__generate_seed_phrase);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__hash_blake3);
    dummy_var ^= ((int64_t) (void*) frbgen_s5_flutter_wire__crate__api__validate_seed_phrase);
    dummy_var ^= ((int64_t) (void*) store_dart_post_cobject);
    return dummy_var;
}
