#ifndef VALKEY_ZIG_STORE_H
#define VALKEY_ZIG_STORE_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle – Zig owns the memory, Go holds the pointer. */
typedef struct ValkeyZigStore ValkeyZigStore;

/* Allocate a new store. Returns NULL on out-of-memory. */
ValkeyZigStore* store_new(void);

/* Free the store and all keys/values it owns. */
void store_free(ValkeyZigStore* store);

/*
 * store_set – insert or overwrite key → value.
 * Zig copies both key and value; the caller retains its slices.
 */
void store_set(ValkeyZigStore* store,
               const char* key, size_t key_len,
               const char* val, size_t val_len);

/*
 * store_get – copy the value for key into the caller-supplied buffer.
 *
 * Return values:
 *   1   hit – bytes written into buf; *out_len = number of bytes written.
 *   0   miss – key not found; *out_len is not modified.
 *  -1   buffer too small (or buf == NULL) – *out_len is set to the
 *       required size so the caller can retry with a larger buffer.
 *
 * Go owns buf; Zig never frees it.
 */
int store_get(ValkeyZigStore* store,
              const char* key, size_t key_len,
              char* buf,       size_t buf_len,
              size_t* out_len);

/*
 * store_del – remove key.
 * Returns 1 if the key existed, 0 if it was absent.
 */
int store_del(ValkeyZigStore* store,
              const char* key, size_t key_len);

/* store_len – number of keys currently in the store. */
size_t store_len(ValkeyZigStore* store);

#ifdef __cplusplus
}
#endif

#endif /* VALKEY_ZIG_STORE_H */
