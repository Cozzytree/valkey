#ifndef VALKEY_CSTORE_H
#define VALKEY_CSTORE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle — C owns the memory, Go holds the pointer. */
typedef struct CStore CStore;

/* Allocate a new store. Returns NULL on OOM. */
CStore *cstore_new(void);

/* Free the store and all keys/values it owns. */
void cstore_free(CStore *s);

/* Insert or overwrite key -> value. C copies both. */
void cstore_set(CStore *s, const char *key, size_t key_len, const char *val,
                size_t val_len);

/*
 * Insert or overwrite key -> value with a TTL.
 * expire_ns: absolute expiry time in nanoseconds since Unix epoch.
 *            0 means no expiry.
 */
void cstore_set_ttl(CStore *s, const char *key, size_t key_len, const char *val,
                    size_t val_len, int64_t expire_ns);

/*
 * Retrieve value for key.
 * Returns:
 *   1  hit  — value copied into buf, *out_len = bytes written.
 *   0  miss — key not found or expired.
 *  -1  buf too small — *out_len = required size, caller retries.
 */
int cstore_get(CStore *s, const char *key, size_t key_len, char *buf,
               size_t buf_len, size_t *out_len);

/* Delete key. Returns 1 if existed, 0 if absent. */
int cstore_del(CStore *s, const char *key, size_t key_len);

/* Number of live (non-expired, non-deleted) entries. */
size_t cstore_len(CStore *s);

/*
 * Set expiry on existing key.
 * expire_ns: absolute time in ns since epoch. 0 removes expiry.
 * Returns 1 if key exists, 0 if not.
 */
int cstore_expire(CStore *s, const char *key, size_t key_len,
                  int64_t expire_ns);

/*
 * Get remaining TTL for key in nanoseconds.
 * Returns:
 *  -2  key does not exist
 *  -1  key exists but has no TTL
 *  >0  remaining nanoseconds
 */
int64_t cstore_ttl(CStore *s, const char *key, size_t key_len);

/* Remove TTL from key. Returns 1 if TTL was removed, 0 otherwise. */
int cstore_persist(CStore *s, const char *key, size_t key_len);

/*
 * Active expiration: scan up to n entries, delete expired ones.
 * Returns number deleted.
 */
int cstore_expire_n(CStore *s, int n);

#ifdef __cplusplus
}
#endif

#endif /* VALKEY_CSTORE_H */
