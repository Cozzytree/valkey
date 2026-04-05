#include "cstore.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ─── entry ──────────────────────────────────────────────────────────────── */

typedef struct {
  char *key;
  size_t key_len;
  char *val;
  size_t val_len;
  int64_t expire_ns; /* 0 = no expiry */
  uint64_t hash;
  int occupied; /* 1 = live, 0 = empty/tombstone */
} Entry;

/* ─── store ──────────────────────────────────────────────────────────────── */

struct CStore {
  Entry *buckets;
  size_t cap;      /* always power of 2 */
  size_t count;    /* live entries */
  size_t scan_pos; /* cursor for expire_n round-robin */
};

#define INITIAL_CAP 64
#define LOAD_FACTOR 0.75

/* ─── FNV-1a hash ────────────────────────────────────────────────────────── */

static uint64_t fnv1a(const char *data, size_t len) {
  uint64_t h = 14695981039346656037ULL;
  for (size_t i = 0; i < len; i++) {
    h ^= (uint8_t)data[i];
    h *= 1099511628211ULL;
  }
  return h;
}

/* ─── time ───────────────────────────────────────────────────────────────── */

static int64_t now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (int64_t)ts.tv_sec * 1000000000LL + (int64_t)ts.tv_nsec;
}

static int is_expired(const Entry *e, int64_t now) {
  return e->expire_ns != 0 && e->expire_ns <= now;
}

/* ─── internal helpers ───────────────────────────────────────────────────── */

static void entry_free(Entry *e) {
  free(e->key);
  free(e->val);
  e->key = NULL;
  e->val = NULL;
  e->key_len = 0;
  e->val_len = 0;
  e->expire_ns = 0;
  e->hash = 0;
  e->occupied = 0;
}

/* Find the bucket for key. Returns pointer to bucket, or NULL if not found. */
static Entry *find(CStore *s, const char *key, size_t key_len, uint64_t h) {
  size_t mask = s->cap - 1;
  size_t idx = h & mask;
  for (size_t i = 0; i < s->cap; i++) {
    Entry *e = &s->buckets[idx];
    if (!e->occupied)
      return NULL;
    if (e->hash == h && e->key_len == key_len &&
        memcmp(e->key, key, key_len) == 0) {
      return e;
    }
    idx = (idx + 1) & mask;
  }
  return NULL;
}

static void insert_entry(CStore *s, char *key, size_t key_len, char *val,
                         size_t val_len, int64_t expire_ns, uint64_t h);

static void grow(CStore *s) {
  size_t old_cap = s->cap;
  Entry *old = s->buckets;

  s->cap *= 2;
  s->buckets = calloc(s->cap, sizeof(Entry));
  s->count = 0;

  for (size_t i = 0; i < old_cap; i++) {
    if (old[i].occupied) {
      /* Transfer ownership — don't copy, just move pointers. */
      insert_entry(s, old[i].key, old[i].key_len, old[i].val, old[i].val_len,
                   old[i].expire_ns, old[i].hash);
    }
  }
  free(old);
}

static void insert_entry(CStore *s, char *key, size_t key_len, char *val,
                         size_t val_len, int64_t expire_ns, uint64_t h) {
  size_t mask = s->cap - 1;
  size_t idx = h & mask;
  for (;;) {
    Entry *e = &s->buckets[idx];
    if (!e->occupied) {
      e->key = key;
      e->key_len = key_len;
      e->val = val;
      e->val_len = val_len;
      e->expire_ns = expire_ns;
      e->hash = h;
      e->occupied = 1;
      s->count++;
      return;
    }
    idx = (idx + 1) & mask;
  }
}

/* ─── public API ─────────────────────────────────────────────────────────── */

CStore *cstore_new(void) {
  CStore *s = calloc(1, sizeof(CStore));
  if (!s)
    return NULL;
  s->cap = INITIAL_CAP;
  s->buckets = calloc(s->cap, sizeof(Entry));
  if (!s->buckets) {
    free(s);
    return NULL;
  }
  return s;
}

void cstore_free(CStore *s) {
  if (!s)
    return;
  for (size_t i = 0; i < s->cap; i++) {
    if (s->buckets[i].occupied) {
      free(s->buckets[i].key);
      free(s->buckets[i].val);
    }
  }
  free(s->buckets);
  free(s);
}

void cstore_set(CStore *s, const char *key, size_t key_len, const char *val,
                size_t val_len) {
  cstore_set_ttl(s, key, key_len, val, val_len, 0);
}

void cstore_set_ttl(CStore *s, const char *key, size_t key_len, const char *val,
                    size_t val_len, int64_t expire_ns) {
  uint64_t h = fnv1a(key, key_len);
  Entry *e = find(s, key, key_len, h);
  if (e) {
    /* Overwrite existing. */
    free(e->val);
    e->val = malloc(val_len);
    if (val_len > 0)
      memcpy(e->val, val, val_len);
    e->val_len = val_len;
    e->expire_ns = expire_ns;
    return;
  }

  /* New entry — check load factor first. */
  if ((double)(s->count + 1) / (double)s->cap > LOAD_FACTOR) {
    grow(s);
  }

  char *k = malloc(key_len);
  if (key_len > 0)
    memcpy(k, key, key_len);
  char *v = malloc(val_len);
  if (val_len > 0)
    memcpy(v, val, val_len);

  insert_entry(s, k, key_len, v, val_len, expire_ns, h);
}

int cstore_get(CStore *s, const char *key, size_t key_len, char *buf,
               size_t buf_len, size_t *out_len) {
  uint64_t h = fnv1a(key, key_len);
  Entry *e = find(s, key, key_len, h);
  if (!e)
    return 0;

  /* Lazy expiration. */
  if (is_expired(e, now_ns())) {
    entry_free(e);
    s->count--;
    return 0;
  }

  if (buf_len < e->val_len) {
    *out_len = e->val_len;
    return -1;
  }
  memcpy(buf, e->val, e->val_len);
  *out_len = e->val_len;
  return 1;
}

int cstore_del(CStore *s, const char *key, size_t key_len) {
  uint64_t h = fnv1a(key, key_len);
  Entry *e = find(s, key, key_len, h);
  if (!e)
    return 0;
  entry_free(e);
  s->count--;
  /* Rehash the cluster after the deleted slot to fix probe chains. */
  size_t mask = s->cap - 1;
  size_t idx = (h & mask);
  idx = (idx + 1) & mask;
  while (s->buckets[idx].occupied) {
    Entry tmp = s->buckets[idx];
    s->buckets[idx].occupied = 0;
    s->count--;
    insert_entry(s, tmp.key, tmp.key_len, tmp.val, tmp.val_len, tmp.expire_ns,
                 tmp.hash);
    idx = (idx + 1) & mask;
  }
  return 1;
}

size_t cstore_len(CStore *s) { return s->count; }

int cstore_expire(CStore *s, const char *key, size_t key_len,
                  int64_t expire_ns) {
  uint64_t h = fnv1a(key, key_len);
  Entry *e = find(s, key, key_len, h);
  if (!e)
    return 0;
  if (is_expired(e, now_ns())) {
    entry_free(e);
    s->count--;
    return 0;
  }
  e->expire_ns = expire_ns;
  return 1;
}

int64_t cstore_ttl(CStore *s, const char *key, size_t key_len) {
  uint64_t h = fnv1a(key, key_len);
  Entry *e = find(s, key, key_len, h);
  if (!e)
    return -2;
  if (e->expire_ns == 0)
    return -1;
  int64_t remaining = e->expire_ns - now_ns();
  if (remaining <= 0)
    return -2; /* expired */
  return remaining;
}

int cstore_persist(CStore *s, const char *key, size_t key_len) {
  uint64_t h = fnv1a(key, key_len);
  Entry *e = find(s, key, key_len, h);
  if (!e)
    return 0;
  if (e->expire_ns == 0)
    return 0;
  e->expire_ns = 0;
  return 1;
}

int cstore_expire_n(CStore *s, int n) {
  if (s->count == 0)
    return 0;
  int64_t now = now_ns();
  int deleted = 0;
  int scanned = 0;
  size_t mask = s->cap - 1;

  while (scanned < n && scanned < (int)s->cap) {
    Entry *e = &s->buckets[s->scan_pos];
    s->scan_pos = (s->scan_pos + 1) & mask;

    if (!e->occupied) {
      scanned++;
      continue;
    }
    if (e->expire_ns == 0) {
      scanned++;
      continue;
    }

    if (is_expired(e, now)) {
      entry_free(e);
      s->count--;
      deleted++;
    }
    scanned++;
  }
  return deleted;
}
