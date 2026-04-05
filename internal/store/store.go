// Package store defines the storage abstraction used by the Valkey server.
//
// Any backend (pure-Go, Zig via CGO, external, …) satisfies Store and can be
// plugged into the server without changing a single line of protocol or
// connection-handling code.
package store

import "time"

// Store is the interface every storage backend must implement.
// All methods must be safe for concurrent use by multiple goroutines.
type Store interface {
	// Set stores value under key, overwriting any existing value.
	// A plain Set clears any existing TTL on the key.
	Set(key string, value []byte)

	// SetWithTTL stores value under key with an expiration duration.
	// A zero or negative ttl means no expiration (equivalent to Set).
	SetWithTTL(key string, value []byte, ttl time.Duration)

	// Get returns the value for key and true if the key exists,
	// or nil and false if the key is absent or expired.
	Get(key string) ([]byte, bool)

	// Del removes key and returns true if it existed.
	Del(key string) bool

	// Expire sets a TTL on an existing key. Returns false if the key does not exist.
	Expire(key string, ttl time.Duration) bool

	// TTL returns the remaining time-to-live for key.
	// Returns (-2, false) if the key does not exist.
	// Returns (-1, true) if the key exists but has no TTL.
	// Returns (remaining, true) otherwise.
	TTL(key string) (time.Duration, bool)

	// Persist removes the TTL from key, making it persistent.
	// Returns false if the key does not exist or has no TTL.
	Persist(key string) bool

	// ExpireN performs one active-expiration cycle, sampling up to n keys
	// from those with TTLs, deleting any that have expired.
	// Returns the number of keys deleted.
	ExpireN(n int) int

	// HSet sets field/value pairs on a hash key. Fields are passed as
	// alternating field, value strings. Returns the number of new fields added.
	// Returns ErrWrongType if the key exists and is not a hash.
	HSet(key string, fields ...string) (int, error)

	// HGet returns the value of a field in a hash.
	// Returns ErrWrongType if the key exists and is not a hash.
	HGet(key, field string) ([]byte, bool, error)

	// HDel removes fields from a hash. Returns the number of fields deleted.
	// Returns ErrWrongType if the key exists and is not a hash.
	HDel(key string, fields ...string) (int, error)

	// HGetAll returns all field/value pairs in a hash.
	// Returns ErrWrongType if the key exists and is not a hash.
	HGetAll(key string) (map[string][]byte, error)

	// HLen returns the number of fields in a hash.
	// Returns ErrWrongType if the key exists and is not a hash.
	HLen(key string) (int, error)

	// HExists returns whether a field exists in a hash.
	// Returns ErrWrongType if the key exists and is not a hash.
	HExists(key, field string) (bool, error)

	// HKeys returns all field names in a hash.
	// Returns ErrWrongType if the key exists and is not a hash.
	HKeys(key string) ([]string, error)

	// HVals returns all values in a hash.
	// Returns ErrWrongType if the key exists and is not a hash.
	HVals(key string) ([][]byte, error)

	// JSONSet stores a JSON value at path under key. Path "$" sets the root.
	// Returns ErrWrongType if the key exists and is not a JSON type.
	JSONSet(key, path string, value any) error

	// JSONGet returns the JSON value at path under key.
	// Returns (nil, false, nil) if the key doesn't exist.
	// Returns ErrWrongType if the key exists and is not a JSON type.
	JSONGet(key, path string) (any, bool, error)

	// JSONDel deletes the value at path. If path is "$", deletes the whole key.
	// Returns the number of paths deleted.
	// Returns ErrWrongType if the key exists and is not a JSON type.
	JSONDel(key, path string) (int, error)

	// JSONType returns the JSON type name at path ("object","array","string","number","boolean","null").
	// Returns ErrWrongType if the key exists and is not a JSON type.
	JSONType(key, path string) (string, error)

	// JSONNumIncrBy increments the number at path by n and returns the new value.
	// Returns ErrNotANumber if the value at path is not a number.
	// Returns ErrWrongType if the key exists and is not a JSON type.
	JSONNumIncrBy(key, path string, n float64) (float64, error)

	// Len returns the number of keys currently in the store.
	Len() int

	// Close releases any resources held by the backend (memory, file handles,
	// CGO-allocated structures, …). The store must not be used after Close.
	Close() error
}
