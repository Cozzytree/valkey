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

	// Len returns the number of keys currently in the store.
	Len() int

	// Close releases any resources held by the backend (memory, file handles,
	// CGO-allocated structures, …). The store must not be used after Close.
	Close() error
}
