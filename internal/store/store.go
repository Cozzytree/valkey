// Package store defines the storage abstraction used by the Valkey server.
//
// Any backend (pure-Go, Zig via CGO, external, …) satisfies Store and can be
// plugged into the server without changing a single line of protocol or
// connection-handling code.
package store

// Store is the interface every storage backend must implement.
// All methods must be safe for concurrent use by multiple goroutines.
type Store interface {
	// Set stores value under key, overwriting any existing value.
	Set(key string, value []byte)

	// Get returns the value for key and true if the key exists,
	// or nil and false if the key is absent.
	Get(key string) ([]byte, bool)

	// Del removes key and returns true if it existed.
	Del(key string) bool

	// Len returns the number of keys currently in the store.
	Len() int

	// Close releases any resources held by the backend (memory, file handles,
	// CGO-allocated structures, …). The store must not be used after Close.
	Close() error
}
