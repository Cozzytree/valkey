// Package store provides the in-memory key-value store used by Valkey.
// All exported methods are safe for concurrent use.
package store

import "sync"

// Store is a thread-safe in-memory key-value map.
// Keys are strings; values are raw byte slices so callers decide encoding.
type Store struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// New allocates an empty Store.
func New() *Store {
	return &Store{data: make(map[string][]byte)}
}

// Set stores value under key, overwriting any previous value.
func (s *Store) Set(key string, value []byte) {
	s.mu.Lock()
	s.data[key] = value
	s.mu.Unlock()
}

// Get returns the value for key and whether the key exists.
func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	v, ok := s.data[key]
	s.mu.RUnlock()
	return v, ok
}

// Del deletes key and returns whether it existed.
func (s *Store) Del(key string) bool {
	s.mu.Lock()
	_, ok := s.data[key]
	delete(s.data, key)
	s.mu.Unlock()
	return ok
}

// Len returns the number of keys in the store.
func (s *Store) Len() int {
	s.mu.RLock()
	n := len(s.data)
	s.mu.RUnlock()
	return n
}
