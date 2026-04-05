package store

import "sync"

// GoStore is the pure-Go, in-memory Store implementation.
// It is the default backend used by the server when CGO is unavailable or
// the Zig backend has not been selected.
type GoStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// compile-time assertion that GoStore satisfies Store.
var _ Store = (*GoStore)(nil)

// NewGoStore allocates an empty GoStore.
func NewGoStore() *GoStore {
	return &GoStore{data: make(map[string][]byte)}
}

func (s *GoStore) Set(key string, value []byte) {
	s.mu.Lock()
	s.data[key] = value
	s.mu.Unlock()
}

func (s *GoStore) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	v, ok := s.data[key]
	s.mu.RUnlock()
	return v, ok
}

func (s *GoStore) Del(key string) bool {
	s.mu.Lock()
	_, ok := s.data[key]
	delete(s.data, key)
	s.mu.Unlock()
	return ok
}

func (s *GoStore) Len() int {
	s.mu.RLock()
	n := len(s.data)
	s.mu.RUnlock()
	return n
}

func (s *GoStore) Close() error { return nil }
