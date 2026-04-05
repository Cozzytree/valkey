package store

import (
	"sync"
	"time"
)

// GoStore is the pure-Go, in-memory Store implementation.
// It is the default backend used by the server when CGO is unavailable or
// the Zig backend has not been selected.
type GoStore struct {
	mu      sync.RWMutex
	data    map[string][]byte
	expires map[string]time.Time // only keys with a TTL appear here
}

// compile-time assertion that GoStore satisfies Store.
var _ Store = (*GoStore)(nil)

// NewGoStore allocates an empty GoStore.
func NewGoStore() *GoStore {
	return &GoStore{
		data:    make(map[string][]byte),
		expires: make(map[string]time.Time),
	}
}

func (s *GoStore) Set(key string, value []byte) {
	s.mu.Lock()
	s.data[key] = value
	delete(s.expires, key) // plain Set clears any existing TTL
	s.mu.Unlock()
}

func (s *GoStore) SetWithTTL(key string, value []byte, ttl time.Duration) {
	if ttl <= 0 {
		s.Set(key, value)
		return
	}
	s.mu.Lock()
	s.data[key] = value
	s.expires[key] = time.Now().Add(ttl)
	s.mu.Unlock()
}

func (s *GoStore) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	deadline, hasExp := s.expires[key]
	if hasExp && time.Now().After(deadline) {
		// Key is expired — upgrade to write lock and delete.
		s.mu.RUnlock()
		s.mu.Lock()
		// Re-check under write lock (another goroutine may have already deleted).
		if d, ok := s.expires[key]; ok && time.Now().After(d) {
			delete(s.data, key)
			delete(s.expires, key)
		}
		s.mu.Unlock()
		return nil, false
	}
	v, ok := s.data[key]
	s.mu.RUnlock()
	return v, ok
}

func (s *GoStore) Del(key string) bool {
	s.mu.Lock()
	_, ok := s.data[key]
	delete(s.data, key)
	delete(s.expires, key)
	s.mu.Unlock()
	return ok
}

func (s *GoStore) Expire(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		return false
	}
	// Check if already expired via lazy path.
	if d, hasExp := s.expires[key]; hasExp && time.Now().After(d) {
		delete(s.data, key)
		delete(s.expires, key)
		return false
	}
	s.expires[key] = time.Now().Add(ttl)
	return true
}

func (s *GoStore) TTL(key string) (time.Duration, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.data[key]; !ok {
		return -2, false
	}
	deadline, hasExp := s.expires[key]
	if !hasExp {
		return -1, true
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return -2, false
	}
	return remaining, true
}

func (s *GoStore) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[key]; !ok {
		return false
	}
	if _, hasExp := s.expires[key]; !hasExp {
		return false
	}
	delete(s.expires, key)
	return true
}

// ExpireN samples up to n keys from the expiry map and deletes any that have
// expired. Go's random map iteration order provides the sampling randomness.
func (s *GoStore) ExpireN(n int) int {
	now := time.Now()

	// Collect a sample under read lock.
	s.mu.RLock()
	type sample struct {
		key      string
		deadline time.Time
	}
	samples := make([]sample, 0, n)
	for k, d := range s.expires {
		if len(samples) >= n {
			break
		}
		samples = append(samples, sample{k, d})
	}
	s.mu.RUnlock()

	// Find which are expired.
	var expired []string
	for _, sm := range samples {
		if now.After(sm.deadline) {
			expired = append(expired, sm.key)
		}
	}
	if len(expired) == 0 {
		return 0
	}

	// Delete under write lock, re-checking deadlines.
	s.mu.Lock()
	deleted := 0
	for _, k := range expired {
		if d, ok := s.expires[k]; ok && now.After(d) {
			delete(s.data, k)
			delete(s.expires, k)
			deleted++
		}
	}
	s.mu.Unlock()
	return deleted
}

func (s *GoStore) Len() int {
	s.mu.RLock()
	n := len(s.data)
	s.mu.RUnlock()
	return n
}

func (s *GoStore) Close() error { return nil }
