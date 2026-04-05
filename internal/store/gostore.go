package store

import (
	"sync"
	"time"
)

// GoStore is the pure-Go, in-memory Store implementation.
// It is the default backend used by the server when CGO is unavailable or
// the Zig backend has not been selected.
type GoStore struct {
	mu   sync.RWMutex
	data map[string]*Entry
}

// compile-time assertion that GoStore satisfies Store.
var _ Store = (*GoStore)(nil)

// NewGoStore allocates an empty GoStore.
func NewGoStore() *GoStore {
	return &GoStore{
		data: make(map[string]*Entry),
	}
}

// ─── internal helpers ────────────────────────────────────────────────────────

// getEntry returns the entry for key if it exists and is not expired.
// If expired, it deletes the entry and returns nil.
// Caller must hold at least a read lock. If the entry is expired, the caller
// must hold a write lock (or upgrade).
func (s *GoStore) getEntryLocked(key string) *Entry {
	e, ok := s.data[key]
	if !ok {
		return nil
	}
	if !e.ExpireAt.IsZero() && time.Now().After(e.ExpireAt) {
		delete(s.data, key)
		return nil
	}
	return e
}

// ─── string operations ───────────────────────────────────────────────────────

func (s *GoStore) Set(key string, value []byte) {
	s.mu.Lock()
	s.data[key] = &Entry{Type: TypeString, Str: value}
	s.mu.Unlock()
}

func (s *GoStore) SetWithTTL(key string, value []byte, ttl time.Duration) {
	if ttl <= 0 {
		s.Set(key, value)
		return
	}
	s.mu.Lock()
	s.data[key] = &Entry{Type: TypeString, Str: value, ExpireAt: time.Now().Add(ttl)}
	s.mu.Unlock()
}

func (s *GoStore) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	e, ok := s.data[key]
	if !ok {
		s.mu.RUnlock()
		return nil, false
	}
	if !e.ExpireAt.IsZero() && time.Now().After(e.ExpireAt) {
		s.mu.RUnlock()
		// Upgrade to write lock for lazy deletion.
		s.mu.Lock()
		if e2, ok := s.data[key]; ok && !e2.ExpireAt.IsZero() && time.Now().After(e2.ExpireAt) {
			delete(s.data, key)
		}
		s.mu.Unlock()
		return nil, false
	}
	if e.Type != TypeString {
		s.mu.RUnlock()
		return nil, false
	}
	v := e.Str
	s.mu.RUnlock()
	return v, true
}

func (s *GoStore) Del(key string) bool {
	s.mu.Lock()
	_, ok := s.data[key]
	delete(s.data, key)
	s.mu.Unlock()
	return ok
}

// ─── TTL operations ──────────────────────────────────────────────────────────

func (s *GoStore) Expire(key string, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.getEntryLocked(key)
	if e == nil {
		return false
	}
	e.ExpireAt = time.Now().Add(ttl)
	return true
}

func (s *GoStore) TTL(key string) (time.Duration, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.data[key]
	if !ok {
		return -2, false
	}
	if e.ExpireAt.IsZero() {
		return -1, true
	}
	remaining := time.Until(e.ExpireAt)
	if remaining <= 0 {
		return -2, false
	}
	return remaining, true
}

func (s *GoStore) Persist(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.getEntryLocked(key)
	if e == nil {
		return false
	}
	if e.ExpireAt.IsZero() {
		return false
	}
	e.ExpireAt = time.Time{}
	return true
}

// ExpireN samples up to n keys and deletes any that have expired.
func (s *GoStore) ExpireN(n int) int {
	now := time.Now()

	s.mu.RLock()
	type sample struct {
		key      string
		expireAt time.Time
	}
	samples := make([]sample, 0, n)
	for k, e := range s.data {
		if len(samples) >= n {
			break
		}
		if !e.ExpireAt.IsZero() {
			samples = append(samples, sample{k, e.ExpireAt})
		}
	}
	s.mu.RUnlock()

	var expired []string
	for _, sm := range samples {
		if now.After(sm.expireAt) {
			expired = append(expired, sm.key)
		}
	}
	if len(expired) == 0 {
		return 0
	}

	s.mu.Lock()
	deleted := 0
	for _, k := range expired {
		if e, ok := s.data[k]; ok && !e.ExpireAt.IsZero() && now.After(e.ExpireAt) {
			delete(s.data, k)
			deleted++
		}
	}
	s.mu.Unlock()
	return deleted
}

// ─── hash operations ─────────────────────────────────────────────────────────

func (s *GoStore) HSet(key string, fields ...string) (int, error) {
	if len(fields)%2 != 0 {
		return 0, ErrWrongType // odd number of args is a usage error
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.getEntryLocked(key)
	if e == nil {
		e = &Entry{Type: TypeHash, Hash: make(map[string][]byte)}
		s.data[key] = e
	}
	if e.Type != TypeHash {
		return 0, ErrWrongType
	}

	added := 0
	for i := 0; i < len(fields); i += 2 {
		field := fields[i]
		val := []byte(fields[i+1])
		if _, exists := e.Hash[field]; !exists {
			added++
		}
		e.Hash[field] = val
	}
	return added, nil
}

func (s *GoStore) HGet(key, field string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.getEntryLocked(key)
	if e == nil {
		return nil, false, nil
	}
	if e.Type != TypeHash {
		return nil, false, ErrWrongType
	}
	v, ok := e.Hash[field]
	return v, ok, nil
}

func (s *GoStore) HDel(key string, fields ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.getEntryLocked(key)
	if e == nil {
		return 0, nil
	}
	if e.Type != TypeHash {
		return 0, ErrWrongType
	}

	deleted := 0
	for _, f := range fields {
		if _, ok := e.Hash[f]; ok {
			delete(e.Hash, f)
			deleted++
		}
	}
	// Remove key entirely if hash is empty.
	if len(e.Hash) == 0 {
		delete(s.data, key)
	}
	return deleted, nil
}

func (s *GoStore) HGetAll(key string) (map[string][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.getEntryLocked(key)
	if e == nil {
		return nil, nil
	}
	if e.Type != TypeHash {
		return nil, ErrWrongType
	}
	// Return a copy to avoid races.
	result := make(map[string][]byte, len(e.Hash))
	for k, v := range e.Hash {
		result[k] = v
	}
	return result, nil
}

func (s *GoStore) HLen(key string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.getEntryLocked(key)
	if e == nil {
		return 0, nil
	}
	if e.Type != TypeHash {
		return 0, ErrWrongType
	}
	return len(e.Hash), nil
}

func (s *GoStore) HExists(key, field string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.getEntryLocked(key)
	if e == nil {
		return false, nil
	}
	if e.Type != TypeHash {
		return false, ErrWrongType
	}
	_, ok := e.Hash[field]
	return ok, nil
}

func (s *GoStore) HKeys(key string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.getEntryLocked(key)
	if e == nil {
		return nil, nil
	}
	if e.Type != TypeHash {
		return nil, ErrWrongType
	}
	keys := make([]string, 0, len(e.Hash))
	for k := range e.Hash {
		keys = append(keys, k)
	}
	return keys, nil
}

func (s *GoStore) HVals(key string) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.getEntryLocked(key)
	if e == nil {
		return nil, nil
	}
	if e.Type != TypeHash {
		return nil, ErrWrongType
	}
	vals := make([][]byte, 0, len(e.Hash))
	for _, v := range e.Hash {
		vals = append(vals, v)
	}
	return vals, nil
}

// ─── general ─────────────────────────────────────────────────────────────────

func (s *GoStore) Len() int {
	s.mu.RLock()
	n := len(s.data)
	s.mu.RUnlock()
	return n
}

func (s *GoStore) Close() error { return nil }
