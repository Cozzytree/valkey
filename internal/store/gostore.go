package store

import (
	"bytes"
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

// ─── list operations ────────────────────────────────────────────────────────

// getOrCreateList retrieves an existing list entry or creates a new one.
// Returns ErrWrongType if the key holds a non-list value.
// Caller must hold a write lock.
func (s *GoStore) getOrCreateList(key string) (*Entry, error) {
	entry := s.getEntryLocked(key)
	if entry == nil {
		entry = &Entry{Type: TypeList, List: make([][]byte, 0)}
		s.data[key] = entry
		return entry, nil
	}
	if entry.Type != TypeList {
		return nil, ErrWrongType
	}
	return entry, nil
}

// requireList retrieves an existing list entry.
// Returns (nil, nil) if the key does not exist.
// Returns ErrWrongType if the key holds a non-list value.
// Caller must hold at least a read lock.
func (s *GoStore) requireList(key string) (*Entry, error) {
	entry := s.getEntryLocked(key)
	if entry == nil {
		return nil, nil
	}
	if entry.Type != TypeList {
		return nil, ErrWrongType
	}
	return entry, nil
}

// removeIfEmpty deletes the key from the store when its list is empty.
// Caller must hold a write lock.
func (s *GoStore) removeIfEmpty(key string, entry *Entry) {
	if len(entry.List) == 0 {
		delete(s.data, key)
	}
}

// resolveIndex converts a potentially-negative index into a valid position
// within a list of the given length.
// Returns the resolved index and whether it falls within bounds.
func resolveIndex(index, length int) (int, bool) {
	if index < 0 {
		index += length
	}
	return index, index >= 0 && index < length
}

// clampRange resolves start/stop into a valid half-open range [lo, hi)
// for a list of the given length. Returns (0, 0) when the range is empty.
func clampRange(start, stop, length int) (lo, hi int) {
	if start < 0 {
		start += length
	}
	if stop < 0 {
		stop += length
	}
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop {
		return 0, 0
	}
	return start, stop + 1 // convert inclusive stop → exclusive hi
}

func (s *GoStore) LPush(key string, values ...[]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, err := s.getOrCreateList(key)
	if err != nil {
		return 0, err
	}
	// Prepend values in order: LPUSH key a b c → list becomes [c, b, a, ...].
	// Redis prepends each value one at a time from left to right, so the
	// last argument ends up at the head.
	newElems := make([][]byte, len(values)+len(entry.List))
	for i, v := range values {
		newElems[len(values)-1-i] = v
	}
	copy(newElems[len(values):], entry.List)
	entry.List = newElems

	return len(entry.List), nil
}

func (s *GoStore) RPush(key string, values ...[]byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, err := s.getOrCreateList(key)
	if err != nil {
		return 0, err
	}
	entry.List = append(entry.List, values...)
	return len(entry.List), nil
}

func (s *GoStore) LPop(key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, err := s.requireList(key)
	if err != nil {
		return nil, false, err
	}
	if entry == nil || len(entry.List) == 0 {
		return nil, false, nil
	}

	head := entry.List[0]
	entry.List = entry.List[1:]
	s.removeIfEmpty(key, entry)
	return head, true, nil
}

func (s *GoStore) RPop(key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, err := s.requireList(key)
	if err != nil {
		return nil, false, err
	}
	if entry == nil || len(entry.List) == 0 {
		return nil, false, nil
	}

	lastIdx := len(entry.List) - 1
	tail := entry.List[lastIdx]
	entry.List = entry.List[:lastIdx]
	s.removeIfEmpty(key, entry)
	return tail, true, nil
}

func (s *GoStore) LLen(key string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, err := s.requireList(key)
	if err != nil {
		return 0, err
	}
	if entry == nil {
		return 0, nil
	}
	return len(entry.List), nil
}

func (s *GoStore) LRange(key string, start, stop int) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, err := s.requireList(key)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}

	lo, hi := clampRange(start, stop, len(entry.List))
	if lo >= hi {
		return nil, nil
	}

	// Return a copy so the caller can't mutate the internal slice.
	elements := make([][]byte, hi-lo)
	copy(elements, entry.List[lo:hi])
	return elements, nil
}

func (s *GoStore) LIndex(key string, index int) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, err := s.requireList(key)
	if err != nil {
		return nil, false, err
	}
	if entry == nil {
		return nil, false, nil
	}

	resolved, inBounds := resolveIndex(index, len(entry.List))
	if !inBounds {
		return nil, false, nil
	}
	return entry.List[resolved], true, nil
}

func (s *GoStore) LSet(key string, index int, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, err := s.requireList(key)
	if err != nil {
		return err
	}
	if entry == nil {
		return ErrIndexOutOfRange
	}

	resolved, inBounds := resolveIndex(index, len(entry.List))
	if !inBounds {
		return ErrIndexOutOfRange
	}
	entry.List[resolved] = value
	return nil
}

func (s *GoStore) LInsert(key string, before bool, pivot, value []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, err := s.requireList(key)
	if err != nil {
		return 0, err
	}
	if entry == nil {
		return 0, nil
	}

	// Find the first occurrence of pivot.
	pivotIdx := -1
	for i, elem := range entry.List {
		if bytes.Equal(elem, pivot) {
			pivotIdx = i
			break
		}
	}
	if pivotIdx == -1 {
		return -1, nil // pivot not found
	}

	insertAt := pivotIdx
	if !before {
		insertAt = pivotIdx + 1
	}

	// Insert value at insertAt position.
	entry.List = append(entry.List, nil)                 // grow by one
	copy(entry.List[insertAt+1:], entry.List[insertAt:]) // shift right
	entry.List[insertAt] = value

	return len(entry.List), nil
}

func (s *GoStore) LRem(key string, count int, value []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, err := s.requireList(key)
	if err != nil {
		return 0, err
	}
	if entry == nil {
		return 0, nil
	}

	removed := 0
	maxToRemove := len(entry.List) // count == 0 means remove all
	if count > 0 {
		maxToRemove = count
	} else if count < 0 {
		maxToRemove = -count
	}

	if count >= 0 {
		// Remove from head to tail.
		filtered := make([][]byte, 0, len(entry.List))
		for _, elem := range entry.List {
			if removed < maxToRemove && bytes.Equal(elem, value) {
				removed++
				continue
			}
			filtered = append(filtered, elem)
		}
		entry.List = filtered
	} else {
		// Remove from tail to head: walk backwards.
		filtered := make([][]byte, 0, len(entry.List))
		for i := len(entry.List) - 1; i >= 0; i-- {
			if removed < maxToRemove && bytes.Equal(entry.List[i], value) {
				removed++
				continue
			}
			filtered = append(filtered, entry.List[i])
		}
		// Reverse filtered back to original order.
		for left, right := 0, len(filtered)-1; left < right; left, right = left+1, right-1 {
			filtered[left], filtered[right] = filtered[right], filtered[left]
		}
		entry.List = filtered
	}

	s.removeIfEmpty(key, entry)
	return removed, nil
}

func (s *GoStore) LTrim(key string, start, stop int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, err := s.requireList(key)
	if err != nil {
		return err
	}
	if entry == nil {
		return nil
	}

	lo, hi := clampRange(start, stop, len(entry.List))
	if lo >= hi {
		entry.List = nil
		delete(s.data, key)
		return nil
	}

	trimmed := make([][]byte, hi-lo)
	copy(trimmed, entry.List[lo:hi])
	entry.List = trimmed

	return nil
}

// ─── JSON operations ────────────────────────────────────────────────────────

func (s *GoStore) JSONSet(key, path string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.getEntryLocked(key)
	if e == nil {
		if path != "$" {
			return ErrPathNotFound
		}
		s.data[key] = &Entry{Type: TypeJSON, JSON: value}
		return nil
	}
	if e.Type != TypeJSON {
		return ErrWrongType
	}
	updated, err := jsonPathSet(e.JSON, path, value)
	if err != nil {
		return err
	}
	e.JSON = updated
	return nil
}

func (s *GoStore) JSONGet(key, path string) (any, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.getEntryLocked(key)
	if e == nil {
		return nil, false, nil
	}
	if e.Type != TypeJSON {
		return nil, false, ErrWrongType
	}
	val, err := jsonPathGet(e.JSON, path)
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

func (s *GoStore) JSONDel(key, path string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.getEntryLocked(key)
	if e == nil {
		return 0, nil
	}
	if e.Type != TypeJSON {
		return 0, ErrWrongType
	}
	if path == "$" {
		delete(s.data, key)
		return 1, nil
	}
	updated, count, err := jsonPathDel(e.JSON, path)
	if err != nil {
		return 0, err
	}
	e.JSON = updated
	return count, nil
}

func (s *GoStore) JSONType(key, path string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.getEntryLocked(key)
	if e == nil {
		return "", nil
	}
	if e.Type != TypeJSON {
		return "", ErrWrongType
	}
	return jsonPathType(e.JSON, path)
}

func (s *GoStore) JSONNumIncrBy(key, path string, n float64) (float64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.getEntryLocked(key)
	if e == nil {
		return 0, ErrPathNotFound
	}
	if e.Type != TypeJSON {
		return 0, ErrWrongType
	}
	updated, result, err := jsonNumIncrBy(e.JSON, path, n)
	if err != nil {
		return 0, err
	}
	e.JSON = updated
	return result, nil
}

// ─── general ─────────────────────────────────────────────────────────────────

func (s *GoStore) Len() int {
	s.mu.RLock()
	n := len(s.data)
	s.mu.RUnlock()
	return n
}

func (s *GoStore) Exists(keys ...string) int {
	count := 0
	if len(keys) == 0 {
		return count
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := range keys {
		if _, ok := s.data[keys[i]]; ok {
			count++
		}
	}
	return count
}

func (s *GoStore) Close() error { return nil }
