// Package cstore provides a Store implementation backed by a C hash map
// compiled via CGO. Use the build tag "cstore" to enable it:
//
//	go build  -tags cstore ./...
//	go test   -tags cstore ./internal/store/cstore/

//go:build cstore

package cstore

/*
#include "cstore.h"
*/
import "C"
import (
	"sync"
	"time"
	"unsafe"

	"github.com/valkey/valkey/internal/store"
)

// CStore implements store.Store using a C hash map via CGO.
// Thread safety is provided by a Go-side RWMutex since the C code is not
// internally synchronized.
type CStore struct {
	mu  sync.RWMutex
	ptr *C.CStore
}

var _ store.Store = (*CStore)(nil)

// New allocates a CStore. Panics on OOM.
func New() *CStore {
	ptr := C.cstore_new()
	if ptr == nil {
		panic("cstore: cstore_new returned NULL")
	}
	return &CStore{ptr: ptr}
}

func (s *CStore) Set(key string, value []byte) {
	s.mu.Lock()
	if len(value) == 0 {
		empty := [1]byte{}
		C.cstore_set(s.ptr,
			cstr(key), C.size_t(len(key)),
			(*C.char)(unsafe.Pointer(&empty[0])), 0)
	} else {
		C.cstore_set(s.ptr,
			cstr(key), C.size_t(len(key)),
			(*C.char)(unsafe.Pointer(&value[0])), C.size_t(len(value)))
	}
	s.mu.Unlock()
}

func (s *CStore) SetWithTTL(key string, value []byte, ttl time.Duration) {
	if ttl <= 0 {
		s.Set(key, value)
		return
	}
	expireNS := C.int64_t(time.Now().Add(ttl).UnixNano())
	s.mu.Lock()
	if len(value) == 0 {
		empty := [1]byte{}
		C.cstore_set_ttl(s.ptr,
			cstr(key), C.size_t(len(key)),
			(*C.char)(unsafe.Pointer(&empty[0])), 0,
			expireNS)
	} else {
		C.cstore_set_ttl(s.ptr,
			cstr(key), C.size_t(len(key)),
			(*C.char)(unsafe.Pointer(&value[0])), C.size_t(len(value)),
			expireNS)
	}
	s.mu.Unlock()
}

func (s *CStore) Get(key string) ([]byte, bool) {
	k := cstr(key)
	klen := C.size_t(len(key))

	s.mu.Lock()
	defer s.mu.Unlock()

	// Fast path with 512-byte buffer.
	buf := make([]byte, 512)
	var outLen C.size_t
	ret := C.cstore_get(s.ptr, k, klen,
		(*C.char)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)), &outLen)

	switch ret {
	case 0:
		return nil, false
	case 1:
		return buf[:outLen], true
	}

	// Buffer too small — retry.
	buf = make([]byte, outLen)
	ret = C.cstore_get(s.ptr, k, klen,
		(*C.char)(unsafe.Pointer(&buf[0])), outLen, &outLen)
	if ret != 1 {
		return nil, false
	}
	return buf[:outLen], true
}

func (s *CStore) Del(key string) bool {
	s.mu.Lock()
	ret := C.cstore_del(s.ptr, cstr(key), C.size_t(len(key)))
	s.mu.Unlock()
	return ret == 1
}

func (s *CStore) Expire(key string, ttl time.Duration) bool {
	expireNS := C.int64_t(time.Now().Add(ttl).UnixNano())
	s.mu.Lock()
	ret := C.cstore_expire(s.ptr, cstr(key), C.size_t(len(key)), expireNS)
	s.mu.Unlock()
	return ret == 1
}

func (s *CStore) TTL(key string) (time.Duration, bool) {
	s.mu.RLock()
	ns := C.cstore_ttl(s.ptr, cstr(key), C.size_t(len(key)))
	s.mu.RUnlock()
	switch {
	case ns == -2:
		return -2, false
	case ns == -1:
		return -1, true
	default:
		return time.Duration(ns), true
	}
}

func (s *CStore) Persist(key string) bool {
	s.mu.Lock()
	ret := C.cstore_persist(s.ptr, cstr(key), C.size_t(len(key)))
	s.mu.Unlock()
	return ret == 1
}

func (s *CStore) ExpireN(n int) int {
	s.mu.Lock()
	ret := C.cstore_expire_n(s.ptr, C.int(n))
	s.mu.Unlock()
	return int(ret)
}

func (s *CStore) Len() int {
	s.mu.RLock()
	n := C.cstore_len(s.ptr)
	s.mu.RUnlock()
	return int(n)
}

func (s *CStore) Close() error {
	s.mu.Lock()
	if s.ptr != nil {
		C.cstore_free(s.ptr)
		s.ptr = nil
	}
	s.mu.Unlock()
	return nil
}

func cstr(s string) *C.char {
	if s == "" {
		return (*C.char)(unsafe.Pointer(&s))
	}
	return (*C.char)(unsafe.Pointer(unsafe.StringData(s)))
}
