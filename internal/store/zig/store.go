//go:build zigstore

// Package zig provides a Store implementation backed by a Zig-compiled
// in-memory hash map (libstore.a).
//
// Build the Zig library first:
//
//	cd internal/store/zig && zig build -Doptimize=ReleaseFast
//
// Then build/run with the tag:
//
//	go build -tags zigstore ./...
//	go test  -tags zigstore ./internal/store/zig/
package zig

/*
#cgo LDFLAGS: -L${SRCDIR}/zig-out/lib -lstore
#include "store.h"
#include <stdlib.h>
*/
import "C"
import (
	"time"
	"unsafe"

	"github.com/valkey/valkey/internal/store"
)

// ZigStore implements store.Store using the Zig-compiled hash map.
// Zig owns all key/value memory; Go only ever passes write-buffers in.
type ZigStore struct {
	ptr *C.ValkeyZigStore
}

// compile-time check that ZigStore satisfies the interface.
var _ store.Store = (*ZigStore)(nil)

// New allocates a ZigStore. Panics if Zig returns NULL (OOM).
func New() *ZigStore {
	ptr := C.store_new()
	if ptr == nil {
		panic("zigstore: store_new returned NULL")
	}
	return &ZigStore{ptr: ptr}
}

func (s *ZigStore) Set(key string, value []byte) {
	if len(value) == 0 {
		// Avoid passing a nil pointer for empty values.
		empty := [1]byte{}
		C.store_set(s.ptr,
			cstr(key), C.size_t(len(key)),
			(*C.char)(unsafe.Pointer(&empty[0])), 0,
		)
		return
	}
	C.store_set(s.ptr,
		cstr(key), C.size_t(len(key)),
		(*C.char)(unsafe.Pointer(&value[0])), C.size_t(len(value)),
	)
}

func (s *ZigStore) Get(key string) ([]byte, bool) {
	k := cstr(key)
	klen := C.size_t(len(key))

	// Fast path: try with an initial 512-byte stack-like buffer.
	buf := make([]byte, 512)
	var outLen C.size_t

	ret := C.store_get(s.ptr, k, klen,
		(*C.char)(unsafe.Pointer(&buf[0])), C.size_t(len(buf)), &outLen)

	switch ret {
	case 0:
		return nil, false // miss
	case 1:
		return buf[:outLen], true // hit, fits in initial buffer
	}

	// ret == -1: buffer too small — retry with exact size.
	buf = make([]byte, outLen)
	ret = C.store_get(s.ptr, k, klen,
		(*C.char)(unsafe.Pointer(&buf[0])), outLen, &outLen)
	if ret != 1 {
		return nil, false
	}
	return buf[:outLen], true
}

func (s *ZigStore) Del(key string) bool {
	return C.store_del(s.ptr, cstr(key), C.size_t(len(key))) == 1
}

func (s *ZigStore) Len() int {
	return int(C.store_len(s.ptr))
}

func (s *ZigStore) SetWithTTL(key string, value []byte, ttl time.Duration) {
	s.Set(key, value) // TTL not supported in Zig backend
}

func (s *ZigStore) Expire(key string, ttl time.Duration) bool { return false }

func (s *ZigStore) TTL(key string) (time.Duration, bool) { return -2, false }

func (s *ZigStore) Persist(key string) bool { return false }

func (s *ZigStore) ExpireN(n int) int { return 0 }

func (s *ZigStore) HSet(key string, fields ...string) (int, error) {
	return 0, store.ErrWrongType
}
func (s *ZigStore) HGet(key, field string) ([]byte, bool, error) { return nil, false, nil }
func (s *ZigStore) HDel(key string, fields ...string) (int, error) {
	return 0, nil
}
func (s *ZigStore) HGetAll(key string) (map[string][]byte, error) { return nil, nil }
func (s *ZigStore) HLen(key string) (int, error)                  { return 0, nil }
func (s *ZigStore) HExists(key, field string) (bool, error)       { return false, nil }
func (s *ZigStore) HKeys(key string) ([]string, error)            { return nil, nil }
func (s *ZigStore) HVals(key string) ([][]byte, error)            { return nil, nil }

func (s *ZigStore) JSONSet(key, path string, value any) error             { return store.ErrWrongType }
func (s *ZigStore) JSONGet(key, path string) (any, bool, error)           { return nil, false, nil }
func (s *ZigStore) JSONDel(key, path string) (int, error)                 { return 0, nil }
func (s *ZigStore) JSONType(key, path string) (string, error)             { return "", nil }
func (s *ZigStore) JSONNumIncrBy(key, path string, n float64) (float64, error) { return 0, nil }

func (s *ZigStore) Close() error {
	C.store_free(s.ptr)
	s.ptr = nil
	return nil
}

// cstr converts a Go string to a *C.char without allocation when possible.
// The returned pointer is valid only for the duration of the current call.
func cstr(s string) *C.char {
	if s == "" {
		return (*C.char)(unsafe.Pointer(&s)) // non-nil, length 0
	}
	return (*C.char)(unsafe.Pointer(unsafe.StringData(s)))
}
