//go:build cstore

package cstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSetGet(t *testing.T) {
	s := New()
	defer s.Close()

	s.Set("key", []byte("value"))
	v, ok := s.Get("key")
	require.True(t, ok)
	require.Equal(t, []byte("value"), v)
}

func TestGetMiss(t *testing.T) {
	s := New()
	defer s.Close()

	v, ok := s.Get("missing")
	require.False(t, ok)
	require.Nil(t, v)
}

func TestSetOverwrite(t *testing.T) {
	s := New()
	defer s.Close()

	s.Set("k", []byte("v1"))
	s.Set("k", []byte("v2"))
	v, ok := s.Get("k")
	require.True(t, ok)
	require.Equal(t, []byte("v2"), v)
}

func TestDel(t *testing.T) {
	s := New()
	defer s.Close()

	s.Set("k", []byte("v"))
	require.True(t, s.Del("k"))
	require.False(t, s.Del("k"))
	_, ok := s.Get("k")
	require.False(t, ok)
}

func TestLen(t *testing.T) {
	s := New()
	defer s.Close()

	require.Equal(t, 0, s.Len())
	s.Set("a", []byte("1"))
	s.Set("b", []byte("2"))
	require.Equal(t, 2, s.Len())
	s.Del("a")
	require.Equal(t, 1, s.Len())
}

func TestSetWithTTL_Expires(t *testing.T) {
	s := New()
	defer s.Close()

	s.SetWithTTL("k", []byte("v"), 50*time.Millisecond)
	v, ok := s.Get("k")
	require.True(t, ok)
	require.Equal(t, []byte("v"), v)

	time.Sleep(60 * time.Millisecond)
	v, ok = s.Get("k")
	require.False(t, ok)
	require.Nil(t, v)
}

func TestExpire(t *testing.T) {
	s := New()
	defer s.Close()

	s.Set("k", []byte("v"))
	require.True(t, s.Expire("k", 1*time.Second))
	require.False(t, s.Expire("nokey", 1*time.Second))

	rem, ok := s.TTL("k")
	require.True(t, ok)
	require.True(t, rem > 0)
}

func TestTTL(t *testing.T) {
	s := New()
	defer s.Close()

	// Missing key.
	rem, ok := s.TTL("nokey")
	require.False(t, ok)
	require.Equal(t, time.Duration(-2), rem)

	// Key without TTL.
	s.Set("k", []byte("v"))
	rem, ok = s.TTL("k")
	require.True(t, ok)
	require.Equal(t, time.Duration(-1), rem)

	// Key with TTL.
	s.SetWithTTL("ttlkey", []byte("v"), 10*time.Second)
	rem, ok = s.TTL("ttlkey")
	require.True(t, ok)
	require.True(t, rem > 0)
}

func TestPersist(t *testing.T) {
	s := New()
	defer s.Close()

	s.SetWithTTL("k", []byte("v"), 10*time.Second)
	require.True(t, s.Persist("k"))

	rem, ok := s.TTL("k")
	require.True(t, ok)
	require.Equal(t, time.Duration(-1), rem)

	// Already persistent.
	require.False(t, s.Persist("k"))
	require.False(t, s.Persist("nokey"))
}

func TestExpireN(t *testing.T) {
	s := New()
	defer s.Close()

	for i := 0; i < 10; i++ {
		s.SetWithTTL(string(rune('a'+i)), []byte("v"), 10*time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)

	deleted := s.ExpireN(100)
	require.Equal(t, 10, deleted)
	require.Equal(t, 0, s.Len())
}

func TestSetClearsTTL(t *testing.T) {
	s := New()
	defer s.Close()

	s.SetWithTTL("k", []byte("v"), 10*time.Second)
	s.Set("k", []byte("v2"))

	rem, ok := s.TTL("k")
	require.True(t, ok)
	require.Equal(t, time.Duration(-1), rem)
}

func TestManyKeys(t *testing.T) {
	s := New()
	defer s.Close()

	// Force several grow() calls.
	for i := 0; i < 1000; i++ {
		key := string(rune(i)) + "_key"
		s.Set(key, []byte("value"))
	}
	require.Equal(t, 1000, s.Len())

	// Verify all are retrievable after grows.
	for i := 0; i < 1000; i++ {
		key := string(rune(i)) + "_key"
		_, ok := s.Get(key)
		require.True(t, ok, "key %q should exist", key)
	}
}
