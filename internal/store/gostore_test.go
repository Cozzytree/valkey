package store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSetWithTTL_ExpiresOnGet(t *testing.T) {
	s := NewGoStore()
	s.SetWithTTL("k", []byte("v"), 50*time.Millisecond)

	// Should exist immediately.
	v, ok := s.Get("k")
	require.True(t, ok)
	require.Equal(t, []byte("v"), v)

	time.Sleep(60 * time.Millisecond)

	// Should be expired now (lazy deletion on Get).
	v, ok = s.Get("k")
	require.False(t, ok)
	require.Nil(t, v)
}

func TestSetWithTTL_ZeroTTL_NoPersistence(t *testing.T) {
	s := NewGoStore()
	s.SetWithTTL("k", []byte("v"), 0)

	// Zero TTL means no expiration.
	_, ok := s.TTL("k")
	require.True(t, ok)
	dur, _ := s.TTL("k")
	require.Equal(t, time.Duration(-1), dur)
}

func TestExpire_ExistingKey(t *testing.T) {
	s := NewGoStore()
	s.Set("k", []byte("v"))

	ok := s.Expire("k", 1*time.Second)
	require.True(t, ok)

	rem, exists := s.TTL("k")
	require.True(t, exists)
	require.True(t, rem > 0)
}

func TestExpire_MissingKey(t *testing.T) {
	s := NewGoStore()
	ok := s.Expire("nokey", 1*time.Second)
	require.False(t, ok)
}

func TestTTL_NoExpiry(t *testing.T) {
	s := NewGoStore()
	s.Set("k", []byte("v"))

	rem, exists := s.TTL("k")
	require.True(t, exists)
	require.Equal(t, time.Duration(-1), rem)
}

func TestTTL_MissingKey(t *testing.T) {
	s := NewGoStore()
	rem, exists := s.TTL("nokey")
	require.False(t, exists)
	require.Equal(t, time.Duration(-2), rem)
}

func TestPersist(t *testing.T) {
	s := NewGoStore()
	s.SetWithTTL("k", []byte("v"), 10*time.Second)

	ok := s.Persist("k")
	require.True(t, ok)

	rem, exists := s.TTL("k")
	require.True(t, exists)
	require.Equal(t, time.Duration(-1), rem)
}

func TestPersist_NoTTL(t *testing.T) {
	s := NewGoStore()
	s.Set("k", []byte("v"))
	require.False(t, s.Persist("k"))
}

func TestPersist_MissingKey(t *testing.T) {
	s := NewGoStore()
	require.False(t, s.Persist("nokey"))
}

func TestSet_ClearsTTL(t *testing.T) {
	s := NewGoStore()
	s.SetWithTTL("k", []byte("v"), 10*time.Second)

	// Plain Set should clear the TTL.
	s.Set("k", []byte("v2"))

	rem, exists := s.TTL("k")
	require.True(t, exists)
	require.Equal(t, time.Duration(-1), rem)
}

func TestExpireN_DeletesExpired(t *testing.T) {
	s := NewGoStore()
	for i := 0; i < 10; i++ {
		s.SetWithTTL(string(rune('a'+i)), []byte("v"), 10*time.Millisecond)
	}

	time.Sleep(20 * time.Millisecond)

	deleted := s.ExpireN(20)
	require.Equal(t, 10, deleted)
	require.Equal(t, 0, s.Len())
}

func TestExpireN_SkipsNonExpired(t *testing.T) {
	s := NewGoStore()
	s.SetWithTTL("expired", []byte("v"), 10*time.Millisecond)
	s.SetWithTTL("alive", []byte("v"), 10*time.Second)

	time.Sleep(20 * time.Millisecond)

	deleted := s.ExpireN(20)
	require.Equal(t, 1, deleted)
	require.Equal(t, 1, s.Len())

	v, ok := s.Get("alive")
	require.True(t, ok)
	require.Equal(t, []byte("v"), v)
}

func TestDel_ClearsExpiry(t *testing.T) {
	s := NewGoStore()
	s.SetWithTTL("k", []byte("v"), 10*time.Second)
	s.Del("k")

	_, ok := s.Get("k")
	require.False(t, ok)
	require.Equal(t, 0, s.Len())
}
