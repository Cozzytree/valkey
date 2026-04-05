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

// ─── hash tests ──────────────────────────────────────────────────────────────

func TestHSet_NewHash(t *testing.T) {
	s := NewGoStore()
	added, err := s.HSet("h", "name", "Alice", "age", "30")
	require.NoError(t, err)
	require.Equal(t, 2, added)
	require.Equal(t, 1, s.Len())
}

func TestHSet_UpdateExisting(t *testing.T) {
	s := NewGoStore()
	s.HSet("h", "name", "Alice")
	added, err := s.HSet("h", "name", "Bob", "email", "bob@test.com")
	require.NoError(t, err)
	require.Equal(t, 1, added) // only email is new

	v, ok, err := s.HGet("h", "name")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("Bob"), v)
}

func TestHGet(t *testing.T) {
	s := NewGoStore()
	s.HSet("h", "f1", "v1")

	v, ok, err := s.HGet("h", "f1")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("v1"), v)

	// Missing field.
	_, ok, err = s.HGet("h", "nope")
	require.NoError(t, err)
	require.False(t, ok)

	// Missing key.
	_, ok, err = s.HGet("nokey", "f1")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestHDel(t *testing.T) {
	s := NewGoStore()
	s.HSet("h", "a", "1", "b", "2", "c", "3")

	deleted, err := s.HDel("h", "a", "b", "nonexistent")
	require.NoError(t, err)
	require.Equal(t, 2, deleted)

	n, _ := s.HLen("h")
	require.Equal(t, 1, n)
}

func TestHDel_RemovesEmptyHash(t *testing.T) {
	s := NewGoStore()
	s.HSet("h", "f", "v")
	s.HDel("h", "f")
	require.Equal(t, 0, s.Len()) // key should be gone
}

func TestHGetAll(t *testing.T) {
	s := NewGoStore()
	s.HSet("h", "a", "1", "b", "2")

	m, err := s.HGetAll("h")
	require.NoError(t, err)
	require.Equal(t, map[string][]byte{"a": []byte("1"), "b": []byte("2")}, m)

	// Missing key.
	m, err = s.HGetAll("nokey")
	require.NoError(t, err)
	require.Nil(t, m)
}

func TestHLen(t *testing.T) {
	s := NewGoStore()
	n, err := s.HLen("nokey")
	require.NoError(t, err)
	require.Equal(t, 0, n)

	s.HSet("h", "a", "1", "b", "2")
	n, err = s.HLen("h")
	require.NoError(t, err)
	require.Equal(t, 2, n)
}

func TestHExists(t *testing.T) {
	s := NewGoStore()
	s.HSet("h", "f", "v")

	ok, err := s.HExists("h", "f")
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = s.HExists("h", "nope")
	require.NoError(t, err)
	require.False(t, ok)

	ok, err = s.HExists("nokey", "f")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestHKeys(t *testing.T) {
	s := NewGoStore()
	s.HSet("h", "b", "2", "a", "1")

	keys, err := s.HKeys("h")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b"}, keys)
}

func TestHVals(t *testing.T) {
	s := NewGoStore()
	s.HSet("h", "a", "1", "b", "2")

	vals, err := s.HVals("h")
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{[]byte("1"), []byte("2")}, vals)
}

func TestWrongType_GetOnHash(t *testing.T) {
	s := NewGoStore()
	s.HSet("h", "f", "v")

	// GET on a hash key should return false (not the hash data).
	v, ok := s.Get("h")
	require.False(t, ok)
	require.Nil(t, v)
}

func TestWrongType_HGetOnString(t *testing.T) {
	s := NewGoStore()
	s.Set("k", []byte("v"))

	_, _, err := s.HGet("k", "f")
	require.ErrorIs(t, err, ErrWrongType)
}

func TestWrongType_HSetOnString(t *testing.T) {
	s := NewGoStore()
	s.Set("k", []byte("v"))

	_, err := s.HSet("k", "f", "v")
	require.ErrorIs(t, err, ErrWrongType)
}

func TestHash_WithExpire(t *testing.T) {
	s := NewGoStore()
	s.HSet("h", "f", "v")
	s.Expire("h", 50*time.Millisecond)

	v, ok, err := s.HGet("h", "f")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("v"), v)

	time.Sleep(60 * time.Millisecond)

	_, ok, err = s.HGet("h", "f")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestDel_RemovesHash(t *testing.T) {
	s := NewGoStore()
	s.HSet("h", "f", "v")
	require.True(t, s.Del("h"))
	require.Equal(t, 0, s.Len())
}

// ─── JSON operations ────────────────────────────────────────────────────────

func TestJSONSet_GetRoot(t *testing.T) {
	s := NewGoStore()
	doc := map[string]any{"name": "Alice", "age": float64(30)}
	require.NoError(t, s.JSONSet("user", "$", doc))

	got, ok, err := s.JSONGet("user", "$")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, doc, got)
}

func TestJSONSet_NestedPath(t *testing.T) {
	s := NewGoStore()
	doc := map[string]any{"name": "Alice", "age": float64(30)}
	require.NoError(t, s.JSONSet("user", "$", doc))
	require.NoError(t, s.JSONSet("user", "$.age", float64(31)))

	got, ok, err := s.JSONGet("user", "$.age")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, float64(31), got)
}

func TestJSONSet_ArrayElement(t *testing.T) {
	s := NewGoStore()
	doc := map[string]any{"tags": []any{"go", "redis"}}
	require.NoError(t, s.JSONSet("k", "$", doc))
	require.NoError(t, s.JSONSet("k", "$.tags[0]", "valkey"))

	got, ok, err := s.JSONGet("k", "$.tags[0]")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "valkey", got)
}

func TestJSONGet_MissingKey(t *testing.T) {
	s := NewGoStore()
	_, ok, err := s.JSONGet("nokey", "$")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestJSONGet_MissingPath(t *testing.T) {
	s := NewGoStore()
	s.JSONSet("k", "$", map[string]any{"name": "Alice"})
	_, _, err := s.JSONGet("k", "$.missing")
	require.Error(t, err)
}

func TestJSONDel_Field(t *testing.T) {
	s := NewGoStore()
	doc := map[string]any{"name": "Alice", "age": float64(30)}
	s.JSONSet("k", "$", doc)

	count, err := s.JSONDel("k", "$.age")
	require.NoError(t, err)
	require.Equal(t, 1, count)

	got, ok, err := s.JSONGet("k", "$")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, map[string]any{"name": "Alice"}, got)
}

func TestJSONDel_Root(t *testing.T) {
	s := NewGoStore()
	s.JSONSet("k", "$", map[string]any{"x": float64(1)})
	count, err := s.JSONDel("k", "$")
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.Equal(t, 0, s.Len())
}

func TestJSONType(t *testing.T) {
	s := NewGoStore()
	doc := map[string]any{"name": "Alice", "age": float64(30), "tags": []any{"a"}}
	s.JSONSet("k", "$", doc)

	typ, err := s.JSONType("k", "$")
	require.NoError(t, err)
	require.Equal(t, "object", typ)

	typ, err = s.JSONType("k", "$.name")
	require.NoError(t, err)
	require.Equal(t, "string", typ)

	typ, err = s.JSONType("k", "$.age")
	require.NoError(t, err)
	require.Equal(t, "number", typ)
}

func TestJSONNumIncrBy(t *testing.T) {
	s := NewGoStore()
	doc := map[string]any{"counter": float64(10)}
	s.JSONSet("k", "$", doc)

	result, err := s.JSONNumIncrBy("k", "$.counter", 5)
	require.NoError(t, err)
	require.Equal(t, float64(15), result)

	got, _, _ := s.JSONGet("k", "$.counter")
	require.Equal(t, float64(15), got)
}

func TestJSONNumIncrBy_NotANumber(t *testing.T) {
	s := NewGoStore()
	s.JSONSet("k", "$", map[string]any{"name": "Alice"})
	_, err := s.JSONNumIncrBy("k", "$.name", 1)
	require.ErrorIs(t, err, ErrNotANumber)
}

func TestWrongType_JSONGetOnString(t *testing.T) {
	s := NewGoStore()
	s.Set("k", []byte("v"))
	_, _, err := s.JSONGet("k", "$")
	require.ErrorIs(t, err, ErrWrongType)
}

func TestWrongType_GetOnJSON(t *testing.T) {
	s := NewGoStore()
	s.JSONSet("k", "$", map[string]any{"x": float64(1)})
	_, ok := s.Get("k")
	require.False(t, ok)
}
