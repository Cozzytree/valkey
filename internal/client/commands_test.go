package client_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/valkey/valkey/internal/client"
)

// ─── strings ─────────────────────────────────────────────────────────────────

func TestClient_SetGet(t *testing.T) {
	cl := dial(t, startServer(t))

	require.NoError(t, cl.Set("name", "Alice"))

	val, err := cl.Get("name")
	require.NoError(t, err)
	require.Equal(t, "Alice", val)
}

func TestClient_GetMissing(t *testing.T) {
	cl := dial(t, startServer(t))

	_, err := cl.Get("nokey")
	require.ErrorIs(t, err, client.ErrNil)
}

func TestClient_GetBytes(t *testing.T) {
	cl := dial(t, startServer(t))

	cl.Set("k", "binary\x00data")
	got, err := cl.GetBytes("k")
	require.NoError(t, err)
	require.Equal(t, []byte("binary\x00data"), got)
}

func TestClient_Del(t *testing.T) {
	cl := dial(t, startServer(t))

	cl.Set("a", "1")
	cl.Set("b", "2")

	n, err := cl.Del("a", "b", "nonexistent")
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
}

// ─── TTL ─────────────────────────────────────────────────────────────────────

func TestClient_SetEX(t *testing.T) {
	cl := dial(t, startServer(t))

	require.NoError(t, cl.SetEX("k", "v", 10))

	ttl, err := cl.TTL("k")
	require.NoError(t, err)
	require.True(t, ttl > 0 && ttl <= 10*time.Second)
}

func TestClient_SetPX(t *testing.T) {
	cl := dial(t, startServer(t))

	require.NoError(t, cl.SetPX("k", "v", 5000))

	ttl, err := cl.PTTL("k")
	require.NoError(t, err)
	require.True(t, ttl > 0 && ttl <= 5000*time.Millisecond)
}

func TestClient_Expire(t *testing.T) {
	cl := dial(t, startServer(t))

	cl.Set("k", "v")
	ok, err := cl.Expire("k", 10)
	require.NoError(t, err)
	require.True(t, ok)

	// Missing key.
	ok, err = cl.Expire("nokey", 10)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestClient_TTL_NoExpiry(t *testing.T) {
	cl := dial(t, startServer(t))

	cl.Set("k", "v")
	ttl, err := cl.TTL("k")
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl)
}

func TestClient_TTL_Missing(t *testing.T) {
	cl := dial(t, startServer(t))

	ttl, err := cl.TTL("nokey")
	require.NoError(t, err)
	require.Equal(t, time.Duration(-2), ttl)
}

func TestClient_Persist(t *testing.T) {
	cl := dial(t, startServer(t))

	cl.SetEX("k", "v", 60)
	ok, err := cl.Persist("k")
	require.NoError(t, err)
	require.True(t, ok)

	ttl, err := cl.TTL("k")
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl)
}

// ─── hashes ──────────────────────────────────────────────────────────────────

func TestClient_HSetHGet(t *testing.T) {
	cl := dial(t, startServer(t))

	added, err := cl.HSet("user:1", "name", "Alice", "age", "30")
	require.NoError(t, err)
	require.Equal(t, int64(2), added)

	name, err := cl.HGet("user:1", "name")
	require.NoError(t, err)
	require.Equal(t, "Alice", name)
}

func TestClient_HGet_Missing(t *testing.T) {
	cl := dial(t, startServer(t))

	_, err := cl.HGet("nokey", "f")
	require.ErrorIs(t, err, client.ErrNil)
}

func TestClient_HDel(t *testing.T) {
	cl := dial(t, startServer(t))

	cl.HSet("h", "a", "1", "b", "2")
	n, err := cl.HDel("h", "a", "nonexistent")
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
}

func TestClient_HGetAll(t *testing.T) {
	cl := dial(t, startServer(t))

	cl.HSet("h", "a", "1", "b", "2")
	m, err := cl.HGetAll("h")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"a": "1", "b": "2"}, m)
}

func TestClient_HGetAll_Missing(t *testing.T) {
	cl := dial(t, startServer(t))

	m, err := cl.HGetAll("nokey")
	require.NoError(t, err)
	require.Empty(t, m)
}

func TestClient_HLen(t *testing.T) {
	cl := dial(t, startServer(t))

	cl.HSet("h", "a", "1", "b", "2")
	n, err := cl.HLen("h")
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
}

func TestClient_HExists(t *testing.T) {
	cl := dial(t, startServer(t))

	cl.HSet("h", "f", "v")
	ok, err := cl.HExists("h", "f")
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = cl.HExists("h", "nope")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestClient_HKeys(t *testing.T) {
	cl := dial(t, startServer(t))

	cl.HSet("h", "a", "1", "b", "2")
	keys, err := cl.HKeys("h")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a", "b"}, keys)
}

func TestClient_HVals(t *testing.T) {
	cl := dial(t, startServer(t))

	cl.HSet("h", "a", "1", "b", "2")
	vals, err := cl.HVals("h")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"1", "2"}, vals)
}

// ─── server ──────────────────────────────────────────────────────────────────

func TestClient_Ping(t *testing.T) {
	cl := dial(t, startServer(t))

	pong, err := cl.Ping()
	require.NoError(t, err)
	require.Equal(t, "PONG", pong)
}

// ─── WRONGTYPE ───────────────────────────────────────────────────────────────

func TestClient_WrongType(t *testing.T) {
	cl := dial(t, startServer(t))

	cl.Set("str", "value")
	_, err := cl.HGet("str", "field")
	require.Error(t, err)
	require.Contains(t, err.Error(), "WRONGTYPE")
}

// ─── JSON ────────────────────────────────────────────────────────────────────

func TestClient_JSONSetGet(t *testing.T) {
	cl := dial(t, startServer(t))

	doc := map[string]any{"name": "Alice", "age": float64(30)}
	require.NoError(t, cl.JSONSet("user", "$", doc))

	got, err := cl.JSONGet("user")
	require.NoError(t, err)

	var parsed map[string]any
	require.NoError(t, json.Unmarshal([]byte(got), &parsed))
	require.Equal(t, "Alice", parsed["name"])
	require.Equal(t, float64(30), parsed["age"])
}

func TestClient_JSONGetPath(t *testing.T) {
	cl := dial(t, startServer(t))

	doc := map[string]any{"name": "Alice", "age": float64(30)}
	cl.JSONSet("user", "$", doc)

	got, err := cl.JSONGet("user", "$.name")
	require.NoError(t, err)
	require.Equal(t, `"Alice"`, got)
}

func TestClient_JSONGetMissing(t *testing.T) {
	cl := dial(t, startServer(t))

	_, err := cl.JSONGet("nokey")
	require.ErrorIs(t, err, client.ErrNil)
}

func TestClient_JSONDel(t *testing.T) {
	cl := dial(t, startServer(t))

	doc := map[string]any{"name": "Alice", "age": float64(30)}
	cl.JSONSet("user", "$", doc)

	n, err := cl.JSONDel("user", "$.age")
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	got, err := cl.JSONGet("user")
	require.NoError(t, err)
	require.Equal(t, `{"name":"Alice"}`, got)
}

func TestClient_JSONDelRoot(t *testing.T) {
	cl := dial(t, startServer(t))

	cl.JSONSet("user", "$", map[string]any{"x": float64(1)})
	n, err := cl.JSONDel("user")
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	_, err = cl.JSONGet("user")
	require.ErrorIs(t, err, client.ErrNil)
}

func TestClient_JSONType(t *testing.T) {
	cl := dial(t, startServer(t))

	doc := map[string]any{"name": "Alice", "age": float64(30)}
	cl.JSONSet("k", "$", doc)

	typ, err := cl.JSONType("k")
	require.NoError(t, err)
	require.Equal(t, "object", typ)

	typ, err = cl.JSONType("k", "$.name")
	require.NoError(t, err)
	require.Equal(t, "string", typ)
}

func TestClient_JSONNumIncrBy(t *testing.T) {
	cl := dial(t, startServer(t))

	cl.JSONSet("k", "$", map[string]any{"counter": float64(10)})

	result, err := cl.JSONNumIncrBy("k", "$.counter", 5)
	require.NoError(t, err)
	require.Equal(t, float64(15), result)
}

func TestClient_JSONWrongType(t *testing.T) {
	cl := dial(t, startServer(t))

	cl.Set("str", "value")
	_, err := cl.JSONGet("str")
	require.Error(t, err)
	require.Contains(t, err.Error(), "WRONGTYPE")
}
