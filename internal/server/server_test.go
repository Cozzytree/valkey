package server_test

import (
	"fmt"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/valkey/valkey/internal/config"
	"github.com/valkey/valkey/internal/proto"
	"github.com/valkey/valkey/internal/server"
)

// ─── test server helpers ──────────────────────────────────────────────────────

// startServer spins up a real Valkey server on a random port and returns it.
// t.Cleanup stops it when the test finishes.
func startServer(t *testing.T) *server.Server {
	t.Helper()

	cfg := config.DefaultConfig()
	cfg.Network.Port = 0 // OS picks a free port

	srv := server.New(cfg, log.New(io.Discard, "", 0))
	require.NoError(t, srv.Start())

	t.Cleanup(srv.Stop)
	return srv
}

// ─── RESP client helpers ──────────────────────────────────────────────────────

// client wraps a net.Conn with our proto.BufReader so the test can send RESP
// commands and parse responses using the same reader the production code uses.
type client struct {
	conn   net.Conn
	reader *proto.BufReader
}

func dial(t *testing.T, srv *server.Server) *client {
	t.Helper()
	nc, err := net.Dial("tcp", srv.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { nc.Close() })
	return &client{
		conn:   nc,
		reader: proto.NewBufReader(nc),
	}
}

// send encodes args as a RESP array and writes it to the server.
//
//	*<n>\r\n
//	$<len>\r\n<arg>\r\n  (repeated n times)
func (c *client) send(t *testing.T, args ...string) {
	t.Helper()
	buf := fmt.Sprintf("*%d\r\n", len(args))
	for _, a := range args {
		buf += fmt.Sprintf("$%d\r\n%s\r\n", len(a), a)
	}
	_, err := c.conn.Write([]byte(buf))
	require.NoError(t, err)
}

// readSimpleString reads a RESP simple string (+OK\r\n → "OK").
func (c *client) readSimpleString(t *testing.T) string {
	t.Helper()
	prefix, err := c.reader.ReadByte()
	require.NoError(t, err)
	require.Equal(t, byte('+'), prefix, "expected simple string prefix '+'")
	line, err := c.reader.ReadLine()
	require.NoError(t, err)
	return string(line)
}

// readBulkString reads a RESP bulk string ($<len>\r\n<data>\r\n).
// Returns nil for a null bulk string ($-1\r\n).
func (c *client) readBulkString(t *testing.T) []byte {
	t.Helper()
	prefix, err := c.reader.ReadByte()
	require.NoError(t, err)
	require.Equal(t, byte('$'), prefix, "expected bulk string prefix '$'")

	line, err := c.reader.ReadLine()
	require.NoError(t, err)

	length := mustParseInt(t, line)
	if length < 0 {
		return nil // null bulk string
	}

	data, err := c.reader.ReadN(length)
	require.NoError(t, err)

	// Consume the trailing \r\n.
	_, err = c.reader.Discard(2)
	require.NoError(t, err)

	return data
}

// readError reads a RESP error (-ERR ...\r\n) and returns the message.
func (c *client) readError(t *testing.T) string {
	t.Helper()
	prefix, err := c.reader.ReadByte()
	require.NoError(t, err)
	require.Equal(t, byte('-'), prefix, "expected error prefix '-'")
	line, err := c.reader.ReadLine()
	require.NoError(t, err)
	return string(line)
}

func mustParseInt(t *testing.T, b []byte) int {
	t.Helper()
	n := 0
	neg := false
	i := 0
	if len(b) > 0 && b[0] == '-' {
		neg = true
		i = 1
	}
	for ; i < len(b); i++ {
		require.True(t, b[i] >= '0' && b[i] <= '9', "non-digit in integer %q", b)
		n = n*10 + int(b[i]-'0')
	}
	if neg {
		return -n
	}
	return n
}

// readArray reads a RESP array of bulk strings and returns them.
func (c *client) readArray(t *testing.T) [][]byte {
	t.Helper()
	prefix, err := c.reader.ReadByte()
	require.NoError(t, err)
	require.Equal(t, byte('*'), prefix, "expected array prefix '*'")
	line, err := c.reader.ReadLine()
	require.NoError(t, err)
	count := mustParseInt(t, line)
	if count < 0 {
		return nil
	}
	result := make([][]byte, count)
	for i := 0; i < count; i++ {
		result[i] = c.readBulkString(t)
	}
	return result
}

// readInteger reads a RESP integer (:<number>\r\n) and returns it.
func (c *client) readInteger(t *testing.T) int {
	t.Helper()
	prefix, err := c.reader.ReadByte()
	require.NoError(t, err)
	require.Equal(t, byte(':'), prefix, "expected integer prefix ':'")
	line, err := c.reader.ReadLine()
	require.NoError(t, err)
	return mustParseInt(t, line)
}

// ─── tests ────────────────────────────────────────────────────────────────────

// TestSetThenGet is the fundamental SET → GET round-trip.
// It verifies that a value written with SET can be retrieved with GET and that
// the bytes are identical — no truncation, no encoding corruption.
func TestSetThenGet(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	// SET name "valkey-store"
	c.send(t, "SET", "name", "valkey-store")
	reply := c.readSimpleString(t)
	require.Equal(t, "OK", reply)

	// GET name → "valkey-store"
	c.send(t, "GET", "name")
	got := c.readBulkString(t)
	require.Equal(t, []byte("valkey-store"), got)
}

// TestGetMissingKey verifies that GET on a key that was never SET returns a
// RESP null bulk string ($-1\r\n), not an error and not an empty string.
// This matches Redis semantics exactly.
func TestGetMissingKey(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "GET", "does-not-exist")
	got := c.readBulkString(t)
	require.Nil(t, got, "GET on missing key must return nil (null bulk string)")
}

// TestSetOverwrite verifies that a second SET on the same key replaces the
// value, and the new value is immediately visible to GET — no stale reads.
func TestSetOverwrite(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "SET", "counter", "1")
	require.Equal(t, "OK", c.readSimpleString(t))

	c.send(t, "SET", "counter", "999")
	require.Equal(t, "OK", c.readSimpleString(t))

	c.send(t, "GET", "counter")
	require.Equal(t, []byte("999"), c.readBulkString(t))
}

// TestConcurrentSetGet fires multiple goroutines each doing SET then GET on
// their own key. It checks that no goroutine reads another goroutine's value
// and that there are no data races (run with -race).
func TestConcurrentSetGet(t *testing.T) {
	const workers = 20

	srv := startServer(t)

	errc := make(chan error, workers)

	for i := 0; i < workers; i++ {
		i := i
		go func() {
			c := &client{}
			nc, err := net.Dial("tcp", srv.Addr().String())
			if err != nil {
				errc <- fmt.Errorf("worker %d dial: %w", i, err)
				return
			}
			defer nc.Close()
			c.conn = nc
			c.reader = proto.NewBufReader(nc)

			key := fmt.Sprintf("key-%d", i)
			val := fmt.Sprintf("value-%d", i)

			// SET
			_, err = nc.Write([]byte(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
				len(key), key, len(val), val)))
			if err != nil {
				errc <- fmt.Errorf("worker %d SET write: %w", i, err)
				return
			}

			prefix, err := c.reader.ReadByte()
			if err != nil || prefix != '+' {
				errc <- fmt.Errorf("worker %d SET response: prefix=%q err=%v", i, prefix, err)
				return
			}
			if _, err := c.reader.ReadLine(); err != nil {
				errc <- fmt.Errorf("worker %d SET line: %w", i, err)
				return
			}

			// GET
			_, err = nc.Write([]byte(fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n",
				len(key), key)))
			if err != nil {
				errc <- fmt.Errorf("worker %d GET write: %w", i, err)
				return
			}

			prefix, err = c.reader.ReadByte()
			if err != nil || prefix != '$' {
				errc <- fmt.Errorf("worker %d GET response: prefix=%q err=%v", i, prefix, err)
				return
			}
			line, err := c.reader.ReadLine()
			if err != nil {
				errc <- fmt.Errorf("worker %d GET length line: %w", i, err)
				return
			}
			length := 0
			for _, b := range line {
				length = length*10 + int(b-'0')
			}
			data, err := c.reader.ReadN(length)
			if err != nil {
				errc <- fmt.Errorf("worker %d GET data: %w", i, err)
				return
			}
			if _, err := c.reader.Discard(2); err != nil {
				errc <- fmt.Errorf("worker %d GET CRLF: %w", i, err)
				return
			}

			if string(data) != val {
				errc <- fmt.Errorf("worker %d: got %q, want %q", i, data, val)
				return
			}

			errc <- nil
		}()
	}

	for i := 0; i < workers; i++ {
		require.NoError(t, <-errc)
	}
}

// ─── TTL tests ───────────────────────────────────────────────────────────────

// TestSetEX verifies SET key value EX seconds expires the key.
func TestSetEX(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "SET", "mykey", "hello", "EX", "1")
	require.Equal(t, "OK", c.readSimpleString(t))

	// Key should exist immediately.
	c.send(t, "GET", "mykey")
	require.Equal(t, []byte("hello"), c.readBulkString(t))

	// TTL should be positive.
	c.send(t, "TTL", "mykey")
	ttl := c.readInteger(t)
	require.True(t, ttl > 0 && ttl <= 1, "expected TTL in (0,1], got %d", ttl)

	// Wait for expiry.
	time.Sleep(1100 * time.Millisecond)

	c.send(t, "GET", "mykey")
	got := c.readBulkString(t)
	require.Nil(t, got, "key should have expired")
}

// TestSetPX verifies SET key value PX milliseconds.
func TestSetPX(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "SET", "mykey", "hello", "PX", "200")
	require.Equal(t, "OK", c.readSimpleString(t))

	c.send(t, "GET", "mykey")
	require.Equal(t, []byte("hello"), c.readBulkString(t))

	time.Sleep(250 * time.Millisecond)

	c.send(t, "GET", "mykey")
	require.Nil(t, c.readBulkString(t), "key should have expired")
}

// TestExpireAndTTL verifies the EXPIRE and TTL commands.
func TestExpireAndTTL(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	// TTL on missing key → -2
	c.send(t, "TTL", "nokey")
	require.Equal(t, -2, c.readInteger(t))

	// SET without TTL → TTL returns -1
	c.send(t, "SET", "mykey", "val")
	c.readSimpleString(t)

	c.send(t, "TTL", "mykey")
	require.Equal(t, -1, c.readInteger(t))

	// EXPIRE sets a TTL
	c.send(t, "EXPIRE", "mykey", "10")
	require.Equal(t, 1, c.readInteger(t))

	c.send(t, "TTL", "mykey")
	ttl := c.readInteger(t)
	require.True(t, ttl > 0 && ttl <= 10)

	// EXPIRE on missing key → 0
	c.send(t, "EXPIRE", "nokey", "10")
	require.Equal(t, 0, c.readInteger(t))
}

// TestPersist verifies the PERSIST command removes TTL.
func TestPersist(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "SET", "mykey", "val", "EX", "10")
	c.readSimpleString(t)

	c.send(t, "PERSIST", "mykey")
	require.Equal(t, 1, c.readInteger(t))

	c.send(t, "TTL", "mykey")
	require.Equal(t, -1, c.readInteger(t))

	// PERSIST on key without TTL → 0
	c.send(t, "PERSIST", "mykey")
	require.Equal(t, 0, c.readInteger(t))

	// PERSIST on missing key → 0
	c.send(t, "PERSIST", "nokey")
	require.Equal(t, 0, c.readInteger(t))
}

// TestPExpireAndPTTL verifies millisecond-precision TTL.
func TestPExpireAndPTTL(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "SET", "mykey", "val")
	c.readSimpleString(t)

	c.send(t, "PEXPIRE", "mykey", "5000")
	require.Equal(t, 1, c.readInteger(t))

	c.send(t, "PTTL", "mykey")
	pttl := c.readInteger(t)
	require.True(t, pttl > 0 && pttl <= 5000, "expected PTTL in (0,5000], got %d", pttl)
}

// TestSetClearsTTL verifies that a plain SET removes an existing TTL.
func TestSetClearsTTL(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "SET", "mykey", "val", "EX", "10")
	c.readSimpleString(t)

	c.send(t, "TTL", "mykey")
	require.True(t, c.readInteger(t) > 0)

	// Plain SET should clear the TTL.
	c.send(t, "SET", "mykey", "newval")
	c.readSimpleString(t)

	c.send(t, "TTL", "mykey")
	require.Equal(t, -1, c.readInteger(t))
}

// ─── Hash tests ──────────────────────────────────────────────────────────────

func TestHSetAndHGet(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "HSET", "user:1", "name", "Alice", "age", "30")
	require.Equal(t, 2, c.readInteger(t))

	c.send(t, "HGET", "user:1", "name")
	require.Equal(t, []byte("Alice"), c.readBulkString(t))

	c.send(t, "HGET", "user:1", "age")
	require.Equal(t, []byte("30"), c.readBulkString(t))

	// Missing field.
	c.send(t, "HGET", "user:1", "email")
	require.Nil(t, c.readBulkString(t))
}

func TestHSetUpdate(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "HSET", "h", "f", "v1")
	require.Equal(t, 1, c.readInteger(t))

	// Update existing field — returns 0 (no new fields).
	c.send(t, "HSET", "h", "f", "v2")
	require.Equal(t, 0, c.readInteger(t))

	c.send(t, "HGET", "h", "f")
	require.Equal(t, []byte("v2"), c.readBulkString(t))
}

func TestHGetAll(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "HSET", "h", "a", "1", "b", "2")
	c.readInteger(t)

	c.send(t, "HGETALL", "h")
	arr := c.readArray(t)
	require.Equal(t, 4, len(arr)) // 2 field/value pairs

	// Convert to map for order-independent comparison.
	m := make(map[string]string)
	for i := 0; i < len(arr); i += 2 {
		m[string(arr[i])] = string(arr[i+1])
	}
	require.Equal(t, map[string]string{"a": "1", "b": "2"}, m)

	// Empty/missing key.
	c.send(t, "HGETALL", "nokey")
	arr = c.readArray(t)
	require.Equal(t, 0, len(arr))
}

func TestHDel(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "HSET", "h", "a", "1", "b", "2", "c", "3")
	c.readInteger(t)

	c.send(t, "HDEL", "h", "a", "b")
	require.Equal(t, 2, c.readInteger(t))

	c.send(t, "HLEN", "h")
	require.Equal(t, 1, c.readInteger(t))
}

func TestHLen(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "HLEN", "nokey")
	require.Equal(t, 0, c.readInteger(t))

	c.send(t, "HSET", "h", "a", "1", "b", "2")
	c.readInteger(t)

	c.send(t, "HLEN", "h")
	require.Equal(t, 2, c.readInteger(t))
}

func TestHExists(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "HSET", "h", "f", "v")
	c.readInteger(t)

	c.send(t, "HEXISTS", "h", "f")
	require.Equal(t, 1, c.readInteger(t))

	c.send(t, "HEXISTS", "h", "nope")
	require.Equal(t, 0, c.readInteger(t))
}

func TestHKeysAndHVals(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "HSET", "h", "a", "1", "b", "2")
	c.readInteger(t)

	c.send(t, "HKEYS", "h")
	keys := c.readArray(t)
	keyStrs := make([]string, len(keys))
	for i, k := range keys {
		keyStrs[i] = string(k)
	}
	require.ElementsMatch(t, []string{"a", "b"}, keyStrs)

	c.send(t, "HVALS", "h")
	vals := c.readArray(t)
	valStrs := make([]string, len(vals))
	for i, v := range vals {
		valStrs[i] = string(v)
	}
	require.ElementsMatch(t, []string{"1", "2"}, valStrs)
}

func TestWrongType_GetOnHash(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "HSET", "h", "f", "v")
	c.readInteger(t)

	// GET on a hash key should return nil (not WRONGTYPE — matches our store behaviour).
	c.send(t, "GET", "h")
	require.Nil(t, c.readBulkString(t))
}

func TestWrongType_HSetOnString(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "SET", "k", "v")
	c.readSimpleString(t)

	c.send(t, "HSET", "k", "f", "v")
	errMsg := c.readError(t)
	require.Contains(t, errMsg, "WRONGTYPE")
}

func TestHash_ExpireWorks(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "HSET", "h", "f", "v")
	c.readInteger(t)

	c.send(t, "EXPIRE", "h", "1")
	require.Equal(t, 1, c.readInteger(t))

	c.send(t, "TTL", "h")
	ttl := c.readInteger(t)
	require.True(t, ttl > 0 && ttl <= 1)
}

// ─── JSON tests ─────────────────────────────────────────────────────────────

func TestJSONSetAndGet(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "JSON.SET", "user", "$", `{"name":"Alice","age":30}`)
	require.Equal(t, "OK", c.readSimpleString(t))

	c.send(t, "JSON.GET", "user")
	got := c.readBulkString(t)
	require.Contains(t, string(got), `"name":"Alice"`)
	require.Contains(t, string(got), `"age":30`)
}

func TestJSONSetNestedUpdate(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "JSON.SET", "user", "$", `{"name":"Alice","age":30}`)
	c.readSimpleString(t)

	c.send(t, "JSON.SET", "user", "$.age", "31")
	require.Equal(t, "OK", c.readSimpleString(t))

	c.send(t, "JSON.GET", "user", "$.age")
	require.Equal(t, []byte("31"), c.readBulkString(t))
}

func TestJSONGetWithPath(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "JSON.SET", "user", "$", `{"name":"Alice","age":30}`)
	c.readSimpleString(t)

	c.send(t, "JSON.GET", "user", "$.name")
	require.Equal(t, []byte(`"Alice"`), c.readBulkString(t))
}

func TestJSONGetMissingKey(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "JSON.GET", "nokey")
	require.Nil(t, c.readBulkString(t))
}

func TestJSONDel_Field(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "JSON.SET", "user", "$", `{"name":"Alice","age":30}`)
	c.readSimpleString(t)

	c.send(t, "JSON.DEL", "user", "$.age")
	require.Equal(t, 1, c.readInteger(t))

	c.send(t, "JSON.GET", "user")
	got := c.readBulkString(t)
	require.Equal(t, `{"name":"Alice"}`, string(got))
}

func TestJSONDel_Root(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "JSON.SET", "user", "$", `{"name":"Alice"}`)
	c.readSimpleString(t)

	c.send(t, "JSON.DEL", "user")
	require.Equal(t, 1, c.readInteger(t))

	c.send(t, "JSON.GET", "user")
	require.Nil(t, c.readBulkString(t))
}

func TestJSONType(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "JSON.SET", "k", "$", `{"name":"Alice","age":30,"tags":["a"]}`)
	c.readSimpleString(t)

	c.send(t, "JSON.TYPE", "k")
	require.Equal(t, "object", c.readSimpleString(t))

	c.send(t, "JSON.TYPE", "k", "$.name")
	require.Equal(t, "string", c.readSimpleString(t))

	c.send(t, "JSON.TYPE", "k", "$.age")
	require.Equal(t, "number", c.readSimpleString(t))

	c.send(t, "JSON.TYPE", "k", "$.tags")
	require.Equal(t, "array", c.readSimpleString(t))
}

func TestJSONNumIncrBy(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "JSON.SET", "k", "$", `{"counter":10}`)
	c.readSimpleString(t)

	c.send(t, "JSON.NUMINCRBY", "k", "$.counter", "5")
	got := c.readBulkString(t)
	require.Equal(t, "15", string(got))

	c.send(t, "JSON.GET", "k", "$.counter")
	require.Equal(t, []byte("15"), c.readBulkString(t))
}

func TestJSONWrongType(t *testing.T) {
	srv := startServer(t)
	c := dial(t, srv)

	c.send(t, "SET", "k", "v")
	c.readSimpleString(t)

	c.send(t, "JSON.GET", "k")
	errMsg := c.readError(t)
	require.Contains(t, errMsg, "WRONGTYPE")
}
