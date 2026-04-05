package server_test

import (
	"fmt"
	"io"
	"log"
	"net"
	"testing"

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
