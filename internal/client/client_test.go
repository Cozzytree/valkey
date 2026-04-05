package client_test

import (
	"io"
	"log"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/valkey/valkey/internal/client"
	"github.com/valkey/valkey/internal/config"
	"github.com/valkey/valkey/internal/server"
)

// startServer spins up a real Valkey server on a random port.
func startServer(t *testing.T) *server.Server {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Network.Port = 0
	srv := server.New(cfg, log.New(io.Discard, "", 0))
	require.NoError(t, srv.Start())
	t.Cleanup(srv.Stop)
	return srv
}

// dial connects a Client to srv.
func dial(t *testing.T, srv *server.Server) *client.Client {
	t.Helper()
	cl, err := client.Dial(srv.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { cl.Close() })
	return cl
}

// TestPingPong verifies the most basic round-trip: PING → PONG.
func TestPingPong(t *testing.T) {
	cl := dial(t, startServer(t))

	val, err := cl.Do("PING")
	require.NoError(t, err)
	require.False(t, val.IsError())
	require.Equal(t, "PONG", val.String())
}

// TestSetGet is the fundamental key-value round-trip through the real stack:
// Client encodes RESP → Server parses + stores → Client reads response.
func TestSetGet(t *testing.T) {
	cl := dial(t, startServer(t))

	set, err := cl.Do("SET", "language", "go")
	require.NoError(t, err)
	require.Equal(t, "OK", set.String())

	get, err := cl.Do("GET", "language")
	require.NoError(t, err)
	require.Equal(t, []byte("go"), get.Bytes())
}

// TestGetMissing verifies that GET on an unknown key returns a null bulk string,
// not an error — matching Redis semantics.
func TestGetMissing(t *testing.T) {
	cl := dial(t, startServer(t))

	val, err := cl.Do("GET", "no-such-key")
	require.NoError(t, err)
	require.True(t, val.IsNull(), "expected null bulk string for missing key")
}
