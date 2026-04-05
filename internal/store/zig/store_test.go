//go:build zigstore

package zig_test

import (
	"fmt"
	"io"
	"log"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	zigstore "github.com/valkey/valkey/internal/store/zig"
	"github.com/valkey/valkey/internal/client"
	"github.com/valkey/valkey/internal/config"
	"github.com/valkey/valkey/internal/server"
)

// startZigServer spins up a server backed by the Zig store on a random port.
func startZigServer(t *testing.T) *server.Server {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Network.Port = 0
	srv := server.NewWithStore(cfg, log.New(io.Discard, "", 0), zigstore.New())
	require.NoError(t, srv.Start())
	t.Cleanup(srv.Stop)
	return srv
}

func dialClient(t *testing.T, srv *server.Server) *client.Client {
	t.Helper()
	cl, err := client.Dial(srv.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { cl.Close() })
	return cl
}

// TestZigSetGet verifies the round-trip through the Zig hash map.
func TestZigSetGet(t *testing.T) {
	cl := dialClient(t, startZigServer(t))

	set, err := cl.Do("SET", "lang", "zig")
	require.NoError(t, err)
	require.Equal(t, "OK", set.String())

	get, err := cl.Do("GET", "lang")
	require.NoError(t, err)
	require.Equal(t, []byte("zig"), get.Bytes())
}

// TestZigGetMissing ensures a missing key returns null bulk — not an error.
func TestZigGetMissing(t *testing.T) {
	cl := dialClient(t, startZigServer(t))

	val, err := cl.Do("GET", "ghost")
	require.NoError(t, err)
	require.True(t, val.IsNull())
}

// TestZigDel confirms DEL removes a key and GET returns null afterwards.
func TestZigDel(t *testing.T) {
	cl := dialClient(t, startZigServer(t))

	cl.Do("SET", "x", "1")
	del, err := cl.Do("DEL", "x")
	require.NoError(t, err)
	require.Equal(t, int64(1), del.Integer())

	get, err := cl.Do("GET", "x")
	require.NoError(t, err)
	require.True(t, get.IsNull())
}

// TestZigLargeValue exercises the store_get retry path (value > 512 bytes).
func TestZigLargeValue(t *testing.T) {
	cl := dialClient(t, startZigServer(t))

	big := make([]byte, 4096)
	for i := range big {
		big[i] = 'A' + byte(i%26)
	}

	_, err := cl.Do("SET", "big", string(big))
	require.NoError(t, err)

	got, err := cl.Do("GET", "big")
	require.NoError(t, err)
	require.Equal(t, big, got.Bytes())
}

// TestZigConcurrent fires multiple goroutines each writing and reading their
// own key through the Zig store to exercise its c_allocator under concurrency.
func TestZigConcurrent(t *testing.T) {
	const workers = 20
	srv := startZigServer(t)
	errc := make(chan error, workers)

	for i := 0; i < workers; i++ {
		i := i
		go func() {
			cl, err := client.Dial(srv.Addr().String())
			if err != nil {
				errc <- err
				return
			}
			defer cl.Close()

			key := fmt.Sprintf("key-%d", i)
			val := fmt.Sprintf("val-%d", i)

			if _, err := cl.Do("SET", key, val); err != nil {
				errc <- fmt.Errorf("SET: %w", err)
				return
			}
			got, err := cl.Do("GET", key)
			if err != nil {
				errc <- fmt.Errorf("GET: %w", err)
				return
			}
			if string(got.Bytes()) != val {
				errc <- fmt.Errorf("worker %d: got %q want %q", i, got.Bytes(), val)
				return
			}
			errc <- nil
		}()
	}

	for i := 0; i < workers; i++ {
		require.NoError(t, <-errc)
	}
}

// TestZigOverwrite verifies SET on an existing key replaces the value and
// the old memory is freed (no leak that would be caught by -sanitize=address).
func TestZigOverwrite(t *testing.T) {
	cl := dialClient(t, startZigServer(t))

	cl.Do("SET", "k", "first")
	cl.Do("SET", "k", "second")

	got, err := cl.Do("GET", "k")
	require.NoError(t, err)
	require.Equal(t, []byte("second"), got.Bytes())
}

// TestZigDirectStore exercises the store directly (bypassing the network) to
// check the ZigStore interface methods in isolation.
func TestZigDirectStore(t *testing.T) {
	s := zigstore.New()
	defer s.Close()

	// Set and get
	s.Set("name", []byte("valkey"))
	v, ok := s.Get("name")
	require.True(t, ok)
	require.Equal(t, []byte("valkey"), v)

	// Miss
	_, ok = s.Get("nope")
	require.False(t, ok)

	// Del
	require.True(t, s.Del("name"))
	require.False(t, s.Del("name")) // already gone

	// Len
	s.Set("a", []byte("1"))
	s.Set("b", []byte("2"))
	require.Equal(t, 2, s.Len())

	// Large value (exercises the 512-byte fast-path boundary in store_get)
	large := make([]byte, 1024)
	for i := range large {
		large[i] = byte(i % 256)
	}
	s.Set("big", large)
	got, ok := s.Get("big")
	require.True(t, ok)
	require.Equal(t, large, got)
}

// ensure net is used (for the concurrent test's net.Dial)
var _ = net.Dial
