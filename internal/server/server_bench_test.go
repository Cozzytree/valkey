package server_test

import (
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/valkey/valkey/internal/config"
	"github.com/valkey/valkey/internal/proto"
	"github.com/valkey/valkey/internal/server"
)

// ─── helpers ────────────────────────────────────────────────────────────────

func startBenchServer(b *testing.B) *server.Server {
	b.Helper()
	cfg := config.DefaultConfig()
	cfg.Network.Port = 0
	srv := server.New(cfg, log.New(io.Discard, "", 0))
	if err := srv.Start(); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(srv.Stop)
	return srv
}

// rawConn dials the server and returns a raw net.Conn + BufReader.
func rawConn(b *testing.B, srv *server.Server) (net.Conn, *proto.BufReader) {
	b.Helper()
	nc, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { nc.Close() })
	return nc, proto.NewBufReader(nc)
}

// respCmd builds a RESP array command.
func respCmd(args ...string) []byte {
	var buf strings.Builder
	fmt.Fprintf(&buf, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&buf, "$%d\r\n%s\r\n", len(a), a)
	}
	return []byte(buf.String())
}

// discardReply reads and discards one RESP reply (simple string, bulk string,
// integer, or error).
func discardReply(r *proto.BufReader) error {
	prefix, err := r.ReadByte()
	if err != nil {
		return err
	}
	switch prefix {
	case '+', '-', ':':
		_, err = r.ReadLine()
		return err
	case '$':
		line, err := r.ReadLine()
		if err != nil {
			return err
		}
		n := 0
		neg := false
		i := 0
		if len(line) > 0 && line[0] == '-' {
			neg = true
			i = 1
		}
		for ; i < len(line); i++ {
			n = n*10 + int(line[i]-'0')
		}
		if neg {
			return nil // null bulk string
		}
		_, err = r.Discard(n + 2) // data + \r\n
		return err
	default:
		return fmt.Errorf("unexpected RESP prefix %q", prefix)
	}
}

// ─── single-connection throughput ───────────────────────────────────────────

func BenchmarkServer_SET(b *testing.B) {
	srv := startBenchServer(b)
	nc, r := rawConn(b, srv)

	cmd := respCmd("SET", "bench-key", "bench-value-data")

	for b.Loop() {
		if _, err := nc.Write(cmd); err != nil {
			b.Fatal(err)
		}
		if err := discardReply(r); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkServer_GET_Hit(b *testing.B) {
	srv := startBenchServer(b)
	nc, r := rawConn(b, srv)

	// Pre-populate.
	nc.Write(respCmd("SET", "bench-key", "bench-value-data"))
	discardReply(r)

	cmd := respCmd("GET", "bench-key")

	for b.Loop() {
		if _, err := nc.Write(cmd); err != nil {
			b.Fatal(err)
		}
		if err := discardReply(r); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkServer_GET_Miss(b *testing.B) {
	srv := startBenchServer(b)
	nc, r := rawConn(b, srv)

	cmd := respCmd("GET", "nonexistent")

	for b.Loop() {
		if _, err := nc.Write(cmd); err != nil {
			b.Fatal(err)
		}
		if err := discardReply(r); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkServer_SET_WithEX(b *testing.B) {
	srv := startBenchServer(b)
	nc, r := rawConn(b, srv)

	cmd := respCmd("SET", "bench-key", "bench-value-data", "EX", "60")

	for b.Loop() {
		if _, err := nc.Write(cmd); err != nil {
			b.Fatal(err)
		}
		if err := discardReply(r); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkServer_EXPIRE(b *testing.B) {
	srv := startBenchServer(b)
	nc, r := rawConn(b, srv)

	nc.Write(respCmd("SET", "bench-key", "val"))
	discardReply(r)

	cmd := respCmd("EXPIRE", "bench-key", "60")

	for b.Loop() {
		if _, err := nc.Write(cmd); err != nil {
			b.Fatal(err)
		}
		if err := discardReply(r); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkServer_TTL(b *testing.B) {
	srv := startBenchServer(b)
	nc, r := rawConn(b, srv)

	nc.Write(respCmd("SET", "bench-key", "val", "EX", "3600"))
	discardReply(r)

	cmd := respCmd("TTL", "bench-key")

	for b.Loop() {
		if _, err := nc.Write(cmd); err != nil {
			b.Fatal(err)
		}
		if err := discardReply(r); err != nil {
			b.Fatal(err)
		}
	}
}

// ─── concurrent connections ─────────────────────────────────────────────────

func BenchmarkServer_ConcurrentSET(b *testing.B) {
	for _, conns := range []int{10, 50, 100, 200} {
		b.Run(fmt.Sprintf("conns=%d", conns), func(b *testing.B) {
			srv := startBenchServer(b)
			cmd := respCmd("SET", "k", "v")
			opsPerConn := max(b.N/conns, 1)

			b.ResetTimer()
			var wg sync.WaitGroup
			wg.Add(conns)
			for range conns {
				go func() {
					defer wg.Done()
					nc, err := net.Dial("tcp", srv.Addr().String())
					if err != nil {
						b.Error(err)
						return
					}
					defer nc.Close()
					r := proto.NewBufReader(nc)
					for range opsPerConn {
						if _, err := nc.Write(cmd); err != nil {
							return
						}
						if err := discardReply(r); err != nil {
							return
						}
					}
				}()
			}
			wg.Wait()
		})
	}
}

func BenchmarkServer_ConcurrentGET(b *testing.B) {
	for _, conns := range []int{10, 50, 100, 200} {
		b.Run(fmt.Sprintf("conns=%d", conns), func(b *testing.B) {
			srv := startBenchServer(b)

			// Pre-populate.
			nc, r := rawConn(b, srv)
			nc.Write(respCmd("SET", "k", "value-data"))
			discardReply(r)

			cmd := respCmd("GET", "k")
			opsPerConn := max(b.N/conns, 1)

			b.ResetTimer()
			var wg sync.WaitGroup
			wg.Add(conns)
			for range conns {
				go func() {
					defer wg.Done()
					nc, err := net.Dial("tcp", srv.Addr().String())
					if err != nil {
						b.Error(err)
						return
					}
					defer nc.Close()
					r := proto.NewBufReader(nc)
					for range opsPerConn {
						if _, err := nc.Write(cmd); err != nil {
							return
						}
						if err := discardReply(r); err != nil {
							return
						}
					}
				}()
			}
			wg.Wait()
		})
	}
}

func BenchmarkServer_ConcurrentMixed(b *testing.B) {
	for _, conns := range []int{10, 50, 100} {
		b.Run(fmt.Sprintf("conns=%d", conns), func(b *testing.B) {
			srv := startBenchServer(b)
			setCmd := respCmd("SET", "k", "value-data")
			getCmd := respCmd("GET", "k")
			opsPerConn := max(b.N/conns, 1)

			b.ResetTimer()
			var wg sync.WaitGroup
			wg.Add(conns)
			for c := range conns {
				c := c
				go func() {
					defer wg.Done()
					nc, err := net.Dial("tcp", srv.Addr().String())
					if err != nil {
						b.Error(err)
						return
					}
					defer nc.Close()
					r := proto.NewBufReader(nc)
					for i := range opsPerConn {
						cmd := getCmd
						if (i+c)%4 == 0 {
							cmd = setCmd // 25% writes
						}
						if _, err := nc.Write(cmd); err != nil {
							return
						}
						if err := discardReply(r); err != nil {
							return
						}
					}
				}()
			}
			wg.Wait()
		})
	}
}

// ─── connection setup/teardown ──────────────────────────────────────────────

func BenchmarkServer_ConnectDisconnect(b *testing.B) {
	srv := startBenchServer(b)
	addr := srv.Addr().String()

	for b.Loop() {
		nc, err := net.Dial("tcp", addr)
		if err != nil {
			b.Fatal(err)
		}
		nc.Close()
	}
	// Let server clean up.
	time.Sleep(10 * time.Millisecond)
}

// ─── pipeline throughput (multiple commands per write) ───────────────────────

func BenchmarkServer_Pipeline(b *testing.B) {
	for _, depth := range []int{1, 10, 50, 100} {
		b.Run(fmt.Sprintf("depth=%d", depth), func(b *testing.B) {
			srv := startBenchServer(b)
			nc, r := rawConn(b, srv)

			// Build a pipeline of SET commands.
			single := respCmd("SET", "pk", "pv")
			pipeline := make([]byte, 0, len(single)*depth)
			for range depth {
				pipeline = append(pipeline, single...)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := nc.Write(pipeline); err != nil {
					b.Fatal(err)
				}
				for range depth {
					if err := discardReply(r); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

// ─── max concurrent connections stress test ─────────────────────────────────

func BenchmarkServer_MaxConnections(b *testing.B) {
	for _, maxConns := range []int{100, 500, 1000} {
		b.Run(fmt.Sprintf("conns=%d", maxConns), func(b *testing.B) {
			srv := startBenchServer(b)
			addr := srv.Addr().String()

			for iter := 0; iter < b.N; iter++ {
				conns := make([]net.Conn, 0, maxConns)
				for i := range maxConns {
					nc, err := net.Dial("tcp", addr)
					if err != nil {
						b.Logf("connected %d/%d before error: %v", i, maxConns, err)
						break
					}
					conns = append(conns, nc)
				}
				require.GreaterOrEqual(b, len(conns), maxConns*9/10,
					"should connect at least 90%% of target")

				// Each connection does a PING to verify it's alive.
				pingCmd := respCmd("PING")
				for _, nc := range conns {
					nc.Write(pingCmd)
				}
				for _, nc := range conns {
					r := proto.NewBufReader(nc)
					discardReply(r)
				}

				// Teardown.
				for _, nc := range conns {
					nc.Close()
				}
			}
			time.Sleep(20 * time.Millisecond)
		})
	}
}
