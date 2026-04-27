// Package server implements the TCP accept loop and per-connection read loop
// for the Valkey server.
//
// Concurrency model
//
//	main goroutine  ──► Server.Start() ──► listener goroutine (acceptLoop)
//	                                             │
//	                                    net.Conn per client
//	                                             │
//	                                    conn goroutine (Conn.serve)
//
// Race-condition protection
//
//   - Server.conns (map[uint64]*Conn) is guarded by Server.mu (RWMutex).
//     Writes (register / unregister) take a full write lock; reads (Len,
//     broadcast) take a read lock.
//   - Conn.nc writes are serialised by Conn.writeMu (Mutex).
//   - Conn.closeOnce (sync.Once) ensures net.Conn.Close is called exactly once
//     regardless of whether shutdown is triggered by the client or the server.
//   - Graceful shutdown uses context cancellation: cancel() signals all
//     goroutines, listener.Close() unblocks Accept(), and Server.wg.Wait()
//     blocks until every goroutine has exited.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valkey/valkey/internal/config"
	"github.com/valkey/valkey/internal/store"
)

// Server is the top-level TCP server.
type Server struct {
	cfg       *config.Config
	listeners []net.Listener
	store     store.Store
	auth      Authenticator

	// conns holds all live client connections. Protected by mu.
	mu    sync.RWMutex
	conns map[uint64]*Conn

	// nextID is an atomically incrementing counter for unique connection IDs.
	nextID atomic.Uint64

	// wg tracks the accept-loop goroutine plus every per-connection goroutine.
	wg sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc

	log *log.Logger
}

// New allocates a Server backed by the default pure-Go store.
func New(cfg *config.Config, logger *log.Logger) *Server {
	return NewWithStore(cfg, logger, store.NewGoStore())
}

// NewWithStore allocates a Server using the provided Store backend.
// Use this to inject the Zig store (or any other Store implementation).
func NewWithStore(cfg *config.Config, logger *log.Logger, st store.Store) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		cfg:    cfg,
		store:  st,
		auth:   NewAuthenticator(&cfg.Security),
		conns:  make(map[uint64]*Conn),
		ctx:    ctx,
		cancel: cancel,
		log:    logger,
	}
}

// Addr returns the address the first listener is bound to.
// Only valid after Start() has returned without error.
func (s *Server) Addr() net.Addr {
	return s.listeners[0].Addr()
}

// TLSAddr returns the address the TLS listener is bound to.
// Only valid when a TLS listener is configured. If there is only one listener
// and it's TLS, this returns the same as Addr().
func (s *Server) TLSAddr() net.Addr {
	if len(s.listeners) > 1 {
		return s.listeners[1].Addr()
	}
	return s.listeners[0].Addr()
}

// Start binds TCP (and optionally TLS) sockets and launches accept loops.
// It returns immediately; the loops run in the background.
func (s *Server) Start() error {
	bindAddr := s.cfg.Network.Bind[0]

	// Plain TCP listener. Port 0 = OS-assigned, Port < 0 = disabled.
	if s.cfg.Network.Port >= 0 {
		addr := fmt.Sprintf("%s:%d", bindAddr, s.cfg.Network.Port)
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("listen %s: %w", addr, err)
		}
		s.listeners = append(s.listeners, ln)
		s.log.Printf("* Listening on %s", ln.Addr())
	}

	// TLS listener. Created when cert/key files are configured.
	// TLSPort 0 = OS-assigned, TLSPort < 0 = disabled.
	if s.cfg.Security.TLSCertFile != "" && s.cfg.Network.TLSPort >= 0 {
		tlsCfg, err := buildTLSConfig(&s.cfg.Security)
		if err != nil {
			return err
		}
		addr := fmt.Sprintf("%s:%d", bindAddr, s.cfg.Network.TLSPort)
		ln, err := tls.Listen("tcp", addr, tlsCfg)
		if err != nil {
			return fmt.Errorf("tls listen %s: %w", addr, err)
		}
		s.listeners = append(s.listeners, ln)
		s.log.Printf("* TLS listening on %s", ln.Addr())
	}

	if len(s.listeners) == 0 {
		return fmt.Errorf("no listeners configured (both port and tls-port are 0)")
	}

	// One accept goroutine per listener, plus the expiration worker.
	s.wg.Add(len(s.listeners) + 1)
	for _, ln := range s.listeners {
		go s.acceptLoop(ln)
	}
	go s.expirationWorker()

	return nil
}

// Stop performs a graceful shutdown:
//  1. Cancels the server context (signals all goroutines).
//  2. Closes the listener (unblocks the Accept call in acceptLoop).
//  3. Closes every active client connection.
//  4. Waits for all goroutines to exit.
func (s *Server) Stop() {
	s.log.Println("* Shutting down…")
	s.cancel()
	for _, ln := range s.listeners {
		ln.Close()
	}

	// Close all live connections so their read loops unblock immediately.
	s.mu.RLock()
	for _, c := range s.conns {
		c.close()
	}
	s.mu.RUnlock()

	s.wg.Wait()
	if err := s.store.Close(); err != nil {
		s.log.Printf("store close: %v", err)
	}
	s.log.Println("* Server stopped")
}

// Len returns the number of currently active connections.
func (s *Server) Len() int {
	s.mu.RLock()
	n := len(s.conns)
	s.mu.RUnlock()
	return n
}

// ─── internal ────────────────────────────────────────────────────────────────

// acceptLoop runs in its own goroutine. It calls Accept in a tight loop,
// spawning a new goroutine per connection. It exits when the listener is
// closed (which sets ctx.Done).
func (s *Server) acceptLoop(ln net.Listener) {
	defer s.wg.Done()

	for {
		nc, err := ln.Accept()
		if err != nil {
			// Distinguish a deliberate close (shutdown) from transient errors.
			select {
			case <-s.ctx.Done():
				return
			default:
			}
			// net.Error.Temporary was deprecated in Go 1.18; check ourselves.
			var netErr net.Error
			if errors.As(err, &netErr) {
				s.log.Printf("accept: transient error: %v", err)
				continue
			}
			s.log.Printf("accept: fatal error: %v", err)
			return
		}

		conn := s.newConn(nc)
		s.register(conn)

		s.wg.Go(func() {
			conn.serve(s.ctx)
			s.unregister(conn.id)
		})
	}
}

func (s *Server) register(c *Conn) {
	s.mu.Lock()
	s.conns[c.id] = c
	s.mu.Unlock()
	s.log.Printf("+ conn %d  %s  (total: %d)", c.id, c.nc.RemoteAddr(), s.Len())
}

func (s *Server) unregister(id uint64) {
	s.mu.Lock()
	delete(s.conns, id)
	n := len(s.conns)
	s.mu.Unlock()
	s.log.Printf("- conn %d  closed  (total: %d)", id, n)
}

// expirationWorker runs in its own goroutine. It periodically samples keys
// with TTLs and deletes any that have expired (active expiration).
// This mirrors Redis's hz=10 approach: 100ms ticks, 20-key samples, adaptive.
func (s *Server) expirationWorker() {
	defer s.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.runExpirationCycle()
		}
	}
}

const (
	expirySampleSize = 20
	expiryThreshold  = 0.25 // loop again if >25% of sample was expired
)

func (s *Server) runExpirationCycle() {
	for {
		deleted := s.store.ExpireN(expirySampleSize)
		if float64(deleted)/float64(expirySampleSize) < expiryThreshold {
			return
		}
		// Check for shutdown between adaptive loops.
		select {
		case <-s.ctx.Done():
			return
		default:
		}
	}
}
