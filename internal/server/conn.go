package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/valkey/valkey/internal/proto"
)

// connState tracks the lifecycle phase of a connection.
type connState uint32

const (
	connStateNew    connState = iota // just accepted, not yet reading
	connStateActive                  // read loop running
	connStateClosed                  // closed, goroutine exiting
)

// Conn represents a single accepted client connection.
type Conn struct {
	// id is unique within the server lifetime, assigned atomically.
	id uint64

	// nc is the underlying TCP connection.
	nc net.Conn

	// reader is the per-connection RESP-aware buffered reader.
	// Its buffer size is cfg.Network.ReadBufSize.
	reader *proto.BufReader

	// srv is a back-pointer used for logging and config access.
	srv *Server

	// writeMu serialises writes to nc so that concurrent response writers
	// (e.g. pub/sub push + a command reply) cannot interleave bytes.
	writeMu sync.Mutex

	// state is read/written atomically to allow lock-free status checks.
	state atomic.Uint32

	// closeOnce guarantees net.Conn.Close is called exactly once, whether
	// the close is triggered by the client, a server shutdown, or an error.
	closeOnce sync.Once
}

// newConn allocates a Conn, wiring up the BufReader with the config-driven
// buffer size and maximum inline command length.
func (s *Server) newConn(nc net.Conn) *Conn {
	id := s.nextID.Add(1)
	r := proto.NewBufReader(
		nc,
		proto.WithBufSize(s.cfg.Network.ReadBufSize),
		proto.WithChunkSize(s.cfg.Network.ReadBufSize),
		proto.WithMaxLineLen(s.cfg.Network.MaxInlineSize),
	)
	return &Conn{
		id:     id,
		nc:     nc,
		reader: r,
		srv:    s,
	}
}

// close shuts down the underlying TCP connection exactly once.
func (c *Conn) close() {
	c.closeOnce.Do(func() {
		c.state.Store(uint32(connStateClosed))
		c.nc.Close()
	})
}

// WriteRaw writes b to the client, holding writeMu for the duration.
// It is safe to call from multiple goroutines concurrently.
func (c *Conn) WriteRaw(b []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err := c.nc.Write(b)
	return err
}

// serve is the per-connection read loop.  It runs in its own goroutine and
// returns when:
//   - the client closes the connection (EOF / read error), or
//   - ctx is cancelled (server shutdown).
//
// It reads from the BufReader in a tight loop.  Each iteration:
//  1. Peeks at the first byte to identify the RESP type prefix.
//  2. Dispatches to the appropriate reader (array/bulk/inline).
//  3. Hands the parsed request to the command handler (stub).
func (c *Conn) serve(ctx context.Context) {
	defer c.close()

	c.state.Store(uint32(connStateActive))

	// Watch for server-level shutdown and close nc so any blocking read
	// returns immediately.  The goroutine exits when serve() returns and
	// calls cancel (via context propagation), so there is no leak.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-ctx.Done()
		c.close() // idempotent — safe if connection already closed
	}()

	for {
		// ── 1. Peek at the RESP type byte ─────────────────────────────────
		typeByte, err := c.reader.PeekByte()
		if err != nil {
			if isClosedErr(err) {
				return
			}
			c.srv.log.Printf("conn %d: peek: %v", c.id, err)
			return
		}

		respType := proto.RESPType(typeByte)

		// ── 2. Read the full request ──────────────────────────────────────
		var req [][]byte
		switch respType {
		case proto.TypeArray:
			// Standard RESP2/3 command: *<count>\r\n $<len>\r\n<data>\r\n ...
			req, err = c.readArray()
		default:
			// Inline command (plain text, e.g. from telnet): PING\r\n
			req, err = c.readInline()
		}

		if err != nil {
			if isClosedErr(err) {
				return
			}
			c.srv.log.Printf("conn %d: read request: %v", c.id, err)
			// Send a protocol error back to the client.
			_ = c.WriteRaw([]byte(fmt.Sprintf("-ERR Protocol error: %v\r\n", err)))
			return
		}

		// ── 3. Dispatch (stub) ────────────────────────────────────────────
		// TODO: replace with real command handler.
		c.handleRequest(req)
	}
}

// ─── RESP readers ─────────────────────────────────────────────────────────────

// readArray reads a RESP array of bulk strings:
//
//	*<count>\r\n
//	$<len>\r\n<data>\r\n   (repeated count times)
func (c *Conn) readArray() ([][]byte, error) {
	// Consume the '*' prefix byte.
	if _, err := c.reader.ReadByte(); err != nil {
		return nil, err
	}

	// Read the element count line.
	line, err := c.reader.ReadLine()
	if err != nil {
		return nil, err
	}
	count, err := parseInt(line)
	if err != nil {
		return nil, fmt.Errorf("invalid array count: %w", err)
	}
	if count < 0 {
		return nil, nil // RESP null array
	}
	if count == 0 {
		return [][]byte{}, nil
	}

	elems := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		elem, err := c.readBulkString()
		if err != nil {
			return nil, fmt.Errorf("element %d: %w", i, err)
		}
		elems = append(elems, elem)
	}
	return elems, nil
}

// readBulkString reads a single RESP bulk string:
//
//	$<len>\r\n<data>\r\n
func (c *Conn) readBulkString() ([]byte, error) {
	prefix, err := c.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if prefix != '$' {
		return nil, fmt.Errorf("expected '$', got %q", prefix)
	}

	line, err := c.reader.ReadLine()
	if err != nil {
		return nil, err
	}
	length, err := parseInt(line)
	if err != nil {
		return nil, fmt.Errorf("invalid bulk string length: %w", err)
	}
	if length < 0 {
		return nil, nil // null bulk string
	}

	// ReadN guarantees exactly `length` bytes.
	data, err := c.reader.ReadN(length)
	if err != nil {
		return nil, err
	}

	// Consume the trailing \r\n after the payload.
	if _, err := c.reader.Discard(2); err != nil {
		return nil, fmt.Errorf("discard CRLF: %w", err)
	}
	return data, nil
}

// readInline reads a single whitespace-delimited inline command, e.g.:
//
//	PING\r\n
//	SET key value\r\n
func (c *Conn) readInline() ([][]byte, error) {
	line, err := c.reader.ReadLine()
	if err != nil {
		return nil, err
	}
	return splitInline(line), nil
}

// ─── command dispatch ─────────────────────────────────────────────────────────

// handleRequest routes a parsed RESP command to the correct handler.
// args[0] is always the command name (case-insensitive).
func (c *Conn) handleRequest(args [][]byte) {
	if len(args) == 0 {
		return
	}

	cmd := strings.ToUpper(string(args[0]))
	c.srv.log.Printf("conn %d: %s args=%d", c.id, cmd, len(args)-1)

	switch cmd {
	case "PING":
		c.cmdPing(args[1:])
	case "SET":
		c.cmdSet(args[1:])
	case "GET":
		c.cmdGet(args[1:])
	case "DEL":
		c.cmdDel(args[1:])
	default:
		_ = c.WriteRaw(respErr("ERR unknown command '" + cmd + "'"))
	}
}

func (c *Conn) cmdPing(args [][]byte) {
	if len(args) == 0 {
		_ = c.WriteRaw([]byte("+PONG\r\n"))
		return
	}
	_ = c.WriteRaw(respBulk(args[0]))
}

func (c *Conn) cmdSet(args [][]byte) {
	if len(args) < 2 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'SET' command"))
		return
	}
	key := string(args[0])
	// Copy value so the store owns its own slice independent of the read buffer.
	val := make([]byte, len(args[1]))
	copy(val, args[1])

	c.srv.store.Set(key, val)
	_ = c.WriteRaw([]byte("+OK\r\n"))
}

func (c *Conn) cmdGet(args [][]byte) {
	if len(args) != 1 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'GET' command"))
		return
	}
	val, ok := c.srv.store.Get(string(args[0]))
	if !ok {
		_ = c.WriteRaw([]byte("$-1\r\n")) // RESP null bulk string
		return
	}
	_ = c.WriteRaw(respBulk(val))
}

// cmdDel deletes one or more keys and returns the count of keys that existed.
func (c *Conn) cmdDel(args [][]byte) {
	if len(args) < 1 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'DEL' command"))
		return
	}
	deleted := int64(0)
	for _, k := range args {
		if c.srv.store.Del(string(k)) {
			deleted++
		}
	}
	_ = c.WriteRaw([]byte(fmt.Sprintf(":%d\r\n", deleted)))
}

// ─── RESP response builders ───────────────────────────────────────────────────

// respBulk encodes v as a RESP bulk string: $<len>\r\n<data>\r\n
func respBulk(v []byte) []byte {
	prefix := fmt.Sprintf("$%d\r\n", len(v))
	out := make([]byte, len(prefix)+len(v)+2)
	n := copy(out, prefix)
	n += copy(out[n:], v)
	copy(out[n:], "\r\n")
	return out
}

// respErr encodes msg as a RESP simple error: -<msg>\r\n
func respErr(msg string) []byte {
	return []byte("-" + msg + "\r\n")
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// parseInt parses a signed decimal integer from a byte slice.
func parseInt(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("empty integer")
	}
	n := 0
	neg := false
	i := 0
	if b[0] == '-' {
		neg = true
		i = 1
	}
	for ; i < len(b); i++ {
		d := b[i]
		if d < '0' || d > '9' {
			return 0, fmt.Errorf("non-digit %q in integer", d)
		}
		n = n*10 + int(d-'0')
	}
	if neg {
		return -n, nil
	}
	return n, nil
}

// splitInline tokenises a line by ASCII whitespace.
func splitInline(line []byte) [][]byte {
	var tokens [][]byte
	start := -1
	for i, b := range line {
		switch b {
		case ' ', '\t':
			if start >= 0 {
				tokens = append(tokens, line[start:i])
				start = -1
			}
		default:
			if start < 0 {
				start = i
			}
		}
	}
	if start >= 0 {
		tokens = append(tokens, line[start:])
	}
	return tokens
}

// isClosedErr reports whether err signals a closed/EOF connection.
func isClosedErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, proto.ErrUnexpectedEOF) {
		return true
	}
	// net.ErrClosed is returned when we explicitly close the connection.
	return errors.Is(err, net.ErrClosed)
}
