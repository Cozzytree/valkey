package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valkey/valkey/internal/proto"
	"github.com/valkey/valkey/internal/store"
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
			_ = c.WriteRaw(fmt.Appendf(nil, "-ERR Protocol error: %v\r\n", err))
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
	case "EXISTS":
		c.cmdExists(args[1:])
	case "EXPIRE":
		c.cmdExpire(args[1:])
	case "PEXPIRE":
		c.cmdPExpire(args[1:])
	case "TTL":
		c.cmdTTL(args[1:])
	case "PTTL":
		c.cmdPTTL(args[1:])
	case "PERSIST":
		c.cmdPersist(args[1:])
	case "HSET":
		c.cmdHSet(args[1:])
	case "HGET":
		c.cmdHGet(args[1:])
	case "HDEL":
		c.cmdHDel(args[1:])
	case "HGETALL":
		c.cmdHGetAll(args[1:])
	case "HLEN":
		c.cmdHLen(args[1:])
	case "HEXISTS":
		c.cmdHExists(args[1:])
	case "HKEYS":
		c.cmdHKeys(args[1:])
	case "HVALS":
		c.cmdHVals(args[1:])
	case "JSON.SET":
		c.cmdJSONSet(args[1:])
	case "JSON.GET":
		c.cmdJSONGet(args[1:])
	case "JSON.DEL":
		c.cmdJSONDel(args[1:])
	case "JSON.TYPE":
		c.cmdJSONType(args[1:])
	case "JSON.NUMINCRBY":
		c.cmdJSONNumIncrBy(args[1:])
	case "LPUSH":
		c.cmdLPush(args[1:])
	case "RPUSH":
		c.cmdRPush(args[1:])
	case "LPOP":
		c.cmdLPop(args[1:])
	case "RPOP":
		c.cmdRPop(args[1:])
	case "LLEN":
		c.cmdLLen(args[1:])
	case "LRANGE":
		c.cmdLRange(args[1:])
	case "LINDEX":
		c.cmdLIndex(args[1:])
	case "LSET":
		c.cmdLSet(args[1:])
	case "LINSERT":
		c.cmdLInsert(args[1:])
	case "LREM":
		c.cmdLRem(args[1:])
	case "LTRIM":
		c.cmdLTrim(args[1:])
	case "DBSIZE":
		c.cmdDBSize()
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

	// Parse optional trailing arguments: EX seconds | PX milliseconds
	var ttl time.Duration
	i := 2
	for i < len(args) {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "EX":
			if i+1 >= len(args) {
				_ = c.WriteRaw(respErr("ERR syntax error"))
				return
			}
			secs, err := strconv.Atoi(string(args[i+1]))
			if err != nil || secs <= 0 {
				_ = c.WriteRaw(respErr("ERR invalid expire time in 'SET' command"))
				return
			}
			ttl = time.Duration(secs) * time.Second
			i += 2
		case "PX":
			if i+1 >= len(args) {
				_ = c.WriteRaw(respErr("ERR syntax error"))
				return
			}
			ms, err := strconv.Atoi(string(args[i+1]))
			if err != nil || ms <= 0 {
				_ = c.WriteRaw(respErr("ERR invalid expire time in 'SET' command"))
				return
			}
			ttl = time.Duration(ms) * time.Millisecond
			i += 2
		default:
			_ = c.WriteRaw(respErr("ERR syntax error"))
			return
		}
	}

	if ttl > 0 {
		c.srv.store.SetWithTTL(key, val, ttl)
	} else {
		c.srv.store.Set(key, val)
	}
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

func (c *Conn) cmdExists(args [][]byte) {
	if len(args) < 1 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'EXISTS' command"))
		return
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = string(a)
	}
	count := c.srv.store.Exists(keys...)
	_ = c.WriteRaw(fmt.Appendf(nil, ":%d\r\n", count))
}

func (c *Conn) cmdExpire(args [][]byte) {
	if len(args) != 2 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'EXPIRE' command"))
		return
	}
	secs, err := strconv.Atoi(string(args[1]))
	if err != nil || secs <= 0 {
		_ = c.WriteRaw(respErr("ERR invalid expire time in 'EXPIRE' command"))
		return
	}
	ok := c.srv.store.Expire(string(args[0]), time.Duration(secs)*time.Second)
	if ok {
		_ = c.WriteRaw([]byte(":1\r\n"))
	} else {
		_ = c.WriteRaw([]byte(":0\r\n"))
	}
}

func (c *Conn) cmdPExpire(args [][]byte) {
	if len(args) != 2 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'PEXPIRE' command"))
		return
	}
	ms, err := strconv.Atoi(string(args[1]))
	if err != nil || ms <= 0 {
		_ = c.WriteRaw(respErr("ERR invalid expire time in 'PEXPIRE' command"))
		return
	}
	ok := c.srv.store.Expire(string(args[0]), time.Duration(ms)*time.Millisecond)
	if ok {
		_ = c.WriteRaw([]byte(":1\r\n"))
	} else {
		_ = c.WriteRaw([]byte(":0\r\n"))
	}
}

func (c *Conn) cmdTTL(args [][]byte) {
	if len(args) != 1 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'TTL' command"))
		return
	}
	remaining, ok := c.srv.store.TTL(string(args[0]))
	if !ok {
		_ = c.WriteRaw([]byte(":-2\r\n"))
		return
	}
	if remaining == -1 {
		_ = c.WriteRaw([]byte(":-1\r\n"))
		return
	}
	// Ceiling: 999ms → 1s (matches Redis behaviour of rounding up).
	secs := max(int64((remaining+time.Second-1)/time.Second), 0)
	_ = c.WriteRaw(fmt.Appendf(nil, ":%d\r\n", secs))
}

func (c *Conn) cmdPTTL(args [][]byte) {
	if len(args) != 1 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'PTTL' command"))
		return
	}
	remaining, ok := c.srv.store.TTL(string(args[0]))
	if !ok {
		_ = c.WriteRaw([]byte(":-2\r\n"))
		return
	}
	if remaining == -1 {
		_ = c.WriteRaw([]byte(":-1\r\n"))
		return
	}
	ms := max(remaining.Milliseconds(), 0)
	_ = c.WriteRaw([]byte(fmt.Sprintf(":%d\r\n", ms)))
}

func (c *Conn) cmdPersist(args [][]byte) {
	if len(args) != 1 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'PERSIST' command"))
		return
	}
	ok := c.srv.store.Persist(string(args[0]))
	if ok {
		_ = c.WriteRaw([]byte(":1\r\n"))
	} else {
		_ = c.WriteRaw([]byte(":0\r\n"))
	}
}

// ─── hash commands ───────────────────────────────────────────────────────────

func (c *Conn) cmdHSet(args [][]byte) {
	if len(args) < 3 || len(args)%2 == 0 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'HSET' command"))
		return
	}
	key := string(args[0])
	fields := make([]string, len(args)-1)
	for i, a := range args[1:] {
		fields[i] = string(a)
	}
	added, err := c.srv.store.HSet(key, fields...)
	if err != nil {
		c.writeErr(err)
		return
	}
	_ = c.WriteRaw(fmt.Appendf(nil, ":%d\r\n", added))
}

func (c *Conn) cmdHGet(args [][]byte) {
	if len(args) != 2 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'HGET' command"))
		return
	}
	val, ok, err := c.srv.store.HGet(string(args[0]), string(args[1]))
	if err != nil {
		c.writeErr(err)
		return
	}
	if !ok {
		_ = c.WriteRaw([]byte("$-1\r\n"))
		return
	}
	_ = c.WriteRaw(respBulk(val))
}

func (c *Conn) cmdHDel(args [][]byte) {
	if len(args) < 2 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'HDEL' command"))
		return
	}
	fields := make([]string, len(args)-1)
	for i, a := range args[1:] {
		fields[i] = string(a)
	}
	deleted, err := c.srv.store.HDel(string(args[0]), fields...)
	if err != nil {
		c.writeErr(err)
		return
	}
	_ = c.WriteRaw(fmt.Appendf(nil, ":%d\r\n", deleted))
}

func (c *Conn) cmdHGetAll(args [][]byte) {
	if len(args) != 1 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'HGETALL' command"))
		return
	}
	m, err := c.srv.store.HGetAll(string(args[0]))
	if err != nil {
		c.writeErr(err)
		return
	}
	if m == nil {
		_ = c.WriteRaw([]byte("*0\r\n"))
		return
	}
	items := make([][]byte, 0, len(m)*2)
	for k, v := range m {
		items = append(items, []byte(k), v)
	}
	_ = c.WriteRaw(respArray(items))
}

func (c *Conn) cmdHLen(args [][]byte) {
	if len(args) != 1 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'HLEN' command"))
		return
	}
	n, err := c.srv.store.HLen(string(args[0]))
	if err != nil {
		c.writeErr(err)
		return
	}
	_ = c.WriteRaw([]byte(fmt.Sprintf(":%d\r\n", n)))
}

func (c *Conn) cmdHExists(args [][]byte) {
	if len(args) != 2 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'HEXISTS' command"))
		return
	}
	ok, err := c.srv.store.HExists(string(args[0]), string(args[1]))
	if err != nil {
		c.writeErr(err)
		return
	}
	if ok {
		_ = c.WriteRaw([]byte(":1\r\n"))
	} else {
		_ = c.WriteRaw([]byte(":0\r\n"))
	}
}

func (c *Conn) cmdHKeys(args [][]byte) {
	if len(args) != 1 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'HKEYS' command"))
		return
	}
	keys, err := c.srv.store.HKeys(string(args[0]))
	if err != nil {
		c.writeErr(err)
		return
	}
	if keys == nil {
		_ = c.WriteRaw([]byte("*0\r\n"))
		return
	}
	items := make([][]byte, len(keys))
	for i, k := range keys {
		items[i] = []byte(k)
	}
	_ = c.WriteRaw(respArray(items))
}

func (c *Conn) cmdHVals(args [][]byte) {
	if len(args) != 1 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'HVALS' command"))
		return
	}
	vals, err := c.srv.store.HVals(string(args[0]))
	if err != nil {
		c.writeErr(err)
		return
	}
	if vals == nil {
		_ = c.WriteRaw([]byte("*0\r\n"))
		return
	}
	_ = c.WriteRaw(respArray(vals))
}

// ─── List commands ──────────────────────────────────────────────────────────

func (c *Conn) cmdLPush(args [][]byte) {
	if len(args) < 2 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'LPUSH' command"))
		return
	}
	key := string(args[0])
	values := args[1:]
	length, err := c.srv.store.LPush(key, values...)
	if err != nil {
		c.writeErr(err)
		return
	}
	_ = c.WriteRaw(fmt.Appendf(nil, ":%d\r\n", length))
}

func (c *Conn) cmdRPush(args [][]byte) {
	if len(args) < 2 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'RPUSH' command"))
		return
	}
	key := string(args[0])
	values := args[1:]
	length, err := c.srv.store.RPush(key, values...)
	if err != nil {
		c.writeErr(err)
		return
	}
	_ = c.WriteRaw(fmt.Appendf(nil, ":%d\r\n", length))
}

func (c *Conn) cmdLPop(args [][]byte) {
	if len(args) != 1 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'LPOP' command"))
		return
	}
	key := string(args[0])
	elem, ok, err := c.srv.store.LPop(key)
	if err != nil {
		c.writeErr(err)
		return
	}
	if !ok {
		_ = c.WriteRaw([]byte("$-1\r\n"))
		return
	}
	_ = c.WriteRaw(respBulk(elem))
}

func (c *Conn) cmdRPop(args [][]byte) {
	if len(args) != 1 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'RPOP' command"))
		return
	}
	key := string(args[0])
	elem, ok, err := c.srv.store.RPop(key)
	if err != nil {
		c.writeErr(err)
		return
	}
	if !ok {
		_ = c.WriteRaw([]byte("$-1\r\n"))
		return
	}
	_ = c.WriteRaw(respBulk(elem))
}

func (c *Conn) cmdLLen(args [][]byte) {
	if len(args) != 1 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'LLEN' command"))
		return
	}
	key := string(args[0])
	length, err := c.srv.store.LLen(key)
	if err != nil {
		c.writeErr(err)
		return
	}
	_ = c.WriteRaw(fmt.Appendf(nil, ":%d\r\n", length))
}

func (c *Conn) cmdLRange(args [][]byte) {
	if len(args) != 3 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'LRANGE' command"))
		return
	}
	key := string(args[0])
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		_ = c.WriteRaw(respErr("ERR value is not an integer or out of range"))
		return
	}
	stop, err := strconv.Atoi(string(args[2]))
	if err != nil {
		_ = c.WriteRaw(respErr("ERR value is not an integer or out of range"))
		return
	}

	elements, err := c.srv.store.LRange(key, start, stop)
	if err != nil {
		c.writeErr(err)
		return
	}
	if elements == nil {
		_ = c.WriteRaw([]byte("*0\r\n"))
		return
	}
	_ = c.WriteRaw(respArray(elements))
}

func (c *Conn) cmdLIndex(args [][]byte) {
	if len(args) != 2 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'LINDEX' command"))
		return
	}
	key := string(args[0])
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		_ = c.WriteRaw(respErr("ERR value is not an integer or out of range"))
		return
	}

	elem, ok, err := c.srv.store.LIndex(key, index)
	if err != nil {
		c.writeErr(err)
		return
	}
	if !ok {
		_ = c.WriteRaw([]byte("$-1\r\n"))
		return
	}
	_ = c.WriteRaw(respBulk(elem))
}

func (c *Conn) cmdLSet(args [][]byte) {
	if len(args) != 3 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'LSET' command"))
		return
	}
	key := string(args[0])
	index, err := strconv.Atoi(string(args[1]))
	if err != nil {
		_ = c.WriteRaw(respErr("ERR value is not an integer or out of range"))
		return
	}
	value := args[2]

	if err := c.srv.store.LSet(key, index, value); err != nil {
		c.writeErr(err)
		return
	}
	_ = c.WriteRaw([]byte("+OK\r\n"))
}

func (c *Conn) cmdLInsert(args [][]byte) {
	if len(args) != 4 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'LINSERT' command"))
		return
	}
	key := string(args[0])
	direction := strings.ToUpper(string(args[1]))
	var before bool
	switch direction {
	case "BEFORE":
		before = true
	case "AFTER":
		before = false
	default:
		_ = c.WriteRaw(respErr("ERR syntax error"))
		return
	}
	pivot := args[2]
	value := args[3]

	length, err := c.srv.store.LInsert(key, before, pivot, value)
	if err != nil {
		c.writeErr(err)
		return
	}
	_ = c.WriteRaw(fmt.Appendf(nil, ":%d\r\n", length))
}

func (c *Conn) cmdLRem(args [][]byte) {
	if len(args) != 3 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'LREM' command"))
		return
	}
	key := string(args[0])
	count, err := strconv.Atoi(string(args[1]))
	if err != nil {
		_ = c.WriteRaw(respErr("ERR value is not an integer or out of range"))
		return
	}
	value := args[2]

	removed, err := c.srv.store.LRem(key, count, value)
	if err != nil {
		c.writeErr(err)
		return
	}
	_ = c.WriteRaw(fmt.Appendf(nil, ":%d\r\n", removed))
}

func (c *Conn) cmdLTrim(args [][]byte) {
	if len(args) != 3 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'LTRIM' command"))
		return
	}
	key := string(args[0])
	start, err := strconv.Atoi(string(args[1]))
	if err != nil {
		_ = c.WriteRaw(respErr("ERR value is not an integer or out of range"))
		return
	}
	stop, err := strconv.Atoi(string(args[2]))
	if err != nil {
		_ = c.WriteRaw(respErr("ERR value is not an integer or out of range"))
		return
	}

	if err := c.srv.store.LTrim(key, start, stop); err != nil {
		c.writeErr(err)
		return
	}
	_ = c.WriteRaw([]byte("+OK\r\n"))
}

// ─── JSON commands ──────────────────────────────────────────────────────────

func (c *Conn) cmdJSONSet(args [][]byte) {
	if len(args) != 3 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'JSON.SET' command"))
		return
	}
	key := string(args[0])
	path := string(args[1])
	var value any
	if err := json.Unmarshal(args[2], &value); err != nil {
		_ = c.WriteRaw(respErr("ERR invalid JSON: " + err.Error()))
		return
	}
	if err := c.srv.store.JSONSet(key, path, value); err != nil {
		c.writeErr(err)
		return
	}
	_ = c.WriteRaw([]byte("+OK\r\n"))
}

func (c *Conn) cmdJSONGet(args [][]byte) {
	if len(args) < 1 || len(args) > 2 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'JSON.GET' command"))
		return
	}
	key := string(args[0])
	path := "$"
	if len(args) == 2 {
		path = string(args[1])
	}
	val, ok, err := c.srv.store.JSONGet(key, path)
	if err != nil {
		c.writeErr(err)
		return
	}
	if !ok {
		_ = c.WriteRaw([]byte("$-1\r\n"))
		return
	}
	data, err := json.Marshal(val)
	if err != nil {
		_ = c.WriteRaw(respErr("ERR failed to marshal JSON: " + err.Error()))
		return
	}
	_ = c.WriteRaw(respBulk(data))
}

func (c *Conn) cmdJSONDel(args [][]byte) {
	if len(args) < 1 || len(args) > 2 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'JSON.DEL' command"))
		return
	}
	key := string(args[0])
	path := "$"
	if len(args) == 2 {
		path = string(args[1])
	}
	count, err := c.srv.store.JSONDel(key, path)
	if err != nil {
		c.writeErr(err)
		return
	}
	_ = c.WriteRaw(fmt.Appendf(nil, ":%d\r\n", count))
}

func (c *Conn) cmdJSONType(args [][]byte) {
	if len(args) < 1 || len(args) > 2 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'JSON.TYPE' command"))
		return
	}
	key := string(args[0])
	path := "$"
	if len(args) == 2 {
		path = string(args[1])
	}
	typeName, err := c.srv.store.JSONType(key, path)
	if err != nil {
		c.writeErr(err)
		return
	}
	if typeName == "" {
		_ = c.WriteRaw([]byte("$-1\r\n"))
		return
	}
	_ = c.WriteRaw([]byte("+" + typeName + "\r\n"))
}

func (c *Conn) cmdJSONNumIncrBy(args [][]byte) {
	if len(args) != 3 {
		_ = c.WriteRaw(respErr("ERR wrong number of arguments for 'JSON.NUMINCRBY' command"))
		return
	}
	key := string(args[0])
	path := string(args[1])
	n, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		_ = c.WriteRaw(respErr("ERR value is not a valid float"))
		return
	}
	result, err := c.srv.store.JSONNumIncrBy(key, path, n)
	if err != nil {
		c.writeErr(err)
		return
	}
	// Return the result as a bulk string (matching RedisJSON).
	s := strconv.FormatFloat(result, 'f', -1, 64)
	_ = c.WriteRaw(respBulk([]byte(s)))
}

func (c *Conn) cmdDBSize() {
	n := c.srv.store.Len()
	_ = c.WriteRaw(fmt.Appendf(nil, ":%d\r\n", n))
}

// writeErr writes a WRONGTYPE or other store error as a RESP error.
func (c *Conn) writeErr(err error) {
	if errors.Is(err, store.ErrWrongType) {
		_ = c.WriteRaw(respErr(store.ErrWrongType.Error()))
	} else {
		_ = c.WriteRaw(respErr("ERR " + err.Error()))
	}
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

// respArray encodes items as a RESP array of bulk strings.
func respArray(items [][]byte) []byte {
	buf := []byte(fmt.Sprintf("*%d\r\n", len(items)))
	for _, item := range items {
		buf = append(buf, respBulk(item)...)
	}
	return buf
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
