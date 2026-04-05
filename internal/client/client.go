// Package client provides a Valkey/Redis RESP client.
package client

import (
	"fmt"
	"net"
	"sync"

	"github.com/valkey/valkey/internal/proto"
)

// Client is a single-connection RESP client.
// Do() is safe to call from multiple goroutines; requests are serialised.
type Client struct {
	conn   net.Conn
	reader *proto.BufReader
	mu     sync.Mutex // serialises the write+read pair
}

// Dial connects to addr (e.g. "127.0.0.1:6379") and returns a Client.
func Dial(addr string) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return &Client{
		conn:   conn,
		reader: proto.NewBufReader(conn),
	}, nil
}

// Do sends a command (encoded as a RESP array) and returns the parsed response.
// The call blocks until the full response is received.
func (c *Client) Do(args ...string) (*Value, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.writeCommand(args); err != nil {
		return nil, err
	}
	return readValue(c.reader)
}

// Close closes the underlying TCP connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// Addr returns the remote address of the server.
func (c *Client) Addr() string {
	return c.conn.RemoteAddr().String()
}

// writeCommand encodes args as a RESP array and writes it to the connection.
//
//	*<n>\r\n
//	$<len>\r\n<arg>\r\n  (× n)
func (c *Client) writeCommand(args []string) error {
	// Pre-allocate a single buffer to avoid multiple small writes.
	buf := make([]byte, 0, 64)
	buf = fmt.Appendf(buf, "*%d\r\n", len(args))
	for _, a := range args {
		buf = fmt.Appendf(buf, "$%d\r\n%s\r\n", len(a), a)
	}
	_, err := c.conn.Write(buf)
	return err
}
