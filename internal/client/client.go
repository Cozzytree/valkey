// Package client provides a Valkey/Redis RESP client.
package client

import (
	"crypto/tls"
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

// Options configures the client connection.
type Options struct {
	// Username is the ACL username for AUTH. Leave empty to use "default".
	Username string
	// Password is sent via AUTH immediately after connecting.
	// Leave empty to skip authentication.
	Password string
	// TLS enables TLS when non-nil. Use &tls.Config{} for default settings
	// or &tls.Config{InsecureSkipVerify: true} for self-signed certs.
	TLS *tls.Config
}

// Dial connects to addr (e.g. "127.0.0.1:6379") and returns a Client.
func Dial(addr string) (*Client, error) {
	return DialWithOptions(addr, Options{})
}

// DialWithOptions connects to addr and applies the given options.
// If a password is set, AUTH is sent automatically after connecting.
func DialWithOptions(addr string, opts Options) (*Client, error) {
	var conn net.Conn
	var err error
	if opts.TLS != nil {
		conn, err = tls.Dial("tcp", addr, opts.TLS)
	} else {
		conn, err = net.Dial("tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	c := &Client{
		conn:   conn,
		reader: proto.NewBufReader(conn),
	}
	if opts.Password != "" {
		var authErr error
		if opts.Username != "" {
			authErr = c.AuthWithUser(opts.Username, opts.Password)
		} else {
			authErr = c.Auth(opts.Password)
		}
		if authErr != nil {
			c.Close()
			return nil, fmt.Errorf("auth: %w", authErr)
		}
	}
	return c, nil
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
