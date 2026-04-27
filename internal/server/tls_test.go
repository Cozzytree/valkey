package server_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/valkey/valkey/internal/config"
	"github.com/valkey/valkey/internal/proto"
	"github.com/valkey/valkey/internal/server"
)

// generateTestCert creates a self-signed certificate and key in dir.
// Returns (certPath, keyPath).
func generateTestCert(t *testing.T, dir string) (string, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")

	certFile, err := os.Create(certPath)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}))
	certFile.Close()

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)
	keyFile, err := os.Create(keyPath)
	require.NoError(t, err)
	require.NoError(t, pem.Encode(keyFile, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	keyFile.Close()

	return certPath, keyPath
}

// startTLSServer starts a server with TLS on an OS-assigned port.
func startTLSServer(t *testing.T, certPath, keyPath string) *server.Server {
	t.Helper()

	cfg := config.DefaultConfig()
	cfg.Network.Port = -1 // disable plain TCP
	cfg.Security.TLSCertFile = certPath
	cfg.Security.TLSKeyFile = keyPath

	srv := server.New(cfg, log.New(io.Discard, "", 0))
	require.NoError(t, srv.Start())
	t.Cleanup(srv.Stop)
	return srv
}

// TestTLSPingPong verifies that a TLS connection can send PING and receive PONG.
func TestTLSPingPong(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	srv := startTLSServer(t, certPath, keyPath)

	tlsCfg := &tls.Config{InsecureSkipVerify: true}
	conn, err := tls.Dial("tcp", srv.Addr().String(), tlsCfg)
	require.NoError(t, err)
	defer conn.Close()

	c := &client{conn: conn, reader: proto.NewBufReader(conn)}

	c.send(t, "PING")
	reply := c.readSimpleString(t)
	require.Equal(t, "PONG", reply)
}

// TestTLSSetGet verifies SET and GET work over TLS.
func TestTLSSetGet(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	srv := startTLSServer(t, certPath, keyPath)

	tlsCfg := &tls.Config{InsecureSkipVerify: true}
	conn, err := tls.Dial("tcp", srv.Addr().String(), tlsCfg)
	require.NoError(t, err)
	defer conn.Close()

	c := &client{conn: conn, reader: proto.NewBufReader(conn)}

	c.send(t, "SET", "tls-key", "tls-value")
	require.Equal(t, "OK", c.readSimpleString(t))

	c.send(t, "GET", "tls-key")
	require.Equal(t, []byte("tls-value"), c.readBulkString(t))
}

// TestDualListeners verifies both plain TCP and TLS work simultaneously.
func TestDualListeners(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := generateTestCert(t, dir)

	cfg := config.DefaultConfig()
	cfg.Network.Port = 0    // OS-assigned plain TCP
	cfg.Network.TLSPort = 0 // OS-assigned TLS
	cfg.Security.TLSCertFile = certPath
	cfg.Security.TLSKeyFile = keyPath

	srv := server.New(cfg, log.New(io.Discard, "", 0))
	require.NoError(t, srv.Start())
	t.Cleanup(srv.Stop)

	// Plain TCP connection.
	plainConn, err := net.Dial("tcp", srv.Addr().String())
	require.NoError(t, err)
	defer plainConn.Close()

	pc := &client{conn: plainConn, reader: proto.NewBufReader(plainConn)}
	pc.send(t, "SET", "plain-key", "plain-value")
	require.Equal(t, "OK", pc.readSimpleString(t))

	// TLS connection.
	tlsCfg := &tls.Config{InsecureSkipVerify: true}
	tlsConn, err := tls.Dial("tcp", srv.TLSAddr().String(), tlsCfg)
	require.NoError(t, err)
	defer tlsConn.Close()

	tc := &client{conn: tlsConn, reader: proto.NewBufReader(tlsConn)}

	// Should be able to read the key set via plain TCP (same store).
	tc.send(t, "GET", "plain-key")
	require.Equal(t, []byte("plain-value"), tc.readBulkString(t))
}
