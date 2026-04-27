package server

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/valkey/valkey/internal/config"
)

// buildTLSConfig creates a *tls.Config from the SecurityConfig fields.
// Returns (nil, nil) if TLSCertFile is empty (TLS not configured).
func buildTLSConfig(sec *config.SecurityConfig) (*tls.Config, error) {
	if sec.TLSCertFile == "" && sec.TLSKeyFile == "" {
		return nil, nil
	}
	if sec.TLSCertFile == "" {
		return nil, fmt.Errorf("tls-key-file set without tls-cert-file")
	}
	if sec.TLSKeyFile == "" {
		return nil, fmt.Errorf("tls-cert-file set without tls-key-file")
	}

	cert, err := tls.LoadX509KeyPair(sec.TLSCertFile, sec.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: load cert/key: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
	}

	if sec.TLSCACertFile != "" {
		caCert, err := os.ReadFile(sec.TLSCACertFile)
		if err != nil {
			return nil, fmt.Errorf("tls: read CA cert: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("tls: failed to parse CA certificate")
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsCfg, nil
}
