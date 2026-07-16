// Package claude provides authentication functionality for Anthropic's Claude API.
// This file implements a custom HTTP transport using utls to bypass TLS fingerprinting.
package claude

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	tls "github.com/refraction-networking/utls"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/http2"
)

const claudeUTLSConnectTimeout = 30 * time.Second

// utlsRoundTripper implements http.RoundTripper using utls with Chrome fingerprint
// to bypass Cloudflare's TLS fingerprinting on Anthropic domains.
type utlsRoundTripper struct {
	// mu protects the connections map and pending map
	mu sync.Mutex
	// connections caches HTTP/2 client connections per host
	connections map[string]*http2.ClientConn
	// pending tracks hosts that are currently being connected to (prevents race condition)
	pending map[string]chan struct{}
	// dialer is used to create network connections, supporting proxies
	dialer         proxyutil.ContextDialer
	proxyMode      proxyutil.Mode
	configErr      error
	connectTimeout time.Duration
}

// newUtlsRoundTripper creates a new utls-based round tripper with optional proxy support
func newUtlsRoundTripper(cfg *config.SDKConfig) *utlsRoundTripper {
	dialer, proxyMode, errBuild := proxyutil.BuildContextDialer("direct")
	if cfg != nil {
		proxyDialer, mode, errProxy := proxyutil.BuildContextDialer(cfg.ProxyURL)
		proxyMode = mode
		errBuild = errProxy
		if errBuild != nil {
			log.Errorf("failed to configure proxy dialer for %q: %v", proxyutil.MaskProxyURL(cfg.ProxyURL), errBuild)
		} else if mode != proxyutil.ModeInherit && proxyDialer != nil {
			dialer = proxyDialer
		}
	}

	return &utlsRoundTripper{
		connections:    make(map[string]*http2.ClientConn),
		pending:        make(map[string]chan struct{}),
		dialer:         dialer,
		proxyMode:      proxyMode,
		configErr:      errBuild,
		connectTimeout: claudeUTLSConnectTimeout,
	}
}

// getOrCreateConnection gets an existing connection or creates a new one.
// It uses a per-host locking mechanism to prevent multiple goroutines from
// creating connections to the same host simultaneously.
func (t *utlsRoundTripper) getOrCreateConnection(ctx context.Context, cacheKey, host, addr string) (*http2.ClientConn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		var stale *http2.ClientConn
		t.mu.Lock()
		if h2Conn, ok := t.connections[cacheKey]; ok {
			if h2Conn.CanTakeNewRequest() {
				t.mu.Unlock()
				return h2Conn, nil
			}
			delete(t.connections, cacheKey)
			stale = h2Conn
		}
		if done, ok := t.pending[cacheKey]; ok {
			t.mu.Unlock()
			closeClaudeHTTP2ClientConn(stale)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				continue
			}
		}

		done := make(chan struct{})
		t.pending[cacheKey] = done
		t.mu.Unlock()
		closeClaudeHTTP2ClientConn(stale)

		h2Conn, errCreate := t.createConnection(ctx, host, addr)
		t.mu.Lock()
		delete(t.pending, cacheKey)
		if errCreate == nil {
			t.connections[cacheKey] = h2Conn
		}
		close(done)
		t.mu.Unlock()
		return h2Conn, errCreate
	}
}

// createConnection creates a new HTTP/2 connection with Chrome TLS fingerprint.
// Chrome's TLS fingerprint is closer to Node.js/OpenSSL (which real Claude Code uses)
// than Firefox, reducing the mismatch between TLS layer and HTTP headers.
func (t *utlsRoundTripper) createConnection(ctx context.Context, host, addr string) (*http2.ClientConn, error) {
	if t.configErr != nil {
		return nil, fmt.Errorf("claude utls proxy configuration: %w", t.configErr)
	}
	conn, err := t.dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{ServerName: host}
	tlsConn := tls.UClient(conn, tlsConfig, tls.HelloChrome_Auto)

	if err := tlsConn.HandshakeContext(ctx); err != nil {
		conn.Close()
		return nil, err
	}

	tr := &http2.Transport{}
	stopClose := context.AfterFunc(ctx, func() { _ = tlsConn.Close() })
	h2Conn, err := tr.NewClientConn(tlsConn)
	stopClose()
	if err != nil {
		tlsConn.Close()
		return nil, err
	}
	if errContext := ctx.Err(); errContext != nil {
		_ = h2Conn.Close()
		return nil, errContext
	}

	return h2Conn, nil
}

// RoundTrip implements http.RoundTripper
func (t *utlsRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil {
		return nil, fmt.Errorf("claude utls: request is nil")
	}
	hostname := req.URL.Hostname()
	if hostname == "" {
		return nil, fmt.Errorf("claude utls: request host is empty")
	}
	port := req.URL.Port()
	if port == "" {
		port = "443"
	}
	addr := net.JoinHostPort(hostname, port)
	connectTimeout := t.connectTimeout
	if connectTimeout <= 0 {
		connectTimeout = claudeUTLSConnectTimeout
	}
	connectCtx, cancelConnect := context.WithTimeout(req.Context(), connectTimeout)
	h2Conn, err := t.getOrCreateConnection(connectCtx, addr, hostname, addr)
	cancelConnect()

	if err != nil {
		return nil, err
	}

	resp, err := h2Conn.RoundTrip(req)
	if err != nil {
		// Connection failed, remove it from cache
		t.mu.Lock()
		if cached, ok := t.connections[addr]; ok && cached == h2Conn {
			delete(t.connections, addr)
		}
		t.mu.Unlock()
		closeClaudeHTTP2ClientConn(h2Conn)
		return nil, err
	}

	return resp, nil
}

func closeClaudeHTTP2ClientConn(conn *http2.ClientConn) {
	if conn != nil {
		_ = conn.Close()
	}
}

// NewAnthropicHttpClient creates an HTTP client that bypasses TLS fingerprinting
// for Anthropic domains by using utls with Chrome fingerprint.
// It accepts optional SDK configuration for proxy settings.
func NewAnthropicHttpClient(cfg *config.SDKConfig) *http.Client {
	return &http.Client{
		Transport: newUtlsRoundTripper(cfg),
	}
}
