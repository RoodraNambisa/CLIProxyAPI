package helps

import (
	"context"
	"crypto/tls"
	"net/http"
	"os"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type stubRoundTripper struct{}

func (s *stubRoundTripper) RoundTrip(*http.Request) (*http.Response, error) { return nil, nil }

func setEnvironmentProxy(t *testing.T, proxyURL string) {
	t.Helper()

	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY"} {
		oldValue, hadValue := os.LookupEnv(key)
		if err := os.Setenv(key, proxyURL); err != nil {
			t.Fatalf("Setenv(%s): %v", key, err)
		}
		cleanupKey := key
		cleanupOldValue := oldValue
		cleanupHadValue := hadValue
		t.Cleanup(func() {
			if cleanupHadValue {
				_ = os.Setenv(cleanupKey, cleanupOldValue)
				return
			}
			_ = os.Unsetenv(cleanupKey)
		})
	}
}

func TestNewProxyAwareHTTPClientDirectBypassesGlobalProxy(t *testing.T) {
	t.Parallel()

	client := NewProxyAwareHTTPClient(
		context.Background(),
		&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"}},
		&cliproxyauth.Auth{ProxyURL: "direct"},
		0,
	)

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", client.Transport)
	}
	if transport.Proxy != nil {
		t.Fatal("expected direct transport to disable proxy function")
	}
}

func TestNewProxyAwareHTTP1ClientDisablesHTTP2OnDefaultTransport(t *testing.T) {
	t.Parallel()

	client := NewProxyAwareHTTP1Client(context.Background(), &config.Config{}, &cliproxyauth.Auth{}, 0)

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", client.Transport)
	}
	if transport.ForceAttemptHTTP2 {
		t.Fatal("expected HTTP/2 attempts to be disabled")
	}
	if transport.TLSNextProto == nil {
		t.Fatal("expected TLSNextProto to be set to disable automatic HTTP/2")
	}
	if len(transport.TLSNextProto) != 0 {
		t.Fatalf("TLSNextProto length = %d, want 0", len(transport.TLSNextProto))
	}
	if transport.Protocols == nil {
		t.Fatal("expected Protocols to be set")
	}
	if !transport.Protocols.HTTP1() || transport.Protocols.HTTP2() || transport.Protocols.UnencryptedHTTP2() {
		t.Fatalf("Protocols = %s, want only HTTP/1", transport.Protocols)
	}
}

func TestNewProxyAwareHTTP1ClientClonesInjectedTransport(t *testing.T) {
	t.Parallel()

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetHTTP2(true)
	injected := &http.Transport{
		ForceAttemptHTTP2: true,
		TLSClientConfig:   &tls.Config{NextProtos: []string{"h2", "http/1.1"}},
		Protocols:         protocols,
	}
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", http.RoundTripper(injected))

	client := NewProxyAwareHTTP1Client(ctx, &config.Config{}, &cliproxyauth.Auth{}, 0)

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", client.Transport)
	}
	if transport == injected {
		t.Fatal("expected HTTP/1 transport to clone the injected transport")
	}
	if transport.ForceAttemptHTTP2 {
		t.Fatal("expected cloned transport to disable HTTP/2 attempts")
	}
	if !injected.ForceAttemptHTTP2 {
		t.Fatal("injected transport was mutated")
	}
	if got := transport.TLSClientConfig.NextProtos; len(got) != 1 || got[0] != "http/1.1" {
		t.Fatalf("NextProtos = %v, want [http/1.1]", got)
	}
	if got := injected.TLSClientConfig.NextProtos; len(got) != 2 || got[0] != "h2" || got[1] != "http/1.1" {
		t.Fatalf("injected NextProtos mutated to %v", got)
	}
	if transport.Protocols == nil {
		t.Fatal("expected Protocols to be set")
	}
	if !transport.Protocols.HTTP1() || transport.Protocols.HTTP2() || transport.Protocols.UnencryptedHTTP2() {
		t.Fatalf("Protocols = %s, want only HTTP/1", transport.Protocols)
	}
	if injected.Protocols == nil || !injected.Protocols.HTTP1() || !injected.Protocols.HTTP2() {
		t.Fatalf("injected Protocols mutated to %v", injected.Protocols)
	}
}

func TestNewProxyAwareHTTP1ClientReusesDefaultTransport(t *testing.T) {
	t.Parallel()

	first := NewProxyAwareHTTP1Client(context.Background(), &config.Config{}, &cliproxyauth.Auth{}, 0)
	second := NewProxyAwareHTTP1Client(context.Background(), &config.Config{}, &cliproxyauth.Auth{}, 0)

	if first.Transport == nil {
		t.Fatal("first transport is nil")
	}
	if first.Transport != second.Transport {
		t.Fatalf("transports differ: %p != %p", first.Transport, second.Transport)
	}
}

func TestNewProxyAwareHTTPClientFallsBackToEnvironmentProxy(t *testing.T) {
	setEnvironmentProxy(t, "http://env-proxy.example.com:8080")

	client := NewProxyAwareHTTPClient(context.Background(), &config.Config{}, &cliproxyauth.Auth{}, 0)

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", client.Transport)
	}
	if transport.Proxy == nil {
		t.Fatal("expected environment proxy transport to configure Proxy function")
	}
	req, errReq := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if errReq != nil {
		t.Fatalf("NewRequest() error = %v", errReq)
	}
	proxyURL, errProxy := transport.Proxy(req)
	if errProxy != nil {
		t.Fatalf("transport.Proxy() error = %v", errProxy)
	}
	if proxyURL == nil || proxyURL.String() != "http://env-proxy.example.com:8080" {
		t.Fatalf("proxy URL = %v, want http://env-proxy.example.com:8080", proxyURL)
	}
}

func TestNewProxyAwareHTTPClientExplicitProxyWinsOverEnvironmentProxy(t *testing.T) {
	setEnvironmentProxy(t, "http://env-proxy.example.com:8080")

	client := NewProxyAwareHTTPClient(
		context.Background(),
		&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://config-proxy.example.com:8080"}},
		nil,
		0,
	)

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", client.Transport)
	}
	req, errReq := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if errReq != nil {
		t.Fatalf("NewRequest() error = %v", errReq)
	}
	proxyURL, errProxy := transport.Proxy(req)
	if errProxy != nil {
		t.Fatalf("transport.Proxy() error = %v", errProxy)
	}
	if proxyURL == nil || proxyURL.String() != "http://config-proxy.example.com:8080" {
		t.Fatalf("proxy URL = %v, want http://config-proxy.example.com:8080", proxyURL)
	}
}

func TestNewProxyAwareHTTPClientContextTransportWinsOverEnvironmentProxy(t *testing.T) {
	setEnvironmentProxy(t, "http://env-proxy.example.com:8080")

	wantTransport := &stubRoundTripper{}
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", http.RoundTripper(wantTransport))

	client := NewProxyAwareHTTPClient(ctx, &config.Config{}, &cliproxyauth.Auth{}, 0)

	if client.Transport != wantTransport {
		t.Fatalf("transport = %T, want injected context RoundTripper", client.Transport)
	}
}

func TestNewProxyAwareHTTPClientHonorsNoProxy(t *testing.T) {
	setEnvironmentProxy(t, "http://env-proxy.example.com:8080")

	oldNoProxy, hadNoProxy := os.LookupEnv("NO_PROXY")
	if err := os.Setenv("NO_PROXY", "example.com"); err != nil {
		t.Fatalf("Setenv(NO_PROXY): %v", err)
	}
	t.Cleanup(func() {
		if hadNoProxy {
			_ = os.Setenv("NO_PROXY", oldNoProxy)
			return
		}
		_ = os.Unsetenv("NO_PROXY")
	})

	client := NewProxyAwareHTTPClient(context.Background(), &config.Config{}, &cliproxyauth.Auth{}, 0)

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", client.Transport)
	}
	req, errReq := http.NewRequest(http.MethodGet, "https://example.com", nil)
	if errReq != nil {
		t.Fatalf("NewRequest() error = %v", errReq)
	}
	proxyURL, errProxy := transport.Proxy(req)
	if errProxy != nil {
		t.Fatalf("transport.Proxy() error = %v", errProxy)
	}
	if proxyURL != nil {
		t.Fatalf("proxy URL = %v, want nil for NO_PROXY match", proxyURL)
	}
}
