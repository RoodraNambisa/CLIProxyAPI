package helps

import (
	"context"
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
