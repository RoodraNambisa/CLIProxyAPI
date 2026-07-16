package helps

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type stubRoundTripper struct{}

func (s *stubRoundTripper) RoundTrip(*http.Request) (*http.Response, error) { return nil, nil }

type countingRoundTripper struct {
	calls atomic.Int32
}

func (t *countingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	t.calls.Add(1)
	return nil, errors.New("injected transport was called")
}

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

func TestNewProxyAwareHTTP1ClientDirectDisablesHTTP2(t *testing.T) {
	t.Parallel()

	client := NewProxyAwareHTTP1Client(
		context.Background(),
		&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}},
		nil,
		0,
	)

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", client.Transport)
	}
	if transport.ForceAttemptHTTP2 {
		t.Fatal("ForceAttemptHTTP2 = true, want false")
	}
	if transport.TLSNextProto == nil {
		t.Fatal("TLSNextProto = nil, want empty map to disable HTTP/2")
	}
	if transport.TLSClientConfig == nil {
		t.Fatal("TLSClientConfig = nil, want HTTP/1.1 ALPN config")
	}
	if len(transport.TLSClientConfig.NextProtos) != 1 || transport.TLSClientConfig.NextProtos[0] != "http/1.1" {
		t.Fatalf("NextProtos = %#v, want [http/1.1]", transport.TLSClientConfig.NextProtos)
	}
}

func TestNewProxyAwareHTTP1ClientUsesInjectedTransportWithoutExplicitProxy(t *testing.T) {
	t.Parallel()

	wantTransport := &stubRoundTripper{}
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", http.RoundTripper(wantTransport))

	client := NewProxyAwareHTTP1Client(ctx, &config.Config{}, &cliproxyauth.Auth{}, 0)

	if client.Transport != wantTransport {
		t.Fatalf("transport = %T, want injected context RoundTripper", client.Transport)
	}
}

func TestProxyAwareClientsFailClosedForInvalidExplicitProxy(t *testing.T) {
	for _, test := range []struct {
		name string
		new  func(context.Context, *config.Config, *cliproxyauth.Auth, time.Duration) *http.Client
	}{
		{name: "default", new: NewProxyAwareHTTPClient},
		{name: "http1", new: NewProxyAwareHTTP1Client},
	} {
		t.Run(test.name, func(t *testing.T) {
			injected := &countingRoundTripper{}
			ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", http.RoundTripper(injected))
			client := test.new(ctx, &config.Config{}, &cliproxyauth.Auth{ProxyURL: "http://user:secret@proxy.example:0"}, 0)
			req, errRequest := http.NewRequest(http.MethodGet, "https://example.test", nil)
			if errRequest != nil {
				t.Fatalf("NewRequest() error = %v", errRequest)
			}
			resp, errDo := client.Do(req)
			if resp != nil {
				_ = resp.Body.Close()
				t.Fatal("client returned a response for invalid explicit proxy")
			}
			if errDo == nil {
				t.Fatal("client error = nil, want fail-closed proxy error")
			}
			if got := injected.calls.Load(); got != 0 {
				t.Fatalf("injected fallback transport calls = %d, want zero", got)
			}
			var proxyFailure interface{ ProxyInfrastructureError() bool }
			if !errors.As(errDo, &proxyFailure) || !proxyFailure.ProxyInfrastructureError() {
				t.Fatalf("error = %T %v, want proxy infrastructure marker", errDo, errDo)
			}
			var skipper interface{ SkipAuthResult() bool }
			if !errors.As(errDo, &skipper) || !skipper.SkipAuthResult() {
				t.Fatalf("error = %T %v, want auth-result skip marker", errDo, errDo)
			}
			var retry interface{ RetryOtherAuth() bool }
			if !errors.As(errDo, &retry) || !retry.RetryOtherAuth() {
				t.Fatalf("error = %T %v, want retry-other-auth marker", errDo, errDo)
			}
			var status interface{ StatusCode() int }
			if !errors.As(errDo, &status) || status.StatusCode() != http.StatusServiceUnavailable {
				t.Fatalf("error = %T %v, want status 503", errDo, errDo)
			}
			if strings.Contains(errDo.Error(), "secret") {
				t.Fatalf("error leaked proxy password: %v", errDo)
			}
		})
	}
}

func TestProxyTransportCacheReplacesRuntimeBindingPerAuth(t *testing.T) {
	t.Parallel()

	cache := newProxyTransportCache()
	auth := &cliproxyauth.Auth{
		ID:                    "auth-a",
		RuntimeProxyURL:       "http://user:secret@proxy-one.example:8080",
		RuntimeProxyBindingID: "binding-one",
	}
	first := cache.transportFor(auth, auth.EffectiveProxyURL(), buildProxyTransport)
	if first == nil || cache.transportFor(auth, auth.EffectiveProxyURL(), buildProxyTransport) != first {
		t.Fatal("same runtime binding did not reuse transport")
	}
	auth.RuntimeProxyURL = "http://user:secret@proxy-two.example:8080"
	auth.RuntimeProxyBindingID = "binding-two"
	second := cache.transportFor(auth, auth.EffectiveProxyURL(), buildProxyTransport)
	if second == nil || second == first {
		t.Fatal("runtime rebind did not replace transport")
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if got := len(cache.entries); got != 1 {
		t.Fatalf("cache entries = %d, want one current binding", got)
	}
	for key := range cache.entries {
		if key == auth.RuntimeProxyURL || key == "http://user:secret@proxy-one.example:8080" {
			t.Fatalf("cache key leaked proxy URL: %q", key)
		}
	}
}

func TestProxyTransportCacheCloseMatchingRemovesOnlySelectedAuth(t *testing.T) {
	cache := newProxyTransportCache()
	authA := &cliproxyauth.Auth{ID: "auth-a", ProxyURL: "http://proxy-a.example:8080"}
	authB := &cliproxyauth.Auth{ID: "auth-b", ProxyURL: "http://proxy-b.example:8080"}
	if cache.transportFor(authA, authA.ProxyURL, buildProxyTransport) == nil || cache.transportFor(authB, authB.ProxyURL, buildProxyTransport) == nil {
		t.Fatal("failed to seed proxy transport cache")
	}
	cache.closeMatching(func(key string) bool { return key == "auth:auth-a" })
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if _, exists := cache.entries["auth:auth-a"]; exists {
		t.Fatal("auth-a transport was not removed")
	}
	if _, exists := cache.entries["auth:auth-b"]; !exists {
		t.Fatal("auth-b transport was removed")
	}
}

func TestProxyTransportCacheDoesNotReinsertAcrossClose(t *testing.T) {
	cache := newProxyTransportCache()
	auth := &cliproxyauth.Auth{ID: "auth-a", ProxyURL: "http://proxy.example:8080"}
	firstBuildStarted := make(chan struct{})
	releaseFirstBuild := make(chan struct{})
	var builds atomic.Int64
	build := func(string) *http.Transport {
		if builds.Add(1) == 1 {
			close(firstBuildStarted)
			<-releaseFirstBuild
		}
		return &http.Transport{}
	}

	result := make(chan *http.Transport, 1)
	go func() {
		result <- cache.transportFor(auth, auth.ProxyURL, build)
	}()
	<-firstBuildStarted
	cache.closeMatching(func(key string) bool { return key == "auth:auth-a" })
	close(releaseFirstBuild)
	transport := <-result
	if transport == nil {
		t.Fatal("transportFor() returned nil")
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("build count = %d, want one uncached transport", got)
	}
	cache.mu.Lock()
	_, cached := cache.entries["auth:auth-a"]
	cache.mu.Unlock()
	if cached {
		t.Fatal("stale transport was inserted after cache close")
	}
	if !transport.DisableKeepAlives {
		t.Fatal("uncached transport kept idle connections enabled")
	}
}

func TestEnvironmentTransportCacheReplacesChangedSignature(t *testing.T) {
	cache := newEnvironmentTransportCache()
	first := cache.transportFor("one", func() *http.Transport { return &http.Transport{} })
	if got := cache.transportFor("one", func() *http.Transport { return &http.Transport{} }); got != first {
		t.Fatal("same environment signature did not reuse transport")
	}
	second := cache.transportFor("two", func() *http.Transport { return &http.Transport{} })
	if second == nil || second == first {
		t.Fatal("changed environment signature did not replace transport")
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.signature != "two" || cache.transport != second {
		t.Fatalf("environment cache = %q %p, want two %p", cache.signature, cache.transport, second)
	}
}
