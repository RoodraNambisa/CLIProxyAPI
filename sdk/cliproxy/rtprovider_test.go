package cliproxy

import (
	"net/http"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
)

func TestRoundTripperForDirectBypassesProxy(t *testing.T) {
	t.Parallel()

	provider := newDefaultRoundTripperProvider()
	rt := provider.RoundTripperFor(&coreauth.Auth{ProxyURL: "direct"})
	transport, ok := rt.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", rt)
	}
	if transport.Proxy != nil {
		t.Fatal("expected direct transport to disable proxy function")
	}
}

func TestRoundTripperForReplacesTransportAfterProxyRebind(t *testing.T) {
	t.Parallel()

	provider := newDefaultRoundTripperProvider()
	auth := &coreauth.Auth{
		ID:                    "auth-a",
		RuntimeProxyURL:       "http://user:secret@proxy-one.example:8080",
		RuntimeProxyBindingID: "binding-one",
	}
	first := provider.RoundTripperFor(auth)
	if first == nil || provider.RoundTripperFor(auth) != first {
		t.Fatal("same binding did not reuse transport")
	}
	auth.RuntimeProxyURL = "http://user:secret@proxy-two.example:8080"
	auth.RuntimeProxyBindingID = "binding-two"
	second := provider.RoundTripperFor(auth)
	if second == nil || second == first {
		t.Fatal("rebind did not replace transport")
	}
	provider.mu.RLock()
	defer provider.mu.RUnlock()
	if got := len(provider.cache); got != 1 {
		t.Fatalf("cache entries = %d, want one current auth binding", got)
	}
	for key := range provider.cache {
		if key == auth.RuntimeProxyURL || key == "http://user:secret@proxy-one.example:8080" {
			t.Fatalf("cache key leaked proxy URL: %q", key)
		}
	}
}

func TestDefaultRoundTripperProviderReleasesAuthAndShutdownCaches(t *testing.T) {
	t.Parallel()

	provider := newDefaultRoundTripperProvider()
	authA := &coreauth.Auth{ID: "auth-a", ProxyURL: "direct"}
	authB := &coreauth.Auth{ID: "auth-b", ProxyURL: "direct"}
	firstA := provider.RoundTripperFor(authA)
	firstB := provider.RoundTripperFor(authB)
	if firstA == nil || firstB == nil {
		t.Fatal("expected auth-scoped transports")
	}

	provider.EvictAuth(authA.ID)
	provider.mu.RLock()
	_, retainedA := provider.cache["auth:"+authA.ID]
	_, retainedB := provider.cache["auth:"+authB.ID]
	provider.mu.RUnlock()
	if retainedA {
		t.Fatal("evicted auth transport remained cached")
	}
	if !retainedB {
		t.Fatal("evicting one auth removed another auth transport")
	}
	if replacementA := provider.RoundTripperFor(authA); replacementA == nil || replacementA == firstA {
		t.Fatal("evicted auth transport was not rebuilt")
	}

	provider.CloseIdleConnections()
	provider.mu.RLock()
	remaining := len(provider.cache)
	provider.mu.RUnlock()
	if remaining != 0 {
		t.Fatalf("cache entries after close = %d, want 0", remaining)
	}
}

func TestDefaultRoundTripperProviderDoesNotReinsertAfterConcurrentEviction(t *testing.T) {
	t.Parallel()

	provider := newDefaultRoundTripperProvider()
	started := make(chan struct{})
	release := make(chan struct{})
	provider.buildTransport = func(raw string) (*http.Transport, proxyutil.Mode, error) {
		close(started)
		<-release
		return proxyutil.BuildHTTPTransport(raw)
	}
	auth := &coreauth.Auth{ID: "auth-a", ProxyURL: "direct"}
	result := make(chan http.RoundTripper, 1)
	go func() {
		result <- provider.RoundTripperFor(auth)
	}()
	<-started
	provider.EvictAuth(auth.ID)
	close(release)
	transport, ok := (<-result).(*http.Transport)
	if !ok || transport == nil {
		t.Fatal("in-flight request lost its transport")
	}
	if !transport.DisableKeepAlives {
		t.Fatal("uncached in-flight transport retained keep-alive connections")
	}

	provider.mu.RLock()
	_, retained := provider.cache["auth:"+auth.ID]
	provider.mu.RUnlock()
	if retained {
		t.Fatal("transport built before eviction was reinserted")
	}
}
