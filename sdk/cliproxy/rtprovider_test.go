package cliproxy

import (
	"net/http"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
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
