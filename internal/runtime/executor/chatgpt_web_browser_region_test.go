package executor

import (
	"context"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxyregion"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type regionPreviewOnlyResolver struct{ calls int }

func (r *regionPreviewOnlyResolver) Resolve(context.Context, *coreauth.Auth) (coreauth.ResolvedProxy, error) {
	r.calls++
	return coreauth.ResolvedProxy{}, nil
}
func (*regionPreviewOnlyResolver) ReportFailure(context.Context, *coreauth.Auth, error) error {
	return nil
}
func TestChatGPTWebBrowserRegionPreviewDoesNotAllocate(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	resolver := &regionPreviewOnlyResolver{}
	manager.SetProxyResolver(resolver)
	e := NewChatGPTWebExecutor(&config.Config{}, manager)
	defer func() { _ = e.Close() }()
	auth := chatGPTWebRuntimeAuth()
	for _, refresh := range []bool{false, true} {
		snapshot, code := e.BrowserRegionPreview(auth, refresh)
		if code != "route_unresolved" || snapshot.Reason != "route_unresolved" || resolver.calls != 0 {
			t.Fatalf("preview allocated a route: %s %+v calls=%d", code, snapshot, resolver.calls)
		}
	}
}

func TestChatGPTWebBrowserRegionPreviewIncludesCredentialProxyOwner(t *testing.T) {
	cfg := &config.Config{}
	cfg.ChatGPTWeb.BrowserRegion.AutoTimezone = true
	cfg.ChatGPTWeb.BrowserRegion.AutoLanguage = true
	manager := coreauth.NewManager(nil, nil, nil)
	e := NewChatGPTWebExecutor(cfg, manager)
	defer func() { _ = e.Close() }()
	auth := &coreauth.Auth{ID: "web-owner", Provider: "chatgpt-web", ProxyURL: "http://example.invalid:80", Metadata: map[string]any{}}
	e.browserRegion.Shared().SetManualSource(func(route proxyregion.Route) (config.ProxyRegionConfig, string) {
		if route.OwnerID != auth.ID {
			t.Errorf("preview lost credential owner: %q", route.OwnerID)
		}
		return config.ProxyRegionConfig{Timezone: "America/Toronto", Language: "en-CA"}, "credential_proxy"
	})
	snapshot, code := e.BrowserRegionPreview(auth, false)
	if code != "" || snapshot.Timezone != "America/Toronto" || snapshot.Language != "en-CA" || snapshot.TimezoneSource != "credential_proxy" {
		t.Fatalf("%s %+v", code, snapshot)
	}
}
