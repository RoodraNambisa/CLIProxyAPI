package proxypool

import (
	"context"
	"path/filepath"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestPreviewProxyDoesNotAllocateAndIgnoresObsoleteBindings(t *testing.T) {
	cfg := proxyPoolTestConfig("3334")
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	calls := 0
	manager.check = func(ctx context.Context, proxy string) TraceResult { calls++; return successfulTrace(ctx, proxy) }
	auth := proxyPoolTestAuth("preview-auth")
	preview, err := manager.PreviewProxy(auth)
	if err != nil || !preview.Pending || preview.Source != "pool" || calls != 0 || len(manager.SortedBindings()) != 0 {
		t.Fatalf("preview performed allocation or probe: %+v %v", preview, err)
	}
	resolved, err := manager.Resolve(t.Context(), auth)
	if err != nil {
		t.Fatal(err)
	}
	before := calls
	preview, err = manager.PreviewProxy(auth)
	if err != nil || preview.Pending || preview.ResolvedProxy != resolved || calls != before {
		t.Fatal("preview lost current binding or probed the network")
	}
	auth.ProxyURL = "direct"
	preview, err = manager.PreviewProxy(auth)
	if err != nil || preview.Source != "auth" || preview.URL != "direct" || preview.BindingID != "" {
		t.Fatal("old binding overrode explicit direct mode")
	}
	auth.ProxyURL = ""
	cfg.ProxyRules = nil
	cfg.ProxyURL = "http://global.example:8080"
	if err := manager.UpdateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	preview, err = manager.PreviewProxy(auth)
	if err != nil || preview.Source != "global" || preview.URL != cfg.ProxyURL || preview.BindingID != "" {
		t.Fatal("removed rule still claimed a pool binding")
	}
	preview, err = manager.PreviewProxy(&coreauth.Auth{Provider: "aistudio"})
	if err != nil || preview.Source != "relay" {
		t.Fatal("relay misidentified as server egress")
	}
}
