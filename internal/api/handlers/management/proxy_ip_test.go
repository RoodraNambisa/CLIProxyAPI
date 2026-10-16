package management

import (
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestSharedProxyIPManagementAndLegacyWriteProtection(t *testing.T) {
	h, path := newPersistedChatGPTWebLoginProxyHandler(t, config.ChatGPTWebLoginProxyConfig{})
	ctx, w := newChatGPTWebLoginProxyRequest(http.MethodPatch, `{"ip-cache-size":100000,"ip-cache-ttl-seconds":1234,"refresh-mode":"scheduled","cold-cache-mode":"wait","temporary-route-check-interval-seconds":600}`)
	h.PatchProxyIP(ctx)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	cfg, err := config.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EffectiveProxyIP().Resolved().IPCacheTTLSeconds != 1234 || *cfg.ProxyHealthCheck.TemporaryRouteCheckIntervalSeconds != 600 {
		t.Fatal("patch not persisted")
	}
	for _, raw := range []string{`{"cache-ttl-seconds":5}`, `{"route-cache-size":1}`, `{"refresh-mode":"eager"}`, `{"ip-cache-size":null}`, `{"refresh-batch-size":100000}`} {
		ctx, w = newChatGPTWebLoginProxyRequest(http.MethodPatch, raw)
		h.PatchProxyIP(ctx)
		if w.Code != 400 {
			t.Fatalf("accepted %s status%d", raw, w.Code)
		}
	}
}
