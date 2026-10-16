package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProxyIPMigrationPreservesPoolAndUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	raw := `port: 8317
proxy-pools:
  - name: p
    check-interval-seconds: 900
    entries:
      - id: one
        url-template: http://p:80
chatgpt-web:
  browser-region:
    auto-timezone: true
    cache-ttl-seconds: 21600
    route-cache-size: 7777
    lookup-url: https://ipwho.is/{ip}
    future-unknown: preserve
`
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ProxyPools[0].CheckIntervalSeconds != 900 || *cfg.ProxyHealthCheck.TemporaryRouteCheckIntervalSeconds != 21600 {
		t.Fatal("interval migration changed pool")
	}
	ip := cfg.EffectiveProxyIP()
	cfg.ProxyIP = &ip
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	text := string(b)
	if !strings.Contains(text, "future-unknown: preserve") || strings.Contains(text, "    cache-ttl-seconds:") || strings.Contains(text, "    route-cache-size:") {
		t.Fatal("known legacy keys not moved or unknown lost")
	}
	after, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.ProxyPools[0].CheckIntervalSeconds != 900 || *after.ProxyHealthCheck.TemporaryRouteCacheSize != 7777 || !after.ChatGPTWeb.BrowserRegion.AutoTimezone {
		t.Fatal("migration changed policy")
	}
}
func TestProxyIPDefaultsAndCodexOverride(t *testing.T) {
	if v := (ProxyIPConfig{}).Resolved(); v.IPCacheSize != 100000 || v.ColdCacheMode != "fallback" || v.RefreshMode != "lazy" {
		t.Fatal(v)
	}
	base := CodexTimeContextConfig{Enabled: true, Mode: "manual", Timezone: "UTC"}
	for _, test := range []struct {
		raw     any
		enabled bool
		mode    string
	}{{nil, true, "manual"}, {map[string]any{"mode": "inherit"}, true, "manual"}, {map[string]any{"mode": "off"}, false, "off"}, {map[string]any{"mode": "proxy"}, true, "proxy"}} {
		got, err := EffectiveCodexTimeContext(base, test.raw)
		if err != nil || got.Enabled != test.enabled || got.Mode != test.mode {
			t.Fatal(got, err)
		}
	}
	if _, err := EffectiveCodexTimeContext(base, map[string]any{"mode": "manual"}); err == nil {
		t.Fatal("manual without timezone accepted")
	}
}
