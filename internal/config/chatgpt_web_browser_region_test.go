package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestChatGPTWebBrowserRegionConfiguration(t *testing.T) {
	defaults := ChatGPTWebBrowserRegionConfig{}.Resolved()
	if defaults.AutoTimezone || defaults.AutoLanguage || defaults.CacheTTLSeconds != 21600 || defaults.RouteCacheSize != 100000 || defaults.IPCacheSize != 100000 {
		t.Fatalf("unexpected defaults: %+v", defaults)
	}
	for _, raw := range []string{
		"lookup-url: http://ipwho.is/", "lookup-url: https://user:pass@example.test/", "lookup-url: https://{ip}/", "lookup-url: https://example.test/{other}",
		"language: not_a_language!", "cache-ttl-seconds: 0", "lookup-timeout-seconds: -1", "route-cache-size: 0", "ip-cache-size: -1",
	} {
		var cfg ChatGPTWebBrowserRegionConfig
		if err := yaml.Unmarshal([]byte(raw), &cfg); err != nil {
			t.Fatal(err)
		}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("invalid config accepted: %s", raw)
		}
	}
	for _, raw := range []string{"{}", "lookup-url: https://ipwho.is/", "lookup-url: https://example.test/query?ip={ip}", "route-cache-size: 100000\nip-cache-size: 100000\nlanguage: ja-JP"} {
		var cfg ChatGPTWebBrowserRegionConfig
		if err := yaml.Unmarshal([]byte(raw), &cfg); err != nil {
			t.Fatal(err)
		}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("valid config rejected: %s %v", raw, err)
		}
	}
}
