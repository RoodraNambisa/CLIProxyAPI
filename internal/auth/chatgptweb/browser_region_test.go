package chatgptweb

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxyregion"
)

func TestBrowserRegionPolicyDoesNotResetSharedService(t *testing.T) {
	shared := proxyregion.New(t.Context(), config.ProxyIPConfig{})
	defer shared.Close()
	r := NewSharedBrowserRegionResolver(t.Context(), shared)
	defer r.Close()
	cfg := config.ChatGPTWebConfig{Timezone: "Asia/Tokyo", BrowserRegion: config.ChatGPTWebBrowserRegionConfig{AutoTimezone: true, Language: "fr-FR"}}
	r.UpdateConfig(cfg)
	v := r.Resolve("direct", DefaultPersona())
	if v.Timezone != "Asia/Tokyo" || v.Language != "fr-FR" || v.Reason != "proxy_required" {
		t.Fatalf("incorrect fallback %+v", v)
	}
	r.Close()
	if got := shared.Refresh(proxyregion.Route{ProxyURL: "direct"}); got != "proxy_required" {
		t.Fatal("provider closed shared service")
	}
}

func TestManualProxyRegionNeedsNoLookupAndRespectsConsumerFlags(t *testing.T) {
	shared := proxyregion.New(t.Context(), config.ProxyIPConfig{})
	defer shared.Close()
	shared.SetManualSource(func(proxyregion.Route) (config.ProxyRegionConfig, string) {
		return config.ProxyRegionConfig{Timezone: "Asia/Tokyo", Language: "ja-JP"}, "global_proxy"
	})
	r := NewSharedBrowserRegionResolver(t.Context(), shared)
	r.UpdateConfig(config.ChatGPTWebConfig{BrowserRegion: config.ChatGPTWebBrowserRegionConfig{AutoTimezone: true, AutoLanguage: true}})
	v := r.Resolve("http://fixture:80", DefaultPersona())
	if v.Timezone != "Asia/Tokyo" || v.Language != "ja-JP" || v.TimezoneSource != "global_proxy" {
		t.Fatal(v)
	}
	if shared.Counts()["pending"] != 0 {
		t.Fatal("manual region started lookup")
	}
	r.UpdateConfig(config.ChatGPTWebConfig{Timezone: "UTC"})
	if v = r.Resolve("http://fixture:80", DefaultPersona()); v.Timezone != "UTC" || v.TimezoneSource == "global_proxy" {
		t.Fatal("manual proxy data enabled disabled consumer")
	}
}
