package chatgptweb

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestBrowserRegionIndependentManualPolicy(t *testing.T) {
	r := NewBrowserRegionResolver(t.Context())
	defer r.Close()
	offset := 123
	cfg := config.ChatGPTWebConfig{Timezone: "Asia/Tokyo", TimezoneOffsetMinutes: &offset, BrowserRegion: config.ChatGPTWebBrowserRegionConfig{Language: "fr-FR"}}
	r.UpdateConfig(cfg)
	v := r.Preview("direct", DefaultPersona())
	if v.OffsetMinutes != 123 || v.Language != "fr-FR" {
		t.Fatalf("manual settings lost %+v", v)
	}
}
