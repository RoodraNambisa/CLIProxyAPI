package helps

import (
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"testing"
)

func TestResolveCodexUpstream(t *testing.T) {
	cfg := &config.Config{Codex: config.CodexConfig{BaseURL: "https://global.test/codex/"}}
	for _, tc := range []struct {
		name        string
		a           *auth.Auth
		cfg         *config.Config
		url, source string
	}{
		{"official", &auth.Auth{}, nil, config.DefaultCodexBaseURL, "official"},
		{"global", &auth.Auth{Metadata: map[string]any{"access_token": "fixture"}}, cfg, "https://global.test/codex", "global"},
		{"attribute", &auth.Auth{Attributes: map[string]string{"base_url": "https://credential.test/prefix/"}}, cfg, "https://credential.test/prefix", "credential"},
		{"metadata", &auth.Auth{Metadata: map[string]any{"base_url": "https://metadata.test/prefix/"}}, cfg, "https://metadata.test/prefix", "credential"},
		{"clear", &auth.Auth{Metadata: map[string]any{"base_url": ""}}, cfg, "https://global.test/codex", "global"},
		{"apikey", &auth.Auth{Attributes: map[string]string{"api_key": "fixture", "base_url": "https://api.test/v1"}}, cfg, "https://api.test/v1", "credential"},
		{"apikey-query", &auth.Auth{Attributes: map[string]string{"api_key": "fixture", "base_url": "https://api.test/v1?tenant=/"}}, cfg, "https://api.test/v1?tenant=/", "credential"},
		{"apikey-no-global", &auth.Auth{Attributes: map[string]string{"api_key": "fixture"}}, cfg, config.DefaultCodexBaseURL, "official"},
		{"apikey-kind", &auth.Auth{Metadata: map[string]any{"auth_kind": "apikey"}}, cfg, config.DefaultCodexBaseURL, "official"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ResolveCodexUpstream(tc.a, tc.cfg)
			if got.BaseURL != tc.url || got.Source != tc.source {
				t.Fatalf("got %#v", got)
			}
		})
	}
}
