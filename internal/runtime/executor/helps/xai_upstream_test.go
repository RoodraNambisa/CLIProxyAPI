package helps

import (
	"net/http/httptest"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestXAIUpstreamExplicitURLAndGlobalDefaults(t *testing.T) {
	for _, tc := range []struct {
		name, global, explicit, want, source string
		usingAPI                             bool
	}{
		{"empty defaults to CLI", "", "", "https://cli-chat-proxy.grok.com/v1", "global", false},
		{"inherit region", "us-east-1", "", "https://us-east-1.api.x.ai/v1", "global", false},
		{"inherit API", "api", "", "https://api.x.ai/v1", "global", false},
		{"API URL is an explicit pin even with old false flag", "cli", "https://api.x.ai/v1", "https://api.x.ai/v1", "credential", false},
		{"CLI pin beats global API and old true flag", "api", "https://cli-chat-proxy.grok.com/v1", "https://cli-chat-proxy.grok.com/v1", "credential", true},
		{"custom relay", "api", "http://127.0.0.1:18080/gateway/v1/", "http://127.0.0.1:18080/gateway/v1", "credential", false},
		{"normalized official pin", "cli", "https://API.X.AI:443/v1/", "https://api.x.ai/v1", "credential", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			auth := &coreauth.Auth{Provider: "xai", Metadata: map[string]any{"auth_kind": "oauth", "base_url": tc.explicit, "using_api": tc.usingAPI}}
			cfg := &config.Config{XAI: config.XAIConfig{DefaultBaseURLMode: tc.global}}
			got := ResolveXAIUpstream(auth, cfg)
			if got.BaseURL != tc.want || got.Source != tc.source {
				t.Fatalf("upstream = %#v, want %s (%s)", got, tc.want, tc.source)
			}
			if auth.Metadata["base_url"] != tc.explicit {
				t.Fatal("resolution persisted a global default into the credential")
			}
			req := httptest.NewRequest("POST", got.BaseURL+"/responses", nil)
			ApplyXAIResourceHeaders(req, auth, cfg)
			wantCLIHeaders := got.Mode == "cli"
			if (req.Header.Get("X-XAI-Token-Auth") != "") != wantCLIHeaders {
				t.Fatalf("CLI headers do not match destination %s", got.BaseURL)
			}
		})
	}
}

func TestXAIAPIOnlyDestinationsRespectRegionAndRelay(t *testing.T) {
	for _, tc := range []struct{ mode, pin, want string }{
		{"cli", "", "https://api.x.ai/v1"},
		{"api", "https://cli-chat-proxy.grok.com/v1", "https://api.x.ai/v1"},
		{"us-west-2", "", "https://us-west-2.api.x.ai/v1"},
		{"cli", "https://eu-west-1.api.x.ai/v1", "https://eu-west-1.api.x.ai/v1"},
		{"cli", "http://localhost:9000/v1", "http://localhost:9000/v1"},
	} {
		auth := &coreauth.Auth{Attributes: map[string]string{"base_url": tc.pin}}
		cfg := &config.Config{XAI: config.XAIConfig{DefaultBaseURLMode: tc.mode}}
		if got := XAIAPIOnlyBaseURL(auth, cfg); got != tc.want {
			t.Fatalf("mode=%s pin=%s: got %s, want %s", tc.mode, tc.pin, got, tc.want)
		}
	}
}

func TestXAIRequestPlanFreezesUpstreamAcrossConfigChanges(t *testing.T) {
	cfg := &config.Config{XAI: config.XAIConfig{DefaultBaseURLMode: "us-east-1"}}
	plan, err := NewXAIRequestPlan(t.Context(), cfg, coreexecutor.Request{Model: "grok-4.6", Payload: []byte(`{"input":"hello"}`)}, coreexecutor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	cfg.XAI.DefaultBaseURLMode = "eu-west-1"
	auth := &coreauth.Auth{Provider: "xai", Metadata: map[string]any{"auth_kind": "oauth"}}
	if got := ResolveXAIUpstream(auth, plan.Config).BaseURL; got != "https://us-east-1.api.x.ai/v1" {
		t.Fatalf("in-flight request switched upstream: %s", got)
	}
	if got := ResolveXAIUpstream(auth, cfg).BaseURL; got != "https://eu-west-1.api.x.ai/v1" {
		t.Fatalf("new request did not use the new default: %s", got)
	}
}
