package auth

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexLiveSupportedAuthenticationModes(t *testing.T) {
	for _, tc := range []struct {
		name string
		auth *Auth
		want bool
	}{
		{"nil", nil, false},
		{"oauth", &Auth{Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}}, true},
		{"explicit oauth", &Auth{Provider: "codex", Metadata: map[string]any{"access_token": "fixture", "auth_mode": "oauth"}}, true},
		{"other provider", &Auth{Provider: "claude", Metadata: map[string]any{"access_token": "fixture"}}, false},
		{"agent identity", &Auth{Provider: "codex", Metadata: map[string]any{"access_token": "fixture", "auth_mode": "agent_identity"}}, false},
		{"refresh only", &Auth{Provider: "codex", Metadata: map[string]any{"refresh_token": "fixture"}}, false},
		{"blank token", &Auth{Provider: "codex", Metadata: map[string]any{"access_token": " "}}, false},
		{"api key", &Auth{Provider: "codex", Attributes: map[string]string{"api_key": "fixture", CodexAlphaSearchAttributeKey: "true"}, Metadata: map[string]any{"access_token": "fixture"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := SupportsCodexLive(tc.auth); got != tc.want {
				t.Fatalf("supported = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCodexLiveSelectionFiltersBeforePriorityAndCapacity(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "scheduler", true: "custom"}[legacy], func(t *testing.T) {
			var selector Selector = &RoundRobinSelector{}
			if legacy {
				selector = &trackingSelector{}
			}
			m := NewManager(nil, selector, nil)
			m.SetConfig(&config.Config{Routing: config.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 1}})
			m.RegisterExecutor(&authFallbackExecutor{id: "codex"})
			pool := []*Auth{
				{ID: "live-key-" + t.Name(), Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "priority": "100", CodexAlphaSearchAttributeKey: "true"}},
				{ID: "live-agent-" + t.Name(), Provider: "codex", Attributes: map[string]string{"priority": "200"}, Metadata: map[string]any{"auth_mode": "agent_identity", "access_token": "fixture"}},
				{ID: "live-oauth-" + t.Name(), Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}},
			}
			for _, a := range pool {
				registerFallbackAuthForModel(t, m, a, "live-selection")
			}
			opts := core.Options{SourceFormat: translator.FormatCodexLive, AuthRequestSlot: &core.AuthRequestSlot{}}
			defer opts.AuthRequestSlot.Release()
			picked, _, errPick := m.pickNext(t.Context(), "codex", "live-selection", opts, nil)
			if errPick != nil || picked == nil || picked.ID != pool[2].ID {
				t.Fatal("live did not select the supported OAuth credential")
			}
			for _, excluded := range pool[:2] {
				if available, _ := m.authRequestLimiter().availableAt(excluded.ID, m.routingAuthRequestLimitPolicyForAuth(excluded), time.Now()); !available {
					t.Fatal("excluded credential consumed capacity")
				}
			}
		})
	}
}
