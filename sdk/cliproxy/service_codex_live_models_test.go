package cliproxy

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestCodexLiveModelRegistrationEligibilityAndExclusions(t *testing.T) {
	for _, mode := range []string{"disabled", "oauth", "apikey", "agent", "refresh-only", "excluded", "prefix"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Codex.LiveEnabled = mode != "disabled"
			a := &coreauth.Auth{ID: "live-registration-" + t.Name(), Provider: "codex", Status: coreauth.StatusActive, Metadata: map[string]any{"access_token": "fixture"}}
			if mode == "apikey" {
				a.Attributes = map[string]string{"api_key": "fixture", "auth_kind": "apikey"}
			}
			if mode == "agent" {
				a.Metadata["auth_mode"] = "agent_identity"
			}
			if mode == "refresh-only" {
				a.Metadata = map[string]any{"refresh_token": "fixture"}
			}
			if mode == "excluded" {
				cfg.OAuthExcludedModels = map[string][]string{"codex": {"gpt-live-*", "gpt-realtime*"}}
			}
			prefix := ""
			if mode == "prefix" {
				a.Prefix = "team"
				prefix = "team/"
			}
			service := &Service{cfg: cfg, coreManager: coreauth.NewManager(nil, nil, nil)}
			service.registerModelsForAuth(a)
			reg := registry.GetGlobalRegistry()
			t.Cleanup(func() { reg.UnregisterClient(a.ID) })
			for _, id := range []string{registry.CodexLiveModelID, registry.CodexRealtimeModelID} {
				want := mode == "oauth" || mode == "prefix"
				if got := reg.ClientSupportsModel(a.ID, prefix+id); got != want {
					t.Fatalf("native route registration = %v, want %v", got, want)
				}
			}
		})
	}
}

func TestCodexLiveModelRegistrationFollowsHotUpdate(t *testing.T) {
	before, enabled, after := &config.Config{}, &config.Config{}, &config.Config{}
	enabled.Codex.LiveEnabled = true
	if !shouldRefreshCodexRegistrations(before, enabled) || !shouldRefreshCodexRegistrations(enabled, after) || shouldRefreshCodexRegistrations(before, after) {
		t.Fatal("Live admission change did not refresh native model registration")
	}
	m := coreauth.NewManager(nil, nil, nil)
	a := &coreauth.Auth{ID: "live-hot-models-" + t.Name(), Provider: "codex", Status: coreauth.StatusActive, Metadata: map[string]any{"access_token": "fixture"}}
	if _, errRegister := m.Register(t.Context(), a); errRegister != nil {
		t.Fatal(errRegister)
	}
	reg := registry.GetGlobalRegistry()
	defer reg.UnregisterClient(a.ID)
	service := &Service{coreManager: m}
	for _, cfg := range []*config.Config{before, enabled, after} {
		service.cfg = cfg
		if !service.refreshModelRegistrationForAuth(a) {
			t.Fatal("credential registration refresh failed")
		}
		if got := reg.ClientSupportsModel(a.ID, registry.CodexLiveModelID); got != cfg.Codex.LiveEnabled {
			t.Fatal("native model registration did not follow the saved setting")
		}
	}
}

func TestCodexLiveModelRegistrationRetainsAliasPrefixAndNativeType(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	cfg.OAuthModelAlias = map[string][]config.OAuthModelAlias{"codex": {{Name: registry.CodexLiveModelID, Alias: "voice"}}}
	a := &coreauth.Auth{ID: "live-alias-" + t.Name(), Provider: "codex", Prefix: "team", Status: coreauth.StatusActive, Metadata: map[string]any{"access_token": "fixture"}}
	service := &Service{cfg: cfg, coreManager: coreauth.NewManager(nil, nil, nil)}
	service.registerModelsForAuth(a)
	reg := registry.GetGlobalRegistry()
	defer reg.UnregisterClient(a.ID)
	for _, info := range reg.GetModelsForClient(a.ID) {
		if info.ID == "team/voice" {
			if info.UpstreamID != registry.CodexLiveModelID || info.Type != registry.CodexRealtimeModelType {
				t.Fatal("native model metadata was lost through alias and prefix mapping")
			}
			return
		}
	}
	t.Fatal("native realtime alias was not registered")
}
