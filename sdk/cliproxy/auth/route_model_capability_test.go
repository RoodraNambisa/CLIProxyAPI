package auth

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestAuthSupportsRouteModelPreservesAliasesAndPrefixes(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.SetOAuthModelAlias(map[string][]config.OAuthModelAlias{"codex": {{Name: "actual", Alias: "client"}}})
	a := &Auth{ID: t.Name(), Provider: "codex", Prefix: "tenant", Metadata: map[string]any{"access_token": "fixture"}}
	registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "actual"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(a.ID) })
	for _, model := range []string{"actual", "actual(high)", "client", "client(high)", "tenant/client", "tenant/client(high)"} {
		if !manager.AuthSupportsRouteModel(a, model) {
			t.Errorf("model %q lost its existing capability mapping", model)
		}
	}
	for _, model := range []string{"", "unknown", "other/client"} {
		if manager.AuthSupportsRouteModel(a, model) {
			t.Errorf("unsupported model %q was accepted", model)
		}
	}
	if manager.AuthSupportsRouteModel(nil, "actual") || (*Manager)(nil).AuthSupportsRouteModel(a, "actual") {
		t.Fatal("missing manager or credential was accepted")
	}
}
