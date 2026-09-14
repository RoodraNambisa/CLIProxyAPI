package cliproxy

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestXAIUpstreamHotReloadDiscardsOnlyInapplicableModelCatalogs(t *testing.T) {
	cfg := &config.Config{XAI: config.XAIConfig{DefaultBaseURLMode: "cli", SessionIdentityPoolSize: 4}}
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: cfg, coreManager: manager}
	for _, pin := range []string{"", "https://cli-chat-proxy.grok.com/v1"} {
		id := "xai-upstream-inherit"
		if pin != "" {
			id = "xai-upstream-pinned"
		}
		catalog := &helps.XAIModelCatalog{UpdatedAt: time.Now().UTC(), Source: "https://cli-chat-proxy.grok.com/v1/models", Models: []*registry.ModelInfo{{ID: "grok-cli-only-fixture", Object: "model", OwnedBy: "xai", Type: "xai"}}}
		auth, err := manager.Register(t.Context(), &coreauth.Auth{ID: id, Provider: "xai", Metadata: map[string]any{"base_url": pin, helps.XAIModelCatalogKey: catalog}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
		service.refreshModelRegistrationForAuth(auth)
		if models := registry.GetGlobalRegistry().GetModelsForClient(id); !containsRegisteredModel(models, "grok-cli-only-fixture") {
			t.Fatal("initial CLI catalog was not registered")
		}
	}
	next, err := config.Clone(cfg)
	if err != nil {
		t.Fatal(err)
	}
	next.XAI.DefaultBaseURLMode = "us-east-1"
	if result, errApply := service.ApplyRuntimeConfig(t.Context(), next); errApply != nil || !result.Applied || result.RestartRequired {
		t.Fatalf("upstream hot reload failed: %#v, %v", result, errApply)
	}
	for _, id := range []string{"xai-upstream-inherit", "xai-upstream-pinned"} {
		models := registry.GetGlobalRegistry().GetModelsForClient(id)
		if containsRegisteredModel(models, "grok-cli-only-fixture") != (id == "xai-upstream-pinned") || len(models) == 0 {
			t.Fatalf("endpoint change published the wrong catalog for %s: %v", id, registeredModelIDs(models))
		}
	}
}
