package cliproxy

import (
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestEnsureExecutorsForInteractionsAuth(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	auth := &coreauth.Auth{
		ID:       "interactions-auth",
		Provider: "gemini-interactions",
		Status:   coreauth.StatusActive,
	}

	service.ensureExecutorsForAuth(auth)
	bound, ok := service.coreManager.Executor("gemini-interactions")
	if !ok || bound == nil {
		t.Fatal("expected gemini-interactions executor to be registered")
	}
	if got := bound.Identifier(); got != "gemini-interactions" {
		t.Fatalf("executor identifier = %q, want gemini-interactions", got)
	}
}

func TestRegisterModelsForInteractionsAuthUsesOwnConfig(t *testing.T) {
	service := &Service{cfg: &config.Config{
		GeminiKey: []config.GeminiKey{{
			APIKey: "shared-key",
			Models: []internalconfig.GeminiModel{{Name: "gemini-2.5-pro", Alias: "regular-only"}},
		}},
		InteractionsKey: []config.GeminiKey{
			{
				APIKey:  "SHARED-KEY",
				BaseURL: "https://INTERACTIONS.example.com",
				Models:  []internalconfig.GeminiModel{{Name: "gemini-2.5-pro", Alias: "wrong-case"}},
			},
			{
				APIKey:         "shared-key",
				BaseURL:        "https://interactions.example.com",
				Models:         []internalconfig.GeminiModel{{Name: "gemini-2.5-flash", Alias: "native-flash"}, {Name: "gemini-2.5-pro", Alias: "hidden"}},
				ExcludedModels: []string{"hidden"},
			},
		},
	}}
	auth := &coreauth.Auth{
		ID:       "interactions-model-registration",
		Provider: "gemini-interactions",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"api_key":  "shared-key",
			"base_url": "https://interactions.example.com",
		},
	}

	modelRegistry := registry.GetGlobalRegistry()
	modelRegistry.UnregisterClient(auth.ID)
	t.Cleanup(func() { modelRegistry.UnregisterClient(auth.ID) })

	service.registerModelsForAuth(auth)
	models := modelRegistry.GetModelsForClient(auth.ID)
	if !containsRegisteredModel(models, "native-flash") {
		t.Fatalf("native-flash not registered: %#v", models)
	}
	if containsRegisteredModel(models, "hidden") || containsRegisteredModel(models, "regular-only") || containsRegisteredModel(models, "wrong-case") {
		t.Fatalf("registered models leaked excluded or regular Gemini config: %#v", models)
	}
}
