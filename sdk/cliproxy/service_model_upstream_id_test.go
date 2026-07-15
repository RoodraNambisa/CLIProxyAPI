package cliproxy

import (
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestRegisterModelsForAuth_PreservesUpstreamIDThroughOAuthAliasAndPrefix(t *testing.T) {
	service := &Service{cfg: &config.Config{
		SDKConfig: config.SDKConfig{ForceModelPrefix: true},
		OAuthModelAlias: map[string][]config.OAuthModelAlias{
			"codex": {
				{Name: "gpt-5.5", Alias: "codex-latest"},
			},
		},
	}}
	auth := &coreauth.Auth{
		ID:       "upstream-id-oauth-alias-prefix",
		Provider: "codex",
		Prefix:   "team-a",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"plan_type": "plus",
		},
	}
	cleanupRegisteredModels(t, auth.ID)

	service.registerModelsForAuth(auth)

	model := requireRegisteredModel(t, auth.ID, "team-a/codex-latest")
	if model.UpstreamID != "gpt-5.5" {
		t.Fatalf("upstream id = %q, want %q", model.UpstreamID, "gpt-5.5")
	}
	if containsRegisteredModel(registry.GetGlobalRegistry().GetModelsForClient(auth.ID), "team-a/gpt-5.5") {
		t.Fatal("OAuth rename unexpectedly retained the original client-visible model")
	}
}

func TestRegisterModelsForAuth_PreservesUpstreamIDThroughAPIKeyAliasAndPrefix(t *testing.T) {
	service := &Service{cfg: &config.Config{
		SDKConfig: config.SDKConfig{ForceModelPrefix: true},
		GeminiKey: []config.GeminiKey{
			{
				APIKey: "test-api-key",
				Models: []internalconfig.GeminiModel{
					{Name: "gemini-2.5-flash", Alias: "fast"},
				},
			},
		},
	}}
	auth := &coreauth.Auth{
		ID:       "upstream-id-api-key-alias-prefix",
		Provider: "gemini",
		Prefix:   "team-b",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"api_key":   "test-api-key",
			"auth_kind": "apikey",
		},
	}
	cleanupRegisteredModels(t, auth.ID)

	service.registerModelsForAuth(auth)

	model := requireRegisteredModel(t, auth.ID, "team-b/fast")
	if model.UpstreamID != "gemini-2.5-flash" {
		t.Fatalf("upstream id = %q, want %q", model.UpstreamID, "gemini-2.5-flash")
	}
	if model.DisplayName != "gemini-2.5-flash" {
		t.Fatalf("display name = %q, want existing upstream display name", model.DisplayName)
	}
}

func TestRegisterModelsForAuth_PreservesUpstreamIDThroughOpenAICompatAliasAndPrefix(t *testing.T) {
	service := &Service{cfg: &config.Config{
		SDKConfig: config.SDKConfig{ForceModelPrefix: true},
		OpenAICompatibility: []config.OpenAICompatibility{
			{
				Name: "compat-provider",
				Models: []config.OpenAICompatibilityModel{
					{Name: "provider-chat-model", Alias: "chat-latest"},
				},
			},
		},
	}}
	auth := &coreauth.Auth{
		ID:       "upstream-id-openai-compat-alias-prefix",
		Provider: "openai-compatibility",
		Label:    "compat-provider",
		Prefix:   "team-c",
		Status:   coreauth.StatusActive,
	}
	cleanupRegisteredModels(t, auth.ID)

	service.registerModelsForAuth(auth)

	model := requireRegisteredModel(t, auth.ID, "team-c/chat-latest")
	if model.UpstreamID != "provider-chat-model" {
		t.Fatalf("upstream id = %q, want %q", model.UpstreamID, "provider-chat-model")
	}
}

func TestApplyModelPrefixes_SetsUpstreamIDWithoutChangingSource(t *testing.T) {
	source := &ModelInfo{ID: "provider-model"}

	models := applyModelPrefixes([]*ModelInfo{source}, "team-d", true)

	if len(models) != 1 {
		t.Fatalf("prefixed model count = %d, want 1", len(models))
	}
	if models[0].ID != "team-d/provider-model" || models[0].UpstreamID != "provider-model" {
		t.Fatalf("prefixed model = %#v, want client prefix with provider upstream id", models[0])
	}
	if source.ID != "provider-model" || source.UpstreamID != "" {
		t.Fatalf("source model = %#v, want unchanged", source)
	}
}

func cleanupRegisteredModels(t *testing.T, authID string) {
	t.Helper()
	reg := GlobalModelRegistry()
	reg.UnregisterClient(authID)
	t.Cleanup(func() { reg.UnregisterClient(authID) })
}

func requireRegisteredModel(t *testing.T, authID, modelID string) *registry.ModelInfo {
	t.Helper()
	models := registry.GetGlobalRegistry().GetModelsForClient(authID)
	for _, model := range models {
		if model != nil && model.ID == modelID {
			return model
		}
	}
	t.Fatalf("registered model %q not found in %v", modelID, registeredModelIDs(models))
	return nil
}
