package cliproxy

import (
	"slices"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestImageModelRegistrationAndReloadAreProviderScoped(t *testing.T) {
	previous := &config.Config{SDKConfig: config.SDKConfig{Images: config.ImagesConfig{
		ImageModels: []string{"tool-only"},
		ChatGPTWeb:  config.ChatGPTWebImageConfig{ImageModels: []string{"web-only", "gpt-image-2.5"}},
	}}}
	service := &Service{cfg: previous}
	for _, provider := range []string{"codex", "chatgpt-web"} {
		auth := &coreauth.Auth{ID: "independent-image-models-" + provider, Provider: provider, Status: coreauth.StatusActive,
			Attributes: map[string]string{"plan_type": "plus"}, Metadata: map[string]any{"lifecycle_state": coreauth.LifecycleStateActive, "access_token": "token"}}
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
		service.registerModelsForAuth(auth)
		models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
		for _, model := range models {
			if provider == "codex" && model.ID == "web-only" || provider == "chatgpt-web" && model.ID == "tool-only" {
				t.Fatalf("model %s leaked into %s", model.ID, provider)
			}
		}
		want := "tool-only"
		if provider == "chatgpt-web" {
			want = "web-only"
		}
		if !containsRegisteredModel(models, want) {
			t.Fatalf("%s missing %s: %v", provider, want, registeredModelIDs(models))
		}
	}
	next := *previous
	next.Images.ChatGPTWeb.ImageModels = []string{"new-web"}
	if shouldRefreshCodexRegistrations(previous, &next) || !shouldRefreshChatGPTWebRegistrations(previous, &next) {
		t.Fatal("Web alias change refreshed the wrong provider")
	}
	next = *previous
	next.Images.ImageModels = []string{"new-tool"}
	if !shouldRefreshCodexRegistrations(previous, &next) || shouldRefreshChatGPTWebRegistrations(previous, &next) {
		t.Fatal("Codex tool change refreshed the wrong provider")
	}
	next = *previous
	next.Images.Native.Generations = config.NativeImageEndpointConfig{Enabled: true, Models: []string{"new-native"}}
	if !shouldRefreshCodexRegistrations(previous, &next) || shouldRefreshChatGPTWebRegistrations(previous, &next) || !slices.Equal(configuredChatGPTWebImageModels(previous), configuredChatGPTWebImageModels(&next)) {
		t.Fatal("native switch affected Web registration")
	}
}

func TestDefaultImage25CatalogRegistration(t *testing.T) {
	service := &Service{cfg: &config.Config{}}
	auth := &coreauth.Auth{ID: "image25-catalog", Provider: "codex", Status: coreauth.StatusActive, Attributes: map[string]string{"plan_type": "plus"}}
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	service.registerModelsForAuth(auth)
	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	for _, id := range []string{"gpt-image-2.5", "gpt-image-2.5-flare", "gpt-image-2.5-sunburst"} {
		found := false
		for _, model := range models {
			if model.ID == id {
				found = true
				if model.Type != "openai" || model.OwnedBy != "openai" {
					t.Fatalf("%s does not match the existing Codex image catalog: %+v", id, model)
				}
			}
		}
		if !found {
			t.Fatalf("missing %s", id)
		}
	}
}
