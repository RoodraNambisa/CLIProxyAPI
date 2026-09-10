package cliproxy

import (
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers/openai"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCompatibilityModelInputModalitiesOverrideAndInheritance(t *testing.T) {
	for _, modalities := range [][]string{nil, {"text"}, {" TEXT ", "image", "image"}, {"audio", "video"}} {
		t.Run(stringKeyForModalities(modalities), func(t *testing.T) {
			client := t.Name()
			cfg := &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{Name: client, Models: []config.OpenAICompatibilityModel{{Name: "gpt-5.5", Alias: "local", InputModalities: modalities}}}}}
			cfg.ForceModelPrefix = true
			s := &Service{cfg: cfg}
			auth := &coreauth.Auth{ID: client, Provider: "openai-compatibility", Prefix: "tenant", Attributes: map[string]string{"compat_name": client, "provider_key": client}}
			s.registerModelsForAuth(auth)
			reg := registry.GetGlobalRegistry()
			t.Cleanup(func() { reg.UnregisterClient(client) })
			models := reg.GetModelsForClient(client)
			want, _ := config.NormalizeModelInputModalities(modalities)
			if len(want) == 0 {
				want = registry.LookupStaticModelInfo("gpt-5.5").SupportedInputModalities
			}
			if len(models) != 1 || models[0].ID != "tenant/local" || !reflect.DeepEqual(models[0].SupportedInputModalities, want) {
				t.Fatal("advertised modalities lost the explicit override, catalog inheritance or model prefix")
			}
			catalog := openai.CodexClientModelsResponse(reg.GetAvailableModels("openai"))
			var found bool
			for _, entry := range catalog["models"].([]map[string]any) {
				if entry["slug"] != "tenant/local" {
					continue
				}
				found = true
				codexWant := make([]any, 0, 2)
				for _, modality := range want {
					if modality == "text" || modality == "image" {
						codexWant = append(codexWant, modality)
					}
				}
				if !reflect.DeepEqual(entry["input_modalities"], codexWant) {
					t.Fatalf("client catalog advertised undeclared inputs: %v, want %v", entry["input_modalities"], codexWant)
				}
			}
			if !found {
				t.Fatal("configured alias disappeared from the client catalog")
			}
			models[0].SupportedInputModalities[0] = "changed"
			if !reflect.DeepEqual(reg.GetModelsForClient(client)[0].SupportedInputModalities, want) {
				t.Fatal("model advertisement returned a shared modality slice")
			}
		})
	}
}

func stringKeyForModalities(modalities []string) string {
	if len(modalities) == 0 {
		return "inherit"
	}
	return modalities[0]
}
