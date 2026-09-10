package cliproxy

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestModelInputModalitiesHotReloadPreservesCredentialLifecycle(t *testing.T) {
	makeConfig := func(modalities []string) *config.Config {
		return &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{
			Name: "modalities-runtime", BaseURL: "https://example.invalid",
			Models: []config.OpenAICompatibilityModel{{Name: "gpt-5.5", Alias: "modalities-live-alias", InputModalities: modalities}},
		}}}
	}
	manager := coreauth.NewManager(nil, nil, nil)
	installed, err := manager.Register(t.Context(), &coreauth.Auth{
		ID: t.Name(), Provider: "openai-compatibility", Status: coreauth.StatusActive,
		Unavailable: true, NextRetryAfter: time.Now().Add(time.Hour),
		Attributes: map[string]string{"api_key": "fixture", "auth_kind": "apikey", "base_url": "https://example.invalid", "compat_name": "modalities-runtime", "provider_key": "modalities-runtime"},
	})
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{cfg: makeConfig(nil), coreManager: manager}
	s.registerModelsForAuth(installed)
	r := registry.GetGlobalRegistry()
	t.Cleanup(func() { r.UnregisterClient(installed.ID) })
	get := func() *ModelInfo {
		t.Helper()
		models := r.GetModelsForClient(installed.ID)
		if len(models) != 1 || models[0].ID != "modalities-live-alias" {
			t.Fatal("configured model alias missing")
		}
		return models[0]
	}
	before := get()
	wantDefault := registry.LookupStaticModelInfo("gpt-5.5").SupportedInputModalities
	r.SuspendClientModel(installed.ID, "modalities-live-alias", "fixture")
	executionCtx, finish, ok := installed.BeginRuntimeExecution(t.Context())
	if !ok {
		t.Fatal("execution fixture did not start")
	}
	defer finish()
	for _, modalities := range [][]string{{"text"}, {"audio", "video"}, nil} {
		if _, err := s.ApplyRuntimeConfig(t.Context(), makeConfig(modalities)); err != nil {
			t.Fatal(err)
		}
		want := modalities
		if len(want) == 0 {
			want = wantDefault
		}
		if !reflect.DeepEqual(get().SupportedInputModalities, want) {
			t.Fatalf("hot reload retained stale input modalities, want %v", want)
		}
		current, _ := manager.GetByID(installed.ID)
		if current.RuntimeInstanceID() != installed.RuntimeInstanceID() || executionCtx.Err() != nil || !current.Unavailable || !current.NextRetryAfter.Equal(installed.NextRetryAfter) {
			t.Fatal("input declaration changed credential lifecycle or cooldown")
		}
		for _, model := range r.GetAvailableModels("openai") {
			if model["id"] == "modalities-live-alias" {
				t.Fatal("catalog refresh cleared model suspension")
			}
		}
	}
	if !reflect.DeepEqual(before.SupportedInputModalities, wantDefault) {
		t.Fatal("hot reload changed the original catalog snapshot")
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.ApplyRuntimeConfig(cancelled, makeConfig([]string{"text"})); err == nil {
		t.Fatal("cancelled configuration update succeeded")
	}
	if !reflect.DeepEqual(get().SupportedInputModalities, wantDefault) {
		t.Fatal("cancelled update changed the published catalog")
	}
}
