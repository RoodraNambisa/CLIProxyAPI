package cliproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestModelThinkingHotReloadPreservesCredentialLifecycle(t *testing.T) {
	for _, tc := range []struct{ family, provider, upstream string }{
		{"gemini-api-key", "gemini", "gemini-2.5-pro"}, {"interactions-api-key", "gemini-interactions", "gemini-2.5-pro"},
		{"claude-api-key", "claude", "claude-haiku-4-5-20251001"}, {"codex-api-key", "codex", "gpt-5.5"},
		{"vertex-api-key", "vertex", "gemini-2.5-pro"}, {"openai-compatibility", "openai-compatibility", "gpt-5.5"},
	} {
		t.Run(tc.family, func(t *testing.T) {
			makeConfig := func(support *registry.ThinkingSupport, isCompat bool) *config.Config {
				thinking, _ := json.Marshal(support)
				body := fmt.Sprintf(`{"%s":[{"api-key":"fixture","name":"thinking-compat","base-url":"https://example.test","models":[{"name":"%s","alias":"thinking-live-alias","thinking":%s,"is-compat":%t}]}]}`, tc.family, tc.upstream, thinking, isCompat)
				var cfg config.Config
				if err := json.Unmarshal([]byte(body), &cfg); err != nil {
					t.Fatal(err)
				}
				return &cfg
			}
			manager := coreauth.NewManager(nil, nil, nil)
			a := &coreauth.Auth{ID: "thinking-live-auth", Provider: tc.provider, Status: coreauth.StatusActive, Unavailable: true, NextRetryAfter: time.Now().Add(time.Hour), Attributes: map[string]string{"api_key": "fixture", "auth_kind": "apikey", "base_url": "https://example.test"}}
			if tc.provider == "openai-compatibility" {
				a.Attributes["compat_name"] = "thinking-compat"
				a.Attributes["provider_key"] = "thinking-compat"
			}
			installed, err := manager.Register(t.Context(), a)
			if err != nil {
				t.Fatal(err)
			}
			initial := &registry.ThinkingSupport{Levels: []string{"low", "high"}}
			s := &Service{cfg: makeConfig(initial, false), coreManager: manager}
			s.registerModelsForAuth(installed)
			r := registry.GetGlobalRegistry()
			t.Cleanup(func() { r.UnregisterClient(installed.ID) })
			get := func() *ModelInfo {
				t.Helper()
				for _, model := range r.GetModelsForClient(installed.ID) {
					if model.ID == "thinking-live-alias" {
						return model
					}
				}
				t.Fatal("configured thinking model missing")
				return nil
			}
			before := get()
			if !reflect.DeepEqual(before.Thinking, initial) || before.IsCompat {
				t.Fatal("initial thinking declaration missing")
			}
			r.SuspendClientModel(installed.ID, "thinking-live-alias", "fixture")
			executionCtx, finish, ok := installed.BeginRuntimeExecution(t.Context())
			if !ok {
				t.Fatal("execution fixture did not start")
			}
			defer finish()
			for _, update := range []struct {
				support  *registry.ThinkingSupport
				isCompat bool
			}{{initial, true}, {initial, false}, {&registry.ThinkingSupport{Levels: []string{" HIGH ", "none", "auto"}}, true}, {nil, true}, {nil, false}} {
				support := update.support
				if _, err := s.ApplyRuntimeConfig(t.Context(), makeConfig(support, update.isCompat)); err != nil {
					t.Fatal(err)
				}
				want := config.NormalizeModelThinkingSupport(support)
				if support == nil {
					want = registry.LookupStaticModelInfo(tc.upstream).Thinking
					if tc.provider == "openai-compatibility" {
						want = &registry.ThinkingSupport{Levels: []string{"low", "medium", "high"}}
					}
				}
				if !reflect.DeepEqual(get().Thinking, want) || get().IsCompat != (update.isCompat && tc.provider != "openai-compatibility") {
					t.Fatal("thinking update did not refresh the catalog")
				}
				current, _ := manager.GetByID(installed.ID)
				if current.RuntimeInstanceID() != installed.RuntimeInstanceID() || executionCtx.Err() != nil || !current.Unavailable || !current.NextRetryAfter.Equal(installed.NextRetryAfter) {
					t.Fatal("thinking update changed credential lifetime or cooldown")
				}
				for _, model := range r.GetAvailableModels("openai") {
					if model["id"] == "thinking-live-alias" {
						t.Fatal("thinking update cleared registry suspension")
					}
				}
			}
			if !reflect.DeepEqual(before.Thinking, initial) || before.IsCompat {
				t.Fatal("old thinking snapshot changed")
			}
			cancelled, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := s.ApplyRuntimeConfig(cancelled, makeConfig(initial, true)); err == nil {
				t.Fatal("cancelled capability update succeeded")
			}
			if reflect.DeepEqual(get().Thinking, initial) || get().IsCompat {
				t.Fatal("cancelled capability update did not roll back")
			}
		})
	}
}
