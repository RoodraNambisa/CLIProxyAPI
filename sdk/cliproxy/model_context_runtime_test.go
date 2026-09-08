package cliproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestModelContextHotReloadPreservesCredentialLifecycle(t *testing.T) {
	for _, tc := range []struct{ family, provider, upstream string }{
		{"gemini-api-key", "gemini", "gemini-2.5-pro"}, {"interactions-api-key", "gemini-interactions", "gemini-2.5-pro"},
		{"claude-api-key", "claude", "claude-haiku-4-5-20251001"}, {"codex-api-key", "codex", "gpt-5.5"},
		{"vertex-api-key", "vertex", "gemini-2.5-pro"}, {"openai-compatibility", "openai-compatibility", "gpt-5.5"},
	} {
		t.Run(tc.family, func(t *testing.T) {
			makeConfig := func(limit int) *config.Config {
				var cfg config.Config
				body := fmt.Sprintf(`{"%s":[{"api-key":"fixture","name":"context-compat","base-url":"https://example.test","models":[{"name":"%s","alias":"context-live-alias","max-context-length":%d}]}]}`, tc.family, tc.upstream, limit)
				if err := json.Unmarshal([]byte(body), &cfg); err != nil {
					t.Fatal(err)
				}
				return &cfg
			}
			manager := coreauth.NewManager(nil, nil, nil)
			a := &coreauth.Auth{ID: "context-live-auth", Provider: tc.provider, Status: coreauth.StatusActive, Unavailable: true, NextRetryAfter: time.Now().Add(time.Hour), Attributes: map[string]string{"api_key": "fixture", "auth_kind": "apikey", "base_url": "https://example.test"}}
			if tc.provider == "openai-compatibility" {
				a.Attributes["compat_name"] = "context-compat"
				a.Attributes["provider_key"] = "context-compat"
			}
			installed, err := manager.Register(t.Context(), a)
			if err != nil {
				t.Fatal(err)
			}
			s := &Service{cfg: makeConfig(131072), coreManager: manager}
			s.registerModelsForAuth(installed)
			r := registry.GetGlobalRegistry()
			t.Cleanup(func() { r.UnregisterClient(installed.ID) })
			get := func() *ModelInfo {
				t.Helper()
				for _, model := range r.GetModelsForClient(installed.ID) {
					if model.ID == "context-live-alias" {
						return model
					}
				}
				t.Fatal("configured context model missing")
				return nil
			}
			before := get()
			if before.MaxContextLength != 131072 {
				t.Fatal("initial override missing")
			}
			r.SuspendClientModel(installed.ID, "context-live-alias", "fixture")
			executionCtx, finish, ok := installed.BeginRuntimeExecution(t.Context())
			if !ok {
				t.Fatal("execution fixture did not start")
			}
			defer finish()
			for _, limit := range []int{1048576, 0} {
				if _, err := s.ApplyRuntimeConfig(t.Context(), makeConfig(limit)); err != nil {
					t.Fatal(err)
				}
				got := get()
				if got.MaxContextLength != limit {
					t.Fatal("hot update did not publish override")
				}
				want := limit
				if limit == 0 {
					base := registry.LookupStaticModelInfo(tc.upstream)
					if base == nil {
						t.Fatal("fixture upstream model missing")
					}
					want = base.ContextLength
					if want == 0 {
						want = base.InputTokenLimit
					}
				}
				if got.ContextLength != want {
					t.Fatal("advertised window differs from override")
				}
				current, _ := manager.GetByID(installed.ID)
				if current.RuntimeInstanceID() != installed.RuntimeInstanceID() || executionCtx.Err() != nil || !current.Unavailable || !current.NextRetryAfter.Equal(installed.NextRetryAfter) {
					t.Fatal("declaration edit changed credential lifecycle or cooldown")
				}
				for _, model := range r.GetAvailableModels("openai") {
					if model["id"] == "context-live-alias" {
						t.Fatal("declaration edit cleared registry suspension")
					}
				}
			}
			if before.MaxContextLength != 131072 {
				t.Fatal("old catalog snapshot changed")
			}
			cancelled, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := s.ApplyRuntimeConfig(cancelled, makeConfig(131072)); err == nil {
				t.Fatal("cancelled update succeeded")
			}
			if get().MaxContextLength != 0 {
				t.Fatal("cancelled update did not roll back")
			}
		})
	}
}
