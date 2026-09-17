package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestPublicModelCatalogRespectsKeyProviderPriorityAndTarget(t *testing.T) {
	s := newTestServerWithConfig(t, func(cfg *config.Config) {
		cfg.APIKeys = []string{"grok-key", "codex-key", "tier-key", "exclude-key", "priority-only-key", "empty-key", "all-key"}
		cfg.APIKeyGroups = []config.APIKeyGroup{
			{APIKey: "grok-key", Providers: []string{"xai"}, AllowCredentialTargeting: true},
			{APIKey: "codex-key", Providers: []string{"codex"}},
			{APIKey: "tier-key", Providers: []string{"xai"}, AllowedPriorities: []int{3}, AllowCredentialTargeting: true},
			{APIKey: "exclude-key", Providers: []string{"xai"}, AllowedPriorities: []int{0, 3}, ExcludedPriorities: []int{3}},
			{APIKey: "priority-only-key", AllowedPriorities: []int{3}},
			{APIKey: "empty-key", AllowedPriorities: []int{99}},
		}
	})
	reg := registry.GetGlobalRegistry()
	for _, entry := range []struct {
		id, provider, model string
		priority            int
	}{
		{"catalog-grok", "xai", "catalog-grok-model", 3},
		{"catalog-other-grok", "xai", "catalog-other-grok-model", 0},
		{"catalog-codex", "codex", "catalog-codex-model", 3},
	} {
		attributes := map[string]string{}
		if entry.priority != 0 {
			attributes["priority"] = fmt.Sprint(entry.priority)
		}
		_, err := s.handlers.AuthManager.Register(t.Context(), &coreauth.Auth{ID: entry.id, Provider: entry.provider, Attributes: attributes, Metadata: map[string]any{"routing_alias": entry.id}})
		if err != nil {
			t.Fatal(err)
		}
		reg.RegisterClient(entry.id, entry.provider, []*registry.ModelInfo{{ID: entry.model, Name: entry.model}})
		t.Cleanup(func() { reg.UnregisterClient(entry.id) })
	}
	for _, format := range []struct{ name, path, agent, array, field string }{
		{"openai", "/v1/models", "", "data", "id"},
		{"codex-client", "/v1/models?client_version=0.134.0", "", "models", "slug"},
		{"grok-build", "/v1/models", "grok-shell/0.2.120", "data", "model"},
		{"claude", "/v1/models", "claude-cli/1.0", "data", "id"},
		{"gemini", "/v1beta/models", "", "models", "name"},
	} {
		for _, test := range []struct {
			key  string
			want []string
		}{
			{"grok-key", []string{"catalog-grok-model", "catalog-other-grok-model"}},
			{"codex-key", []string{"catalog-codex-model"}},
			{"tier-key", []string{"catalog-grok-model"}},
			{"exclude-key", []string{"catalog-other-grok-model"}},
			{"priority-only-key", []string{"catalog-grok-model", "catalog-codex-model"}},
			{"empty-key", nil},
			{"grok-key-auth-catalog-other-grok", []string{"catalog-other-grok-model"}},
			{"all-key", []string{"catalog-grok-model", "catalog-other-grok-model", "catalog-codex-model"}},
		} {
			t.Run(format.name+"/"+test.key, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodGet, format.path, nil)
				req.Header.Set("Authorization", "Bearer "+test.key)
				req.Header.Set("User-Agent", format.agent)
				w := httptest.NewRecorder()
				s.engine.ServeHTTP(w, req)
				if w.Code != 200 {
					t.Fatalf("catalog status=%d %s", w.Code, w.Body.String())
				}
				if w.Header().Get("Cache-Control") != "private, no-store" || !strings.Contains(strings.Join(w.Header().Values("Vary"), ","), "Authorization") {
					t.Fatal("authenticated catalog permits reuse across keys")
				}
				var got []string
				for _, model := range gjson.GetBytes(w.Body.Bytes(), format.array).Array() {
					got = append(got, strings.TrimPrefix(model.Get(format.field).String(), "models/"))
				}
				want := slices.Clone(test.want)
				slices.Sort(want)
				slices.Sort(got)
				if !slices.Equal(got, want) {
					t.Fatalf("models=%v want=%v", got, want)
				}
			})
		}
	}
	for _, test := range []struct {
		key    string
		status int
	}{
		{"grok-key-auth-catalog-codex", http.StatusForbidden},
		{"tier-key-auth-catalog-other-grok", http.StatusForbidden},
		{"invalid-key", http.StatusUnauthorized},
	} {
		req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
		req.Header.Set("Authorization", "Bearer "+test.key)
		w := httptest.NewRecorder()
		s.engine.ServeHTTP(w, req)
		if w.Code != test.status {
			t.Fatalf("%s status=%d, want %d", test.key, w.Code, test.status)
		}
	}

	updated, err := config.Clone(s.currentConfig())
	if err != nil {
		t.Fatal(err)
	}
	updated.APIKeyGroups[2].AllowedPriorities = []int{0}
	if errUpdate := s.UpdateClients(updated); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	for _, header := range []string{"Authorization", "X-Api-Key", "X-Goog-Api-Key", "query"} {
		t.Run("hot-reload/"+header, func(t *testing.T) {
			path := "/v1/models"
			if header == "query" {
				path += "?key=tier-key"
			}
			req := httptest.NewRequest(http.MethodGet, path, nil)
			switch header {
			case "Authorization":
				req.Header.Set(header, "Bearer tier-key")
			case "query":
			default:
				req.Header.Set(header, "tier-key")
			}
			w := httptest.NewRecorder()
			s.engine.ServeHTTP(w, req)
			if w.Code != http.StatusOK || gjson.GetBytes(w.Body.Bytes(), "data.#").Int() != 1 || gjson.GetBytes(w.Body.Bytes(), "data.0.id").String() != "catalog-other-grok-model" {
				t.Fatalf("hot reload catalog: %d %s", w.Code, w.Body.String())
			}
		})
	}
}
