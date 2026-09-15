package helps

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestXAIModelRoutesPrecedenceAndFrozenPlan(t *testing.T) {
	cfg := &config.Config{XAI: config.XAIConfig{DefaultBaseURLMode: "cli", ModelRoutes: []config.XAIModelRoute{
		{Models: []string{"*"}, Upstream: "api"},
		{Models: []string{"grok-4.6"}, Upstream: "cli"},
	}}}
	auth := &coreauth.Auth{Metadata: map[string]any{"base_url": "https://custom.example/v1", XAIModelRoutesKey: []config.XAIModelRoute{
		{Models: []string{"grok-4.3"}, Upstream: "eu-west-1"},
		{Models: []string{"grok-4.5"}, Upstream: "default"},
	}}}
	for _, tc := range []struct{ model, url, source string }{
		{"grok-4.6(high)", "https://cli-chat-proxy.grok.com/v1", "global-rule"},
		{"grok-4.3", "https://eu-west-1.api.x.ai/v1", "credential-rule"},
		{"grok-4.5", "https://custom.example/v1", "credential-rule"},
		{"grok-imagine-image", "https://api.x.ai/v1", "global-rule"},
	} {
		got, err := ResolveXAIModelUpstream(auth, cfg, tc.model)
		if err != nil || got.BaseURL != tc.url || got.Source != tc.source {
			t.Fatalf("%s: %+v %v", tc.model, got, err)
		}
	}
	plan, err := NewXAIRequestPlan(t.Context(), cfg, coreexecutor.Request{Payload: []byte(`{}`)}, coreexecutor.Options{})
	if err != nil {
		t.Fatal(err)
	}
	cfg.XAI.ModelRoutes[1].Upstream = "api"
	if got, _ := ResolveXAIModelUpstream(auth, plan.Config, "grok-4.6"); got.Mode != "cli" {
		t.Fatal("plan's route changed")
	}
	if got, _ := XAIModelAPIOnlyBaseURL(auth, plan.Config, "grok-4.6"); got != "https://api.x.ai/v1" {
		t.Fatal("CLI transport fallback lost")
	}
	auth.Metadata[XAIModelRoutesKey] = []config.XAIModelRoute{{Models: []string{"grok-*"}, Upstream: "us-west-2"}}
	if got, _ := ResolveXAIModelUpstream(auth, plan.Config, "grok-4.6"); got.Mode != "us-west-2" {
		t.Fatal("account wildcard must outrank global exact rule")
	}
	auth.Metadata[XAIModelRoutesKey] = "invalid"
	if _, err := ResolveXAIModelUpstream(auth, cfg, "grok-4.6"); err == nil {
		t.Fatal("invalid account routing silently accepted")
	}
}

func TestXAICatalogMergeProvenanceLegacyAndManualModels(t *testing.T) {
	cli := "https://cli-chat-proxy.grok.com/v1/models"
	api := "https://api.x.ai/v1/models"
	mk := func(source string, ids ...string) *XAIModelCatalog {
		c := &XAIModelCatalog{Source: source, UpdatedAt: time.Now().UTC()}
		for _, id := range ids {
			c.Models = append(c.Models, &registry.ModelInfo{ID: id})
		}
		return c
	}
	auth := &coreauth.Auth{Metadata: map[string]any{XAIModelCatalogKey: mk(cli, "grok-4.6", "grok-4.5"), XAIModelCatalogsKey: []*XAIModelCatalog{mk(api, "grok-4.6", "grok-4.3")}}}
	cfg := &config.Config{XAI: config.XAIConfig{ModelCatalogSources: []string{"cli", "api"}, ModelRoutes: []config.XAIModelRoute{{Models: []string{"grok-manual", "not-listed-*"}, Upstream: "api"}}}}
	models := XAIMergedModelsForAuth(auth, cfg)
	if len(models) != 4 {
		t.Fatalf("union lost IDs or invented wildcard: %v", models)
	}
	if got := XAIModelCatalogProvenance(auth, cfg)["grok-4.6"]; !reflect.DeepEqual(got, []string{cli, api}) {
		t.Fatalf("provenance: %v", got)
	}
	raw, _ := json.Marshal(auth.Metadata)
	var loaded map[string]any
	_ = json.Unmarshal(raw, &loaded)
	if got := XAIMergedModelsForAuth(&coreauth.Auth{Metadata: loaded}, cfg); len(got) != 4 {
		t.Fatal("restart lost catalogs")
	}
	auth.Metadata[XAICatalogSourcesKey] = []string{"default"}
	models = XAIMergedModelsForAuth(auth, cfg)
	for _, m := range models {
		if m.ID == "grok-4.3" {
			t.Fatal("inactive API catalog leaked into registration")
		}
	}
	other := &coreauth.Auth{Metadata: map[string]any{XAICatalogSourcesKey: []string{"cli", "https://CLI-CHAT-PROXY.grok.com:443/v1/"}}}
	endpoints, err := XAICatalogEndpoints(other, cfg)
	if err != nil || !reflect.DeepEqual(endpoints, []string{cli}) {
		t.Fatalf("duplicate source normalization: %v %v", endpoints, err)
	}
}

func TestXAIEmptyCatalogIsAuthoritativeAndConcurrentSnapshotsAreMonotonic(t *testing.T) {
	endpoint := "https://api.x.ai/v1/models"
	empty, err := ParseXAIModels([]byte(`{"data":[]}`), endpoint)
	if err != nil {
		t.Fatal(err)
	}
	auth := &coreauth.Auth{Metadata: map[string]any{XAIModelCatalogsKey: []*XAIModelCatalog{empty}}}
	cfg := &config.Config{XAI: config.XAIConfig{ModelCatalogSources: []string{"api"}}}
	if models := XAIMergedModelsForAuth(auth, cfg); len(models) != 0 {
		t.Fatal("empty authoritative directory was replaced by all builtin models")
	}
	newer := *empty
	newer.UpdatedAt = empty.UpdatedAt.Add(time.Minute)
	older := *empty
	older.UpdatedAt = empty.UpdatedAt.Add(-time.Minute)
	other := *empty
	other.Source = "https://cli-chat-proxy.grok.com/v1/models"
	auth.Metadata[XAIModelCatalogsKey] = []*XAIModelCatalog{&newer, &other}
	merged := MergeXAIModelCatalogs(auth, map[string]*XAIModelCatalog{endpoint: &older}, []string{endpoint})
	if len(merged) != 2 || !merged[0].UpdatedAt.Equal(newer.UpdatedAt) {
		t.Fatal("late refresh lost a newer or deselected snapshot")
	}
}
