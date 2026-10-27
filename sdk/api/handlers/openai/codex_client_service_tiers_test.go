package openai

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	"github.com/tidwall/gjson"
)

func TestCodexClientServiceTierProjection(t *testing.T) {
	original := map[string]any{"slug": "alias", "service_tiers": []any{map[string]any{"id": "priority", "name": "Fast", "description": "upstream description"}}, "additional_speed_tiers": []any{"fast", "ultrafast"}, "default_service_tier": "priority", "context_window": 1234}
	rules := []config.CodexServiceTierRule{{Models: []string{"alias"}, Tiers: []string{"priority", "ultrafast", "flex"}, Default: "ultrafast"}}
	for _, allow := range []bool{false, true} {
		entry := cloneCodexClientModelMap(original)
		applyCodexClientServiceTiers(entry, &config.SDKConfig{CodexAllowClientServiceTier: allow, CodexServiceTierRules: rules})
		if entry["context_window"] != 1234 {
			t.Fatal("unrelated metadata changed")
		}
		if allow {
			tiers := entry["service_tiers"].([]any)
			if len(tiers) != 3 || entry["default_service_tier"] != "ultrafast" || tiers[0].(map[string]any)["description"] != "upstream description" {
				t.Fatal("configured tiers/default or upstream description lost")
			}
		} else if len(entry["service_tiers"].([]any)) != 0 || len(entry["additional_speed_tiers"].([]any)) != 0 || entry["default_service_tier"] != nil {
			t.Fatal("disabled tiers still advertised")
		}
	}
	inherited := cloneCodexClientModelMap(original)
	applyCodexClientServiceTiers(inherited, &config.SDKConfig{CodexAllowClientServiceTier: true})
	if !reflect.DeepEqual(inherited, original) {
		t.Fatal("unmatched model lost catalog defaults")
	}
	entry := cloneCodexClientModelMap(original)
	applyCodexClientServiceTiers(entry, &config.SDKConfig{CodexAllowClientServiceTier: true, CodexServiceTierRules: []config.CodexServiceTierRule{{Models: []string{"alias"}}}})
	if len(entry["service_tiers"].([]any)) != 0 || entry["default_service_tier"] != nil {
		t.Fatal("standard-only declaration failed")
	}
	synthetic := buildCodexClientModels([]map[string]any{{"id": "new-synthetic-tier-model"}}, nil, nil)[0]
	if len(synthetic["additional_speed_tiers"].([]any)) != 0 || synthetic["default_service_tier"] != nil {
		t.Fatal("synthetic model inherited stale speed metadata")
	}
}

func TestCodexClientServiceTierEndpointReloadAndProviderScope(t *testing.T) {
	r := registry.GetGlobalRegistry()
	for _, provider := range []string{"codex", "claude"} {
		id := t.Name() + provider
		r.RegisterClient(id, provider, []*registry.ModelInfo{{ID: "tier-alias"}})
		t.Cleanup(func() { r.UnregisterClient(id) })
	}
	h := NewOpenAIAPIHandler(handlers.NewBaseAPIHandlers(&config.SDKConfig{}, nil))
	for _, allow := range []bool{false, true, false} {
		h.UpdateClients(&config.SDKConfig{CodexAllowClientServiceTier: allow, CodexServiceTierRules: []config.CodexServiceTierRule{{Models: []string{"tier-alias"}, Tiers: []string{"ultrafast"}, Default: "ultrafast"}}})
		for _, provider := range []string{"codex", "claude", ""} {
			rec := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(rec)
			ctx.Request = httptest.NewRequest(http.MethodGet, "/v1/models?client_version=0.162.1", nil)
			ctx.Set("accessMetadata", map[string]string{sdkaccess.MetadataAllowedProviders: provider})
			h.OpenAIModels(ctx)
			entry := gjson.GetBytes(rec.Body.Bytes(), `models.#(slug=="tier-alias")`)
			if !entry.Exists() {
				t.Fatal("alias not returned")
			}
			want := allow && provider == "codex"
			if (len(entry.Get("service_tiers").Array()) == 1) != want {
				t.Fatalf("scope=%s allow=%t has wrong tiers: %s", provider, allow, entry.Raw)
			}
		}
	}
}
