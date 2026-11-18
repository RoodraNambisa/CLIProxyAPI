package auth

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestManagedAPIModelAliasesAndCapabilitiesArePerKey(t *testing.T) {
	models := []config.OpenAICompatibilityModel{{Name: "native-b", Alias: "public", Thinking: &registry.ThinkingSupport{Levels: []string{"high"}}}}
	cfg := &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{Name: "upstream", Models: []config.OpenAICompatibilityModel{{Name: "native-a", Alias: "public", MaxContextLength: 1000}}, APIKeyEntries: []config.OpenAICompatibilityAPIKey{
		{APIKey: "same-key", APIManagement: config.APIManagement{CredentialID: "first"}},
		{APIKey: "same-key", APIManagement: config.APIManagement{CredentialID: "second"}, Models: &models},
	}}}}
	auths := map[string]*Auth{}
	for _, id := range []string{"first", "second"} {
		auths[id] = &Auth{ID: id, Provider: "upstream", Attributes: map[string]string{"api_key": "same-key", "config_id": id, "compat_name": "upstream", "provider_key": "upstream", "auth_kind": "apikey"}}
	}
	snapshot := buildAPIKeyModelRoutingSnapshot(auths, cfg)
	if snapshot.aliases["first"]["public"] != "native-a" || snapshot.aliases["second"]["public"] != "native-b" {
		t.Fatal("model alias leaked across keys", snapshot.aliases)
	}
	info, ok := lookupAPIKeyModelCapability(snapshot, auths["second"], "public", "native-b")
	if !ok || info.Thinking == nil || len(info.Thinking.Levels) != 1 || info.Thinking.Levels[0] != "high" {
		t.Fatal("per-key capabilities missing", info)
	}
	models[0].Name = "mutated"
	if snapshot.config.OpenAICompatibility[0].APIKeyEntries[1].Models == nil || (*snapshot.config.OpenAICompatibility[0].APIKeyEntries[1].Models)[0].Name != "native-b" {
		t.Fatal("in-flight snapshot retained mutable model list")
	}
}
