package auth

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestConfiguredModelCapabilityRoutesAreExactAndPrivate(t *testing.T) {
	models := []config.CodexModel{
		{Name: "upstream", Alias: "shared", IsCompat: true, Thinking: &registry.ThinkingSupport{Levels: []string{" LOW ", "none", "none"}}},
		{Name: "upstream(high)", Alias: "shared", Thinking: &registry.ThinkingSupport{Levels: []string{"high"}}},
		{Name: "other", Alias: "shared", Thinking: &registry.ThinkingSupport{Levels: []string{"xhigh"}}},
		{Name: "upstream", Alias: "shared", Thinking: &registry.ThinkingSupport{Levels: []string{"max"}}},
		{Name: "gpt-5.4(high)", Alias: "inherited"},
		{Name: "private-unknown", Alias: "custom"},
		{Name: "empty", Thinking: &registry.ThinkingSupport{Levels: []string{}}},
	}
	byRoute := make(map[string][]apiKeyModelCapabilityRoute)
	compileConfiguredModelCapabilities(byRoute, models, "codex")
	auth := &Auth{ID: "one", Provider: "codex", Prefix: "tenant", Attributes: map[string]string{"api_key": "fixture"}}
	snapshot := &apiKeyModelRoutingSnapshot{capabilities: apiKeyModelCapabilityTable{"one": byRoute}}
	for _, tc := range []struct{ route, upstream, level string }{
		{"tenant/SHARED(high)", "upstream(high)", "high"},
		{"shared", "upstream(low)", "low"},
		{"shared", "other(high)", "xhigh"},
		{"upstream(high)", "upstream(high)", "high"},
	} {
		info, ok := lookupAPIKeyModelCapability(snapshot, auth, tc.route, tc.upstream)
		if !ok || info.UserDefined || info.Thinking.Levels[0] != tc.level || info.IsCompat != (tc.level == "low") {
			t.Fatalf("wrong capability route %q -> %q", tc.route, tc.upstream)
		}
	}
	for _, tc := range []struct{ route, upstream string }{{"missing", "upstream"}, {"shared", "unrelated"}, {"tenant2/shared", "upstream"}, {"inherited", "gpt-5.4(low)"}} {
		if _, ok := lookupAPIKeyModelCapability(snapshot, auth, tc.route, tc.upstream); ok {
			t.Fatalf("unrelated route matched: %v", tc)
		}
	}
	for _, candidate := range []*Auth{nil, {ID: "two", Provider: "codex", Attributes: auth.Attributes}, {ID: "one", Provider: "codex"}} {
		if _, ok := lookupAPIKeyModelCapability(snapshot, candidate, "shared", "upstream"); ok {
			t.Fatal("cross-credential or OAuth match")
		}
	}
	for _, route := range []string{"custom", "inherited"} {
		info := byRoute[route][0].modelInfo
		if !info.UserDefined {
			t.Fatal("missing native override lost legacy passthrough")
		}
	}
	if byRoute["inherited"][0].modelInfo.Thinking == nil {
		t.Fatal("configured suffix prevented static inheritance")
	}
	if byRoute["inherited"][0].modelInfo.MaxCompletionTokens != 0 {
		t.Fatal("thinking declaration changed the local configured-model output default")
	}
	if info := byRoute["empty"][0].modelInfo; info.UserDefined || info.Thinking == nil || len(info.Thinking.Levels) != 0 {
		t.Fatal("empty declaration became inheritance")
	}
	source := cliproxyexecutor.Request{Model: "upstream(low)", Metadata: map[string]any{"keep": "value"}}
	bound := attachResolvedAPIKeyModelInfo(snapshot, source, auth, "shared", source.Model)
	if len(source.Metadata) != 1 || bound.Metadata["keep"] != "value" {
		t.Fatal("attachment mutated incoming metadata")
	}
	first, ok := ResolvedAPIKeyModelInfo(bound)
	if !ok || !first.IsCompat || !first.Thinking.ZeroAllowed || !reflect.DeepEqual(first.Thinking.Levels, []string{"low", "none"}) {
		t.Fatal("normalization or derived capability lost")
	}
	first.Thinking.Levels[0], first.ID = "changed", "changed"
	first.IsCompat = false
	models[0].Thinking.Levels[0] = "changed-source"
	models[0].IsCompat = false
	second, _ := ResolvedAPIKeyModelInfo(bound)
	if second.ID != "upstream" || second.Thinking.Levels[0] != "low" || !second.IsCompat {
		t.Fatal("caller or config mutation reached published snapshot")
	}
	public, err := json.Marshal(second)
	if err != nil || strings.Contains(string(public), "compat") {
		t.Fatal("internal compatibility policy leaked into the public catalog")
	}
	var remote registry.ModelInfo
	if err := json.Unmarshal([]byte(`{"id":"fixture","is-compat":true,"is_compat":true,"IsCompat":true}`), &remote); err != nil || remote.IsCompat {
		t.Fatal("remote model data enabled a local compatibility policy")
	}
	if _, ok := ResolvedAPIKeyModelInfo(source); ok {
		t.Fatal("unbound source gained capabilities")
	}
	cleared := attachResolvedAPIKeyModelInfo(snapshot, bound, nil, "shared", source.Model)
	if _, ok := ResolvedAPIKeyModelInfo(cleared); ok {
		t.Fatal("unmatched attempt retained another credential's binding")
	}
	if _, ok := ResolvedAPIKeyModelInfo(bound); !ok {
		t.Fatal("clearing changed the prior request")
	}
	if _, ok := ResolvedAPIKeyModelInfo(cliproxyexecutor.Request{Metadata: map[string]any{resolvedAPIKeyModelInfoMetadataKey: second}}); ok {
		t.Fatal("untyped metadata accepted as an internal binding")
	}
}

func TestConfiguredModelCapabilityCompilerSelectsEachCredentialFamily(t *testing.T) {
	support := &registry.ThinkingSupport{Levels: []string{"high"}}
	cfg := &config.Config{
		GeminiKey:           []config.GeminiKey{{APIKey: "fixture", Models: []config.GeminiModel{{Name: "native", Alias: "local", IsCompat: true, Thinking: support}}}},
		InteractionsKey:     []config.GeminiKey{{APIKey: "fixture", BaseURL: "https://case.invalid", Models: []config.GeminiModel{{Name: "native", Alias: "local", IsCompat: true, Thinking: support}}}},
		ClaudeKey:           []config.ClaudeKey{{APIKey: "fixture", Models: []config.ClaudeModel{{Name: "native", Alias: "local", IsCompat: true, Thinking: support}}}},
		CodexKey:            []config.CodexKey{{APIKey: "fixture", Models: []config.CodexModel{{Name: "native", Alias: "local", IsCompat: true, Thinking: support}}}},
		VertexCompatAPIKey:  []config.VertexCompatKey{{APIKey: "fixture", Models: []config.VertexCompatModel{{Name: "native", Alias: "local", IsCompat: true, Thinking: support}}}},
		OpenAICompatibility: []config.OpenAICompatibility{{Name: "compat", Models: []config.OpenAICompatibilityModel{{Name: "native", Alias: "local", Thinking: support}, {Name: "default"}}}},
	}
	for _, provider := range []string{"gemini", "gemini-interactions", "claude", "codex", "vertex", "compat"} {
		t.Run(provider, func(t *testing.T) {
			auth := &Auth{ID: provider, Provider: provider, Attributes: map[string]string{"api_key": "fixture"}}
			if provider == "gemini-interactions" {
				auth.Attributes["base_url"] = "https://case.invalid"
			}
			if provider == "compat" {
				auth.Attributes["compat_name"] = "compat"
			}
			compiled := compileAPIKeyModelCapabilitiesForAuth(cfg, auth)
			if len(compiled["local"]) != 1 || compiled["local"][0].modelInfo.IsCompat != (provider != "compat") || !reflect.DeepEqual(compiled["local"][0].modelInfo.Thinking.Levels, []string{"high"}) {
				t.Fatal("selected credential definition not compiled")
			}
			if provider == "compat" {
				info := compiled["default"][0].modelInfo
				if info.UserDefined || info.IsCompat || !reflect.DeepEqual(info.Thinking.Levels, []string{"low", "medium", "high"}) {
					t.Fatal("compatibility default changed")
				}
			}
			if provider == "gemini-interactions" {
				auth.Attributes["base_url"] = "https://CASE.invalid"
				if len(compileAPIKeyModelCapabilitiesForAuth(cfg, auth)) != 0 {
					t.Fatal("Interactions ignored exact credential matching")
				}
			}
		})
	}
	if compileAPIKeyModelCapabilitiesForAuth(nil, nil) != nil {
		t.Fatal("nil input produced capabilities")
	}
}
