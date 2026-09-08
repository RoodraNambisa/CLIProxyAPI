package cliproxy

import (
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestConfiguredModelCatalogThinkingIsPrivateAndExplicit(t *testing.T) {
	support := &registry.ThinkingSupport{Levels: []string{" HIGH ", "none", "high", "auto"}}
	for _, model := range []modelEntry{
		config.GeminiModel{Name: "private", Alias: "public", Thinking: support},
		config.ClaudeModel{Name: "private", Alias: "public", Thinking: support},
		config.CodexModel{Name: "private", Alias: "public", Thinking: support},
		config.VertexCompatModel{Name: "private", Alias: "public", Thinking: support},
		config.OpenAICompatibilityModel{Name: "private", Alias: "public", Thinking: support},
	} {
		models := buildConfigModels([]modelEntry{model}, "fixture", "fixture")
		if len(models) != 1 {
			t.Fatal("configured model missing")
		}
		info := models[0]
		if info.ID != "public" || info.UpstreamID != "private" || info.UserDefined || !reflect.DeepEqual(info.Thinking.Levels, []string{"high", "none", "auto"}) || !info.Thinking.ZeroAllowed || !info.Thinking.DynamicAllowed {
			t.Fatal("catalog lost explicit thinking or alias mapping")
		}
		info.Thinking.Levels[0] = "low"
		if support.Levels[0] != " HIGH " {
			t.Fatal("catalog shares its capability with configuration")
		}
	}
	models := buildConfigModels([]config.CodexModel{{Name: "unlisted-private", Alias: "public"}}, "openai", "codex")
	if len(models) != 1 || !models[0].UserDefined || models[0].Thinking != nil {
		t.Fatal("missing override changed legacy unknown-model behavior")
	}
	static := registry.LookupStaticModelInfo("gpt-5.4")
	if static == nil || static.Thinking == nil {
		t.Fatal("static reasoning fixture missing")
	}
	for _, upstream := range []string{"gpt-5.4", "gpt-5.4(high)"} {
		models = buildConfigModels([]config.CodexModel{{Name: upstream, Alias: "public"}}, "openai", "codex")
		if !reflect.DeepEqual(models[0].Thinking, static.Thinking) || models[0].UpstreamID != upstream {
			t.Fatalf("missing override for %s did not inherit its base catalog", upstream)
		}
	}
	first := buildConfigModels([]config.CodexModel{{Name: "gpt-5.4", Alias: "same", Thinking: &registry.ThinkingSupport{Levels: []string{"low"}}}}, "openai", "codex")[0]
	second := buildConfigModels([]config.CodexModel{{Name: "gpt-5.4", Alias: "same", Thinking: &registry.ThinkingSupport{Levels: []string{"high", "xhigh"}}}}, "openai", "codex")[0]
	if first.Thinking.Levels[0] != "low" || len(second.Thinking.Levels) != 2 || !reflect.DeepEqual(registry.LookupStaticModelInfo("gpt-5.4").Thinking, static.Thinking) {
		t.Fatal("same alias overwrote another credential or static model")
	}
}
