package thinking_test

import (
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/thinking/provider/codex"
	"github.com/tidwall/gjson"
)

func TestGeneratedCodexThinkingEffortRespectsLevelCapabilities(t *testing.T) {
	for _, tc := range []struct {
		name, requested, want string
		levels                []string
		zero, dynamic         bool
	}{
		{"auto without medium", "auto", "low", []string{"low", "high"}, false, false},
		{"auto only high", "auto", "high", []string{"high"}, false, false},
		{"auto medium supported", "auto", "medium", []string{"low", "medium", "high"}, false, false},
		{"dynamic auto preserved", "auto", "auto", []string{"low", "high"}, false, true},
		{"none cannot disable", "none", "low", []string{"low", "high"}, false, false},
		{"none permitted", "none", "none", []string{"low", "high"}, true, false},
		{"none explicit level", "none", "none", []string{"none", "low", "high"}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := &registry.ModelInfo{ID: "fixture", Type: "codex", Thinking: &registry.ThinkingSupport{Levels: tc.levels, ZeroAllowed: tc.zero, DynamicAllowed: tc.dynamic}}
			body := []byte(fmt.Sprintf(`{"reasoning":{"effort":%q},"local_extension":true}`, tc.requested))
			for _, suffix := range []bool{false, true} {
				model := "fixture"
				if suffix {
					model += "(" + tc.requested + ")"
				}
				out, err := thinking.ApplyThinkingWithModelInfo(body, nil, model, "openai-response", "codex", "codex", info)
				if err != nil {
					t.Fatal(err)
				}
				if gjson.GetBytes(out, "reasoning.effort").String() != tc.want || !gjson.GetBytes(out, "local_extension").Bool() {
					t.Fatalf("suffix=%v: generated unsupported effort: %s", suffix, out)
				}
			}
		})
	}
	info := &registry.ModelInfo{ID: "fixture", Type: "codex", Thinking: &registry.ThinkingSupport{Levels: []string{"low", "high"}}}
	if _, err := thinking.ApplyThinkingWithModelInfo([]byte(`{"reasoning":{"effort":"medium"}}`), nil, "fixture", "openai-response", "codex", "codex", info); err == nil {
		t.Fatal("explicit unsupported client effort was silently clamped")
	}
}
