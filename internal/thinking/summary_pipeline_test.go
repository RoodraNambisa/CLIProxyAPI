package thinking_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/thinking/provider/antigravity"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/thinking/provider/claude"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/thinking/provider/codex"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/thinking/provider/gemini"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/thinking/provider/interactions"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/thinking/provider/openai"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestSummaryPipelineKeepsSuffixAndVisibilityIndependent(t *testing.T) {
	for _, tc := range []struct{ name, source, target, suffix, wantEffort, wantSummary string }{
		{"source detail", `{"reasoning":{"effort":"high","summary":"detailed"}}`, `{"reasoning":{"effort":"low","summary":"auto"}}`, "", "high", "detailed"},
		{"source hide", `{"reasoning":{"effort":"high","summary":null}}`, `{"reasoning":{"effort":"low","summary":"auto"}}`, "", "high", ""},
		{"suffix priority", `{"reasoning":{"effort":"high","summary":"concise"}}`, `{"reasoning":{"effort":"high"}}`, "(low)", "low", "concise"},
		{"source missing", ``, `{"reasoning":{"effort":"high","summary":"concise"}}`, "(low)", "low", "concise"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := &registry.ModelInfo{ID: "fixture", Type: "codex", Thinking: &registry.ThinkingSupport{Levels: []string{"low", "high"}}}
			body, source := []byte(tc.target), []byte(tc.source)
			beforeBody, beforeSource := bytes.Clone(body), bytes.Clone(source)
			out, err := thinking.ApplyThinkingWithModelInfo(body, source, "fixture"+tc.suffix, "codex", "codex", "codex", info)
			if err != nil || gjson.GetBytes(out, "reasoning.effort").String() != tc.wantEffort || gjson.GetBytes(out, "reasoning.summary").String() != tc.wantSummary {
				t.Fatalf("unexpected summary/effort result: %s; %v", out, err)
			}
			if !bytes.Equal(body, beforeBody) || !bytes.Equal(source, beforeSource) {
				t.Fatal("mutated input")
			}
		})
	}
}

func TestSummaryPipelineUsesSelectedClaudeModeAndFinalVisibility(t *testing.T) {
	for _, tc := range []struct {
		name, source, target, model, wantType, wantDisplay string
		summary                                            thinking.SummaryConfig
		wantBudget                                         int64
	}{
		{"manual activation", `{"reasoning":{"summary":"auto"}}`, `{"max_tokens":4096,"thinking":{"type":"adaptive","display":"summarized"}}`, "fixture", "enabled", "summarized", thinking.SummaryConfig{Mode: thinking.SummaryEnabled}, 1024},
		{"removed summary removes inference", `{"reasoning":{"summary":"auto"}}`, `{"max_tokens":4096,"thinking":{"type":"adaptive"}}`, "fixture", "", "", thinking.SummaryConfig{}, 0},
		{"too small output", `{"reasoning":{"summary":"auto"}}`, `{"max_tokens":1024,"thinking":{"type":"adaptive","display":"summarized"}}`, "fixture", "", "", thinking.SummaryConfig{Mode: thinking.SummaryEnabled}, 0},
		{"hidden no activation", `{"reasoning":{"summary":null}}`, `{"max_tokens":4096}`, "fixture", "", "", thinking.SummaryConfig{Mode: thinking.SummaryDisabled}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := &registry.ModelInfo{ID: "fixture", Type: "claude", Thinking: &registry.ThinkingSupport{Min: 1024, Max: 32000, ZeroAllowed: true}}
			out, err := thinking.ApplyThinkingWithModelInfoAndSummary([]byte(tc.target), []byte(tc.source), tc.model, "openai-response", "claude", "claude", info, tc.summary)
			if err != nil || gjson.GetBytes(out, "thinking.type").String() != tc.wantType || gjson.GetBytes(out, "thinking.display").String() != tc.wantDisplay || gjson.GetBytes(out, "thinking.budget_tokens").Int() != tc.wantBudget {
				t.Fatalf("incorrect selected-model activation: %s; %v", out, err)
			}
		})
	}
	info := &registry.ModelInfo{ID: "fixture", Type: "claude", Thinking: &registry.ThinkingSupport{Levels: []string{"low", "high"}, ZeroAllowed: true}}
	out, err := thinking.ApplyThinkingWithModelInfoAndSummary([]byte(`{"thinking":{"type":"disabled"}}`), []byte(`{"reasoning":{"summary":null}}`), "fixture(high)", "openai-response", "claude", "claude", info, thinking.SummaryConfig{Mode: thinking.SummaryDisabled})
	if err != nil || gjson.GetBytes(out, "thinking.type").String() != "adaptive" || gjson.GetBytes(out, "thinking.display").String() != "omitted" {
		t.Fatalf("suffix lost hidden visibility: %s; %v", out, err)
	}
}

func TestSummaryPipelineDisabledAndUnsupportedModelsDoNotRegainThinking(t *testing.T) {
	for _, provider := range []string{"claude", "gemini", "antigravity", "interactions", "codex", "openai"} {
		t.Run(provider, func(t *testing.T) {
			paths := map[string]string{"claude": "thinking.display", "gemini": "generationConfig.thinkingConfig.includeThoughts", "antigravity": "request.generationConfig.thinkingConfig.includeThoughts", "interactions": "generation_config.thinking_summaries", "codex": "reasoning.summary", "openai": "reasoning.exclude"}
			info := &registry.ModelInfo{ID: "fixture", Type: provider, Thinking: &registry.ThinkingSupport{Levels: []string{"low", "high"}, ZeroAllowed: true}}
			out, err := thinking.ApplyThinkingWithModelInfoAndSummary([]byte(`{}`), nil, "fixture(none)", provider, provider, provider, info, thinking.SummaryConfig{Mode: thinking.SummaryEnabled})
			if err != nil || gjson.GetBytes(out, paths[provider]).Exists() {
				t.Fatalf("summary revived disabled thinking: %s; %v", out, err)
			}
			body := thinking.ApplySummaryConfig([]byte(`{"thinking":{"type":"adaptive"}}`), provider, thinking.SummaryConfig{Mode: thinking.SummaryEnabled})
			info.Thinking = nil
			out, err = thinking.ApplyThinkingWithModelInfoAndSummary(body, nil, "fixture", provider, provider, provider, info, thinking.SummaryConfig{Mode: thinking.SummaryEnabled})
			if err != nil || gjson.GetBytes(out, paths[provider]).Exists() {
				t.Fatalf("unsupported model retained summary: %s; %v", out, err)
			}
		})
	}
	for _, registered := range []bool{false, true} {
		body := []byte(`{"thinking":{"type":"adaptive","display":"summarized"},"output_config":{"effort":"high"}}`)
		var info *registry.ModelInfo
		if registered {
			info = &registry.ModelInfo{ID: "fixture", Type: "claude", Thinking: &registry.ThinkingSupport{Levels: []string{"low", "high"}, ZeroAllowed: true}}
		}
		out, err := thinking.ApplyThinkingWithModelInfo(body, nil, "fixture(none)", "claude", "claude", "claude", info)
		if err != nil || gjson.GetBytes(out, "thinking.type").String() != "disabled" || gjson.GetBytes(out, "thinking.display").Exists() {
			t.Fatalf("disabled Claude retained display: %s; %v", out, err)
		}
	}
}

func TestSummaryPipelineErrorAndUnknownProviderPreserveBody(t *testing.T) {
	body := []byte(`{"reasoning":{"effort":"high","summary":"detailed"}}`)
	info := &registry.ModelInfo{ID: "fixture", Type: "codex", Thinking: &registry.ThinkingSupport{Levels: []string{"low"}}}
	out, err := thinking.ApplyThinkingWithModelInfo(body, body, "fixture", "codex", "codex", "codex", info)
	var issue *thinking.ThinkingError
	if !errors.As(err, &issue) || issue.Code != thinking.ErrLevelNotSupported || !bytes.Equal(out, body) {
		t.Fatalf("validation changed its error/body contract: %v", err)
	}
	out, err = thinking.ApplyThinkingWithModelInfo(body, body, "fixture", "codex", "unsupported", "unsupported", info)
	if err != nil || !bytes.Equal(out, body) {
		t.Fatal("unknown provider no longer passed through")
	}
	for _, provider := range []string{"openai", "openrouter"} {
		out, err = thinking.ApplyThinkingWithSummary([]byte(`{"reasoning_effort":"high"}`), "unknown(none)", "openai", "openai", provider, thinking.SummaryConfig{Mode: thinking.SummaryEnabled})
		if err != nil || gjson.GetBytes(out, "reasoning_effort").String() != "none" {
			t.Fatalf("summary overrode Chat none suffix: %s; %v", out, err)
		}
	}
}

func TestSummaryPipelineGoogleAmountDoesNotInferVisibility(t *testing.T) {
	for _, provider := range []string{"gemini", "antigravity", "interactions"} {
		for _, mode := range []string{"level", "budget", "clamped-none"} {
			for _, visibility := range []string{"missing", "true", "false", "invalid"} {
				t.Run(provider+"/"+mode+"/"+visibility, func(t *testing.T) {
					prefix := "generationConfig.thinkingConfig"
					if provider == "antigravity" {
						prefix = "request." + prefix
					}
					amount, display := prefix+".thinkingLevel", prefix+".include_thoughts"
					canonicalDisplay := prefix + ".includeThoughts"
					if provider == "interactions" {
						amount, display, canonicalDisplay = "generation_config.thinking_level", "generation_config.thinking_config.include_thoughts", "generation_config.thinking_summaries"
					}
					info := &registry.ModelInfo{ID: "fixture", Type: provider, Thinking: &registry.ThinkingSupport{Min: 128, Max: 8192, Levels: []string{"low", "medium", "high"}}}
					var value any = "high"
					if mode == "budget" {
						amount = prefix + ".thinkingBudget"
						if provider == "interactions" {
							amount = "generation_config.thinking_budget"
						}
						value = 2048
					} else if mode == "clamped-none" {
						value = "none"
					}
					body, _ := sjson.SetBytes([]byte(`{}`), amount, value)
					if visibility != "missing" {
						var enabled any = visibility == "true"
						if visibility == "invalid" {
							enabled = "true"
						}
						body, _ = sjson.SetBytes(body, display, enabled)
					}
					out, err := thinking.ApplyThinkingWithModelInfo(body, body, "fixture", provider, provider, provider, info)
					if err != nil {
						t.Fatal(err)
					}
					got := gjson.GetBytes(out, canonicalDisplay)
					want := visibility
					if provider == "interactions" {
						if visibility == "true" {
							want = "auto"
						} else if visibility == "false" {
							want = "none"
						}
					}
					if visibility == "missing" || visibility == "invalid" {
						if got.Exists() {
							t.Fatalf("amount added visibility: %s", out)
						}
					} else if got.String() != want {
						t.Fatalf("explicit visibility lost: %s", out)
					}
					if gjson.GetBytes(out, display).Exists() {
						t.Fatal("retained noncanonical visibility alias")
					}
				})
			}
		}
	}
}
