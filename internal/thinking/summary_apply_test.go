package thinking

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/tidwall/gjson"
)

func TestSummaryApplicationPreservesEffortAndProviderSemantics(t *testing.T) {
	for _, tc := range []struct {
		name, format, provider, body, path, want string
		mode                                     SummaryMode
	}{
		{"chat no invented field", "openai", "openai", `{}`, "reasoning", "", SummaryEnabled},
		{"chat no invented effort", "openai", "deepseek", `{}`, "reasoning_effort", "", SummaryEnabled},
		{"chat hide preserves effort", "openai", "kimi", `{"reasoning_effort":"max"}`, "reasoning_effort", "max", SummaryDisabled},
		{"chat show preserves none", "openai", "openai", `{"reasoning_effort":"none"}`, "reasoning_effort", "none", SummaryEnabled},
		{"openrouter hide", "openai", "prod-openrouter", `{"reasoning_effort":"high"}`, "reasoning.exclude", "true", SummaryDisabled},
		{"openrouter show", "openai", "openrouter", `{}`, "reasoning.exclude", "false", SummaryEnabled},
		{"name substring is not dialect", "openai", "notopenrouter", `{}`, "reasoning", "", SummaryDisabled},
		{"unknown existing extension", "openai", "compat", `{"reasoning":{"exclude":false}}`, "reasoning.exclude", "true", SummaryDisabled},
		{"legacy include alias", "openai", "compat", `{"include_reasoning":true}`, "include_reasoning", "false", SummaryDisabled},
		{"claude adaptive hide", "claude", "claude", `{"thinking":{"type":"adaptive"},"output_config":{"effort":"high"}}`, "thinking.display", "omitted", SummaryDisabled},
		{"claude manual show", "claude", "claude", `{"thinking":{"type":"enabled","budget_tokens":2048}}`, "thinking.display", "summarized", SummaryEnabled},
		{"gemini hide", "gemini", "gemini", `{"generationConfig":{"thinkingConfig":{"thinkingBudget":2048}}}`, "generationConfig.thinkingConfig.includeThoughts", "false", SummaryDisabled},
		{"antigravity show", "antigravity", "antigravity", `{}`, "request.generationConfig.thinkingConfig.includeThoughts", "true", SummaryEnabled},
		{"interactions detail supported value", "interactions", "gemini", `{}`, "generation_config.thinking_summaries", "auto", SummaryEnabled},
		{"interactions hide", "interactions", "gemini", `{}`, "generation_config.thinking_summaries", "none", SummaryDisabled},
		{"responses detail", "openai-response", "compat", `{"reasoning":{"effort":"high"}}`, "reasoning.summary", "detailed", SummaryEnabled},
		{"codex hide does not disable effort", "codex", "codex", `{"reasoning":{"effort":"high","summary":"auto"}}`, "reasoning.effort", "high", SummaryDisabled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(tc.body)
			before := bytes.Clone(body)
			out := applySummaryConfigForProvider(body, tc.format, "fixture", tc.provider, nil, SummaryConfig{Mode: tc.mode, Detail: "detailed"})
			if got := gjson.GetBytes(out, tc.path).String(); got != tc.want {
				t.Fatalf("%s=%q want=%q", tc.path, got, tc.want)
			}
			if !bytes.Equal(body, before) {
				t.Fatal("modified input buffer")
			}
		})
	}
}

func TestSummaryApplicationClaudeUsesExactCapabilitiesAndOutputBudget(t *testing.T) {
	for _, tc := range []struct {
		name, body, wantType string
		cap                  *registry.ThinkingSupport
		mode                 SummaryMode
		budget               int64
	}{
		{"adaptive", `{"max_tokens":4096}`, "adaptive", &registry.ThinkingSupport{Levels: []string{"low", "high"}}, SummaryEnabled, 0},
		{"manual", `{"max_tokens":4096}`, "enabled", &registry.ThinkingSupport{Min: 1024, Max: 32000}, SummaryEnabled, 1024},
		{"manual boundary", `{"max_tokens":1025}`, "enabled", &registry.ThinkingSupport{Min: 1024}, SummaryEnabled, 1024},
		{"no output space", `{"max_tokens":1024}`, "", &registry.ThinkingSupport{Min: 1024}, SummaryEnabled, 0},
		{"zero output budget", `{"max_tokens":0}`, "", &registry.ThinkingSupport{Min: 1024}, SummaryEnabled, 0},
		{"hidden missing mode", `{}`, "", &registry.ThinkingSupport{Levels: []string{"high"}}, SummaryDisabled, 0},
		{"unsupported model", `{}`, "", nil, SummaryEnabled, 0},
		{"no valid budget", `{}`, "", &registry.ThinkingSupport{}, SummaryEnabled, 0},
		{"explicit disabled mode", `{"thinking":{"type":"disabled"}}`, "disabled", &registry.ThinkingSupport{Levels: []string{"high"}}, SummaryEnabled, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := &registry.ModelInfo{ID: "fixture", Thinking: tc.cap}
			before := *info
			if info.Thinking != nil {
				capability := *info.Thinking
				capability.Levels = append([]string(nil), info.Thinking.Levels...)
				before.Thinking = &capability
			}
			out := applySummaryConfigForModel([]byte(tc.body), "claude", "claude-opus-5", info, SummaryConfig{Mode: tc.mode})
			if gjson.GetBytes(out, "thinking.type").String() != tc.wantType || gjson.GetBytes(out, "thinking.budget_tokens").Int() != tc.budget {
				t.Fatalf("unexpected thinking mode: %s", out)
			}
			active := tc.wantType == "adaptive" || tc.wantType == "enabled"
			if gjson.GetBytes(out, "thinking.display").Exists() != active {
				t.Fatalf("invalid display presence: %s", out)
			}
			if !reflect.DeepEqual(info, &before) {
				t.Fatal("mutated selected capability")
			}
		})
	}
	for _, body := range []string{`{}`, `{"thinking":{"type":"disabled"}}`, `{"thinking":{"type":"enabled","budget_tokens":0}}`} {
		if out := ApplySummaryConfig([]byte(body), "claude", SummaryConfig{Mode: SummaryEnabled}); !bytes.Equal(out, []byte(body)) {
			t.Fatalf("unknown model activated thinking: %s", out)
		}
	}
}

func TestSummaryApplicationNormalizesOnlyProtocolFields(t *testing.T) {
	for _, tc := range []struct{ format, body, canonical, alias string }{
		{"gemini", `{"generationConfig":{"thinkingConfig":{"include_thoughts":true}}}`, "generationConfig.thinkingConfig.includeThoughts", "generationConfig.thinkingConfig.include_thoughts"},
		{"antigravity", `{"request":{"generationConfig":{"thinking_config":{"includeThoughts":true}}}}`, "request.generationConfig.thinkingConfig.includeThoughts", "request.generationConfig.thinking_config.includeThoughts"},
		{"interactions", `{"generation_config":{"thinkingSummaries":"auto"}}`, "generation_config.thinking_summaries", "generation_config.thinkingSummaries"},
		{"codex", `{"reasoning":{"generate_summary":"concise"}}`, "reasoning.summary", "reasoning.generate_summary"},
	} {
		out := ApplySummaryConfig([]byte(tc.body), tc.format, SummaryConfig{Mode: SummaryEnabled, Detail: "concise"})
		if !gjson.GetBytes(out, tc.canonical).Exists() || gjson.GetBytes(out, tc.alias).Exists() {
			t.Fatalf("incorrect alias normalization: %s", out)
		}
	}
	for _, format := range []string{"codex", "openai-response"} {
		body := []byte(`{"reasoning":{"summary":null,"generate_summary":"auto"},"input":[{"type":"reasoning","summary":[{"type":"summary_text","text":"fixture"}]}],"metadata":{"reasoning":{"summary":"auto"}}}`)
		out := ApplySummaryConfig(body, format, SummaryConfig{Mode: SummaryDisabled})
		if gjson.GetBytes(out, "reasoning").Exists() {
			t.Fatal("empty reasoning object retained")
		}
		for _, path := range []string{"input", "metadata"} {
			if gjson.GetBytes(out, path).Raw != gjson.GetBytes(body, path).Raw {
				t.Fatalf("modified business/history field %s", path)
			}
		}
	}
}

func TestSummaryApplicationPreservesUnspecifiedUnsupportedAndMalformedBodies(t *testing.T) {
	for _, format := range []string{"openai", "codex", "claude", "gemini", "antigravity", "interactions", "openai-response"} {
		body := []byte(`{"thinking":{"type":"adaptive"},"reasoning":{"summary":"auto"}}`)
		if out := ApplySummaryConfig(body, format, SummaryConfig{}); !bytes.Equal(out, body) {
			t.Fatal("unspecified visibility changed input")
		}
		for _, body := range [][]byte{nil, {}, []byte(`{"reasoning":`)} {
			if out := ApplySummaryConfig(body, format, SummaryConfig{Mode: SummaryEnabled}); !bytes.Equal(out, body) {
				t.Fatal("malformed body changed")
			}
		}
	}
	body := []byte(`{"reasoning":{"effort":"high"}}`)
	if out := ApplySummaryConfig(body, "unsupported", SummaryConfig{Mode: SummaryEnabled}); !bytes.Equal(out, body) {
		t.Fatal("unsupported protocol changed")
	}
}
