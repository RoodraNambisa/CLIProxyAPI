package thinking

import (
	"bytes"
	"testing"

	"github.com/tidwall/sjson"
)

func TestSummaryExtractionSeparatesVisibilityFromAmount(t *testing.T) {
	for _, tc := range []struct {
		name, format, body string
		mode               SummaryMode
		detail             string
	}{
		{"chat legacy effort", "openai", `{"reasoning_effort":"high"}`, SummaryEnabled, "auto"},
		{"chat none", "openai", `{"reasoning_effort":"none"}`, SummaryDisabled, ""},
		{"chat empty effort", "openai", `{"reasoning_effort":" "}`, SummaryUnspecified, ""},
		{"chat invalid effort", "openai", `{"reasoning_effort":true}`, SummaryUnspecified, ""},
		{"chat explicit hide wins", "openai", `{"reasoning_effort":"high","reasoning":{"exclude":true},"include_reasoning":true}`, SummaryDisabled, ""},
		{"chat legacy show", "openai", `{"include_reasoning":true}`, SummaryEnabled, "auto"},
		{"chat legacy hide", "openai", `{"include_reasoning":false}`, SummaryDisabled, ""},
		{"chat explicit enabled", "openai", `{"reasoning":{"enabled":true}}`, SummaryEnabled, "auto"},
		{"chat explicit disabled", "openai", `{"reasoning":{"enabled":false}}`, SummaryDisabled, ""},
		{"google explicit wins", "openai", `{"extra_body":{"google":{"thinking_config":{"include_thoughts":false}}},"reasoning":{"summary":"auto"}}`, SummaryDisabled, ""},
		{"responses amount only", "openai-response", `{"reasoning":{"effort":"high"}}`, SummaryUnspecified, ""},
		{"responses detailed", "openai-response", `{"reasoning":{"summary":"detailed"}}`, SummaryEnabled, "detailed"},
		{"codex concise", "codex", `{"reasoning":{"summary":"concise"}}`, SummaryEnabled, "concise"},
		{"responses null wins", "openai-response", `{"reasoning":{"summary":null,"generate_summary":"auto"}}`, SummaryDisabled, ""},
		{"responses none", "openai-response", `{"reasoning":{"summary":"none"}}`, SummaryDisabled, ""},
		{"legacy summary", "codex", `{"reasoning":{"generate_summary":"auto"}}`, SummaryEnabled, "auto"},
		{"claude adaptive hide", "claude", `{"thinking":{"type":"adaptive","display":"omitted"}}`, SummaryDisabled, ""},
		{"claude budget show", "claude", `{"thinking":{"type":"enabled","budget_tokens":1024,"display":"summarized"}}`, SummaryEnabled, "auto"},
		{"claude auto budget", "claude", `{"thinking":{"type":"enabled","budget_tokens":-1,"display":"omitted"}}`, SummaryDisabled, ""},
		{"claude unfinished budget", "claude", `{"thinking":{"type":"enabled","display":"summarized"}}`, SummaryEnabled, "auto"},
		{"claude zero budget", "claude", `{"thinking":{"type":"enabled","budget_tokens":0,"display":"summarized"}}`, SummaryUnspecified, ""},
		{"claude inactive", "claude", `{"thinking":{"type":"disabled","display":"summarized"}}`, SummaryUnspecified, ""},
		{"claude no type", "claude", `{"thinking":{"display":"summarized"}}`, SummaryUnspecified, ""},
		{"claude amount only", "claude", `{"thinking":{"type":"adaptive"},"output_config":{"effort":"high"}}`, SummaryUnspecified, ""},
		{"gemini amount only", "gemini", `{"generationConfig":{"thinkingConfig":{"thinkingBudget":2048}}}`, SummaryUnspecified, ""},
		{"interactions amount only", "interactions", `{"generation_config":{"thinking_level":"high"}}`, SummaryUnspecified, ""},
		{"interactions canonical wins", "interactions", `{"generation_config":{"thinking_summaries":"none","thinkingSummaries":"auto","thinking_config":{"include_thoughts":true}},"reasoning":{"summary":"auto"}}`, SummaryDisabled, ""},
		{"interactions compatibility", "interactions", `{"reasoning":{"summary":"auto"}}`, SummaryEnabled, "auto"},
		{"interactions detailed unsupported", "interactions", `{"generation_config":{"thinking_summaries":"detailed"}}`, SummaryUnspecified, ""},
		{"normalized enum", " CODEX ", `{"reasoning":{"summary":" CONCISE "}}`, SummaryEnabled, "concise"},
		{"history is not configuration", "openai-response", `{"input":[{"type":"reasoning","summary":[{"type":"summary_text","text":"fixture"}]}],"metadata":{"reasoning":{"summary":"auto"}}}`, SummaryUnspecified, ""},
		{"unknown protocol", "unknown", `{"reasoning":{"summary":"auto"}}`, SummaryUnspecified, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(tc.body)
			before := bytes.Clone(body)
			got := ExtractSummaryConfig(body, tc.format)
			if got != (SummaryConfig{Mode: tc.mode, Detail: tc.detail}) || !bytes.Equal(body, before) {
				t.Fatalf("summary=%+v, expected mode=%v detail=%q; input changed=%t", got, tc.mode, tc.detail, !bytes.Equal(body, before))
			}
		})
	}
}

func TestSummaryExtractionBooleanAliasesAndTypes(t *testing.T) {
	for format, paths := range map[string][]string{
		"gemini":       {"generationConfig.thinkingConfig.includeThoughts", "generationConfig.thinkingConfig.include_thoughts", "generation_config.thinking_config.include_thoughts", "generation_config.thinking_config.includeThoughts"},
		"antigravity":  {"request.generationConfig.thinkingConfig.includeThoughts", "request.generationConfig.thinkingConfig.include_thoughts", "request.generationConfig.thinking_config.includeThoughts", "request.generationConfig.thinking_config.include_thoughts"},
		"interactions": {"generation_config.thinking_config.include_thoughts", "generation_config.thinking_config.includeThoughts", "generation_config.thinkingConfig.include_thoughts", "generation_config.thinkingConfig.includeThoughts"},
		"openai":       {"extra_body.google.thinking_config.include_thoughts", "extra_body.google.thinking_config.includeThoughts", "extra_body.google.thinkingConfig.include_thoughts", "extra_body.google.thinkingConfig.includeThoughts", "extra_body.extra_body.google.thinking_config.include_thoughts", "extra_body.extra_body.google.thinking_config.includeThoughts", "google.thinking_config.include_thoughts", "google.thinking_config.includeThoughts", "thinking.includeThoughts", "thinking.include_thoughts", "reasoning.includeThoughts", "reasoning.include_thoughts", "generationConfig.thinkingConfig.includeThoughts", "generationConfig.thinkingConfig.include_thoughts", "generation_config.thinking_config.include_thoughts", "generation_config.thinking_config.includeThoughts"},
	} {
		for _, path := range paths {
			for _, value := range []any{true, false, "true", "false", 0, 1, nil, []any{}, map[string]any{}} {
				body, err := sjson.SetBytes([]byte(`{}`), path, value)
				if err != nil {
					t.Fatal(err)
				}
				want := SummaryUnspecified
				if value, ok := value.(bool); ok {
					if value {
						want = SummaryEnabled
					} else {
						want = SummaryDisabled
					}
				}
				if got := ExtractSummaryConfig(body, format); got.Mode != want {
					t.Fatalf("%s %s value type %T: mode=%v want=%v", format, path, value, got.Mode, want)
				}
			}
		}
	}
}

func TestSummaryExtractionDoesNotInferExplicitChatVisibility(t *testing.T) {
	for _, effort := range []string{"high", "none"} {
		body, _ := sjson.SetBytes([]byte(`{}`), "reasoning_effort", effort)
		if got := ExtractExplicitSummaryConfig(body, "openai"); got.Mode != SummaryUnspecified {
			t.Fatalf("effort became explicit visibility: %+v", got)
		}
		body, _ = sjson.SetBytes(body, "reasoning.exclude", true)
		if got := ExtractExplicitSummaryConfig(body, "openai"); got.Mode != SummaryDisabled {
			t.Fatalf("lost explicit hide: %+v", got)
		}
	}
	for _, body := range [][]byte{nil, {}, []byte(`{"reasoning":{"summary":"auto"}`), []byte(`{"thinking":{"display":"summarized"}}invalid`)} {
		for _, format := range []string{"openai", "codex", "claude", "gemini", "antigravity", "interactions", "openai-response"} {
			if got := ExtractSummaryConfig(body, format); got.Mode != SummaryUnspecified {
				t.Fatalf("invalid input selected visibility: %+v", got)
			}
			if got := ExtractExplicitSummaryConfig(body, format); got.Mode != SummaryUnspecified {
				t.Fatalf("invalid input selected explicit visibility: %+v", got)
			}
		}
	}
}
