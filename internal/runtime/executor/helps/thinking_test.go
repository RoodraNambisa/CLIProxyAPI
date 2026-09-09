package helps_test

import (
	"bytes"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/thinking/provider/claude"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/thinking/provider/gemini"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/thinking/provider/openai"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/translator"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestRequestSummarySharedSourcesPreserveResolution(t *testing.T) {
	for _, from := range []string{"codex", "openai-response"} {
		for _, raw := range []string{`{}`, `{"reasoning":{"summary":"auto"}}`, `{"reasoning":{"summary":null}}`} {
			source := []byte(raw)
			for _, original := range [][]byte{source, bytes.Clone(source)} {
				for _, target := range [][]byte{source, []byte(`{}`)} {
					want := thinking.ExtractSummaryConfig(target, "codex")
					got := helps.RequestSummaryConfig(target, core.Request{Payload: source}, core.Options{OriginalRequest: original}, from, "codex")
					if got != want {
						t.Fatalf("shared source changed target priority or restored a removed native field: got=%+v want=%+v", got, want)
					}
				}
			}
		}
	}
	original := []byte(`{"reasoning":{"summary":"detailed"}}`)
	got := helps.RequestSummaryConfig([]byte(`{}`), core.Request{Payload: original[:1]}, core.Options{OriginalRequest: original}, "openai-response", "codex")
	if got.Mode != thinking.SummaryEnabled || got.Detail != "detailed" {
		t.Fatal("different-length slices sharing a start lost the original fallback")
	}
}

func TestThinkingSourceSummaryRespectsFinalTargetAndOriginalFallback(t *testing.T) {
	for _, tc := range []struct{ name, from, to, current, original, target, provider, path, want string }{
		{"target hide wins", "openai-response", "gemini", `{"reasoning":{"summary":"auto"}}`, `{"reasoning":{"summary":"auto"}}`, `{"generationConfig":{"thinkingConfig":{"includeThoughts":false}}}`, "gemini", "generationConfig.thinkingConfig.includeThoughts", "false"},
		{"removed target stays absent", "openai-response", "gemini", `{"reasoning":{"summary":"auto"}}`, `{"reasoning":{"summary":"auto"}}`, `{}`, "gemini", "generationConfig.thinkingConfig.includeThoughts", ""},
		{"original only hide", "openai-response", "gemini", `{}`, `{"reasoning":{"summary":null}}`, `{"generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}`, "gemini", "generationConfig.thinkingConfig.includeThoughts", "false"},
		{"native removed stays absent", "gemini", "gemini", `{"generationConfig":{"thinkingConfig":{"includeThoughts":true}}}`, `{"generationConfig":{"thinkingConfig":{"includeThoughts":true}}}`, `{}`, "gemini", "generationConfig.thinkingConfig.includeThoughts", ""},
		{"native original only", "gemini", "gemini", `{}`, `{"generationConfig":{"thinkingConfig":{"includeThoughts":false}}}`, `{}`, "gemini", "generationConfig.thinkingConfig.includeThoughts", "false"},
		{"dialect deferred hide", "openai-response", "openai", `{"reasoning":{"summary":null}}`, `{"reasoning":{"summary":null}}`, `{"reasoning_effort":"high"}`, "openrouter", "reasoning.exclude", "true"},
		{"native default format", "", "openai", `{"reasoning_effort":"high"}`, `{"reasoning_effort":"high"}`, `{"reasoning_effort":"high"}`, "openrouter", "reasoning.exclude", "false"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := helps.ApplyThinkingWithSourcePayload([]byte(tc.target), []byte(tc.current), []byte(tc.original), "source-summary-fixture", tc.from, tc.to, tc.provider)
			if err != nil || gjson.GetBytes(out, tc.path).Raw != tc.want {
				t.Fatalf("unexpected final summary: %s; %v", out, err)
			}
		})
	}
}

func TestThinkingSourceSummaryDoesNotMixMissingRoute(t *testing.T) {
	for _, from := range []translator.Format{translator.FormatCodex, translator.FromString(" OpenAI-Response ")} {
		to := translator.FormatGemini
		if translator.HasRequestTransformer(from, to) {
			t.Fatal("fixture requires a missing request conversion")
		}
		for _, current := range []string{`{}`, `{"reasoning":{"summary":"auto"}}`} {
			out, err := helps.ApplyThinkingWithSourcePayload([]byte(current), []byte(current), []byte(`{"reasoning":{"summary":"auto"}}`), "source-summary-fixture", from.String(), to.String(), "gemini")
			if err != nil || gjson.GetBytes(out, "generationConfig").Exists() {
				t.Fatalf("fallback gained target summary fields: %s; %v", out, err)
			}
		}
	}
}

func TestThinkingSourceSummarySurvivesDeferredClaudeDisplay(t *testing.T) {
	out, err := helps.ApplyThinkingWithSourcePayload([]byte(`{"thinking":{"type":"disabled"}}`), []byte(`{"reasoning":{"summary":null}}`), []byte(`{"reasoning":{"summary":null}}`), "source-summary-fixture(high)", "openai-response", "claude", "claude")
	if err != nil || gjson.GetBytes(out, "thinking.display").String() != "omitted" {
		t.Fatalf("suffix lost unrepresentable hidden summary: %s; %v", out, err)
	}
}
