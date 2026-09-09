package executor

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestGeminiInteractionsUnboundSummaryRetainsSourceAndTargetAuthority(t *testing.T) {
	for _, source := range []struct {
		format     translator.Format
		body, path string
		hide, show any
	}{
		{translator.FormatInteractions, `{"input":"fixture","generation_config":{"thinking_level":"high"}}`, "generation_config.thinking_summaries", "none", "auto"},
		{translator.FormatOpenAIResponse, `{"input":"fixture","reasoning":{"effort":"high"}}`, "reasoning.summary", nil, "auto"},
		{translator.FormatClaude, `{"messages":[{"role":"user","content":"fixture"}],"thinking":{"type":"enabled","budget_tokens":8192}}`, "thinking.display", "omitted", "summarized"},
	} {
		for _, mode := range []string{"missing", "original-hide", "target-show"} {
			for _, stream := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/stream=%t", source.format, mode, stream), func(t *testing.T) {
					current := []byte(source.body)
					var original []byte
					var err error
					want := ""
					if mode != "missing" {
						original, err = sjson.SetBytes(current, source.path, source.hide)
						want = "none"
						if err != nil {
							t.Fatal(err)
						}
					}
					if mode == "target-show" {
						current, err = sjson.SetBytes(current, source.path, source.show)
						want = "auto"
						if err != nil {
							t.Fatal(err)
						}
					}
					beforeCurrent, beforeOriginal := bytes.Clone(current), bytes.Clone(original)
					body, err := NewGeminiExecutor(nil).buildInteractionsBody(t.Context(), core.Request{Model: "interactions-summary-fixture", Payload: current}, core.Options{SourceFormat: source.format, OriginalRequest: original}, stream)
					if err != nil {
						t.Fatal(err)
					}
					value := gjson.GetBytes(body, "generation_config.thinking_summaries")
					if value.String() != want || (want == "" && value.Exists()) {
						t.Fatalf("summary=%s want=%q", value.Raw, want)
					}
					if !bytes.Equal(current, beforeCurrent) || !bytes.Equal(original, beforeOriginal) {
						t.Fatal("mutated source buffers")
					}
				})
			}
		}
	}
}

func TestGeminiInteractionsUnboundSummaryKeepsNativeEffortValidation(t *testing.T) {
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(t.Name(), "gemini", []*registry.ModelInfo{{ID: t.Name(), Type: "gemini", Thinking: &registry.ThinkingSupport{Levels: []string{"low"}}}})
	t.Cleanup(func() { reg.UnregisterClient(t.Name()) })
	_, err := NewGeminiExecutor(nil).buildInteractionsBody(t.Context(), core.Request{Model: t.Name(), Payload: []byte(`{"input":"fixture","generation_config":{"thinking_level":"high"}}`)}, core.Options{SourceFormat: translator.FormatInteractions, OriginalRequest: []byte(`{"input":"fixture","generation_config":{"thinking_level":"high","thinking_summaries":"none"}}`)}, false)
	var issue *thinking.ThinkingError
	if !errors.As(err, &issue) || issue.Code != thinking.ErrLevelNotSupported {
		t.Fatalf("summary changed native effort validation: %v", err)
	}
}
