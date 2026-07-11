package xai

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	"github.com/tidwall/gjson"
)

func TestApplierWritesResponsesReasoningEffort(t *testing.T) {
	applier := NewApplier()
	model := &registry.ModelInfo{Thinking: &registry.ThinkingSupport{Levels: []string{"low", "medium", "high"}}}
	out, err := applier.Apply([]byte(`{"input":"hello"}`), thinking.ThinkingConfig{Mode: thinking.ModeLevel, Level: thinking.LevelHigh}, model)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	if got := gjson.GetBytes(out, "reasoning.effort").String(); got != "high" {
		t.Fatalf("reasoning.effort = %q, want high; body=%s", got, out)
	}
}

func TestApplyThinkingUsesRegisteredXAIApplier(t *testing.T) {
	body := []byte(`{"input":"hello","reasoning":{"effort":"medium"}}`)
	out, err := thinking.ApplyThinking(body, "custom-grok-model", "xai", "xai", "xai")
	if err != nil {
		t.Fatalf("ApplyThinking() error = %v", err)
	}
	if got := gjson.GetBytes(out, "reasoning.effort").String(); got != "medium" {
		t.Fatalf("reasoning.effort = %q, want medium; body=%s", got, out)
	}
}
