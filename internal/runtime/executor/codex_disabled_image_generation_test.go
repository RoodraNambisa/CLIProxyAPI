package executor

import (
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestCodexExecutorDisabledImageGenerationToolRemove(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{
		AuthModelExclusions: []config.AuthModelExclusionRule{
			{DisableImageGeneration: true, Priorities: []int{-1}},
		},
	})
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Attributes: map[string]string{
			"priority": "-1",
		},
	}
	body := []byte(`{"tools":[{"type":"image_generation","model":"gpt-image-2"},{"type":"function","name":"lookup"}],"tool_choice":{"type":"image_generation"},"input":"draw"}`)

	got, err := executor.applyDisabledImageGenerationToolPolicy(auth, body)
	if err != nil {
		t.Fatalf("applyDisabledImageGenerationToolPolicy() error = %v", err)
	}
	if codexHasImageGenerationTool(got) {
		t.Fatalf("body still has image_generation tool: %s", got)
	}
	if gjson.GetBytes(got, "tool_choice").Exists() {
		t.Fatalf("tool_choice still exists: %s", got)
	}
	if gjson.GetBytes(got, "tools.0.type").String() != "function" {
		t.Fatalf("tools = %s, want retained function tool", gjson.GetBytes(got, "tools").Raw)
	}
}

func TestCodexExecutorDisabledImageGenerationToolRemoveDeletesStringToolChoiceWhenNoToolsRemain(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{
		AuthModelExclusions: []config.AuthModelExclusionRule{
			{DisableImageGeneration: true, Priorities: []int{-1}},
		},
	})
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Attributes: map[string]string{
			"priority": "-1",
		},
	}
	body := []byte(`{"tools":[{"type":"image_generation","model":"gpt-image-2"}],"tool_choice":"required","parallel_tool_calls":true,"input":"draw"}`)

	got, err := executor.applyDisabledImageGenerationToolPolicy(auth, body)
	if err != nil {
		t.Fatalf("applyDisabledImageGenerationToolPolicy() error = %v", err)
	}
	if gjson.GetBytes(got, "tools").Exists() {
		t.Fatalf("tools still exists: %s", got)
	}
	if gjson.GetBytes(got, "tool_choice").Exists() {
		t.Fatalf("tool_choice still exists: %s", got)
	}
	if gjson.GetBytes(got, "parallel_tool_calls").Exists() {
		t.Fatalf("parallel_tool_calls still exists: %s", got)
	}
}

func TestCodexExecutorDisabledImageGenerationToolError(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{
		DisabledImageGenerationToolAction: config.DisabledImageGenerationToolActionError,
		DisabledImageGenerationToolError: config.DisabledImageGenerationToolErrorConfig{
			StatusCode: http.StatusUnavailableForLegalReasons,
			Message:    "image disabled",
			Type:       "policy_error",
			Code:       "image_disabled",
		},
		AuthModelExclusions: []config.AuthModelExclusionRule{
			{DisableImageGeneration: true, KeywordContains: []string{"trial"}},
		},
	})
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Label:    "trial account",
	}
	body := []byte(`{"tools":[{"type":"image_generation","model":"gpt-image-2"}],"tool_choice":{"type":"image_generation"},"input":"draw"}`)

	_, err := executor.applyDisabledImageGenerationToolPolicy(auth, body)
	if err == nil {
		t.Fatal("applyDisabledImageGenerationToolPolicy() error = nil, want configured error")
	}
	status, ok := err.(interface{ StatusCode() int })
	if !ok || status.StatusCode() != http.StatusUnavailableForLegalReasons {
		t.Fatalf("status = %T %[1]v, want %d", err, http.StatusUnavailableForLegalReasons)
	}
	if !strings.Contains(err.Error(), "image disabled") || !strings.Contains(err.Error(), "policy_error") {
		t.Fatalf("error body = %s, want custom policy body", err.Error())
	}
	skipper, ok := err.(interface{ SkipAuthResult() bool })
	if !ok || !skipper.SkipAuthResult() {
		t.Fatalf("SkipAuthResult = %v, want true", ok)
	}
}
