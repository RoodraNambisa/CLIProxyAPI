package helps

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestParseOpenAIUsageChatCompletions(t *testing.T) {
	data := []byte(`{"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3,"prompt_tokens_details":{"cached_tokens":4},"completion_tokens_details":{"reasoning_tokens":5}}}`)
	detail := ParseOpenAIUsage(data)
	if detail.InputTokens != 1 {
		t.Fatalf("input tokens = %d, want %d", detail.InputTokens, 1)
	}
	if detail.OutputTokens != 2 {
		t.Fatalf("output tokens = %d, want %d", detail.OutputTokens, 2)
	}
	if detail.TotalTokens != 3 {
		t.Fatalf("total tokens = %d, want %d", detail.TotalTokens, 3)
	}
	if detail.CachedTokens != 4 {
		t.Fatalf("cached tokens = %d, want %d", detail.CachedTokens, 4)
	}
	if detail.ReasoningTokens != 5 {
		t.Fatalf("reasoning tokens = %d, want %d", detail.ReasoningTokens, 5)
	}
}

func TestParseOpenAIUsageResponses(t *testing.T) {
	data := []byte(`{"usage":{"input_tokens":10,"output_tokens":20,"total_tokens":30,"input_tokens_details":{"cached_tokens":7},"output_tokens_details":{"reasoning_tokens":9}}}`)
	detail := ParseOpenAIUsage(data)
	if detail.InputTokens != 10 {
		t.Fatalf("input tokens = %d, want %d", detail.InputTokens, 10)
	}
	if detail.OutputTokens != 20 {
		t.Fatalf("output tokens = %d, want %d", detail.OutputTokens, 20)
	}
	if detail.TotalTokens != 30 {
		t.Fatalf("total tokens = %d, want %d", detail.TotalTokens, 30)
	}
	if detail.CachedTokens != 7 {
		t.Fatalf("cached tokens = %d, want %d", detail.CachedTokens, 7)
	}
	if detail.ReasoningTokens != 9 {
		t.Fatalf("reasoning tokens = %d, want %d", detail.ReasoningTokens, 9)
	}
}

func TestParseCodexImageToolUsage(t *testing.T) {
	data := []byte(`{"response":{"tool_usage":{"image_gen":{"input_tokens":0,"output_tokens":7024,"total_tokens":7024,"output_tokens_details":{"image_tokens":7024,"text_tokens":0}}}}}`)
	detail, ok := ParseCodexImageToolUsage(data)
	if !ok {
		t.Fatal("expected image tool usage")
	}
	if detail.InputTokens != 0 {
		t.Fatalf("input tokens = %d, want 0", detail.InputTokens)
	}
	if detail.OutputTokens != 7024 {
		t.Fatalf("output tokens = %d, want 7024", detail.OutputTokens)
	}
	if detail.TotalTokens != 7024 {
		t.Fatalf("total tokens = %d, want 7024", detail.TotalTokens)
	}
}

func TestUsageReporterBuildRecordIncludesLatency(t *testing.T) {
	reporter := &UsageReporter{
		provider:    "openai",
		model:       "gpt-5.4",
		requestedAt: time.Now().Add(-1500 * time.Millisecond),
	}

	record := reporter.buildRecord(usage.Detail{TotalTokens: 3}, false)
	if record.Latency < time.Second {
		t.Fatalf("latency = %v, want >= 1s", record.Latency)
	}
	if record.Latency > 3*time.Second {
		t.Fatalf("latency = %v, want <= 3s", record.Latency)
	}
}

func TestUsageReporterAdditionalModelSkipsZeroUsage(t *testing.T) {
	reporter := &UsageReporter{provider: "codex", model: "gpt-5.4", requestedAt: time.Now()}
	if _, ok := reporter.buildAdditionalModelRecord("gpt-image-2", usage.Detail{}); ok {
		t.Fatal("expected zero-token additional model usage to be skipped")
	}
	if record, ok := reporter.buildAdditionalModelRecord("gpt-image-2", usage.Detail{OutputTokens: 10}); !ok || record.Model != "gpt-image-2" || record.Detail.TotalTokens != 10 {
		t.Fatalf("unexpected additional model record: %#v ok=%v", record, ok)
	}
}
