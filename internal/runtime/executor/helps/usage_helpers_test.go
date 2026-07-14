package helps

import (
	"bytes"
	"context"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	"github.com/tidwall/gjson"
)

type usageReporterTestPlugin struct {
	authID  string
	records chan usage.Record
}

func (p *usageReporterTestPlugin) HandleUsage(_ context.Context, record usage.Record) {
	if record.AuthID != p.authID {
		return
	}
	p.records <- record
}

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
	data := []byte(`{"service_tier":"priority","usage":{"input_tokens":10,"output_tokens":20,"total_tokens":30,"input_tokens_details":{"cached_tokens":7,"cache_write_tokens":6},"output_tokens_details":{"reasoning_tokens":9}}}`)
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
	if detail.CacheCreationTokens != 6 {
		t.Fatalf("cache creation tokens = %d, want %d", detail.CacheCreationTokens, 6)
	}
	if detail.ResponseServiceTier != "priority" {
		t.Fatalf("response service tier = %q, want priority", detail.ResponseServiceTier)
	}
}

func TestStreamUsageBufferRetainsTierAcrossUsageEvents(t *testing.T) {
	var buffer StreamUsageBuffer
	buffer.ObserveOpenAIStream([]byte(`data: {"service_tier":"priority"}`))
	buffer.ObserveOpenAIStream([]byte(`data: {"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`))

	detail, ok := buffer.Detail()
	if !ok {
		t.Fatal("expected buffered usage")
	}
	if detail.TotalTokens != 7 || detail.ResponseServiceTier != "priority" {
		t.Fatalf("unexpected buffered detail: %+v", detail)
	}
}

func TestParseOpenAIStreamUsageSkipsIrrelevantChunks(t *testing.T) {
	if _, ok := ParseOpenAIStreamUsage([]byte(`data: {"type":"response.output_text.delta","delta":"usage"}`)); ok {
		t.Fatal("irrelevant stream chunk should not be parsed as usage")
	}
}

func TestParseClaudeUsagePreservesCacheCreationTokens(t *testing.T) {
	detail := ParseClaudeUsage([]byte(`{"usage":{"input_tokens":2,"output_tokens":3,"cache_read_input_tokens":4,"cache_creation_input_tokens":5}}`))
	if detail.CachedTokens != 4 || detail.CacheCreationTokens != 5 {
		t.Fatalf("unexpected Claude cache tokens: %+v", detail)
	}
	if detail.InputTokens != 11 || detail.TotalTokens != 14 {
		t.Fatalf("Claude inclusive input totals = %+v, want input=11 total=14", detail)
	}
}

func TestParseClaudeStreamUsageUsesInclusiveInputTotal(t *testing.T) {
	detail, ok := ParseClaudeStreamUsage([]byte(`data: {"usage":{"input_tokens":2,"output_tokens":3,"cache_read_input_tokens":4,"cache_creation_input_tokens":5}}`))
	if !ok {
		t.Fatal("ParseClaudeStreamUsage() ok = false, want true")
	}
	if detail.InputTokens != 11 || detail.CachedTokens != 4 || detail.CacheCreationTokens != 5 || detail.TotalTokens != 14 {
		t.Fatalf("unexpected Claude stream usage: %+v", detail)
	}
}

func TestParseAntigravityUsageAcceptsResponseSnakeCase(t *testing.T) {
	detail := ParseAntigravityUsage([]byte(`{"response":{"usage_metadata":{"promptTokenCount":2,"candidatesTokenCount":3,"thoughtsTokenCount":5,"totalTokenCount":10}}}`))
	if detail.InputTokens != 2 || detail.OutputTokens != 8 || detail.ReasoningTokens != 5 || detail.TotalTokens != 10 {
		t.Fatalf("unexpected Antigravity snake-case usage: %+v", detail)
	}
}

func TestStripUsageMetadataFromJSONHandlesSnakeCase(t *testing.T) {
	for _, payload := range [][]byte{
		[]byte(`{"usage_metadata":{"totalTokenCount":5}}`),
		[]byte(`{"response":{"usage_metadata":{"totalTokenCount":5}}}`),
	} {
		cleaned, changed := StripUsageMetadataFromJSON(payload)
		if !changed {
			t.Fatalf("snake-case usage was not stripped: %s", payload)
		}
		if gjson.GetBytes(cleaned, "usage_metadata").Exists() || gjson.GetBytes(cleaned, "response.usage_metadata").Exists() {
			t.Fatalf("snake-case usage remained in payload: %s", cleaned)
		}
		if !gjson.GetBytes(cleaned, "cpaUsageMetadata").Exists() && !gjson.GetBytes(cleaned, "response.cpaUsageMetadata").Exists() {
			t.Fatalf("stripped usage was not preserved: %s", cleaned)
		}
	}
	terminal := []byte(`{"candidates":[{"finishReason":"STOP"}],"usage_metadata":{"totalTokenCount":5}}`)
	if cleaned, changed := StripUsageMetadataFromJSON(terminal); changed || !bytes.Equal(cleaned, terminal) {
		t.Fatalf("terminal usage changed: payload=%s changed=%t", cleaned, changed)
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

func TestParseInteractionsUsage(t *testing.T) {
	detail := ParseInteractionsUsage([]byte(`{"service_tier":"priority","usage":{"input_tokens":3,"output_tokens":4,"reasoning_tokens":5,"total_tokens":12,"cached_tokens":2}}`))
	if detail.InputTokens != 3 {
		t.Fatalf("input tokens = %d, want 3", detail.InputTokens)
	}
	if detail.OutputTokens != 9 {
		t.Fatalf("output tokens = %d, want 9", detail.OutputTokens)
	}
	if detail.ReasoningTokens != 5 {
		t.Fatalf("reasoning tokens = %d, want 5", detail.ReasoningTokens)
	}
	if detail.TotalTokens != 12 {
		t.Fatalf("total tokens = %d, want 12", detail.TotalTokens)
	}
	if detail.CachedTokens != 2 {
		t.Fatalf("cached tokens = %d, want 2", detail.CachedTokens)
	}
	if detail.ResponseServiceTier != "priority" {
		t.Fatalf("response service tier = %q, want priority", detail.ResponseServiceTier)
	}
}

func TestParseInteractionsStreamUsage(t *testing.T) {
	detail, ok := ParseInteractionsStreamUsage([]byte(`{"type":"interaction.completed","interaction":{"usage":{"input_tokens":2,"output_tokens":6,"total_tokens":8}}}`))
	if !ok {
		t.Fatal("ParseInteractionsStreamUsage() ok = false, want true")
	}
	if detail.TotalTokens != 8 {
		t.Fatalf("total tokens = %d, want 8", detail.TotalTokens)
	}
}

func TestParseInteractionsStreamUsageOfficialMetadata(t *testing.T) {
	detail, ok := ParseInteractionsStreamUsage([]byte(`data: {"event_type":"finish","metadata":{"total_usage":{"total_input_tokens":2,"total_output_tokens":6,"total_thought_tokens":3,"total_cached_tokens":1,"total_tokens":11}}}`))
	if !ok {
		t.Fatal("ParseInteractionsStreamUsage() ok = false, want true")
	}
	if detail.InputTokens != 2 {
		t.Fatalf("input tokens = %d, want 2", detail.InputTokens)
	}
	if detail.OutputTokens != 9 {
		t.Fatalf("output tokens = %d, want 9", detail.OutputTokens)
	}
	if detail.ReasoningTokens != 3 {
		t.Fatalf("reasoning tokens = %d, want 3", detail.ReasoningTokens)
	}
	if detail.CachedTokens != 1 {
		t.Fatalf("cached tokens = %d, want 1", detail.CachedTokens)
	}
	if detail.TotalTokens != 11 {
		t.Fatalf("total tokens = %d, want 11", detail.TotalTokens)
	}
}

func TestParseInteractionsUsageTotalFallbackUsesExplicitCacheRead(t *testing.T) {
	detail := ParseInteractionsUsage([]byte(`{"usage":{"input_tokens":13,"output_tokens":3,"reasoning_tokens":4,"total_cached_tokens":7,"cache_read_tokens":5,"cache_creation_tokens":6}}`))
	if detail.CachedTokens != 7 {
		t.Fatalf("cached tokens = %d, want 7", detail.CachedTokens)
	}
	if detail.TotalTokens != 20 {
		t.Fatalf("total tokens = %d, want 20", detail.TotalTokens)
	}
	if detail.CachedTokens > detail.InputTokens || detail.CacheCreationTokens > detail.InputTokens {
		t.Fatalf("cache fields are not input subsets: %+v", detail)
	}
}

func TestNormalizeUsageDetailTotalTreatsReasoningAsOutputSubset(t *testing.T) {
	detail := normalizeUsageDetailTotal(usage.Detail{InputTokens: 2, OutputTokens: 7, ReasoningTokens: 5})
	if detail.TotalTokens != 9 {
		t.Fatalf("total tokens = %d, want 9", detail.TotalTokens)
	}
}

func TestNormalizeUsageDetailTotalUsesChildFieldsWhenParentIsMissing(t *testing.T) {
	detail := normalizeUsageDetailTotal(usage.Detail{
		InputTokens:         1,
		ReasoningTokens:     5,
		CachedTokens:        100,
		CacheCreationTokens: 80,
	})
	if detail.TotalTokens != 105 {
		t.Fatalf("total tokens = %d, want max input subtype 100 plus reasoning 5", detail.TotalTokens)
	}
}

func TestUsageReporterMergesPartialGeminiReasoningUsage(t *testing.T) {
	reporter := NewUsageReporter(context.Background(), "gemini", "gemini-test", nil)
	reporter.Observe(usage.Detail{InputTokens: 2, OutputTokens: 8, ReasoningTokens: 5, TotalTokens: 10})
	reporter.Observe(usage.Detail{InputTokens: 2, OutputTokens: 4, TotalTokens: 11})
	if reporter.observedDetail.OutputTokens != 9 || reporter.observedDetail.ReasoningTokens != 5 || reporter.observedDetail.TotalTokens != 11 {
		t.Fatalf("merged Gemini usage = %+v, want output=9 reasoning=5 total=11", reporter.observedDetail)
	}
}

func TestUsageReporterMergesReasoningOnlyGeminiUsage(t *testing.T) {
	reporter := NewUsageReporter(context.Background(), "gemini", "gemini-test", nil)
	reporter.Observe(usage.Detail{InputTokens: 2, OutputTokens: 3, TotalTokens: 5})
	reporter.Observe(usage.Detail{InputTokens: 2, OutputTokens: 5, ReasoningTokens: 5, TotalTokens: 10})
	if reporter.observedDetail.OutputTokens != 8 || reporter.observedDetail.ReasoningTokens != 5 || reporter.observedDetail.TotalTokens != 10 {
		t.Fatalf("merged Gemini usage = %+v, want output=8 reasoning=5 total=10", reporter.observedDetail)
	}
}

func TestParseInteractionsUsageNestedMetadata(t *testing.T) {
	detail := ParseInteractionsUsage([]byte(`{"interaction":{"metadata":{"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}}}`))
	if detail.TotalTokens != 5 {
		t.Fatalf("total tokens = %d, want 5", detail.TotalTokens)
	}
}

func TestUsageReporterBuildRecordIncludesLatency(t *testing.T) {
	reporter := &UsageReporter{
		provider:           "openai",
		model:              "gpt-5.4",
		requestServiceTier: "priority",
		requestedAt:        time.Now().Add(-1500 * time.Millisecond),
	}

	record := reporter.buildRecord(usage.Detail{TotalTokens: 3, ResponseServiceTier: "flex"}, false)
	if record.Latency < time.Second {
		t.Fatalf("latency = %v, want >= 1s", record.Latency)
	}
	if record.Latency > 3*time.Second {
		t.Fatalf("latency = %v, want <= 3s", record.Latency)
	}
	if record.RequestServiceTier != "priority" || record.ResponseServiceTier != "flex" {
		t.Fatalf("unexpected service tiers: request=%q response=%q", record.RequestServiceTier, record.ResponseServiceTier)
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

func TestUsageReporterObservedUsageDoesNotHideTerminalFailure(t *testing.T) {
	const authID = "usage-reporter-observed-terminal-failure"
	records := make(chan usage.Record, 2)
	usage.RegisterPlugin(&usageReporterTestPlugin{authID: authID, records: records})
	reporter := NewUsageReporter(context.Background(), "codex", "gpt-5.4", &cliproxyauth.Auth{ID: authID})
	reporter.Observe(usage.Detail{InputTokens: 3, OutputTokens: 4})
	reporter.PublishFailure(context.Background(), context.Canceled)
	reporter.EnsurePublished(context.Background())

	select {
	case record := <-records:
		if !record.Failed {
			t.Fatalf("record failed = false, want true: %#v", record)
		}
		if record.Detail.TotalTokens != 0 {
			t.Fatalf("failed record retained observed tokens: %#v", record.Detail)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for usage record")
	}
	select {
	case record := <-records:
		t.Fatalf("received a second usage record: %#v", record)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestUsageReporterObserveMergesSplitStreamUsage(t *testing.T) {
	reporter := &UsageReporter{}
	reporter.Observe(usage.Detail{InputTokens: 11, CachedTokens: 4})
	reporter.Observe(usage.Detail{OutputTokens: 7})

	reporter.mu.Lock()
	detail := reporter.observedDetail
	reporter.mu.Unlock()
	if detail.InputTokens != 11 || detail.OutputTokens != 7 || detail.CachedTokens != 4 || detail.TotalTokens != 18 {
		t.Fatalf("merged observed usage = %#v", detail)
	}
}

func TestSetRequestServiceTierFromPayloadPreservesContextTierWhenAbsent(t *testing.T) {
	ctx := usage.WithServiceTier(context.Background(), "priority")
	reporter := NewUsageReporter(ctx, "codex", "gpt-5.4", nil)

	reporter.SetRequestServiceTierFromPayload([]byte(`{"model":"gpt-5.4"}`))
	if reporter.requestServiceTier != "priority" {
		t.Fatalf("request service tier = %q, want priority", reporter.requestServiceTier)
	}
	reporter.SetRequestServiceTierFromPayload([]byte(`{"service_tier":"flex"}`))
	if reporter.requestServiceTier != "flex" {
		t.Fatalf("request service tier = %q, want flex", reporter.requestServiceTier)
	}
}
