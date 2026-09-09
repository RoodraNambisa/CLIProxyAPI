package helps

import (
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

func TestClaudeUsageReporterUsesExactPartialSnapshots(t *testing.T) {
	reporter := NewUsageReporter(t.Context(), "claude", "model", nil)
	reporter.ObserveClaudeStreamUsage([]byte(`data: {"type":"message_start","message":{"usage":{"input_tokens":2,"cache_read_input_tokens":4,"cache_creation_input_tokens":5}}}`))
	reporter.ObserveClaudeStreamUsage([]byte(`data: {"type":"message_delta","usage":{"output_tokens":3,"thinking_tokens":2}}`))
	want := usage.Detail{InputTokens: 11, OutputTokens: 3, CachedTokens: 4, CacheCreationTokens: 5, ReasoningTokens: 2, TotalTokens: 14}
	if reporter.observedDetail != want {
		t.Fatal("partial snapshot lost counters or estimated reasoning")
	}
	reporter.ObserveClaudeStreamUsage([]byte(`data: {"usage":{"input_tokens":1,"cache_read_input_tokens":0,"thinking_tokens":0}}`))
	want.InputTokens, want.CachedTokens, want.ReasoningTokens, want.TotalTokens = 6, 0, 0, 9
	if reporter.observedDetail != want {
		t.Fatal("zero correction was ignored by generic usage merging")
	}
	for _, line := range []string{`event: ping`, `data: {"type":"content_block_delta","delta":{"text":"usage"}}`, `data: {"usage":{"input_tokens":"8"}}`, `data: {"usage":`} {
		reporter.ObserveClaudeStreamUsage([]byte(line))
		if reporter.observedDetail != want {
			t.Fatal("non-usage data replaced the current snapshot")
		}
	}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() { reporter.ObserveClaudeStreamUsage([]byte(`data: {"usage":{"output_tokens":3}}`)) })
	}
	wg.Wait()
	if reporter.observedDetail != want {
		t.Fatal("concurrent observation changed complete counters")
	}
	reporter.ObserveClaudeStreamUsage([]byte(`data: {"type":"message_stop"}`))
	reporter.ObserveClaudeStreamUsage([]byte(`data: {"usage":{"output_tokens":999}}`))
	if reporter.observedDetail != want {
		t.Fatal("late counters replaced terminal usage")
	}
	reporter = NewUsageReporter(t.Context(), "claude", "new-request", nil)
	reporter.ObserveClaudeStreamUsage([]byte(`data: {"usage":{"input_tokens":9}}`))
	reporter.ObserveClaudeStreamUsage([]byte(`data: {"type":"message_start","message":{"id":"reset"}}`))
	if reporter.observed || reporter.observedDetail != (usage.Detail{}) {
		t.Fatal("new message retained old counts")
	}
	reporter.published = true
	reporter.ObserveClaudeStreamUsage([]byte(`data: {"usage":{"output_tokens":9}}`))
	if reporter.observed {
		t.Fatal("published usage was mutated")
	}
}

func TestClaudeUsageParsersReadStartAndReasoningWithoutDoubleCounting(t *testing.T) {
	for _, line := range []string{
		`data: {"type":"message_start","message":{"usage":{"input_tokens":2,"output_tokens":5,"cache_read_input_tokens":3,"cache_creation_input_tokens":4,"output_tokens_details":{"thinking_tokens":2}}}}`,
		`data: {"usage":{"input_tokens":2,"output_tokens":5,"cache_read_input_tokens":3,"cache_creation_input_tokens":4,"thinking_tokens":2}}`,
	} {
		detail, ok := ParseClaudeStreamUsage([]byte(line))
		if !ok || detail != (usage.Detail{InputTokens: 9, OutputTokens: 5, CachedTokens: 3, CacheCreationTokens: 4, ReasoningTokens: 2, TotalTokens: 14}) {
			t.Fatal("stream usage parser lost documented counts")
		}
	}
}
