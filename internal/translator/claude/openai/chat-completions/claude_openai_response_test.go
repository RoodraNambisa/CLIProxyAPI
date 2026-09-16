package chat_completions

import (
	"context"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestClaudeChatUsageMergesStartAndPartialDeltas(t *testing.T) {
	frames := []string{
		`data: {"type":"message_start","message":{"id":"msg_partial","model":"claude-sonnet-4","usage":{"input_tokens":12,"output_tokens":0,"cache_read_input_tokens":5,"cache_creation_input_tokens":3}}}`,
		`data: {"type":"message_delta","usage":{"output_tokens":4}}`,
		`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":7,"output_tokens_details":{"thinking_tokens":2}}}`,
	}
	var state any
	var stream [][]byte
	for _, frame := range frames {
		stream = ConvertClaudeResponseToOpenAI(context.Background(), "claude-sonnet-4", nil, nil, []byte(frame), &state)
	}
	nonStream := ConvertClaudeResponseToOpenAINonStream(context.Background(), "", nil, nil, []byte(strings.Join(frames, "\n")), nil)
	for _, payload := range [][]byte{stream[0], nonStream} {
		for path, want := range map[string]int64{
			"usage.prompt_tokens": 20, "usage.completion_tokens": 7, "usage.total_tokens": 27,
			"usage.prompt_tokens_details.cached_tokens": 5, "usage.prompt_tokens_details.cache_creation_tokens": 3,
			"usage.completion_tokens_details.reasoning_tokens": 2,
		} {
			if got := gjson.GetBytes(payload, path).Int(); got != want {
				t.Fatalf("%s: got %d, want %d in %s", path, got, want, payload)
			}
		}
	}
	ConvertClaudeResponseToOpenAI(context.Background(), "claude-sonnet-4", nil, nil, []byte(`data: {"type":"message_start","message":{"id":"new-message","usage":{"input_tokens":1}}}`), &state)
	stream = ConvertClaudeResponseToOpenAI(context.Background(), "claude-sonnet-4", nil, nil, []byte(`data: {"type":"message_delta","usage":{"output_tokens":2}}`), &state)
	if gjson.GetBytes(stream[0], "usage.total_tokens").Int() != 3 {
		t.Fatal("usage from the previous message leaked")
	}
}

func TestConvertClaudeResponseToOpenAI_StreamUsageIncludesCachedTokens(t *testing.T) {
	ctx := context.Background()
	var param any

	out := ConvertClaudeResponseToOpenAI(
		ctx,
		"claude-opus-4-6",
		nil,
		nil,
		[]byte(`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":13,"output_tokens":4,"cache_read_input_tokens":22000,"cache_creation_input_tokens":31}}`),
		&param,
	)
	if len(out) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(out))
	}

	if gotPromptTokens := gjson.GetBytes(out[0], "usage.prompt_tokens").Int(); gotPromptTokens != 22044 {
		t.Fatalf("expected prompt_tokens %d, got %d", 22044, gotPromptTokens)
	}
	if gotCompletionTokens := gjson.GetBytes(out[0], "usage.completion_tokens").Int(); gotCompletionTokens != 4 {
		t.Fatalf("expected completion_tokens %d, got %d", 4, gotCompletionTokens)
	}
	if gotTotalTokens := gjson.GetBytes(out[0], "usage.total_tokens").Int(); gotTotalTokens != 22048 {
		t.Fatalf("expected total_tokens %d, got %d", 22048, gotTotalTokens)
	}
	if gotCachedTokens := gjson.GetBytes(out[0], "usage.prompt_tokens_details.cached_tokens").Int(); gotCachedTokens != 22000 {
		t.Fatalf("expected cached_tokens %d, got %d", 22000, gotCachedTokens)
	}
}

func TestConvertClaudeResponseToOpenAINonStream_UsageIncludesCachedTokens(t *testing.T) {
	rawJSON := []byte("data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_123\",\"model\":\"claude-opus-4-6\"}}\n" +
		"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":13,\"output_tokens\":4,\"cache_read_input_tokens\":22000,\"cache_creation_input_tokens\":31}}\n")

	out := ConvertClaudeResponseToOpenAINonStream(context.Background(), "", nil, nil, rawJSON, nil)

	if gotPromptTokens := gjson.GetBytes(out, "usage.prompt_tokens").Int(); gotPromptTokens != 22044 {
		t.Fatalf("expected prompt_tokens %d, got %d", 22044, gotPromptTokens)
	}
	if gotCompletionTokens := gjson.GetBytes(out, "usage.completion_tokens").Int(); gotCompletionTokens != 4 {
		t.Fatalf("expected completion_tokens %d, got %d", 4, gotCompletionTokens)
	}
	if gotTotalTokens := gjson.GetBytes(out, "usage.total_tokens").Int(); gotTotalTokens != 22048 {
		t.Fatalf("expected total_tokens %d, got %d", 22048, gotTotalTokens)
	}
	if gotCachedTokens := gjson.GetBytes(out, "usage.prompt_tokens_details.cached_tokens").Int(); gotCachedTokens != 22000 {
		t.Fatalf("expected cached_tokens %d, got %d", 22000, gotCachedTokens)
	}
}
