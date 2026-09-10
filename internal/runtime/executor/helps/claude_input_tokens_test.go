package helps

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tiktoken-go/tokenizer"
)

func TestClaudeInputTokenEstimateCountsVisibleTextAndTools(t *testing.T) {
	payload := []byte(`{"system":[{"type":"text","text":"System"}],"messages":[{"role":"user","content":[{"type":"text","text":"Question"},{"type":"image","source":{"type":"base64","data":"private-image"}},{"type":"document","source":{"type":"text","data":"Document"}},{"type":"document","source":{"type":"base64","data":"private-pdf"}}]},{"role":"assistant","content":[{"type":"thinking","thinking":"Reason","signature":"opaque-signature"},{"type":"tool_use","id":"call_1","name":"lookup","input":{"n": 1}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":[{"type":"text","text":"Result"}]}]}],"tools":[{"name":"lookup","description":"Search","input_schema":{"type":"object"}}],"tool_choice":{"type":"tool","name":"lookup"}}`)
	before := bytes.Clone(payload)
	codec, err := tokenizer.Get(tokenizer.O200kBase)
	if err != nil {
		t.Fatal(err)
	}
	want, err := codec.Count("System\nuser\nQuestion\nDocument\nassistant\nReason\ncall_1\nlookup\n{\"n\":1}\nuser\ncall_1\nResult\nlookup\nSearch\n{\"type\":\"object\"}\ntool\nlookup")
	if err != nil {
		t.Fatal(err)
	}
	if got := EstimateClaudeInputTokens(t.Context(), payload); got != int64(want) {
		t.Fatalf("count = %d, want %d", got, want)
	}
	if !bytes.Equal(payload, before) {
		t.Fatal("estimation mutated the request")
	}
}

func TestClaudeInputTokenEstimateBoundaries(t *testing.T) {
	for _, payload := range []string{"", "invalid", "{}", "null", `{"system":[{"type":"image","text":"ignored"}],"messages":[]}`} {
		if EstimateClaudeInputTokens(t.Context(), []byte(payload)) != 0 {
			t.Fatal("empty or invalid input estimated")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if EstimateClaudeInputTokens(ctx, []byte(`{"messages":[{"role":"user","content":"hello"}]}`)) != 0 {
		t.Fatal("canceled input estimated")
	}
	for range 8 {
		t.Run("concurrent", func(t *testing.T) {
			t.Parallel()
			if EstimateClaudeInputTokens(t.Context(), []byte(`{"system":"Hello","messages":[{"role":"user","content":"world"}]}`)) <= 0 {
				t.Fatal("missing estimate")
			}
		})
	}
}

func TestClaudeInputTokenEstimateServerToolResults(t *testing.T) {
	for _, input := range []string{
		`{"type":"web_search_tool_result","tool_use_id":"c","content":[{"type":"web_search_result","title":"Title","url":"https://example.test","content":"found"}]}`,
		`{"type":"web_fetch_tool_result","content":{"type":"web_fetch_result","url":"https://example.test","content":{"type":"document","source":{"type":"text","data":"page"}}}}`,
		`{"type":"code_execution_tool_result","content":{"type":"code_execution_result","stdout":"result","return_code":0}}`,
		`{"type":"mcp_tool_use","id":"mcp_1","name":"tool","input":{"x":true}}`,
		`{"type":"tool_reference","tool_name":"tool"}`,
	} {
		if EstimateClaudeInputTokens(t.Context(), []byte(`{"messages":[{"content":[`+input+`]}]}`)) <= 0 {
			t.Fatal("server tool result omitted")
		}
	}
	for _, kind := range []string{"image", "input_audio", "audio", "video", "redacted_thinking"} {
		if EstimateClaudeInputTokens(t.Context(), []byte(fmt.Sprintf(`{"messages":[{"content":[{"type":%q,"text":"secret","data":"opaque"}]}]}`, kind))) != 0 {
			t.Fatal("opaque media counted as text")
		}
	}
}

func TestClaudeInputTokenStateOnlyReplacesFirstStart(t *testing.T) {
	for _, prefix := range []string{"data: ", " \tdata:\t", "data: \u2003"} {
		state := ClaudeInputTokenState{Estimate: 42}
		created := []byte("event: ping\n\n")
		start := []byte("event: message_start\r\n" + prefix + `{"type":"message_start","message":{"usage":{"input_tokens":0,"cache_read_input_tokens":5}}}` + " \r\n\r\n")
		chunks := [][]byte{created, start}
		result := state.Apply(chunks)
		want := bytes.Replace(start, []byte(`"input_tokens":0`), []byte(`"input_tokens":42`), 1)
		if !bytes.Equal(result[0], created) || !bytes.Equal(result[1], want) {
			t.Fatalf("SSE framing changed: %q", result[1])
		}
		final := []byte(`data: {"type":"message_delta","usage":{"input_tokens":11,"output_tokens":2}}`)
		later := state.Apply([][]byte{start, final})
		if !bytes.Equal(later[0], start) || !bytes.Equal(later[1], final) {
			t.Fatal("duplicate start or actual usage changed")
		}
	}
}

func TestClaudeInputTokenStateKeepsKnownUsageAndDisabledStreams(t *testing.T) {
	for _, estimate := range []int64{-1, 0, 42} {
		for _, known := range []int{0, 12} {
			state := ClaudeInputTokenState{Estimate: estimate}
			input := []byte(fmt.Sprintf("data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":%d}}}\n\n", known))
			output := state.Apply([][]byte{input})[0]
			want := int64(known)
			if estimate > 0 && known == 0 {
				want = estimate
			}
			payload := strings.TrimSpace(strings.TrimPrefix(string(output), "data:"))
			if gjson.Get(payload, "message.usage.input_tokens").Int() != want {
				t.Fatal("wrong estimate/known usage priority")
			}
		}
	}
}

func BenchmarkClaudeInputTokenEstimate(b *testing.B) {
	for _, size := range []int{128, 1 << 20, 10 << 20} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			payload, _ := json.Marshal(map[string]any{"messages": []any{map[string]any{"role": "user", "content": strings.Repeat("text ", size/5)}}})
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for b.Loop() {
				if EstimateClaudeInputTokens(b.Context(), payload) == 0 {
					b.Fatal("no estimate")
				}
			}
		})
	}
}
