package chat_completions

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func codexChatArrayResponse(items, imageBytes int) []byte {
	var out strings.Builder
	out.WriteString(`{"type":"response.completed","response":{"id":"resp_fixture","created_at":1,"status":"completed","usage":{"input_tokens":12,"output_tokens":5,"total_tokens":17,"input_tokens_details":{"cached_tokens":3}},"output":[{"type":"message","content":[{"type":"output_text","text":"answer"}]},{"type":"reasoning","summary":[{"type":"summary_text","text":"summary"}]}]}}`)
	base := out.String()
	out.Reset()
	out.WriteString(base[:len(base)-3])
	for index := range items {
		fmt.Fprintf(&out, `,{"type":"function_call","call_id":"call_%d","name":"normal","arguments":"{\"n\":9007199254740993}"},{"type":"custom_tool_call","call_id":"custom_%d","name":"custom","input":"plain"},{"type":"image_generation_call","output_format":"png","result":%q}`, index, index, strings.Repeat("A", imageBytes))
	}
	out.WriteString(`]}}`)
	return []byte(out.String())
}

func TestCodexChatResponseArrayBatchPreservesOrderAndEmptyValues(t *testing.T) {
	request := []byte(`{"tools":[{"type":"custom","custom":{"name":"custom"}}]}`)
	for _, count := range []int{0, 1, 3} {
		body := codexChatArrayResponse(count, 4)
		original := bytes.Clone(body)
		out := ConvertCodexResponseToOpenAINonStream(t.Context(), "fixture", request, nil, body, nil)
		if !bytes.Equal(body, original) || !gjson.ValidBytes(out) {
			t.Fatal("response conversion changed its input or returned invalid JSON")
		}
		message := gjson.GetBytes(out, "choices.0.message")
		if message.Get("role").String() != "assistant" || message.Get("content").String() != "answer" || message.Get("reasoning_content").String() != "summary" || gjson.GetBytes(out, "usage.prompt_tokens_details.cached_tokens").Int() != 3 {
			t.Fatal("non-array response fields changed")
		}
		if count == 0 {
			if message.Get("tool_calls").Raw != "null" || message.Get("images").Exists() {
				t.Fatal("empty output arrays changed legacy null/absent fields")
			}
			continue
		}
		calls, images := message.Get("tool_calls").Array(), message.Get("images").Array()
		if len(calls) != 2*count || len(images) != count {
			t.Fatal("output array lost items")
		}
		for index := range count {
			if calls[2*index].Get("id").String() != fmt.Sprintf("call_%d", index) || calls[2*index].Get("function.arguments").String() != `{"n":9007199254740993}` || calls[2*index+1].Get("id").String() != fmt.Sprintf("custom_%d", index) || calls[2*index+1].Get("type").String() != "custom" || calls[2*index+1].Get("custom.input").String() != "plain" {
				t.Fatal("tool ordering, arguments or custom envelope changed")
			}
			if images[index].Get("index").Int() != int64(index) || images[index].Get("image_url.url").String() != "data:image/png;base64,AAAA" {
				t.Fatal("image ordering or content changed")
			}
		}
	}
}

func BenchmarkCodexChatResponseArrays(b *testing.B) {
	for _, tc := range []struct{ count, imageBytes int }{{0, 0}, {1, 128}, {100, 128}, {1000, 128}, {1, 1 << 20}, {1, 10 << 20}} {
		b.Run(fmt.Sprintf("items=%d/imageBytes=%d", tc.count, tc.imageBytes), func(b *testing.B) {
			body := codexChatArrayResponse(tc.count, tc.imageBytes)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				ConvertCodexResponseToOpenAINonStream(context.Background(), "fixture", nil, nil, body, nil)
			}
		})
	}
}
