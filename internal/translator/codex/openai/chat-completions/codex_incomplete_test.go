package chat_completions

import (
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexIncompleteChatRetainsPartialContentAndFinishReason(t *testing.T) {
	for _, tc := range []struct{ reason, finish string }{{"max_tokens", "length"}, {"max_output_tokens", "length"}, {"content_filter", "content_filter"}} {
		for _, custom := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/custom=%t", tc.reason, custom), func(t *testing.T) {
				request := []byte(`{"tools":[{"type":"function","function":{"name":"run"}}]}`)
				kind, field, prefix, deltaKind := "function_call", "arguments", "fc_", "response.function_call_arguments.delta"
				if custom {
					request = []byte(`{"tools":[{"type":"custom","custom":{"name":"run"}}]}`)
					kind, field, prefix, deltaKind = "custom_tool_call", "input", "ctc_", "response.custom_tool_call_input.delta"
				}
				partial := `{\"unfinished\":`
				item := fmt.Sprintf(`{"type":%q,"id":%q,"call_id":"pair","name":"run",%q:"%s"}`, kind, prefix+"pair", field, partial)
				terminal := []byte(fmt.Sprintf(`{"type":"response.incomplete","response":{"id":"partial","status":"incomplete","incomplete_details":{"reason":%q},"output":[{"type":"message","content":[{"type":"output_text","text":"partial answer"}]},%s],"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}}`, tc.reason, item))
				nonstream := gjson.ParseBytes(ConvertCodexResponseToOpenAINonStream(t.Context(), "model", request, nil, terminal, nil))
				if nonstream.Get("choices.0.message.content").String() != "partial answer" || nonstream.Get("choices.0.finish_reason").String() != tc.finish || nonstream.Get("choices.0.native_finish_reason").String() != tc.reason || nonstream.Get("usage.total_tokens").Int() != 5 {
					t.Fatal("non-stream incomplete response lost partial content or stop reason")
				}
				toolPath := "choices.0.message.tool_calls.0.function.arguments"
				if custom {
					toolPath = "choices.0.message.tool_calls.0.custom.input"
				}
				if nonstream.Get(toolPath).String() != `{"unfinished":` {
					t.Fatal("partial tool content was normalized into a complete call")
				}
				var state any
				ConvertCodexResponseToOpenAI(t.Context(), "model", request, nil, []byte(`data: {"type":"response.created","response":{"id":"partial","model":"model"}}`), &state)
				start := fmt.Sprintf(`data: {"type":"response.output_item.added","output_index":0,"item":{"type":%q,"id":%q,"call_id":"pair","name":"run",%q:""}}`, kind, prefix+"pair", field)
				ConvertCodexResponseToOpenAI(t.Context(), "model", request, nil, []byte(start), &state)
				delta := fmt.Sprintf(`data: {"type":%q,"item_id":%q,"output_index":0,"delta":"%s"}`, deltaKind, prefix+"pair", partial)
				ConvertCodexResponseToOpenAI(t.Context(), "model", request, nil, []byte(delta), &state)
				out := ConvertCodexResponseToOpenAI(t.Context(), "model", request, nil, append([]byte("data: "), terminal...), &state)
				if len(out) != 1 || gjson.GetBytes(out[0], "choices.0.finish_reason").String() != tc.finish || gjson.GetBytes(out[0], "choices.0.native_finish_reason").String() != tc.reason || gjson.GetBytes(out[0], "usage.total_tokens").Int() != 5 {
					t.Fatal("stream incomplete terminal was dropped or reported as a normal tool finish")
				}
				if late := ConvertCodexResponseToOpenAI(t.Context(), "model", request, nil, []byte(`data: {"type":"response.completed","response":{"status":"completed"}}`), &state); len(late) != 0 {
					t.Fatal("late completion replaced the first incomplete terminal")
				}
			})
		}
	}
}
