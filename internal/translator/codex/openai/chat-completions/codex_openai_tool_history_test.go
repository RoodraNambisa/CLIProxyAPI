package chat_completions

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/tidwall/gjson"
)

func TestCodexChatToolHistoryBatchPairing(t *testing.T) {
	for _, tc := range []struct {
		name, request string
		want          []string
	}{
		{
			"missing-custom-id",
			`{"messages":[{"role":"assistant","tool_calls":[{"type":"custom","custom":{"name":"run","input":"work"}}]},{"role":"tool","content":"done"}]}`,
			[]string{"custom_tool_call|call_missing_0_0", "custom_tool_call_output|call_missing_0_0|done"},
		},
		{
			"wrapped-custom-id",
			`{"tools":[{"type":"custom","name":"run"}],"messages":[{"role":"assistant","tool_calls":[{"type":"function","function":{"name":"run","arguments":"work"}}]},{"role":"tool","content":"done"}]}`,
			[]string{"custom_tool_call|call_missing_0_0", "custom_tool_call_output|call_missing_0_0|done"},
		},
		{
			"generated-id-collision",
			`{"messages":[{"role":"assistant","tool_calls":[{"type":"function","function":{"name":"a","arguments":"{}"}},{"id":"call_missing_0_0","type":"function","function":{"name":"b","arguments":"{}"}},{"id":"call_missing_0_0_1","type":"function","function":{"name":"c","arguments":"{}"}}]},{"role":"tool","tool_call_id":"call_missing_0_0","content":"b"},{"role":"tool","tool_call_id":"call_missing_0_0_1","content":"c"},{"role":"tool","content":"a"}]}`,
			[]string{"function_call|call_missing_0_0_2", "function_call|call_missing_0_0", "function_call|call_missing_0_0_1", "function_call_output|call_missing_0_0|b", "function_call_output|call_missing_0_0_1|c", "function_call_output|call_missing_0_0_2|a"},
		},
		{
			"duplicate-source-id-is-ambiguous",
			`{"messages":[{"role":"assistant","tool_calls":[{"id":"same","type":"function","function":{"name":"a","arguments":"{}"}},{"id":"same","type":"custom","custom":{"name":"b","input":"work"}}]},{"role":"tool","tool_call_id":"same","content":"ambiguous"}]}`,
			nil,
		},
		{
			"consume-once-and-reuse-in-next-batch",
			`{"messages":[{"role":"assistant","tool_calls":[{"id":"same","type":"custom","custom":{"name":"run","input":"work"}}]},{"role":"tool","tool_call_id":"same","content":"first"},{"role":"tool","tool_call_id":"same","content":"duplicate"},{"role":"assistant","tool_calls":[{"id":"same","type":"function","function":{"name":"other","arguments":"{}"}}]},{"role":"tool","tool_call_id":"same","content":"second"}]}`,
			[]string{"custom_tool_call|same", "custom_tool_call_output|same|first", "function_call|same", "function_call_output|same|second"},
		},
		{
			"new-message-ends-batch",
			`{"messages":[{"role":"assistant","tool_calls":[{"id":"old","type":"custom","custom":{"name":"run","input":"work"}}]},{"role":"user","content":"next"},{"role":"tool","tool_call_id":"old","content":"stale"}]}`,
			[]string{"custom_tool_call|old"},
		},
		{
			"missing-result-id-needs-unique-candidate",
			`{"messages":[{"role":"assistant","tool_calls":[{"id":"a","type":"function","function":{"name":"a","arguments":"{}"}},{"id":"b","type":"function","function":{"name":"b","arguments":"{}"}}]},{"role":"tool","content":"ambiguous"},{"role":"tool","tool_call_id":"b","content":"for-b"},{"role":"tool","content":"for-a"}]}`,
			[]string{"function_call|a", "function_call|b", "function_call_output|b|for-b", "function_call_output|a|for-a"},
		},
		{
			"explicit-unknown-result-id-is-not-guessed",
			`{"messages":[{"role":"assistant","tool_calls":[{"id":"a","type":"function","function":{"name":"a","arguments":"{}"}}]},{"role":"tool","tool_call_id":"unknown","content":"wrong"},{"role":"tool","content":"right"}]}`,
			[]string{"function_call|a", "function_call_output|a|right"},
		},
		{"orphan-result", `{"messages":[{"role":"tool","tool_call_id":"unknown","content":"orphan"}]}`, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := []byte(tc.request)
			before := bytes.Clone(request)
			out := ConvertOpenAIRequestToCodex("fixture", request, true)
			var got []string
			for _, item := range gjson.GetBytes(out, "input").Array() {
				kind := item.Get("type").String()
				if kind != "function_call" && kind != "custom_tool_call" && kind != "function_call_output" && kind != "custom_tool_call_output" {
					continue
				}
				value := kind + "|" + item.Get("call_id").String()
				if output := item.Get("output"); output.Exists() {
					value += "|" + output.String()
				}
				got = append(got, value)
			}
			if !reflect.DeepEqual(got, tc.want) || !bytes.Equal(request, before) || !bytes.Equal(out, ConvertOpenAIRequestToCodex("fixture", request, true)) {
				t.Fatalf("history pairing changed: got %v, want %v", got, tc.want)
			}
		})
	}
}
