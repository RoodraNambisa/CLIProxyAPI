package responses

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestClaudeResponsesCustomDeclarationsHistoryAndSelection(t *testing.T) {
	for _, input := range []string{"", "line1\nline2", `literal {"input":"nested"} and \\ escapes`, "工具内容 😀"} {
		raw := []byte(`{"tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"value":{"default":9007199254740993}}}}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"custom","name":"patch","description":"Apply text","format":{"type":"text"}}]}]},{"type":"custom_tool_call","namespace":"editor","name":"patch","call_id":"pair","input":""},{"type":"custom_tool_call_output","call_id":"pair","output":"done\nnext"}],"tool_choice":{"type":"custom","namespace":"editor","name":"patch"}}`)
		raw, _ = sjson.SetBytes(raw, "input.1.input", input)
		original := bytes.Clone(raw)
		got := ConvertOpenAIResponsesRequestToClaude("claude-sonnet-4-6", raw, false)
		if gjson.GetBytes(got, "tools.#").Int() != 2 || gjson.GetBytes(got, "tools.1.name").String() != "editor__patch" || gjson.GetBytes(got, "tools.1.description").String() != "Apply text" || gjson.GetBytes(got, "tools.1.input_schema.properties.input.type").String() != "string" || gjson.GetBytes(got, "tools.1.input_schema.required.0").String() != "input" {
			t.Fatal("custom tool did not become a named string-input tool")
		}
		if gjson.GetBytes(got, "tools.0.input_schema").Raw != gjson.GetBytes(raw, "tools.0.parameters").Raw {
			t.Fatal("ordinary function schema changed")
		}
		if gjson.GetBytes(got, "messages.0.content.0.name").String() != "editor__patch" || gjson.GetBytes(got, "messages.0.content.0.id").String() != "pair" || gjson.GetBytes(got, "messages.0.content.0.input.input").String() != input || gjson.GetBytes(got, "messages.1.content.0.tool_use_id").String() != "pair" || gjson.GetBytes(got, "messages.1.content.0.content").String() != "done\nnext" {
			t.Fatal("custom history lost pairing, input text or result ordering")
		}
		if gjson.GetBytes(got, "tool_choice.type").String() != "tool" || gjson.GetBytes(got, "tool_choice.name").String() != "editor__patch" || !bytes.Equal(raw, original) {
			t.Fatal("selection disagrees with its declaration or the source was mutated")
		}
	}
}

func TestClaudeResponsesCustomDeclarationDoesNotOverrideRootFunction(t *testing.T) {
	raw := []byte(`{"tools":[{"type":"function","name":"editor__patch","parameters":{"type":"object","properties":{"kept":{"type":"integer"}}}}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"custom","name":"patch"}]}]}]}`)
	got := ConvertOpenAIResponsesRequestToClaude("claude-sonnet-4-6", raw, false)
	if gjson.GetBytes(got, "tools.#").Int() != 1 || gjson.GetBytes(got, "tools.0.input_schema.properties.kept.type").String() != "integer" || gjson.GetBytes(got, "tools.0.input_schema.properties.input").Exists() {
		t.Fatal("an additional custom declaration replaced the root function contract")
	}
}

func TestClaudeResponsesCustomOutputInterleavingAndEscapes(t *testing.T) {
	for _, tc := range []struct{ arguments, input string }{
		{`{"input":""}`, ""},
		{`{"input":"line1\n\uD83D\uDE00"}`, "line1\n😀"},
		{`{"input":"literal {\"input\":\"nested\"}"}`, `literal {"input":"nested"}`},
		{`{"input":"unfinished`, `{"input":"unfinished`},
	} {
		request := []byte(`{"tools":[{"type":"function","name":"lookup"}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"custom","name":"patch"}]}]}]}`)
		original := bytes.Clone(request)
		var state any
		start := `{"type":"message_start","message":{"id":"result"}}`
		ConvertClaudeResponseToOpenAIResponses(t.Context(), "", request, nil, []byte("data: "+start), &state)
		clear(request)
		delta := func(index int, fragment string) string {
			data := []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":""}}`)
			data, _ = sjson.SetBytes(data, "index", index)
			data, _ = sjson.SetBytes(data, "delta.partial_json", fragment)
			return string(data)
		}
		cut := len(tc.arguments) / 2
		events := []string{
			`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"custom","name":"editor__patch","input":{}}}`,
			`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"normal","name":"lookup","input":{}}}`,
			delta(0, tc.arguments[:cut]), delta(1, `{"value":9007199254740993}`), delta(0, tc.arguments[cut:]),
			`{"type":"content_block_stop","index":0}`, `{"type":"content_block_stop","index":1}`, `{"type":"message_stop"}`, `{"type":"message_stop"}`,
		}
		counts := make(map[string]int)
		var completed gjson.Result
		for _, event := range events {
			for _, chunk := range ConvertClaudeResponseToOpenAIResponses(t.Context(), "", nil, nil, []byte("data: "+event), &state) {
				for _, line := range strings.Split(string(chunk), "\n") {
					if !strings.HasPrefix(line, "data:") {
						continue
					}
					data := gjson.Parse(strings.TrimPrefix(line, "data:"))
					kind := data.Get("type").String()
					counts[kind]++
					if kind == "response.custom_tool_call_input.done" && (data.Get("item_id").String() != "ctc_custom" || data.Get("input").String() != tc.input) {
						t.Fatal("custom input completion lost its identity or text")
					}
					if kind == "response.custom_tool_call_input.delta" && data.Get("delta").String() != tc.input {
						t.Fatal("partial JSON wrapper escapes leaked to custom text")
					}
					if item := data.Get("item"); item.Get("call_id").String() == "custom" {
						if item.Get("type").String() != "custom_tool_call" || item.Get("id").String() != "ctc_custom" || item.Get("namespace").String() != "editor" || item.Get("name").String() != "patch" || item.Get("arguments").Exists() {
							t.Fatal("custom item retained the function representation")
						}
					}
					if kind == "response.completed" {
						completed = data.Get("response")
					}
				}
			}
		}
		wantDelta := 1
		if tc.input == "" {
			wantDelta = 0
		}
		if counts["response.custom_tool_call_input.delta"] != wantDelta || counts["response.custom_tool_call_input.done"] != 1 || counts["response.function_call_arguments.delta"] != 1 || counts["response.function_call_arguments.done"] != 1 || counts["response.completed"] != 1 {
			t.Fatalf("custom and ordinary events diverged: %v", counts)
		}
		nonstream := gjson.ParseBytes(ConvertClaudeResponseToOpenAIResponsesNonStream(t.Context(), "", original, nil, []byte("data: "+start+"\n\ndata: "+strings.Join(events, "\n\ndata: ")+"\n\n"), nil))
		for _, result := range []gjson.Result{completed, nonstream} {
			if result.Get("output.#").Int() != 2 || result.Get("output.0.type").String() != "custom_tool_call" || result.Get("output.0.input").String() != tc.input || result.Get("output.0.call_id").String() != "custom" || result.Get("output.1.type").String() != "function_call" || result.Get("output.1.arguments").String() != `{"value":9007199254740993}` {
				t.Fatal("custom and ordinary completed output were mixed")
			}
		}
	}
}
