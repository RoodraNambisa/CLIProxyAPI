package responses

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestInteractionsCustomDeclarationHistoryAndChoice(t *testing.T) {
	for _, input := range []string{"", "line1\nline2", `literal {"input":"nested"}`, "中文 😀"} {
		raw := []byte(`{"tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"value":{"default":9007199254740993}}}}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","children":[{"type":"custom","name":"patch","description":"Apply text"}]}]},{"type":"custom_tool_call","namespace":"editor","name":"patch","call_id":"pair","input":""},{"type":"custom_tool_call_output","call_id":"pair","output":{"accepted":true}}],"tool_choice":{"type":"custom","namespace":"editor","name":"patch"}}`)
		raw, _ = sjson.SetBytes(raw, "input.1.input", input)
		original := bytes.Clone(raw)
		got := ConvertOpenAIResponsesRequestToInteractions("gemini-2.5-flash", raw, false)
		if gjson.GetBytes(got, "tools.#").Int() != 2 || gjson.GetBytes(got, "tools.1.name").String() != "editor__patch" || gjson.GetBytes(got, "tools.1.type").String() != "function" || gjson.GetBytes(got, "tools.1.parameters.properties.input.type").String() != "string" || gjson.GetBytes(got, "tools.0.parameters").Raw != gjson.GetBytes(raw, "tools.0.parameters").Raw {
			t.Fatal("custom or ordinary declaration changed its contract")
		}
		if gjson.GetBytes(got, "input.0.type").String() != "function_call" || gjson.GetBytes(got, "input.0.name").String() != "editor__patch" || gjson.GetBytes(got, "input.0.call_id").String() != "pair" || gjson.GetBytes(got, "input.0.arguments.input").String() != input || gjson.GetBytes(got, "input.1.type").String() != "function_result" || gjson.GetBytes(got, "input.1.name").String() != "editor__patch" || gjson.GetBytes(got, "input.1.call_id").String() != "pair" || !gjson.GetBytes(got, "input.1.result.accepted").Bool() {
			t.Fatal("custom history lost input, output or pairing")
		}
		if gjson.GetBytes(got, "generation_config.tool_choice.type").String() != "function" || gjson.GetBytes(got, "generation_config.tool_choice.name").String() != "editor__patch" || gjson.GetBytes(got, "generation_config.tool_choice.namespace").Exists() || !bytes.Equal(raw, original) {
			t.Fatal("choice differs from its declaration or source was mutated")
		}
	}
}

func TestInteractionsCustomOutputInterleavingAndSourceRelease(t *testing.T) {
	for _, tc := range []struct{ arguments, input string }{
		{`{"input":""}`, ""},
		{`{"input":"line1\n\uD83D\uDE00"}`, "line1\n😀"},
		{`{"input":"literal {\"input\":\"nested\"}"}`, `literal {"input":"nested"}`},
		{`{"input":"unfinished`, `{"input":"unfinished`},
	} {
		request := []byte(`{"tools":[{"type":"function","name":"lookup"}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","children":[{"type":"custom","name":"patch"}]}]}]}`)
		original := bytes.Clone(request)
		var state any
		ConvertInteractionsResponseToOpenAIResponses(t.Context(), "fixture", request, nil, []byte(`{"event_type":"interaction.created"}`), &state)
		clear(request)
		delta := func(index int, fragment string) string {
			data := []byte(`{"event_type":"step.delta","index":0,"delta":{"type":"arguments_delta"}}`)
			data, _ = sjson.SetBytes(data, "index", index)
			data, _ = sjson.SetBytes(data, "delta.arguments", fragment)
			return string(data)
		}
		cut := len(tc.arguments) / 2
		events := []string{
			`{"event_type":"step.start","index":0,"step":{"type":"function_call","id":"upstream_item","call_id":"pair","name":"editor__patch","arguments":{}}}`,
			`{"event_type":"step.start","index":1,"step":{"type":"function_call","id":"normal_item","call_id":"normal","name":"lookup","arguments":{}}}`,
			delta(0, tc.arguments[:cut]), delta(1, `{"value":9007199254740993}`), delta(0, tc.arguments[cut:]),
			`{"event_type":"step.stop","index":0}`, `{"event_type":"step.stop","index":0}`,
			`{"event_type":"interaction.completed"}`, `{"event_type":"finish"}`,
		}
		counts := make(map[string]int)
		var completed gjson.Result
		for _, event := range events {
			for _, chunk := range ConvertInteractionsResponseToOpenAIResponses(t.Context(), "fixture", nil, nil, []byte(event), &state) {
				data := gjson.ParseBytes(interactionsSSEPayload(chunk))
				kind := data.Get("type").String()
				counts[kind]++
				if kind == "response.custom_tool_call_input.done" && (data.Get("item_id").String() != "ctc_pair" || data.Get("input").String() != tc.input) {
					t.Fatal("custom input completion lost its identity or text")
				}
				if kind == "response.custom_tool_call_input.delta" && data.Get("delta").String() != tc.input {
					t.Fatal("partial JSON wrapper leaked to custom input")
				}
				if item := data.Get("item"); item.Get("call_id").String() == "pair" {
					if item.Get("type").String() != "custom_tool_call" || item.Get("id").String() != "ctc_pair" || item.Get("name").String() != "patch" || item.Get("namespace").String() != "editor" || item.Get("arguments").Exists() {
						t.Fatal("custom item retained its function representation")
					}
				}
				if kind == "response.completed" {
					completed = data.Get("response")
				}
			}
		}
		wantDelta := 1
		if tc.input == "" {
			wantDelta = 0
		}
		if counts["response.custom_tool_call_input.delta"] != wantDelta || counts["response.custom_tool_call_input.done"] != 1 || counts["response.function_call_arguments.delta"] != 1 || counts["response.function_call_arguments.done"] != 1 || counts["response.completed"] != 1 {
			t.Fatalf("custom and function lifecycle mismatch: %v", counts)
		}
		raw := []byte(`{"steps":[{"type":"function_call","id":"upstream_item","call_id":"pair","name":"editor__patch"},{"type":"function_call","id":"normal_item","call_id":"normal","name":"lookup","arguments":{"value":9007199254740993}}]}`)
		raw, _ = sjson.SetBytes(raw, "steps.0.arguments", tc.arguments)
		nonstream := gjson.ParseBytes(ConvertInteractionsResponseToOpenAIResponsesNonStream(t.Context(), "fixture", original, nil, raw, nil))
		for _, result := range []gjson.Result{completed, nonstream} {
			if result.Get("output.#").Int() != 2 || result.Get("output.0.type").String() != "custom_tool_call" || result.Get("output.0.input").String() != tc.input || result.Get("output.0.call_id").String() != "pair" || result.Get("output.1.call_id").String() != "normal" || result.Get("output.1.type").String() != "function_call" || result.Get("output.1.arguments").String() != `{"value":9007199254740993}` {
				t.Fatal("custom and ordinary completed output were mixed")
			}
		}
	}
}
