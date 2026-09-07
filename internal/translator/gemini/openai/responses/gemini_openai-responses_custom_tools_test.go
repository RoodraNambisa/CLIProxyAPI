package responses

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestGeminiResponsesCustomDeclarationsAndMixedHistory(t *testing.T) {
	for _, input := range []string{"", "one\ntwo", `literal {"input":"nested"}`, "中文 😀"} {
		raw := []byte(`{"tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"value":{"default":9007199254740993}}}}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"custom","name":"patch/file","description":"Apply text","format":{"type":"text"}}]}]},{"type":"function_call","name":"lookup","call_id":"normal","arguments":"{\"value\":1}"},{"type":"custom_tool_call","namespace":"editor","name":"patch/file","call_id":"custom","input":""},{"type":"custom_tool_call_output","call_id":"custom","output":{"accepted":true}},{"type":"function_call_output","call_id":"normal","output":"found"}],"tool_choice":{"type":"custom","namespace":"editor","name":"patch/file"}}`)
		raw, _ = sjson.SetBytes(raw, "input.2.input", input)
		original := bytes.Clone(raw)
		got := ConvertOpenAIResponsesRequestToGemini("gemini-2.5-flash", raw, false)
		tools := gjson.GetBytes(got, "tools.0.functionDeclarations")
		if len(tools.Array()) != 2 || tools.Get("1.name").String() != "editor__patch_file" || tools.Get("1.parametersJsonSchema.properties.input.type").String() != "string" || tools.Get("1.parametersJsonSchema.required.0").String() != "input" || tools.Get("0.parametersJsonSchema").Raw != gjson.GetBytes(raw, "tools.0.parameters").Raw {
			t.Fatal("custom declaration or normal schema changed")
		}
		calls := make(map[string]gjson.Result)
		results := make(map[string]gjson.Result)
		for _, content := range gjson.GetBytes(got, "contents").Array() {
			for _, part := range content.Get("parts").Array() {
				if call := part.Get("functionCall"); call.Exists() {
					calls[call.Get("id").String()] = call
				}
				if result := part.Get("functionResponse"); result.Exists() {
					results[result.Get("id").String()] = result
				}
			}
		}
		if len(calls) != 2 || len(results) != 2 || calls["custom"].Get("name").String() != "editor__patch_file" || calls["custom"].Get("args.input").String() != input || results["custom"].Get("name").String() != "editor__patch_file" || !results["custom"].Get("response.result.accepted").Bool() || results["normal"].Get("name").String() != "lookup" || results["normal"].Get("response.result").String() != "found" {
			t.Fatal("mixed custom/function history lost names, pairing, input or results")
		}
		if gjson.GetBytes(got, "toolConfig.functionCallingConfig.allowedFunctionNames.0").String() != "editor__patch_file" || !bytes.Equal(raw, original) {
			t.Fatal("selection or source ownership changed")
		}
	}
}

func TestGeminiResponsesCustomOutputTypesEventsAndOwnership(t *testing.T) {
	for _, input := range []string{"", "line\n😀", `literal {"input":"nested"}`} {
		original := []byte(`{"tools":[{"type":"function","name":"lookup"}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"custom","name":"patch/file"}]}]}]}`)
		response := []byte(`{"candidates":[{"content":{"parts":[{"functionCall":{"name":"editor__patch_file","args":{"input":""}}},{"functionCall":{"name":"lookup","args":{"value":9007199254740993}}}]},"finishReason":"STOP"}]}`)
		response, _ = sjson.SetBytes(response, "candidates.0.content.parts.0.functionCall.args.input", input)
		nonstream := gjson.ParseBytes(ConvertGeminiResponseToOpenAIResponsesNonStream(t.Context(), "", original, nil, response, nil))
		var state any
		ConvertGeminiResponseToOpenAIResponses(t.Context(), "", original, nil, []byte(`{"candidates":[{"content":{"parts":[]}}]}`), &state)
		clear(original)
		counts := make(map[string]int)
		var completed gjson.Result
		var callID string
		for _, chunk := range ConvertGeminiResponseToOpenAIResponses(t.Context(), "", nil, nil, response, &state) {
			kind, data := parseSSEEvent(t, chunk)
			counts[kind]++
			if item := data.Get("item"); item.Get("type").String() == "custom_tool_call" {
				if callID == "" {
					callID = item.Get("call_id").String()
				}
				if item.Get("call_id").String() != callID || item.Get("id").String() != "ctc_"+callID || item.Get("arguments").Exists() || item.Get("name").String() != "patch/file" || item.Get("namespace").String() != "editor" {
					t.Fatal("custom event lost its original identity or retained function fields")
				}
			}
			if kind == "response.custom_tool_call_input.done" && (data.Get("input").String() != input || data.Get("item_id").String() != "ctc_"+callID) {
				t.Fatal("custom input completion disagrees with its item")
			}
			if kind == "response.custom_tool_call_input.delta" && data.Get("delta").String() != input {
				t.Fatal("JSON wrapper leaked into the input delta")
			}
			if kind == "response.completed" {
				completed = data.Get("response")
			}
		}
		wantDelta := 1
		if input == "" {
			wantDelta = 0
		}
		if counts["response.custom_tool_call_input.delta"] != wantDelta || counts["response.custom_tool_call_input.done"] != 1 || counts["response.function_call_arguments.delta"] != 1 || counts["response.function_call_arguments.done"] != 1 {
			t.Fatalf("tool event types diverged: %v", counts)
		}
		for _, output := range []gjson.Result{completed, nonstream} {
			if output.Get("output.#").Int() != 2 || output.Get("output.0.type").String() != "custom_tool_call" || output.Get("output.0.input").String() != input || output.Get("output.0.name").String() != "patch/file" || output.Get("output.1.type").String() != "function_call" || output.Get("output.1.arguments").String() != `{"value":9007199254740993}` {
				t.Fatal("custom and ordinary completed results disagree")
			}
		}
	}
}

func TestGeminiResponsesCustomOutputDoesNotGuessAfterNameCollision(t *testing.T) {
	original := []byte(`{"tools":[{"type":"custom","name":"patch/file"},{"type":"function","name":"patch_file"}]}`)
	response := []byte(`{"candidates":[{"content":{"parts":[{"functionCall":{"name":"patch_file","args":{"input":"preserve"}}},{"functionCall":{"name":"unknown"}}]},"finishReason":"STOP"}]}`)
	got := ConvertGeminiResponseToOpenAIResponsesNonStream(t.Context(), "", original, nil, response, nil)
	if gjson.GetBytes(got, "output.0.type").String() != "function_call" || gjson.GetBytes(got, "output.0.arguments").String() != `{"input":"preserve"}` || gjson.GetBytes(got, "output.1.arguments").String() != "{}" {
		t.Fatal("ambiguous custom identity was guessed or empty function arguments lost their object value")
	}
}
