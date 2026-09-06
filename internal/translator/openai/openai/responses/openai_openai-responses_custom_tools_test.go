package responses

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestResponsesLiteToolsMergeAndReplay(t *testing.T) {
	request := []byte(`{"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}],"input":[{"type":"additional_tools","tools":[{"type":"custom","name":"lookup"},{"type":"namespace","name":"editor","tools":[{"type":"custom","name":"patch"}]}]},{"type":"function_call","name":"lookup","call_id":"call_a","arguments":"{}"},{"type":"custom_tool_call","namespace":"editor","name":"patch","call_id":"call_b","input":"line1\nline2"},{"type":"message","role":"user","content":"later"},{"type":"function_call_output","call_id":"call_a","output":"found"},{"type":"custom_tool_call_output","call_id":"call_b","output":[{"type":"input_text","text":"part1"},{"type":"input_text","text":"part2"}]}],"tool_choice":{"type":"custom","namespace":"editor","name":"patch"}}`)
	converted := ConvertOpenAIResponsesRequestToOpenAIChatCompletions("model", request, true)
	tools := gjson.GetBytes(converted, "tools").Array()
	if len(tools) != 2 || tools[0].Get("function.name").String() != "lookup" || tools[0].Get("function.parameters.properties.input").Exists() {
		t.Fatal("first-wins declaration order changed")
	}
	if tools[1].Get("function.name").String() != "editor__patch" || tools[1].Get("function.parameters.required.0").String() != "input" {
		t.Fatal("custom namespace was not converted")
	}
	messages := gjson.GetBytes(converted, "messages").Array()
	if len(messages) != 4 || len(messages[0].Get("tool_calls").Array()) != 2 || messages[1].Get("role").String() != "tool" || messages[2].Get("role").String() != "tool" || messages[3].Get("content").String() != "later" {
		t.Fatal("tool replay lost grouping or result order")
	}
	wrapped := messages[0].Get("tool_calls.1.function.arguments").String()
	if gjson.Get(wrapped, "input").String() != "line1\nline2" || messages[2].Get("content").String() != "part1part2" {
		t.Fatal("custom input/output was not preserved")
	}
	if gjson.GetBytes(converted, "tool_choice.function.name").String() != "editor__patch" {
		t.Fatal("custom tool choice was lost")
	}
}

func TestResponsesToolRequestKeepsQualifiedNamesAndExactSchema(t *testing.T) {
	request := []byte(`{"tools":[{"type":"namespace","name":"editor__","tools":[{"type":"function","name":"editor__patch","strict":false,"parameters":{"type":"object","properties":{"value":{"type":"integer","default":9007199254740993}}}}]}],"input":[{"type":"function_call","namespace":"editor__","name":"editor__patch","call_id":"call_x","arguments":"{}"},{"type":"function_call_output","call_id":"call_x","output":"ok"}],"tool_choice":{"type":"function","namespace":"editor__","name":"editor__patch"}}`)
	converted := ConvertOpenAIResponsesRequestToOpenAIChatCompletions("model", request, false)
	for _, path := range []string{"tools.0.function.name", "messages.0.tool_calls.0.function.name", "tool_choice.function.name"} {
		if gjson.GetBytes(converted, path).String() != "editor__patch" {
			t.Fatal("already qualified tool name gained a second namespace prefix")
		}
	}
	if gjson.GetBytes(converted, "tools.0.function.parameters").Raw != gjson.GetBytes(request, "tools.0.tools.0.parameters").Raw || gjson.GetBytes(converted, "tools.0.function.strict").Raw != "false" {
		t.Fatal("tool schema numeric precision or explicit strict=false changed")
	}
}

func TestResponsesCustomToolsNonStreamIdentityUsesWinningDeclaration(t *testing.T) {
	for _, custom := range []bool{false, true} {
		request := []byte(`{"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"custom","name":"patch"}]}]}]}`)
		if !custom {
			request = []byte(`{"tools":[{"type":"function","name":"editor__patch","parameters":{}}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"custom","name":"patch"}]}]}]}`)
		}
		response := []byte(`{"id":"reply","choices":[{"message":{"tool_calls":[{"id":"call_x","function":{"name":"editor__patch","arguments":"{\"input\":\"raw\\ntext\"}"}}]},"finish_reason":"tool_calls"}]}`)
		out := ConvertOpenAIChatCompletionsResponseToOpenAIResponsesNonStream(t.Context(), "model", request, nil, response, nil)
		item := gjson.GetBytes(out, "output.0")
		if custom {
			if item.Get("type").String() != "custom_tool_call" || item.Get("input").String() != "raw\ntext" || item.Get("namespace").String() != "editor" || item.Get("name").String() != "patch" {
				t.Fatal("custom output identity or input was lost")
			}
		} else if item.Get("type").String() != "function_call" || item.Get("name").String() != "editor__patch" || item.Get("namespace").Exists() || item.Get("input").Exists() {
			t.Fatal("discarded custom declaration changed an ordinary function")
		}
	}
}

func TestResponsesCustomToolsStreamingLateNameAndInterleavedOrdinaryTool(t *testing.T) {
	request := []byte(`{"input":[{"type":"additional_tools","tools":[{"type":"custom","name":"exec"},{"type":"function","name":"lookup","parameters":{}}]}]}`)
	lines := []string{
		`{"id":"resp_custom","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_custom","function":{"arguments":"{\"input\":\"hello"}}]}}]}`,
		`{"id":"resp_custom","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"exec","arguments":"\\nworld\"}"}},{"index":1,"id":"call_regular","function":{"name":"lookup","arguments":"{}"}}]}}]}`,
		`{"id":"resp_custom","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		`{"id":"resp_custom","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`,
		`[DONE]`,
		`[DONE]`,
	}
	var state any
	counts := map[string]int{}
	var completed gjson.Result
	sequence := int64(0)
	for index, line := range lines {
		for _, chunk := range ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "model", request, nil, []byte("data: "+line), &state) {
			event, data := parseOpenAIResponsesSSEEvent(t, chunk)
			if data.Get("sequence_number").Int() <= sequence {
				t.Fatal("event sequence is not increasing")
			}
			sequence = data.Get("sequence_number").Int()
			counts[event]++
			if index == 0 && event == "response.output_item.added" {
				t.Fatal("tool was announced before its name arrived")
			}
			if event == "response.function_call_arguments.delta" && data.Get("item_id").String() != "fc_call_regular" {
				t.Fatal("custom JSON wrapper leaked as a function delta")
			}
			if event == "response.custom_tool_call_input.done" && data.Get("input").String() != "hello\nworld" {
				t.Fatal("custom input fragments were not decoded")
			}
			if event == "response.completed" {
				completed = data
			}
		}
	}
	if counts["response.output_item.added"] != 2 || counts["response.output_item.done"] != 2 || counts["response.custom_tool_call_input.done"] != 1 || counts["response.completed"] != 1 {
		t.Fatal("tool event chain was missing or duplicated")
	}
	if completed.Get("response.output.0.type").String() != "custom_tool_call" || completed.Get("response.output.0.input").String() != "hello\nworld" || completed.Get("response.output.1.type").String() != "function_call" || completed.Get("response.usage.total_tokens").Int() != 7 {
		t.Fatal("completed response disagrees with emitted tools or late usage")
	}
}

func TestResponsesSingleCustomToolMissingIdentity(t *testing.T) {
	request := []byte(`{"tools":[{"type":"custom","name":"exec"}],"input":[{"type":"additional_tools","tools":[{"type":"custom","name":"exec"}]}]}`)
	var state any
	var customItems []gjson.Result
	for _, line := range []string{`{"id":"resp_missing","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"input\":\"run\"}"}}]},"finish_reason":"tool_calls"}]}`, `[DONE]`} {
		for _, chunk := range ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "model", request, nil, []byte(line), &state) {
			event, data := parseOpenAIResponsesSSEEvent(t, chunk)
			if event == "response.output_item.done" {
				customItems = append(customItems, data.Get("item"))
			}
		}
	}
	if len(customItems) != 1 || customItems[0].Get("name").String() != "exec" || customItems[0].Get("input").String() != "run" || !strings.HasPrefix(customItems[0].Get("call_id").String(), "call_") {
		t.Fatal("single custom tool identity fallback failed")
	}
}

func TestResponsesCustomToolsFirstChunkUsageAndOriginalDefinitions(t *testing.T) {
	request := []byte(`{"tools":[{"type":"custom","name":"exec"}]}`)
	translated := ConvertOpenAIResponsesRequestToOpenAIChatCompletions("model", request, true)
	var state any
	chunk := []byte(`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"input\":\"run\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}`)
	ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "model", request, translated, chunk, &state)
	out := ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "model", request, translated, []byte("[DONE]"), &state)
	if len(out) != 1 {
		t.Fatal("missing completed response")
	}
	_, data := parseOpenAIResponsesSSEEvent(t, out[0])
	if data.Get("response.usage.total_tokens").Int() != 5 || data.Get("response.tools.0.type").String() != "custom" || data.Get("response.id").String() == "" {
		t.Fatal("first-chunk usage, source definitions, or generated response identity was lost")
	}
}

func TestResponsesToolStreamRejectsMalformedIndices(t *testing.T) {
	request := []byte(`{"tools":[{"type":"function","name":"read","parameters":{}}]}`)
	for _, scope := range []string{"choice", "tool"} {
		for _, invalid := range []string{"-1", "0.5", `"0"`, "null", "9223372036854775808"} {
			t.Run(scope+"/"+invalid, func(t *testing.T) {
				bad := `{"choices":[{"index":INDEX,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"bad"}}]}}]}`
				if scope == "tool" {
					bad = `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":INDEX,"function":{"arguments":"bad"}}]}}]}`
				}
				var state any
				var completed gjson.Result
				for _, line := range []string{
					`{"id":"reply_indices","choices":[{"delta":{"tool_calls":[{"id":"call_a","function":{"name":"read","arguments":"{\"v\":"}}]}}]}`,
					strings.ReplaceAll(bad, "INDEX", invalid),
					`{"choices":[{"delta":{"tool_calls":[{"function":{"arguments":"1}"}}]},"finish_reason":"tool_calls"}]}`,
					`[DONE]`,
				} {
					for _, chunk := range ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "model", request, nil, []byte(line), &state) {
						event, data := parseOpenAIResponsesSSEEvent(t, chunk)
						if event == "response.completed" {
							completed = data.Get("response")
						}
					}
				}
				if completed.Get("output.#").Int() != 1 || completed.Get("output.0.arguments").String() != `{"v":1}` {
					t.Fatal("malformed index attached arguments to another call or invented a call")
				}
			})
		}
	}
}
