package responses

import (
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
