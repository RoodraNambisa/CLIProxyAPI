package chat_completions

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestCodexCustomToolsReplayDefinitionsChoiceAndNames(t *testing.T) {
	name := "custom_" + strings.Repeat("long_name_", 9)
	for _, envelope := range []string{"custom", "function"} {
		request := []byte(`{"tools":[{"type":"custom","name":"NAME","format":{"type":"grammar","syntax":"lark","definition":"start: /.+/"}}],"messages":[{"role":"assistant","tool_calls":[{"id":"call_custom","type":"custom","custom":{"name":"NAME","input":"free form"}}]},{"role":"tool","tool_call_id":"call_custom","content":"done"}],"tool_choice":{"type":"custom","name":"NAME"}}`)
		request = []byte(strings.ReplaceAll(string(request), "NAME", name))
		if envelope == "function" {
			request, _ = sjson.SetBytes(request, "messages.0.tool_calls.0.type", "function")
			request, _ = sjson.SetBytes(request, "messages.0.tool_calls.0.function.name", name)
			request, _ = sjson.SetBytes(request, "messages.0.tool_calls.0.function.arguments", "free form")
			request, _ = sjson.DeleteBytes(request, "messages.0.tool_calls.0.custom")
		}
		out := ConvertOpenAIRequestToCodex("gpt-5.4", request, true)
		short := gjson.GetBytes(out, "tools.0.name").String()
		if len(short) > 64 || short == name {
			t.Fatal("custom name was not shortened")
		}
		if gjson.GetBytes(out, "tools.0.format").Raw != gjson.GetBytes(request, "tools.0.format").Raw {
			t.Fatal("custom grammar changed")
		}
		if gjson.GetBytes(out, "input.0.type").String() != "custom_tool_call" || gjson.GetBytes(out, "input.0.input").String() != "free form" || gjson.GetBytes(out, "input.1.type").String() != "custom_tool_call_output" || gjson.GetBytes(out, "input.1.call_id").String() != "call_custom" {
			t.Fatal("custom replay or result pairing changed")
		}
		if gjson.GetBytes(out, "input.0.name").String() != short || gjson.GetBytes(out, "tool_choice.name").String() != short || gjson.GetBytes(out, "tool_choice.type").String() != "custom" {
			t.Fatal("definition, replay, and choice names disagree")
		}
	}
}

func TestCodexCustomToolNameCollisionKeepsFunctionReplay(t *testing.T) {
	request := []byte(`{"tools":[{"type":"custom","name":"same"},{"type":"function","function":{"name":"same","parameters":{}}}],"messages":[{"role":"assistant","tool_calls":[{"id":"call_x","type":"function","function":{"name":"same","arguments":"{}"}}]},{"role":"tool","tool_call_id":"call_x","content":"ok"}]}`)
	out := ConvertOpenAIRequestToCodex("gpt-5.4", request, false)
	if gjson.GetBytes(out, "input.0.type").String() != "function_call" || gjson.GetBytes(out, "input.1.type").String() != "function_call_output" {
		t.Fatal("ambiguous definition converted an explicit function envelope to custom")
	}
}

func TestCodexNativeCustomToolDefinitionAndChoice(t *testing.T) {
	request := []byte(`{"tools":[{"type":"custom","custom":{"name":"code_exec","description":"run code","format":{"type":"grammar","syntax":"lark","definition":"start: /.+/"}}}],"tool_choice":{"type":"custom","custom":{"name":"code_exec"}},"messages":[{"role":"assistant","tool_calls":[{"id":"call_x","type":"custom","custom":{"name":"code_exec","input":"print(1)"}}]},{"role":"tool","tool_call_id":"call_x","content":"1"}]}`)
	out := ConvertOpenAIRequestToCodex("gpt-5.4", request, true)
	tool := gjson.GetBytes(out, "tools.0")
	if tool.Get("custom").Exists() || tool.Get("name").String() != "code_exec" || tool.Get("description").String() != "run code" || tool.Get("format").Raw != gjson.GetBytes(request, "tools.0.custom.format").Raw {
		t.Fatal("native custom definition was not flattened without changing grammar")
	}
	if gjson.GetBytes(out, "tool_choice.type").String() != "custom" || gjson.GetBytes(out, "tool_choice.name").String() != "code_exec" {
		t.Fatal("native custom tool choice lost its target")
	}
	if gjson.GetBytes(out, "input.0.input").String() != "print(1)" || gjson.GetBytes(out, "input.1.type").String() != "custom_tool_call_output" || gjson.GetBytes(out, "input.1.call_id").String() != "call_x" {
		t.Fatal("native custom history lost its input or result pairing")
	}
}
