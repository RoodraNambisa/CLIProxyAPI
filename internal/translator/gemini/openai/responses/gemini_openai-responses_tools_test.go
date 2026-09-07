package responses

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

func TestGeminiResponsesNamespacedDeclarationsHistoryAndSelection(t *testing.T) {
	raw := []byte(`{"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","description":"Keep","parameters":{"type":"object","properties":{"value":{"type":"integer","default":9007199254740993}},"additionalProperties":false}}]}],"input":[{"type":"function_call","namespace":"collaboration","name":"spawn_agent","call_id":"pair","arguments":"{\"value\":1}"},{"type":"function_call_output","call_id":"pair","output":"done"},{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"function","name":"patch","parameters":{"type":"object"}}]}]}],"tool_choice":{"type":"function","namespace":"collaboration","name":"spawn_agent"}}`)
	original := bytes.Clone(raw)
	got := ConvertOpenAIResponsesRequestToGemini("gemini-2.5-flash", raw, false)
	if gjson.GetBytes(got, "tools.0.functionDeclarations.#").Int() != 2 ||
		gjson.GetBytes(got, "tools.0.functionDeclarations.0.name").String() != "collaboration__spawn_agent" ||
		gjson.GetBytes(got, "tools.0.functionDeclarations.1.name").String() != "editor__patch" ||
		gjson.GetBytes(got, "tools.0.functionDeclarations.0.parametersJsonSchema").Raw != gjson.GetBytes(raw, "tools.0.tools.0.parameters").Raw {
		t.Fatal("namespace or additional tool declarations lost their identity or schema")
	}
	if gjson.GetBytes(got, "contents.0.parts.0.functionCall.name").String() != "collaboration__spawn_agent" ||
		gjson.GetBytes(got, "contents.0.parts.0.functionCall.id").String() != "pair" ||
		gjson.GetBytes(got, "contents.1.parts.0.functionResponse.name").String() != "collaboration__spawn_agent" ||
		gjson.GetBytes(got, "contents.1.parts.0.functionResponse.id").String() != "pair" ||
		gjson.GetBytes(got, "toolConfig.functionCallingConfig.mode").String() != "ANY" ||
		gjson.GetBytes(got, "toolConfig.functionCallingConfig.allowedFunctionNames.0").String() != "collaboration__spawn_agent" {
		t.Fatal("history, pairing or explicit selection disagrees with the declared tool")
	}
	if !bytes.Equal(raw, original) {
		t.Fatal("conversion mutated the source")
	}
}

func TestGeminiResponsesToolChoiceModesAndRootPrecedence(t *testing.T) {
	for _, tc := range []struct{ choice, mode string }{{"auto", "AUTO"}, {"none", "NONE"}, {"required", "ANY"}, {"unknown", ""}} {
		raw := []byte(`{"tool_choice":"` + tc.choice + `","tools":[{"type":"function","name":"editor__patch","description":"first"}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"function","name":"patch","description":"second"}]}]}]}`)
		got := ConvertOpenAIResponsesRequestToGemini("gemini-2.5-flash", raw, false)
		if gjson.GetBytes(got, "tools.0.functionDeclarations.#").Int() != 1 || gjson.GetBytes(got, "tools.0.functionDeclarations.0.description").String() != "first" {
			t.Fatal("additional tools overrode a root declaration")
		}
		if gjson.GetBytes(got, "toolConfig.functionCallingConfig.mode").String() != tc.mode {
			t.Fatal("tool choice mode changed")
		}
	}
}
