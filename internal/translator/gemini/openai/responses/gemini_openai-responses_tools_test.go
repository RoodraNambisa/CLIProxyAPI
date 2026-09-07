package responses

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestGeminiResponsesRestoreToolIdentityAfterSourceRelease(t *testing.T) {
	original := []byte(`{"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"function","name":"patch/file"}]}]}]}`)
	var state any
	ConvertGeminiResponseToOpenAIResponses(t.Context(), "", original, nil, []byte(`{"candidates":[{"content":{"parts":[]}}]}`), &state)
	clear(original)
	response := []byte(`{"candidates":[{"content":{"parts":[{"functionCall":{"name":"editor__patch_file","args":{"value":9007199254740993}}}]},"finishReason":"STOP"}]}`)
	outputs := ConvertGeminiResponseToOpenAIResponses(t.Context(), "", nil, nil, response, &state)
	var callID string
	count := 0
	for _, output := range outputs {
		event, data := parseSSEEvent(t, output)
		item := data.Get("item")
		if event == "response.completed" {
			item = data.Get("response.output.0")
		}
		if item.Get("type").String() != "function_call" {
			continue
		}
		count++
		if item.Get("namespace").String() != "editor" || item.Get("name").String() != "patch/file" {
			t.Fatal("stream lost the original tool identity")
		}
		if callID == "" {
			callID = item.Get("call_id").String()
		}
		if callID == "" || item.Get("call_id").String() != callID {
			t.Fatal("tool pairing changed between events")
		}
		if event != "response.output_item.added" && item.Get("arguments").String() != `{"value":9007199254740993}` {
			t.Fatal("tool argument precision changed")
		}
	}
	if count != 3 {
		t.Fatalf("identity-bearing events = %d, want 3", count)
	}
}

func TestGeminiResponsesToolIdentityCollisionAndUnknownNames(t *testing.T) {
	for _, tc := range []struct{ tools, wire, name, namespace string }{
		{`[{"type":"function","name":"editor__patch"},{"type":"namespace","name":"editor","tools":[{"type":"function","name":"patch"}]}]`, "editor__patch", "editor__patch", ""},
		{`[{"type":"function","name":"a/b"},{"type":"function","name":"a_b"},{"type":"function","name":"a b"}]`, "a_b", "a_b", ""},
		{`[{"type":"function","name":"` + strings.Repeat("a", 64) + `1"},{"type":"function","name":"` + strings.Repeat("a", 64) + `2"}]`, strings.Repeat("a", 64), strings.Repeat("a", 64), ""},
		{`[{"type":"namespace","name":"editor","tools":[{"type":"function","name":"patch/file"}]}]`, "editor__patch_file", "patch/file", "editor"},
		{`[]`, "unknown__tool", "unknown__tool", ""},
	} {
		raw := []byte(`{"tools":` + tc.tools + `}`)
		response := []byte(`{"candidates":[{"content":{"parts":[{"functionCall":{"name":"` + tc.wire + `","args":{}}}]},"finishReason":"STOP"}]}`)
		got := ConvertGeminiResponseToOpenAIResponsesNonStream(t.Context(), "", raw, nil, response, nil)
		if gjson.GetBytes(got, "output.0.name").String() != tc.name || gjson.GetBytes(got, "output.0.namespace").String() != tc.namespace {
			t.Fatal("ambiguous or unknown tool identity was guessed")
		}
	}
}

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
