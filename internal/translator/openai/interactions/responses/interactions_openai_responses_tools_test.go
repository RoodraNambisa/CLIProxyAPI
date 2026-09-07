package responses

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

func TestResponsesInteractionsNamespacedToolsAndHistory(t *testing.T) {
	raw := []byte(`{"tools":[{"type":"namespace","name":"editor","children":[{"type":"function","name":"patch","parameters":{"type":"object","properties":{"value":{"default":9007199254740993}},"additionalProperties":false}}]}],"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"editor","tools":[{"type":"function","name":"patch","description":"ignored"},{"type":"function","name":"read"}]}]},{"type":"function_call","namespace":"editor","name":"patch","call_id":"pair","arguments":"{\"value\":1}"},{"type":"function_call_output","call_id":"pair","output":"done"}],"tool_choice":{"type":"function","namespace":"editor","name":"patch"}}`)
	original := bytes.Clone(raw)
	got := ConvertOpenAIResponsesRequestToInteractions("gemini-2.5-flash", raw, false)
	if gjson.GetBytes(got, "tools.#").Int() != 2 || gjson.GetBytes(got, "tools.0.name").String() != "editor__patch" || gjson.GetBytes(got, "tools.1.name").String() != "editor__read" || gjson.GetBytes(got, "tools.0.parameters").Raw != gjson.GetBytes(raw, "tools.0.children.0.parameters").Raw {
		t.Fatal("namespace, additional tools or root schema precedence was lost")
	}
	if gjson.GetBytes(got, "input.0.name").String() != "editor__patch" || gjson.GetBytes(got, "input.1.name").String() != "editor__patch" || gjson.GetBytes(got, "input.0.call_id").String() != "pair" || gjson.GetBytes(got, "input.1.call_id").String() != "pair" || gjson.GetBytes(got, "generation_config.tool_choice.name").String() != "editor__patch" || gjson.GetBytes(got, "generation_config.tool_choice.namespace").Exists() {
		t.Fatal("history, result or choice disagrees with its declaration")
	}
	if !bytes.Equal(raw, original) {
		t.Fatal("conversion mutated the source request")
	}
}

func TestInteractionsResponsesIdentitySurvivesSourceRelease(t *testing.T) {
	original := []byte(`{"tools":[{"type":"namespace","name":"editor","tools":[{"type":"function","name":"patch"}]}]}`)
	step := `{"id":"pair","type":"function_call","name":"editor__patch","arguments":{"value":9007199254740993}}`
	nonstream := ConvertInteractionsResponseToOpenAIResponsesNonStream(t.Context(), "", original, nil, []byte(`{"steps":[`+step+`]}`), nil)
	var state any
	ConvertInteractionsResponseToOpenAIResponses(t.Context(), "", original, nil, []byte(`{"event_type":"interaction.created","interaction":{"id":"result"}}`), &state)
	clear(original)
	var items []gjson.Result
	items = append(items, gjson.GetBytes(nonstream, "output.0"))
	for _, event := range []string{`{"event_type":"step.start","index":0,"step":` + step + `}`, `{"event_type":"step.stop","index":0}`, `{"event_type":"interaction.completed","interaction":{"id":"result"}}`} {
		for _, chunk := range ConvertInteractionsResponseToOpenAIResponses(t.Context(), "", nil, nil, []byte(event), &state) {
			for _, line := range bytes.Split(chunk, []byte("\n")) {
				if !bytes.HasPrefix(line, []byte("data:")) {
					continue
				}
				data := gjson.ParseBytes(bytes.TrimSpace(line[5:]))
				if item := data.Get("item"); item.Get("type").String() == "function_call" {
					items = append(items, item)
				} else if data.Get("type").String() == "response.completed" {
					items = append(items, data.Get("response.output.0"))
				}
			}
		}
	}
	if len(items) != 4 {
		t.Fatalf("identity outputs = %d, want 4", len(items))
	}
	for i, item := range items {
		if item.Get("name").String() != "patch" || item.Get("namespace").String() != "editor" || item.Get("call_id").String() != "pair" {
			t.Fatal("response identity was lost after source release")
		}
		if i != 1 && item.Get("arguments").String() != `{"value":9007199254740993}` {
			t.Fatal("argument precision changed")
		}
	}
}

func TestResponsesInteractionsLegacyToolsAndRootIdentity(t *testing.T) {
	raw := []byte(`{"tools":[{"type":"function","function":{"name":"editor__patch","parameters":{"type":"object"}}},{"type":"namespace","name":"editor","tools":[{"type":"function","name":"patch"}]}],"input":[{"type":"function_call","namespace":"editor","name":"patch","call_id":"pair","arguments":"{}"},{"type":"function_call_output","namespace":"editor","call_id":"pair","output":"done"}]}`)
	got := ConvertOpenAIResponsesRequestToInteractions("", raw, false)
	if gjson.GetBytes(got, "tools.#").Int() != 1 || gjson.GetBytes(got, "tools.0.parameters.type").String() != "object" || gjson.GetBytes(got, "input.1.name").String() != "editor__patch" {
		t.Fatal("legacy schema, duplicate precedence or result name fallback changed")
	}
	for _, wire := range []string{"editor__patch", "unknown__tool"} {
		out := ConvertInteractionsResponseToOpenAIResponsesNonStream(t.Context(), "", raw, nil, []byte(`{"steps":[{"type":"function_call","name":"`+wire+`","id":"pair","arguments":{}}]}`), nil)
		if gjson.GetBytes(out, "output.0.name").String() != wire || gjson.GetBytes(out, "output.0.namespace").Exists() {
			t.Fatal("literal root tool or unknown name was assigned to a namespace")
		}
	}
}
