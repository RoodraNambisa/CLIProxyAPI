package chat_completions

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestCodexCustomToolResponsePreservesNativeAndCompatibleEnvelopes(t *testing.T) {
	name := "custom_" + strings.Repeat("long_name_", 9)
	for _, envelope := range []string{"flat", "native"} {
		t.Run(envelope, func(t *testing.T) {
			request := []byte(`{"tools":[{"type":"custom","name":"NAME"}],"messages":[]}`)
			namePath := "tools.0.name"
			if envelope == "native" {
				request = []byte(`{"tools":[{"type":"custom","custom":{}}],"messages":[]}`)
				namePath = "tools.0.custom.name"
			}
			request, _ = sjson.SetBytes(request, namePath, name)
			translated := ConvertOpenAIRequestToCodex("gpt-5.4", request, false)
			response := []byte(`{"type":"response.completed","response":{"status":"completed","output":[{"type":"custom_tool_call","call_id":"call_custom","name":"NAME","input":"free form"}]}}`)
			response, _ = sjson.SetBytes(response, "response.output.0.name", gjson.GetBytes(translated, "tools.0.name").String())
			client := ConvertCodexResponseToOpenAINonStream(t.Context(), "gpt-5.4", request, translated, response, nil)
			call := gjson.GetBytes(client, "choices.0.message.tool_calls.0")
			nameField, inputField, kind := "function.name", "function.arguments", "function"
			if envelope == "native" {
				nameField, inputField, kind = "custom.name", "custom.input", "custom"
			}
			if call.Get("type").String() != kind || call.Get("id").String() != "call_custom" || call.Get(nameField).String() != name || call.Get(inputField).String() != "free form" {
				t.Fatal("custom response lost its declaration shape, original name or freeform input")
			}
		})
	}
}
