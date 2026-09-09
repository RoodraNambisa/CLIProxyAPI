package responses

import (
	"fmt"
	"testing"
)

func TestResponsesDoneWithoutFinishReason(t *testing.T) {
	for _, tc := range []struct {
		name, delta, request string
		complete             bool
	}{
		{"text", `{"content":"answer"}`, `{}`, true},
		{"reasoning", `{"reasoning":"thought"}`, `{}`, true},
		{"tool", `{"tool_calls":[{"index":0,"id":"call","function":{"name":"tool","arguments":"{}"}}]}`, `{}`, true},
		{"custom", `{"tool_calls":[{"index":0,"id":"call","function":{"name":"tool","arguments":"{\"input\":\"run\"}"}}]}`, `{"tools":[{"type":"custom","name":"tool"}]}`, true},
		{"empty tool", `{"tool_calls":[{"index":0,"id":"call","function":{"name":"tool","arguments":""}}]}`, `{}`, false},
		{"partial tool", `{"tool_calls":[{"index":0,"id":"call","function":{"name":"tool","arguments":"{\"x\":"}}]}`, `{}`, false},
		{"unidentified tool", `{"tool_calls":[{"index":0,"id":"call","function":{"arguments":"{}"}}]}`, `{}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var state any
			request := []byte(tc.request)
			ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "", request, nil, []byte(`data: {"id":"done","choices":[{"index":0,"delta":`+tc.delta+`}]}`), &state)
			completed, toolDone := false, false
			for _, chunk := range ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "", request, nil, []byte("data: [DONE]"), &state) {
				kind, event := parseOpenAIResponsesSSEEvent(t, chunk)
				if kind == "response.completed" {
					completed = true
					if event.Get("response.output.#").Int() != 1 {
						t.Fatal("complete output was lost")
					}
				}
				if kind == "response.function_call_arguments.done" || kind == "response.custom_tool_call_input.done" {
					toolDone = true
				}
			}
			if completed != tc.complete || (!tc.complete && toolDone) {
				t.Fatal("DONE incorrectly accepted or discarded a partial tool")
			}
			for _, raw := range []string{"data: [DONE]", fmt.Sprintf(`data: {"choices":[{"index":0,"delta":%s,"finish_reason":"stop"}]}`, tc.delta)} {
				if len(ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "", request, nil, []byte(raw), &state)) != 0 {
					t.Fatal("terminal source accepted late events")
				}
			}
		})
	}
}
