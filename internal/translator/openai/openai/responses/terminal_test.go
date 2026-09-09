package responses

import (
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestResponsesNonStreamIncompleteStates(t *testing.T) {
	for _, reason := range []string{"stop", "tool_calls", "length", "max_tokens", "content_filter", ""} {
		for _, kind := range []string{"text", "reasoning", "function", "custom"} {
			t.Run(reason+"/"+kind, func(t *testing.T) {
				message := `{"content":"partial"}`
				if kind == "reasoning" {
					message = `{"reasoning":"partial"}`
				}
				if kind == "function" || kind == "custom" {
					message = `{"tool_calls":[{"id":"paired","function":{"name":"tool","arguments":""}}]}`
				}
				request := []byte(`{"tools":[{"type":"` + kind + `","name":"tool"}]}`)
				raw := []byte(fmt.Sprintf(`{"id":"limited","choices":[{"index":0,"message":%s,"finish_reason":%q}]}`, message, reason))
				result := gjson.ParseBytes(ConvertOpenAIChatCompletionsResponseToOpenAIResponsesNonStream(t.Context(), "", request, nil, raw, nil))
				status, wantReason := "completed", ""
				switch reason {
				case "length", "max_tokens":
					status, wantReason = "incomplete", "max_output_tokens"
				case "content_filter":
					status, wantReason = "incomplete", "content_filter"
				}
				if result.Get("status").String() != status || result.Get("output.0.status").String() != status || result.Get("incomplete_details.reason").String() != wantReason {
					t.Fatal("response or output item falsely reports completion")
				}
				if kind == "function" {
					wantArgs := ""
					if status == "completed" {
						wantArgs = "{}"
					}
					if result.Get("output.0.arguments").String() != wantArgs {
						t.Fatal("truncated tool received synthetic arguments")
					}
				}
			})
		}
	}
}

func TestResponsesNonStreamIncompleteIsScopedToChoice(t *testing.T) {
	raw := []byte(`{"choices":[{"index":0,"message":{"content":"first"},"finish_reason":"stop"},{"index":1,"message":{"tool_calls":[{"id":"partial","function":{"name":"tool","arguments":"{\"x\":"}}]},"finish_reason":"length"},{"index":2,"message":{"content":"last"},"finish_reason":"stop"}]}`)
	result := gjson.ParseBytes(ConvertOpenAIChatCompletionsResponseToOpenAIResponsesNonStream(t.Context(), "", nil, nil, raw, nil))
	if result.Get("status").String() != "incomplete" || result.Get("output.0.status").String() != "completed" || result.Get("output.1.status").String() != "incomplete" || result.Get("output.2.status").String() != "completed" || result.Get("output.1.arguments").String() != `{"x":` {
		t.Fatal("one choice's finish reason changed other choices or lost partial arguments")
	}
	for _, raw := range []string{`{"choices":[]}`, `{"choices":[{"message":{},"finish_reason":"length"}]}`} {
		result := gjson.ParseBytes(ConvertOpenAIChatCompletionsResponseToOpenAIResponsesNonStream(t.Context(), "", nil, nil, []byte(raw), nil))
		if result.Get("output").Raw != "[]" {
			t.Fatal("empty output was omitted")
		}
	}
}
