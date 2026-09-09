package responses

import (
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestChatResponsesUsageDetailsAndReasoningAliases(t *testing.T) {
	for _, tc := range []struct {
		usage             string
		reasoning, cached int64
	}{
		{``, 0, 0},
		{`{"prompt_tokens":2,"completion_tokens":4,"total_tokens":6}`, 0, 0},
		{`{"prompt_tokens":2,"completion_tokens":4,"total_tokens":6,"completion_tokens_details":{"reasoning_tokens":3}}`, 3, 0},
		{`{"prompt_tokens":2,"completion_tokens":4,"total_tokens":6,"output_tokens_details":{"reasoning_tokens":1},"completion_tokens_details":{"reasoning_tokens":3},"prompt_tokens_details":{"cached_tokens":2}}`, 1, 2},
	} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/usage=%s", stream, tc.usage), func(t *testing.T) {
				usageField := ""
				if tc.usage != "" {
					usageField = `,"usage":` + tc.usage
				}
				var response gjson.Result
				if stream {
					var state any
					ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "fixture", nil, nil, []byte(`data: {"id":"fixture","choices":[{"index":0,"delta":{"content":"answer"},"finish_reason":"stop"}]`+usageField+`}`), &state)
					for _, chunk := range ConvertOpenAIChatCompletionsResponseToOpenAIResponses(t.Context(), "fixture", nil, nil, []byte("data: [DONE]"), &state) {
						kind, event := parseOpenAIResponsesSSEEvent(t, chunk)
						if kind == "response.completed" {
							response = event.Get("response")
						}
					}
				} else {
					response = gjson.ParseBytes(ConvertOpenAIChatCompletionsResponseToOpenAIResponsesNonStream(t.Context(), "fixture", nil, nil, []byte(`{"id":"fixture","choices":[{"message":{"content":"answer"},"finish_reason":"stop"}]`+usageField+`}`), nil))
				}
				usage := response.Get("usage")
				if !response.Exists() || usage.Exists() != (tc.usage != "") {
					t.Fatal("usage was lost or fabricated")
				}
				if tc.usage != "" {
					if usage.Get("output_tokens_details.reasoning_tokens").Type != gjson.Number || usage.Get("output_tokens_details.reasoning_tokens").Int() != tc.reasoning || usage.Get("input_tokens_details.cached_tokens").Type != gjson.Number || usage.Get("input_tokens_details.cached_tokens").Int() != tc.cached || usage.Get("total_tokens").Int() != 6 {
						t.Fatal("usage details or reasoning alias precedence changed")
					}
				}
			})
		}
	}
}
