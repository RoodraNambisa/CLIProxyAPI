package responses

import (
	"fmt"
	"testing"

	"github.com/tidwall/gjson"
)

func TestGeminiResponsesUsageDetailsAcrossModes(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			for _, withUsage := range []bool{false, true} {
				t.Run(fmt.Sprintf("wrapped=%t/stream=%t/usage=%t", wrapped, stream, withUsage), func(t *testing.T) {
					usageField := ""
					if withUsage {
						usageField = `,"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":1,"totalTokenCount":3}`
					}
					body := []byte(`{"responseId":"fixture","candidates":[{"content":{"role":"model","parts":[{"text":"answer"}]},"finishReason":"STOP"}]` + usageField + `}`)
					if wrapped {
						body = append(append([]byte(`{"response":`), body...), '}')
					}
					var response gjson.Result
					if stream {
						var state any
						for _, chunk := range ConvertGeminiResponseToOpenAIResponses(t.Context(), "fixture", nil, nil, body, &state) {
							kind, event := parseSSEEvent(t, chunk)
							if kind == "response.completed" {
								response = event.Get("response")
							}
						}
					} else {
						response = gjson.ParseBytes(ConvertGeminiResponseToOpenAIResponsesNonStream(t.Context(), "fixture", nil, nil, body, nil))
					}
					usage := response.Get("usage")
					if !response.Exists() || usage.Exists() != withUsage {
						t.Fatal("usage was lost or fabricated")
					}
					if withUsage && (usage.Get("input_tokens_details.cached_tokens").Type != gjson.Number || usage.Get("output_tokens_details.reasoning_tokens").Type != gjson.Number || usage.Get("output_tokens_details.reasoning_tokens").Int() != 0 || usage.Get("total_tokens").Int() != 3) {
						t.Fatal("missing Gemini details were not represented consistently")
					}
				})
			}
		}
	}
}
