package responses

import (
	"fmt"
	"math"
	"testing"

	agi "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/antigravity/openai/chat-completions"
	chat "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/gemini/openai/chat-completions"
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

func TestGeminiOpenAIInclusiveOutputUsage(t *testing.T) {
	for _, tc := range []struct {
		name, counts string
		output       int64
	}{
		{"content-and-thoughts", `"candidatesTokenCount":3,"thoughtsTokenCount":7`, 10},
		{"content-only", `"candidatesTokenCount":3`, 3},
		{"thoughts-only", `"thoughtsTokenCount":7`, 7},
		{"zero", `"candidatesTokenCount":0,"thoughtsTokenCount":0`, 0},
		{"negative-content", `"candidatesTokenCount":-1,"thoughtsTokenCount":7`, 7},
		{"negative-thoughts", `"candidatesTokenCount":3,"thoughtsTokenCount":-1`, 3},
		{"overflow", `"candidatesTokenCount":9223372036854775807,"thoughtsTokenCount":7`, math.MaxInt64},
	} {
		for _, format := range []string{"responses", "wrapped-responses", "chat", "antigravity-chat"} {
			for _, stream := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/stream=%t", tc.name, format, stream), func(t *testing.T) {
					body := []byte(`{"responseId":"fixture","candidates":[{"content":{"role":"model","parts":[{"text":"answer"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":2,"toolUsePromptTokenCount":5,"cachedContentTokenCount":1,"totalTokenCount":12,` + tc.counts + `}}`)
					if format == "wrapped-responses" || format == "antigravity-chat" {
						body = append(append([]byte(`{"response":`), body...), '}')
					}
					var state any
					var response gjson.Result
					if format == "chat" || format == "antigravity-chat" {
						convertStream, convertNonStream := chat.ConvertGeminiResponseToOpenAI, chat.ConvertGeminiResponseToOpenAINonStream
						if format == "antigravity-chat" {
							convertStream, convertNonStream = agi.ConvertAntigravityResponseToOpenAI, agi.ConvertAntigravityResponseToOpenAINonStream
						}
						if stream {
							for _, chunk := range convertStream(t.Context(), "fixture", nil, nil, body, &state) {
								if value := gjson.ParseBytes(chunk); value.Get("usage").Exists() {
									response = value
								}
							}
						} else {
							response = gjson.ParseBytes(convertNonStream(t.Context(), "fixture", nil, nil, body, nil))
						}
					} else if stream {
						for _, chunk := range ConvertGeminiResponseToOpenAIResponses(t.Context(), "fixture", nil, nil, body, &state) {
							kind, event := parseSSEEvent(t, chunk)
							if kind == "response.completed" {
								response = event.Get("response")
							}
						}
					} else {
						response = gjson.ParseBytes(ConvertGeminiResponseToOpenAIResponsesNonStream(t.Context(), "fixture", nil, nil, body, nil))
					}
					inputField, outputField, cacheField := "input_tokens", "output_tokens", "input_tokens_details.cached_tokens"
					if format == "chat" || format == "antigravity-chat" {
						inputField, outputField, cacheField = "prompt_tokens", "completion_tokens", "prompt_tokens_details.cached_tokens"
					}
					usage := response.Get("usage")
					if !usage.Exists() || usage.Get(inputField).Int() != 7 || usage.Get(outputField).Int() != tc.output || usage.Get("total_tokens").Int() != 12 || usage.Get(cacheField).Int() != 1 {
						t.Fatal("inclusive output count or original usage totals were not preserved")
					}
				})
			}
		}
	}
}
