package helps

import (
	"fmt"
	"math"
	"testing"

	"github.com/tidwall/sjson"
)

func TestGoogleUsageIncludesSeparateToolPrompts(t *testing.T) {
	for _, field := range []string{"toolUsePromptTokenCount", "tool_use_prompt_token_count", "tool_use_tokens", "total_tool_use_tokens", "toolUseTokens", "totalToolUseTokens"} {
		for _, withTotal := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/total=%t", field, withTotal), func(t *testing.T) {
				gemini := field == "toolUsePromptTokenCount" || field == "tool_use_prompt_token_count"
				body := []byte(`{"usage":{"total_input_tokens":10,"total_output_tokens":2,"total_thought_tokens":3,"cached_tokens":4}}`)
				path := "usage."
				if gemini {
					body = []byte(`{"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":2,"thoughtsTokenCount":3,"cachedContentTokenCount":4}}`)
					path = "usageMetadata."
				}
				body, _ = sjson.SetBytes(body, path+field, 5)
				if withTotal {
					totalField := "total_tokens"
					if gemini {
						totalField = "totalTokenCount"
					}
					body, _ = sjson.SetBytes(body, path+totalField, 25)
				}
				detail := ParseInteractionsUsage(body)
				if gemini {
					detail = ParseGeminiUsage(body)
					if wrapped := ParseAntigravityUsage([]byte(`{"response":` + string(body) + `}`)); wrapped != detail {
						t.Fatal("wrapped Gemini usage differs")
					}
				}
				wantTotal := int64(20)
				if withTotal {
					wantTotal = 25
				}
				if detail.InputTokens != 15 || detail.OutputTokens != 5 || detail.ReasoningTokens != 3 || detail.CachedTokens != 4 || detail.TotalTokens != wantTotal {
					t.Fatal("tool prompts were lost, reasoning counted twice, or the reported total changed")
				}
			})
		}
	}
}

func TestGoogleToolPromptInputBoundsAndDefaults(t *testing.T) {
	for _, tc := range []struct {
		fields string
		input  int64
	}{
		{`"promptTokenCount":10`, 10},
		{`"promptTokenCount":10,"toolUsePromptTokenCount":-1`, 10},
		{`"promptTokenCount":10,"toolUsePromptTokenCount":0,"tool_use_prompt_token_count":7`, 10},
		{`"promptTokenCount":9223372036854775807,"toolUsePromptTokenCount":1`, math.MaxInt64},
	} {
		body := []byte(`{"usageMetadata":{` + tc.fields + `}}`)
		detail := ParseGeminiUsage(body)
		if detail.InputTokens != tc.input || detail.TotalTokens != tc.input {
			t.Fatal("tool input changed the default, explicit zero or saturated sum")
		}
	}
}
