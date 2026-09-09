package common

import (
	"math"

	"github.com/tidwall/gjson"
)

// GeminiOutputTokens includes thoughts in OpenAI's inclusive output count.
func GeminiOutputTokens(usage gjson.Result) int64 {
	return sumGeminiUsageTokens(usage.Get("candidatesTokenCount").Int(), usage.Get("thoughtsTokenCount").Int())
}

// GeminiInputTokens includes separately reported server-side tool-use prompts.
func GeminiInputTokens(usage gjson.Result) int64 {
	toolUse := usage.Get("toolUsePromptTokenCount")
	if !toolUse.Exists() {
		toolUse = usage.Get("tool_use_prompt_token_count")
	}
	return sumGeminiUsageTokens(usage.Get("promptTokenCount").Int(), toolUse.Int())
}

func sumGeminiUsageTokens(counts ...int64) int64 {
	var total int64
	for _, count := range counts {
		if count <= 0 {
			continue
		}
		if count > math.MaxInt64-total {
			return math.MaxInt64
		}
		total += count
	}
	return total
}
