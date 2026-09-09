package chat_completions

import (
	"strings"

	"github.com/tidwall/gjson"
)

func codexOpenAIFinishReason(event gjson.Result, hasToolCall bool) (string, string) {
	if event.Get("type").String() == "response.incomplete" || strings.EqualFold(strings.TrimSpace(event.Get("response.status").String()), "incomplete") {
		reason := event.Get("response.incomplete_details.reason").String()
		switch strings.ToLower(strings.TrimSpace(reason)) {
		case "max_tokens", "max_output_tokens":
			return "length", reason
		case "content_filter":
			return "content_filter", reason
		default:
			return "stop", reason
		}
	}
	if hasToolCall {
		return "tool_calls", "tool_calls"
	}
	return "stop", "stop"
}
