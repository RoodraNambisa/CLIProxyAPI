package responses

import (
	"strings"

	"github.com/tidwall/gjson"
)

func combineResponsesInputReasoning(existing, incoming string) string {
	left, right := strings.TrimSpace(existing), strings.TrimSpace(incoming)
	if right == "" || left == right {
		return existing
	}
	if left == "" {
		return incoming
	}
	return existing + "\n\n" + incoming
}

func responsesInputReasoningString(value gjson.Result) string {
	if value.Type == gjson.String {
		return value.String()
	}
	return ""
}

func responsesInputReasoningText(item gjson.Result) string {
	var text string
	// Only explicit plaintext fields can be carried into Chat reasoning_content.
	// encrypted_content is opaque and must never be interpreted as text here.
	for _, field := range []string{"summary", "content"} {
		parts := item.Get(field)
		if !parts.IsArray() {
			continue
		}
		for _, part := range parts.Array() {
			switch part.Get("type").String() {
			case "summary_text", "reasoning_text", "text":
				text = combineResponsesInputReasoning(text, responsesInputReasoningString(part.Get("text")))
			}
		}
	}
	return text
}
