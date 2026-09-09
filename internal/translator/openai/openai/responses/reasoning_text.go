package responses

import "github.com/tidwall/gjson"

func responsesReasoningText(message gjson.Result) string {
	for _, field := range []string{"reasoning_content", "reasoning"} {
		if value := message.Get(field); value.Type == gjson.String && value.String() != "" {
			return value.String()
		}
	}
	return ""
}
