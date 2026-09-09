package responses

import "strings"

func claudeResponsesTerminalState(stopReason string) (event, status string, details []byte) {
	if strings.EqualFold(strings.TrimSpace(stopReason), "max_tokens") {
		return "response.incomplete", "incomplete", []byte(`{"reason":"max_output_tokens"}`)
	}
	return "response.completed", "completed", nil
}
