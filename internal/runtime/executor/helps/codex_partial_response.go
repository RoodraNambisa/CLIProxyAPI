package helps

import (
	"strings"

	"github.com/tidwall/gjson"
)

// IsCodexPartialResponse distinguishes a valid partial result from a failed or
// malformed terminal. It must not authorize committing replay or tool state.
func IsCodexPartialResponse(payload []byte) bool {
	root := gjson.ParseBytes(payload)
	switch root.Get("type").String() {
	case "response.incomplete", "response.completed", "response.done":
	default:
		return false
	}
	if !gjson.ValidBytes(payload) {
		return false
	}
	response := root.Get("response")
	if !response.IsObject() || !strings.EqualFold(strings.TrimSpace(response.Get("status").String()), "incomplete") || CodexTerminalErrorNode(payload).Exists() || CodexTerminalHTTPStatus(payload) != 0 {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(response.Get("incomplete_details.reason").String())) {
	case "max_tokens", "max_output_tokens", "content_filter":
		return true
	default:
		return false
	}
}
