package helps

import (
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// NormalizeCodexToolSelection removes tool-only controls when no tools remain.
func NormalizeCodexToolSelection(body []byte) []byte {
	if HasCodexToolDeclarations(body) {
		return body
	}
	if gjson.GetBytes(body, "tool_choice").Exists() {
		body, _ = sjson.DeleteBytes(body, "tool_choice")
	}
	if gjson.GetBytes(body, "parallel_tool_calls").Exists() {
		body, _ = sjson.DeleteBytes(body, "parallel_tool_calls")
	}
	return body
}
