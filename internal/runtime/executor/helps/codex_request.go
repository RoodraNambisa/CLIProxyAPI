package helps

import "github.com/tidwall/sjson"

// NormalizeCodexToolSelection removes tool-only controls when no tools remain.
func NormalizeCodexToolSelection(body []byte) []byte {
	if HasCodexToolDeclarations(body) {
		return body
	}
	body, _ = sjson.DeleteBytes(body, "tool_choice")
	body, _ = sjson.DeleteBytes(body, "parallel_tool_calls")
	return body
}
