package helps

import (
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// NormalizeCodexToolSelection removes tool-only controls when no tools remain.
func NormalizeCodexToolSelection(body []byte) []byte {
	tools := gjson.GetBytes(body, "tools")
	if tools.Exists() && tools.IsArray() && len(tools.Array()) > 0 {
		return body
	}
	body, _ = sjson.DeleteBytes(body, "tool_choice")
	body, _ = sjson.DeleteBytes(body, "parallel_tool_calls")
	return body
}
