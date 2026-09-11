package helps

import (
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// NormalizeCodexToolSelection removes tool-only controls when no tools remain.
func NormalizeCodexToolSelection(body []byte) []byte {
	if HasCodexToolDeclarations(body) {
		return body
	}
	if util.JSONMayContainAnyField(body, "tool_choice") && gjson.GetBytes(body, "tool_choice").Exists() {
		body, _ = sjson.DeleteBytes(body, "tool_choice")
	}
	if util.JSONMayContainAnyField(body, "parallel_tool_calls") && gjson.GetBytes(body, "parallel_tool_calls").Exists() {
		body, _ = sjson.DeleteBytes(body, "parallel_tool_calls")
	}
	return body
}
