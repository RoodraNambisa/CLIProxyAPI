package helps

import (
	"bytes"
	"strings"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// PruneCodexAllowedImageTools keeps allowed selections consistent with image
// declaration removal. Direct choices retain the executor's existing policy.
func PruneCodexAllowedImageTools(body []byte) []byte {
	choice := gjson.GetBytes(body, "tool_choice")
	if !strings.EqualFold(strings.TrimSpace(choice.Get("type").String()), "allowed_tools") {
		return body
	}
	for _, path := range []string{"tool_choice.tools", "tool_choice.allowed_tools.tools"} {
		tools := gjson.GetBytes(body, path)
		if !tools.IsArray() {
			continue
		}
		var retained bytes.Buffer
		retained.WriteByte('[')
		count, removed := 0, false
		for _, tool := range tools.Array() {
			if cliproxyauth.ToolChoiceSelectsImageGeneration(tool) {
				removed = true
				continue
			}
			if count > 0 {
				retained.WriteByte(',')
			}
			retained.WriteString(tool.Raw)
			count++
		}
		if !removed {
			continue
		}
		if count == 0 {
			body, _ = sjson.DeleteBytes(body, "tool_choice")
			return body
		}
		retained.WriteByte(']')
		body, _ = sjson.SetRawBytes(body, path, retained.Bytes())
	}
	return body
}
