package helps

import "github.com/tidwall/gjson"

// CodexMultiAgentDeclaresTools distinguishes retained incremental context from a
// new tool set. Unrelated additional_tools do not replace collaboration tools.
func CodexMultiAgentDeclaresTools(payload []byte) bool {
	if gjson.GetBytes(payload, "tools").Exists() {
		return true
	}
	scan := scanCodexCollaborationTools(payload)
	return len(scan.messagePaths) > 0 || scan.conflict
}
