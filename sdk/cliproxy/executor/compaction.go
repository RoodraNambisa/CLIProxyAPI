package executor

import (
	"bytes"
	"strings"

	"github.com/tidwall/gjson"
)

// IsResponsesCompaction recognizes the dedicated endpoint or a final v2 trigger.
// Quoted text, nested tool output, and earlier history items are not triggers.
func IsResponsesCompaction(payload []byte, alt string) bool {
	if strings.EqualFold(strings.Trim(strings.TrimSpace(alt), "/"), "responses/compact") {
		return true
	}
	if !bytes.Contains(payload, []byte("compaction_trigger")) && !bytes.Contains(payload, []byte(`\u`)) {
		return false
	}
	input := gjson.GetBytes(payload, "input")
	if !input.IsArray() || !gjson.ValidBytes(payload) {
		return false
	}
	items := input.Array()
	return len(items) > 0 && items[len(items)-1].Get("type").String() == "compaction_trigger"
}
