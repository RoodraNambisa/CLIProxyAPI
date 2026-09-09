package claude

import "github.com/tidwall/gjson"

// alignCodexClaudeToolResults only reorders complete, unambiguous pairs. IDs
// and all non-result content retain their original values and relative order.
func alignCodexClaudeToolResults(parts []gjson.Result, ids []string) ([]gjson.Result, bool) {
	if len(ids) == 0 {
		return parts, false
	}
	positions := make(map[string]int, len(ids))
	for index, id := range ids {
		if _, duplicate := positions[id]; id == "" || duplicate {
			return parts, false
		}
		positions[id] = index
	}
	ordered := make([]gjson.Result, len(ids), len(ids)+len(parts))
	var other []gjson.Result
	matched := 0
	for _, part := range parts {
		if part.Get("type").String() != "tool_result" {
			other = append(other, part)
			continue
		}
		index, exists := positions[part.Get("tool_use_id").String()]
		if !exists || ordered[index].Exists() {
			return parts, false
		}
		ordered[index] = part
		matched++
	}
	if matched != len(ids) {
		return parts, false
	}
	return append(ordered, other...), true
}
