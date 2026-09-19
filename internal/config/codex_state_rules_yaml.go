package config

import "gopkg.in/yaml.v3"

func isCodexStateRuleYAMLPath(path []string) bool {
	return len(path) >= 3 && path[0] == "codex" && path[1] == "state-override" && path[2] == "rules"
}

// Explicit false/empty values are policy choices, not removable defaults.
// Preserve their ancestors too when the whole section is newly introduced.
func preservesCodexStateRuleYAMLValue(path []string, node *yaml.Node) bool {
	if isCodexStateRuleYAMLPath(path) {
		return true
	}
	if node == nil || len(path) == 0 || path[0] != "codex" {
		return false
	}
	if len(path) == 1 {
		index := findMapKeyIndex(node, "state-override")
		if index < 0 {
			return false
		}
		node = node.Content[index+1]
	} else if len(path) != 2 || path[1] != "state-override" {
		return false
	}
	return findMapKeyIndex(node, "rules") >= 0
}
