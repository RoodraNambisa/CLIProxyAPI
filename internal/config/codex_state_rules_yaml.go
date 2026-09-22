package config

import "gopkg.in/yaml.v3"

func isCodexStateRuleYAMLPath(path []string) bool {
	return len(path) >= 3 && path[0] == "codex" && path[1] == "state-override" && path[2] == "rules"
}

// Explicit false/empty values are policy choices, not removable defaults.
// Preserve their ancestors too when the whole section is newly introduced.
func preservesCodexStateRuleYAMLValue(path []string, node *yaml.Node) bool {
	if isCodexResponseGuardYAMLPath(path) || len(path) == 1 && path[0] == "codex" && findMapKeyIndex(node, "response-guard") >= 0 {
		return true
	}
	if isCodexStateRuleYAMLPath(path) || len(path) >= 3 && path[0] == "codex" && path[1] == "state-override" && path[2] == "model-overrides" {
		return true
	}
	if len(path) == 3 && path[0] == "codex" && path[1] == "state-override" {
		for _, key := range codexStateStrategyYAMLKeys {
			if path[2] == key {
				return true
			}
		}
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
	if findMapKeyIndex(node, "rules") >= 0 {
		return true
	}
	for _, key := range codexStateStrategyYAMLKeys {
		if findMapKeyIndex(node, key) >= 0 {
			return true
		}
	}
	return false
}

var codexStateStrategyYAMLKeys = []string{"strategy", "cookie-verify-after-acquire", "cookie-pool-mode", "cookie-pool-group", "cookie-acquisition-model", "cookie-backup-count", "cookie-max-age-seconds", "cookie-refresh-before-seconds", "ttl-seconds", "refresh-before-seconds", "missing-returned-state"}

func pruneMissingCodexStateStrategy(dst, src *yaml.Node) {
	for _, key := range codexStateStrategyYAMLKeys {
		if findMapKeyIndex(src, key) < 0 {
			removeMapKey(dst, key)
		}
	}
}
