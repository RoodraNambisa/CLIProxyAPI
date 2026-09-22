package config

import "gopkg.in/yaml.v3"

func isCodexResponseGuardYAMLPath(path []string) bool {
	return len(path) >= 2 && path[0] == "codex" && path[1] == "response-guard"
}

func pruneMissingResponseGuardKeys(dst, src *yaml.Node) {
	for _, key := range []string{"mode", "match-model", "allowed-returned-models", "length-mode", "lengths", "missing-model", "missing-state", "on-reject", "clear-affinity", "late-mismatch", "error-type", "error-code", "error-message", "rules", "enabled", "model-overrides"} {
		if findMapKeyIndex(src, key) < 0 {
			removeMapKey(dst, key)
		}
	}
}
