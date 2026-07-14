//go:build windows

package authfileguard

import (
	"path/filepath"
	"strings"
)

func normalizePathKey(path string) string {
	return resolvedPathKey(lexicalPathKey(path))
}

func lexicalPathKey(path string) string {
	key := strings.TrimSpace(path)
	if key == "" {
		return ""
	}
	key = filepath.Clean(key)
	if absolute, err := filepath.Abs(key); err == nil {
		key = absolute
	}
	return normalizeWindowsPathKey(key)
}

func resolvedPathKey(lexicalKey string) string {
	if lexicalKey == "" {
		return ""
	}
	return normalizeWindowsPathKey(resolvePathFromExistingAncestor(lexicalKey))
}

func normalizeWindowsPathKey(key string) string {
	key = strings.ReplaceAll(key, "/", `\`)
	lower := strings.ToLower(key)
	switch {
	case strings.HasPrefix(lower, `\\?\unc\`):
		key = `\\` + key[len(`\\?\UNC\`):]
	case strings.HasPrefix(lower, `\\?\`):
		key = key[len(`\\?\`):]
	}
	return strings.ToLower(filepath.Clean(key))
}
