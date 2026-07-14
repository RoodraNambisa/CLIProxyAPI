//go:build !windows

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
	return filepath.Clean(key)
}

func resolvedPathKey(lexicalKey string) string {
	if lexicalKey == "" {
		return ""
	}
	return resolvePathFromExistingAncestor(lexicalKey)
}
