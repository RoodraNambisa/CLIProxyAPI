package authfileguard

import (
	"os"
	"path/filepath"
	"strings"
)

// PathIdentity returns a stable, platform-normalized path identity. Missing
// leaf components are resolved from the nearest existing ancestor.
func PathIdentity(path string) string {
	return normalizePathKey(path)
}

func pathIdentityKeys(path string) []string {
	lexicalKey := pathLexicalKey(path)
	if lexicalKey == "" {
		return nil
	}
	return sortedUniqueKeys([]string{lexicalKey, resolvedPathKey(lexicalKey)})
}

func pathLexicalKey(path string) string {
	path = strings.TrimSpace(path)
	if path == "" || filepath.Clean(path) == "." {
		return ""
	}
	lexicalKey := lexicalPathKey(path)
	if lexicalKey == "" {
		return ""
	}
	return lexicalKey
}

func resolvePathFromExistingAncestor(path string) string {
	candidate := path
	missing := make([]string, 0, 2)
	for {
		resolved, errEval := filepath.EvalSymlinks(candidate)
		if errEval == nil {
			for i := len(missing) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, missing[i])
			}
			return filepath.Clean(resolved)
		}
		if !os.IsNotExist(errEval) {
			return path
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			return path
		}
		missing = append(missing, filepath.Base(candidate))
		candidate = parent
	}
}
