package util

import "strings"

// IsRetiredGeminiCLIPath reports whether path belonged to the removed Gemini CLI integration.
func IsRetiredGeminiCLIPath(path string) bool {
	return strings.HasPrefix(path, "/v1internal:") ||
		path == "/google/callback" ||
		(strings.HasPrefix(path, "/") && strings.HasSuffix(path, "/google/callback"))
}
