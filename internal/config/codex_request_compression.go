package config

import (
	"fmt"
	"strings"
)

// NormalizeCodexRequestCompression keeps outbound compression opt-in.
func NormalizeCodexRequestCompression(value string) (string, error) {
	switch normalized := strings.ToLower(strings.TrimSpace(value)); normalized {
	case "", "off":
		return "off", nil
	case "zstd":
		return normalized, nil
	default:
		return "", fmt.Errorf("codex.upstream-request-compression must be off or zstd")
	}
}
