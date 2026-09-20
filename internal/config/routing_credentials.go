package config

import (
	"fmt"
	"strings"
	"unicode"
)

func normalizeRoutingCredentials(values []string) ([]string, error) {
	if len(values) > 1024 {
		return nil, fmt.Errorf("at most 1024 credentials are allowed")
	}
	var result []string
	seen := make(map[string]bool, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if len(value) > 512 || strings.ContainsFunc(value, unicode.IsControl) {
			return nil, fmt.Errorf("credential identifiers must be at most 512 bytes without control characters")
		}
		if !seen[value] {
			result = append(result, value)
			seen[value] = true
		}
	}
	return result, nil
}
