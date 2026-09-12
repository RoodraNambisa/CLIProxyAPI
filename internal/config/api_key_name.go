package config

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

const MaxAPIKeyNameLength = 100

// NormalizeAPIKeyName keeps labels optional and safe for single-line controls.
func NormalizeAPIKeyName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if !utf8.ValidString(name) || utf8.RuneCountInString(name) > MaxAPIKeyNameLength || strings.ContainsFunc(name, unicode.IsControl) {
		return "", fmt.Errorf("api-key-groups.name must contain at most %d characters and no control characters", MaxAPIKeyNameLength)
	}
	return name, nil
}
