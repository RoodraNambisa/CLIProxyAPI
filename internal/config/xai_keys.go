package config

import (
	"fmt"
	"strings"
)

// XAIKey shares the native key model and routing fields with Codex keys.
type XAIKey = CodexKey

func (cfg *Config) ValidateXAIKeys() error {
	if cfg == nil {
		return nil
	}
	for index, key := range cfg.XAIKey {
		if _, errURL := NormalizeXAIBaseURL(key.BaseURL); errURL != nil {
			return fmt.Errorf("xai-api-key[%d].base-url: %w", index, errURL)
		}
	}
	return nil
}

func (cfg *Config) SanitizeXAIKeys() {
	if cfg == nil {
		return
	}
	values := append([]CodexKey(nil), cfg.XAIKey...)
	for i := range values {
		values[i].APIKey = strings.TrimSpace(values[i].APIKey)
		if strings.TrimSpace(values[i].BaseURL) == "" {
			values[i].BaseURL, _ = XAIBaseURLForMode("api")
		}
		if canonical, errURL := NormalizeXAIBaseURL(values[i].BaseURL); errURL == nil {
			values[i].BaseURL = canonical
		}
		values[i].AlphaSearch = false
	}
	temporary := Config{CodexKey: values}
	temporary.SanitizeCodexKeys()
	cfg.XAIKey = temporary.CodexKey
}
