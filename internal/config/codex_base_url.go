package config

import "fmt"

const DefaultCodexBaseURL = "https://chatgpt.com/backend-api/codex"

// NormalizeCodexBaseURL uses the same endpoint grammar as credential URL edits
// for Grok: HTTP(S), optional path, no embedded credentials, query or fragment.
func NormalizeCodexBaseURL(raw string) (string, error) {
	return NormalizeXAIBaseURL(raw)
}

func (cfg *Config) ValidateCodexBaseURL() error {
	if cfg == nil {
		return nil
	}
	value, err := NormalizeCodexBaseURL(cfg.Codex.BaseURL)
	if err != nil {
		return fmt.Errorf("codex.base-url: %w", err)
	}
	cfg.Codex.BaseURL = value
	return nil
}
