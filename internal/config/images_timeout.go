package config

import "fmt"

const MaxImageRequestTimeoutSeconds = 86400

// ValidateRequestTimeouts checks the opt-in logical image budgets independently
// of transport settings. Zero preserves the existing unlimited behavior.
func (cfg ImagesConfig) ValidateRequestTimeouts() error {
	for name, seconds := range map[string]int{
		"images.codex-request-timeout-seconds":       cfg.CodexRequestTimeoutSeconds,
		"images.chatgpt-web.request-timeout-seconds": cfg.ChatGPTWeb.RequestTimeoutSeconds,
	} {
		if seconds < 0 || seconds > MaxImageRequestTimeoutSeconds {
			return fmt.Errorf("%s must be an integer between 0 and %d", name, MaxImageRequestTimeoutSeconds)
		}
	}
	return nil
}
