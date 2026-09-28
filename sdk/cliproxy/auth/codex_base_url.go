package auth

import (
	"fmt"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

// ApplyCodexBaseURLMetadata projects only the explicit override. Global
// defaults are resolved at execution time and must not be saved into a file.
func ApplyCodexBaseURLMetadata(auth *Auth) error {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return nil
	}
	raw, exists := auth.Metadata["base_url"]
	if !exists {
		return nil
	}
	value, ok := raw.(string)
	if raw != nil && !ok {
		return fmt.Errorf("base_url must be a string")
	}
	baseURL, err := config.NormalizeCodexBaseURL(value)
	if err != nil {
		return err
	}
	if baseURL == "" {
		delete(auth.Attributes, "base_url")
		return nil
	}
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	auth.Attributes["base_url"] = baseURL
	return nil
}
