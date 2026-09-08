package auth

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

const CodexAlphaSearchAttributeKey = "codex_alpha_search"

// Protocol capabilities are pool membership constraints, evaluated before
// priority, affinity, cooldown waiting and request-capacity reservations.
func credentialSupportsExecutionFormat(auth *Auth, format translator.Format) bool {
	return format != translator.FormatCodexAlphaSearch || SupportsCodexAlphaSearch(auth)
}

// SupportsCodexAlphaSearch follows the credential source used by the Codex
// executor. API keys require opt-in even when OAuth-looking metadata is present.
// Availability, model access and runtime retirement remain selection concerns.
func SupportsCodexAlphaSearch(auth *Auth) bool {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return false
	}
	if auth.Attributes["api_key"] != "" {
		return strings.EqualFold(strings.TrimSpace(auth.Attributes[CodexAlphaSearchAttributeKey]), "true")
	}
	for _, name := range []string{"access_token", "refresh_token"} {
		if value, ok := auth.Metadata[name].(string); ok && strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}
