package auth

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

const CodexAlphaSearchAttributeKey = "codex_alpha_search"

// Protocol capabilities are pool membership constraints, evaluated before
// priority, affinity, cooldown waiting and request-capacity reservations.
func credentialSupportsExecutionFormat(auth *Auth, format translator.Format) bool {
	switch format {
	case translator.FormatCodexAlphaSearch:
		return SupportsCodexAlphaSearch(auth)
	case translator.FormatCodexLive:
		return SupportsCodexLive(auth)
	default:
		return true
	}
}

// SupportsCodexAlphaSearch follows the credential source used by the Codex
// executor. API keys require opt-in even when OAuth-looking metadata is present.
// Stored OAuth tokens do not authorize search while another auth mode is active.
// Availability, model access and runtime retirement remain selection concerns.
func SupportsCodexAlphaSearch(auth *Auth) bool {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return false
	}
	for _, key := range []string{"auth_mode", "authMode"} {
		value, exists := auth.Metadata[key]
		if !exists || value == nil {
			continue
		}
		mode, ok := value.(string)
		if !ok {
			return false
		}
		if mode = strings.TrimSpace(mode); mode == "" {
			continue
		}
		if !strings.EqualFold(mode, "oauth") {
			return false
		}
		break
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
