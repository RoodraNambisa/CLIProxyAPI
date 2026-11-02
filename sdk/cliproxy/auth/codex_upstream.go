package auth

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

type CodexUpstream struct {
	BaseURL string
	Source  string
}

// ResolveCodexUpstream keeps explicit credential/API-key addresses authoritative.
// The provider default applies only to OAuth credentials, including agent identities.
func ResolveCodexUpstream(auth *Auth, cfg *config.Config) CodexUpstream {
	apiKey := codexUpstreamUsesAPIKey(auth)
	if auth != nil {
		for _, value := range []string{auth.Attributes["base_url"], chatGPTWebIdentityMetadataString(auth.Metadata, "base_url")} {
			if value = strings.TrimSpace(value); value != "" {
				// API-key Alpha Search URLs may contain a query ending in '/'.
				if !apiKey {
					value = strings.TrimRight(value, "/")
				}
				return CodexUpstream{BaseURL: value, Source: "credential"}
			}
		}
	}
	if !apiKey && cfg != nil {
		if baseURL := strings.TrimRight(strings.TrimSpace(cfg.Codex.BaseURL), "/"); baseURL != "" {
			return CodexUpstream{BaseURL: baseURL, Source: "global"}
		}
	}
	return CodexUpstream{BaseURL: config.DefaultCodexBaseURL, Source: "official"}
}

func codexUpstreamUsesAPIKey(auth *Auth) bool {
	if auth == nil {
		return false
	}
	if strings.TrimSpace(auth.Attributes["api_key"]) != "" {
		return true
	}
	for _, kind := range []string{auth.Attributes["auth_kind"], chatGPTWebIdentityMetadataString(auth.Metadata, "auth_kind")} {
		switch strings.ToLower(strings.TrimSpace(kind)) {
		case "apikey", "api_key":
			return true
		}
	}
	return false
}
