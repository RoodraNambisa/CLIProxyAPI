package helps

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type CodexUpstream struct {
	BaseURL string
	Source  string
}

// ResolveCodexUpstream keeps explicit credential/API-key addresses authoritative.
// The provider default applies only to OAuth credentials, including agent identities.
func ResolveCodexUpstream(auth *coreauth.Auth, cfg *config.Config) CodexUpstream {
	apiKey := codexUsesAPIKeyAuth(auth)
	if auth != nil {
		for _, value := range []string{auth.Attributes["base_url"], codexMetadataString(auth, "base_url")} {
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

func codexUsesAPIKeyAuth(auth *coreauth.Auth) bool {
	if auth == nil {
		return false
	}
	if strings.TrimSpace(auth.Attributes["api_key"]) != "" {
		return true
	}
	for _, kind := range []string{auth.Attributes["auth_kind"], codexMetadataString(auth, "auth_kind")} {
		switch strings.ToLower(strings.TrimSpace(kind)) {
		case "apikey", "api_key":
			return true
		}
	}
	return false
}

func codexMetadataString(auth *coreauth.Auth, key string) string {
	value, _ := auth.Metadata[key].(string)
	return value
}
