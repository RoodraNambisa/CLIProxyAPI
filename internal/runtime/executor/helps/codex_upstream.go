package helps

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type CodexUpstream = coreauth.CodexUpstream

func ResolveCodexUpstream(auth *coreauth.Auth, cfg *config.Config) CodexUpstream {
	return coreauth.ResolveCodexUpstream(auth, cfg)
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
