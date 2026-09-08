package auth

import "strings"

// SupportsCodexLive requires the supported OAuth mode and a usable access token.
// API keys and Agent Identity are not implicitly enabled by stored OAuth data.
// The normal background refresh lifecycle may make a credential eligible later.
func SupportsCodexLive(auth *Auth) bool {
	if !SupportsCodexAlphaSearch(auth) || auth.Attributes["api_key"] != "" {
		return false
	}
	token, _ := auth.Metadata["access_token"].(string)
	return strings.TrimSpace(token) != ""
}
