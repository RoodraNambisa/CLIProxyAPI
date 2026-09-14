package helps

import (
	"net/url"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type XAIUpstream struct {
	BaseURL string
	Mode    string
	Source  string
}

func xaiAuthField(auth *coreauth.Auth, key string) string {
	if auth == nil {
		return ""
	}
	if value := strings.TrimSpace(auth.Attributes[key]); value != "" {
		return value
	}
	value, _ := auth.Metadata[key].(string)
	return strings.TrimSpace(value)
}

// ResolveXAIUpstream preserves an explicit base_url before consulting the global
// default. Authentication type and legacy boolean fields never override a URL.
func ResolveXAIUpstream(auth *coreauth.Auth, cfg *config.Config) XAIUpstream {
	baseURL := strings.TrimRight(xaiAuthField(auth, "base_url"), "/")
	if normalized, err := config.NormalizeXAIBaseURL(baseURL); err == nil {
		baseURL = normalized
	}
	if baseURL != "" {
		return XAIUpstream{BaseURL: baseURL, Mode: xaiBaseURLMode(baseURL), Source: "credential"}
	}
	mode := config.DefaultXAIBaseURLMode
	if cfg != nil && strings.TrimSpace(cfg.XAI.DefaultBaseURLMode) != "" {
		mode = strings.ToLower(strings.TrimSpace(cfg.XAI.DefaultBaseURLMode))
	}
	baseURL, ok := config.XAIBaseURLForMode(mode)
	if !ok {
		mode = config.DefaultXAIBaseURLMode
		baseURL, _ = config.XAIBaseURLForMode(mode)
	}
	return XAIUpstream{BaseURL: baseURL, Mode: mode, Source: "global"}
}

func xaiBaseURLMode(baseURL string) string {
	for _, mode := range []string{"cli", "api", "us-east-1", "us-west-2", "eu-west-1"} {
		if candidate, _ := config.XAIBaseURLForMode(mode); candidate == baseURL {
			return mode
		}
	}
	return "custom"
}

// XAIAPIOnlyBaseURL keeps transports unsupported by the CLI gateway on the API,
// while preserving explicitly selected API regions and custom relays.
func XAIAPIOnlyBaseURL(auth *coreauth.Auth, cfg *config.Config) string {
	baseURL := ResolveXAIUpstream(auth, cfg).BaseURL
	if parsed, err := url.Parse(baseURL); err == nil && strings.EqualFold(parsed.Hostname(), "cli-chat-proxy.grok.com") {
		baseURL, _ = config.XAIBaseURLForMode("api")
	}
	return baseURL
}
