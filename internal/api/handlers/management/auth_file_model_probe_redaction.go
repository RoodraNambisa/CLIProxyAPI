package management

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func probeRequestID(headers http.Header) string {
	for _, key := range []string{"x-request-id", "x-oai-request-id", "x-grok-req-id", "request-id"} {
		if value := headers.Get(key); value != "" {
			return value
		}
	}
	return ""
}

func probeSafeURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	u.User, u.RawQuery, u.Fragment = nil, "", ""
	return u.String()
}

func probeTruncate(value string, length int) string {
	runes := []rune(value)
	if len(runes) > length {
		return string(runes[:length]) + "…"
	}
	return value
}

func probeSafeText(value string, auth *coreauth.Auth, cfg *config.Config, limit int) string {
	redact := func(secret string) {
		if len(secret) >= 4 {
			value = strings.ReplaceAll(value, secret, "[redacted]")
			if encoded, err := json.Marshal(secret); err == nil {
				value = strings.ReplaceAll(value, string(encoded[1:len(encoded)-1]), "[redacted]")
			}
		}
	}
	if auth != nil {
		for _, key := range []string{"access_token", "refresh_token", "id_token", "api_key", "token", "cookie", "session_cookie", "session_token", "client_secret", "password", helps.XAIIdentitySeedKey} {
			if secret, ok := auth.Metadata[key].(string); ok {
				redact(secret)
			}
			redact(auth.Attributes[key])
		}
		for key, secret := range auth.Attributes {
			if strings.HasPrefix(key, "header:") {
				redact(secret)
			}
		}
		if !strings.EqualFold(auth.ProxyURL, "direct") {
			redact(auth.ProxyURL)
		}
	}
	if cfg != nil {
		if !strings.EqualFold(cfg.ProxyURL, "direct") {
			redact(cfg.ProxyURL)
		}
		for _, secret := range cfg.APIKeys {
			redact(secret)
		}
		for _, secret := range cfg.XAI.Headers {
			redact(secret)
		}
	}
	return probeTruncate(value, limit)
}
