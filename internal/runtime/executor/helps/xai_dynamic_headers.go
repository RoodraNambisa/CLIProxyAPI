package helps

import (
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"golang.org/x/net/http/httpguts"
)

// ApplyXAIDynamicHeaders is opt-in and uses the frozen downstream envelope.
// Resource requests with no downstream identity omit unresolved templates.
func ApplyXAIDynamicHeaders(headers http.Header, auth *coreauth.Auth, cfg *config.Config, client http.Header, sessionID string) {
	if cfg == nil || !cfg.XAI.DynamicHeaders || headers == nil {
		return
	}
	apply := func(name, value string) {
		value = strings.TrimSpace(value)
		if value == "" {
			return
		}
		if strings.Contains(strings.ToUpper(value), "$CPA-SESSION-ID") {
			if sessionID == "" {
				headers.Del(name)
				return
			}
			for {
				index := strings.Index(strings.ToUpper(value), "$CPA-SESSION-ID")
				if index < 0 {
					break
				}
				value = value[:index] + sessionID + value[index+len("$CPA-SESSION-ID"):]
			}
		} else if strings.HasPrefix(value, "$") {
			source := strings.TrimSpace(strings.TrimPrefix(value, "$"))
			value = client.Get(source)
			if value == "" {
				for key, values := range client {
					if strings.EqualFold(key, source) && len(values) > 0 {
						value = values[0]
						break
					}
				}
			}
		}
		if value == "" || len(value) > 8192 || !httpguts.ValidHeaderFieldValue(value) {
			headers.Del(name)
			return
		}
		headers.Set(name, value)
	}
	for name, value := range cfg.XAI.Headers {
		apply(name, value)
	}
	if auth != nil {
		for key, value := range auth.Attributes {
			if strings.HasPrefix(key, "header:") {
				apply(strings.TrimSpace(strings.TrimPrefix(key, "header:")), value)
			}
		}
	}
}
