package helps

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
)

const (
	CodexAlphaSearchMaxRequestBytes  = 16 << 20
	CodexAlphaSearchMaxResponseBytes = 32 << 20
	CodexAlphaSearchOAuthURL         = "https://chatgpt.com/backend-api/codex/alpha/search"
)

// CodexAlphaSearchRouting contains only the fields needed for local routing.
// Search commands, response items and future fields remain opaque JSON.
type CodexAlphaSearchRouting struct {
	Model     string
	SessionID string
}

func parseCodexAlphaSearchBody(body []byte) (map[string]json.RawMessage, CodexAlphaSearchRouting, error) {
	var routing CodexAlphaSearchRouting
	if len(body) > CodexAlphaSearchMaxRequestBytes {
		return nil, routing, errors.New("Codex search request exceeds 16 MiB")
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil || payload == nil {
		return nil, routing, errors.New("Codex search request must be a JSON object")
	}
	if err := json.Unmarshal(payload["model"], &routing.Model); err != nil || strings.TrimSpace(routing.Model) == "" {
		return nil, routing, errors.New("Codex search model must be a non-empty string")
	}
	if id, exists := payload["id"]; exists && string(id) != "null" {
		if err := json.Unmarshal(id, &routing.SessionID); err != nil {
			return nil, routing, errors.New("Codex search id must be a string")
		}
	}
	return payload, routing, nil
}

// ParseCodexAlphaSearchRouting validates the envelope without retaining its body.
func ParseCodexAlphaSearchRouting(body []byte) (CodexAlphaSearchRouting, error) {
	_, routing, err := parseCodexAlphaSearchBody(body)
	return routing, err
}

// RewriteCodexAlphaSearchBody only changes top-level routing/cache fields.
// In particular, nested models, tool arguments and large numbers are preserved.
func RewriteCodexAlphaSearchBody(body []byte, model string) ([]byte, error) {
	payload, routing, err := parseCodexAlphaSearchBody(body)
	if err != nil {
		return nil, err
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return nil, errors.New("Codex search upstream model is empty")
	}
	changed := false
	for _, field := range []string{"prompt_cache_key", "prompt_cache_retention"} {
		if _, exists := payload[field]; exists {
			delete(payload, field)
			changed = true
		}
	}
	if routing.Model != model {
		payload["model"], err = json.Marshal(model)
		if err != nil {
			return nil, err
		}
		changed = true
	}
	if !changed {
		return body, nil
	}
	return json.Marshal(payload)
}

// CodexAlphaSearchURL uses the OAuth endpoint or the opted-in API key's base.
// Query parameters and escaped path components belong to that configured base.
func CodexAlphaSearchURL(baseURL string, isAPIKey bool) (string, error) {
	if !isAPIKey {
		return CodexAlphaSearchOAuthURL, nil
	}
	u, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return "", errors.New("Codex search requires an HTTP(S) base URL without user info or a fragment")
	}
	return u.JoinPath("alpha", "search").String(), nil
}

type codexAlphaSearchReadError struct{ responseError, readError error }

func (e codexAlphaSearchReadError) Error() string   { return e.responseError.Error() }
func (e codexAlphaSearchReadError) Unwrap() []error { return []error{e.responseError, e.readError} }

// WithCodexAlphaSearchReadError retains both HTTP evidence and the read cause.
// A broken or oversized error body must not hide an actual upstream 429.
func WithCodexAlphaSearchReadError(responseError, readError error) error {
	if readError == nil {
		return responseError
	}
	if responseError == nil {
		return readError
	}
	return codexAlphaSearchReadError{responseError: responseError, readError: readError}
}
