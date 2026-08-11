package chatgptweb

import (
	"fmt"
	"net/url"
	"strings"
)

// LoginMethod selects the interactive authentication material used during a
// ChatGPT Web login or re-login.
type LoginMethod string

const (
	LoginMethodAuto         LoginMethod = "auto"
	LoginMethodPasskey      LoginMethod = "passkey"
	LoginMethodPasswordTOTP LoginMethod = "password_totp"
	LoginMethodAPI798       LoginMethod = "api798"
)

const API798LoginFeature = "api798_login_v1"

// NormalizeLoginMethod validates a persisted login method. Credentials that
// predate the field retain the existing automatic behavior.
func NormalizeLoginMethod(method LoginMethod) (LoginMethod, error) {
	method = LoginMethod(strings.ToLower(strings.TrimSpace(string(method))))
	if method == "" {
		return LoginMethodAuto, nil
	}
	switch method {
	case LoginMethodAuto, LoginMethodPasskey, LoginMethodPasswordTOTP, LoginMethodAPI798:
		return method, nil
	default:
		return "", fmt.Errorf("unsupported chatgpt web login method %q", method)
	}
}

// ValidateAPI798URL validates the persisted receive URL without rewriting it.
// The caller may retain the original bytes, including the auth_code encoding.
func ValidateAPI798URL(rawURL, email string) error {
	_, err := normalizeAPI798RequestURL(rawURL, email)
	return err
}

// normalizeAPI798RequestURL returns the URL used for a direct provider request.
// Plain HTTP is upgraded to HTTPS while RawQuery is preserved verbatim.
func normalizeAPI798RequestURL(rawURL, email string) (string, error) {
	if rawURL == "" || strings.TrimSpace(rawURL) != rawURL {
		return "", fmt.Errorf("api798_url must be a complete API798 get_code URL")
	}
	parsed, errParse := url.Parse(rawURL)
	if errParse != nil || parsed == nil || parsed.Opaque != "" || parsed.Host == "" {
		return "", fmt.Errorf("api798_url must be a valid URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("api798_url must use http or https")
	}
	if parsed.User != nil || parsed.Fragment != "" || !strings.EqualFold(parsed.Hostname(), "api798.com") || parsed.Port() != "" {
		return "", fmt.Errorf("api798_url must target api798.com without credentials, a port, or a fragment")
	}
	if parsed.EscapedPath() != "/get_code" {
		return "", fmt.Errorf("api798_url path must be /get_code")
	}
	query, errQuery := parseAPI798RawQuery(parsed.RawQuery)
	if errQuery != nil {
		return "", errQuery
	}
	if len(query.email) != 1 || !strings.EqualFold(strings.TrimSpace(query.email[0]), strings.TrimSpace(email)) {
		return "", fmt.Errorf("api798_url email must match the credential email")
	}
	if len(query.rawAuthCode) != 1 || strings.TrimSpace(query.rawAuthCode[0]) == "" {
		return "", fmt.Errorf("api798_url auth_code is required")
	}
	parsed.Scheme = "https"
	return parsed.String(), nil
}

type api798RawQuery struct {
	email       []string
	rawAuthCode []string
}

// parseAPI798RawQuery deliberately does not decode auth_code. API798 treats it
// as an opaque authorization value, so its original URL encoding must survive.
func parseAPI798RawQuery(rawQuery string) (api798RawQuery, error) {
	var result api798RawQuery
	if rawQuery == "" {
		return result, fmt.Errorf("api798_url query is required")
	}
	for _, pair := range strings.Split(rawQuery, "&") {
		if pair == "" {
			return result, fmt.Errorf("api798_url query is invalid")
		}
		rawKey, rawValue, found := strings.Cut(pair, "=")
		if !found {
			return result, fmt.Errorf("api798_url query is invalid")
		}
		key, errKey := url.QueryUnescape(rawKey)
		if errKey != nil {
			return result, fmt.Errorf("api798_url query is invalid")
		}
		switch key {
		case "email":
			value, errValue := url.QueryUnescape(rawValue)
			if errValue != nil {
				return result, fmt.Errorf("api798_url email is invalid")
			}
			result.email = append(result.email, value)
		case "auth_code":
			result.rawAuthCode = append(result.rawAuthCode, rawValue)
		default:
			return result, fmt.Errorf("api798_url contains an unsupported query parameter")
		}
	}
	return result, nil
}
