package chatgptweb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// DecodeImportCredential normalizes supported native and compatibility JSON
// shapes without interpreting generic session_token fields as browser cookies.
func DecodeImportCredential(data []byte) (*Credential, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var raw map[string]any
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode chatgpt web import: %w", err)
	}
	if raw == nil {
		return nil, fmt.Errorf("chatgpt web import must be a JSON object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("chatgpt web import contains trailing JSON values")
	}
	copyAlias(raw, "access_token", "accessToken")
	copyAlias(raw, "refresh_token", "refreshToken")
	copyAlias(raw, "id_token", "idToken")
	if credentialType := strings.TrimSpace(importString(raw["type"])); credentialType != "" && !strings.EqualFold(credentialType, Provider) {
		return nil, fmt.Errorf("credential type %q is not %q", credentialType, Provider)
	}
	raw["type"] = Provider

	cookies, errCookies := decodeImportCookies(raw["cookies"])
	if errCookies != nil {
		return nil, errCookies
	}
	if header := strings.TrimSpace(importString(raw["cookie_header"])); header != "" {
		parsed, errParse := ParseCookieHeader(header, SessionBaseURL)
		if errParse != nil {
			return nil, errParse
		}
		cookies = mergeImportedCookies(cookies, parsed)
	}
	if session := strings.TrimSpace(importString(raw["session_cookie"])); session != "" {
		session = "__Secure-next-auth.session-token=" + session
		parsed, errParse := ParseCookieHeader(session, SessionBaseURL)
		if errParse != nil {
			return nil, errParse
		}
		cookies = mergeImportedCookies(cookies, parsed)
	}
	raw["cookies"] = cookies
	delete(raw, "cookie_header")
	delete(raw, "session_cookie")

	normalized, errMarshal := json.Marshal(raw)
	if errMarshal != nil {
		return nil, fmt.Errorf("normalize chatgpt web import: %w", errMarshal)
	}
	credential, errDecode := DecodeCredential(normalized)
	if errDecode != nil {
		return nil, errDecode
	}
	if credentialIdentitySetConflicts(credentialIdentityEvidence(credential)) {
		return nil, fmt.Errorf("chatgpt web import contains conflicting account identity")
	}
	PopulateCredentialIdentity(credential)
	switch credential.RefreshStrategy {
	case RefreshStrategyWebOAuthRT:
		if strings.TrimSpace(credential.RefreshToken) == "" {
			return nil, fmt.Errorf("web_oauth_rt requires refresh_token")
		}
	case RefreshStrategyChatGPTSession:
		if !HasSessionCookie(credential.Cookies) {
			return nil, fmt.Errorf("chatgpt_session requires an explicit session cookie")
		}
	case RefreshStrategyCodexSource:
		return nil, fmt.Errorf("codex_source credentials must use the conversion task")
	case RefreshStrategyTokenOnly:
		if strings.TrimSpace(credential.AccessToken) == "" {
			if strings.TrimSpace(credential.Email) != "" && (strings.TrimSpace(credential.Password) != "" || strings.TrimSpace(credential.TOTPSecret) != "") {
				return nil, fmt.Errorf("email, password, and TOTP credentials must use the login task")
			}
			return nil, fmt.Errorf("chatgpt web import has no usable token or session")
		}
	}
	return credential, nil
}

// ParseCookieHeader converts a Cookie header into scoped persisted cookies.
func ParseCookieHeader(header, rawURL string) ([]Cookie, error) {
	parsedURL, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || parsedURL.Hostname() == "" {
		return nil, fmt.Errorf("invalid chatgpt session origin")
	}
	request := &http.Request{Header: make(http.Header)}
	request.Header.Set("Cookie", strings.TrimSpace(header))
	httpCookies := request.Cookies()
	if len(httpCookies) == 0 {
		return nil, fmt.Errorf("cookie header contains no cookies")
	}
	cookies := make([]Cookie, 0, len(httpCookies))
	for _, cookie := range httpCookies {
		if cookie == nil || strings.TrimSpace(cookie.Name) == "" || strings.TrimSpace(cookie.Value) == "" {
			continue
		}
		cookies = append(cookies, Cookie{
			Name:     cookie.Name,
			Value:    cookie.Value,
			Path:     "/",
			Domain:   parsedURL.Hostname(),
			Host:     parsedURL.Hostname(),
			Secure:   strings.EqualFold(parsedURL.Scheme, "https"),
			HTTPOnly: true,
		})
	}
	if len(cookies) == 0 {
		return nil, fmt.Errorf("cookie header contains no usable cookies")
	}
	return cookies, nil
}

func decodeImportCookies(value any) ([]Cookie, error) {
	if value == nil {
		return []Cookie{}, nil
	}
	payload, errMarshal := json.Marshal(value)
	if errMarshal != nil {
		return nil, fmt.Errorf("encode imported cookies: %w", errMarshal)
	}
	var entries []map[string]json.RawMessage
	if err := json.Unmarshal(payload, &entries); err == nil {
		cookies := make([]Cookie, 0, len(entries))
		for _, entry := range entries {
			cookie, errDecode := decodeImportCookie(entry)
			if errDecode != nil {
				return nil, errDecode
			}
			cookies = append(cookies, cookie)
		}
		return cookies, nil
	}
	var values map[string]string
	if err := json.Unmarshal(payload, &values); err != nil {
		return nil, fmt.Errorf("cookies must be an array or string map")
	}
	result := make([]Cookie, 0, len(values))
	for name, value := range values {
		parsed, errParse := ParseCookieHeader(name+"="+value, SessionBaseURL)
		if errParse != nil {
			return nil, errParse
		}
		result = append(result, parsed...)
	}
	return result, nil
}

func decodeImportCookie(entry map[string]json.RawMessage) (Cookie, error) {
	copyImportCookieField(entry, "http_only", "httpOnly")
	copyImportCookieField(entry, "raw_expires", "rawExpires")
	copyImportCookieField(entry, "max_age", "maxAge")
	copyImportCookieField(entry, "same_site", "sameSite")
	copyImportCookieField(entry, "expires", "expirationDate")
	if rawExpires, ok := entry["expires"]; ok {
		normalized, errNormalize := normalizeImportedCookieExpires(rawExpires)
		if errNormalize != nil {
			return Cookie{}, errNormalize
		}
		entry["expires"] = normalized
	}
	if rawSameSite, ok := entry["same_site"]; ok {
		normalized, errNormalize := normalizeImportedSameSite(rawSameSite)
		if errNormalize != nil {
			return Cookie{}, errNormalize
		}
		entry["same_site"] = normalized
	}
	payload, errMarshal := json.Marshal(entry)
	if errMarshal != nil {
		return Cookie{}, fmt.Errorf("encode imported cookie: %w", errMarshal)
	}
	var cookie Cookie
	if errUnmarshal := json.Unmarshal(payload, &cookie); errUnmarshal != nil {
		return Cookie{}, fmt.Errorf("decode imported cookie: %w", errUnmarshal)
	}
	if errLifetime := normalizeImportedCookieLifetime(&cookie, time.Now()); errLifetime != nil {
		return Cookie{}, errLifetime
	}
	return cookie, nil
}

func normalizeImportedCookieExpires(raw json.RawMessage) (json.RawMessage, error) {
	var text string
	if errText := json.Unmarshal(raw, &text); errText == nil {
		text = strings.TrimSpace(text)
		if text == "" {
			return json.Marshal("")
		}
		if parsed, errParse := time.Parse(time.RFC3339Nano, text); errParse == nil {
			return json.Marshal(parsed.UTC().Format(time.RFC3339Nano))
		}
		seconds, errSeconds := strconv.ParseFloat(text, 64)
		if errSeconds != nil {
			return nil, fmt.Errorf("cookie expires must be RFC3339 or Unix seconds")
		}
		return importedCookieUnixExpiry(seconds)
	}
	var seconds float64
	if errNumber := json.Unmarshal(raw, &seconds); errNumber != nil {
		return nil, fmt.Errorf("cookie expires must be a string or number")
	}
	return importedCookieUnixExpiry(seconds)
}

func importedCookieUnixExpiry(seconds float64) (json.RawMessage, error) {
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return nil, fmt.Errorf("cookie expires must be finite")
	}
	if seconds < 0 {
		return json.Marshal("")
	}
	whole, fraction := math.Modf(seconds)
	expiresAt := time.Unix(int64(whole), int64(fraction*float64(time.Second))).UTC()
	return json.Marshal(expiresAt.Format(time.RFC3339Nano))
}

func normalizeImportedCookieLifetime(cookie *Cookie, now time.Time) error {
	if cookie == nil || cookie.MaxAge <= 0 || strings.TrimSpace(cookie.Expires) != "" || strings.TrimSpace(cookie.RawExpires) != "" {
		return nil
	}
	const maxDurationSeconds = int64((time.Duration(1<<63 - 1)) / time.Second)
	if int64(cookie.MaxAge) > maxDurationSeconds {
		return fmt.Errorf("cookie max_age is too large")
	}
	cookie.Expires = now.Add(time.Duration(cookie.MaxAge) * time.Second).UTC().Format(time.RFC3339Nano)
	cookie.MaxAge = 0
	return nil
}

func copyImportCookieField(entry map[string]json.RawMessage, canonical string, aliases ...string) {
	if entry == nil {
		return
	}
	if _, exists := entry[canonical]; exists {
		return
	}
	for _, alias := range aliases {
		if value, exists := entry[alias]; exists {
			entry[canonical] = value
			return
		}
	}
}

func normalizeImportedSameSite(raw json.RawMessage) (json.RawMessage, error) {
	var numeric int
	if errNumeric := json.Unmarshal(raw, &numeric); errNumeric == nil {
		return json.Marshal(numeric)
	}
	var text string
	if errText := json.Unmarshal(raw, &text); errText != nil {
		return nil, fmt.Errorf("cookie sameSite must be a string or integer")
	}
	var sameSite http.SameSite
	switch strings.ToLower(strings.TrimSpace(text)) {
	case "", "default", "unspecified":
		sameSite = http.SameSiteDefaultMode
	case "lax":
		sameSite = http.SameSiteLaxMode
	case "strict":
		sameSite = http.SameSiteStrictMode
	case "none", "no_restriction":
		sameSite = http.SameSiteNoneMode
	default:
		return nil, fmt.Errorf("cookie sameSite value %q is unsupported", text)
	}
	return json.Marshal(int(sameSite))
}

func mergeImportedCookies(existing, incoming []Cookie) []Cookie {
	return MergeCookieDelta(existing, nil, incoming)
}

func copyAlias(values map[string]any, canonical string, aliases ...string) {
	if strings.TrimSpace(importString(values[canonical])) != "" {
		return
	}
	for _, alias := range aliases {
		if value := strings.TrimSpace(importString(values[alias])); value != "" {
			values[canonical] = value
			return
		}
	}
}

func importString(value any) string {
	text, _ := value.(string)
	return text
}
