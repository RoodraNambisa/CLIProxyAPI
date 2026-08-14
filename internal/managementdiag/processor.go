package managementdiag

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const (
	DetailLevelSafe = "safe"
	DetailLevelFull = "full"
)

const managementOnlyPlaceholder = "<management-only-diagnostic>"

// ManagementOnlyValue carries content exclusively for authenticated management
// consumers while exposing only a fixed placeholder to ordinary log formatters.
type ManagementOnlyValue struct {
	value    string
	fallback string
}

// NewManagementOnlyValue wraps diagnostic content that must not enter ordinary logs.
func NewManagementOnlyValue(value string) ManagementOnlyValue {
	return ManagementOnlyValue{value: value, fallback: managementOnlyPlaceholder}
}

// NewManagementOnlyValueWithFallback keeps the management value private while
// preserving an explicitly safe representation for ordinary logs.
func NewManagementOnlyValueWithFallback(value, fallback string) ManagementOnlyValue {
	return ManagementOnlyValue{value: value, fallback: fallback}
}

// Value returns the management-only content to trusted in-process consumers.
func (value ManagementOnlyValue) Value() string {
	return value.value
}

// String prevents text log formatters from exposing management-only content.
func (value ManagementOnlyValue) String() string {
	if value.fallback == "" {
		return managementOnlyPlaceholder
	}
	return value.fallback
}

// MarshalJSON prevents JSON log formatters from exposing management-only content.
func (value ManagementOnlyValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(value.String())
}

var (
	jwtPattern        = regexp.MustCompile(`(?i)\beyJ[a-z0-9_-]{8,}\.[a-z0-9_-]{8,}(?:\.[a-z0-9_-]{8,})?\b`)
	apiKeyPattern     = regexp.MustCompile(`(?i)\bsk-[a-z0-9_-]{8,}\b`)
	bearerPattern     = regexp.MustCompile(`(?i)\bbearer\s+[^\s,;]+`)
	cookieHeader      = regexp.MustCompile(`(?im)\b(set-cookie|cookie)\s*:\s*[^\r\n]*`)
	emailPattern      = regexp.MustCompile(`(?i)\b[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}\b`)
	assignmentPattern = regexp.MustCompile(`(?i)(["']?(authorization|token|bearer[_-]?token|access[_-]?token|refresh[_-]?token|id[_-]?token|session(?:[_-]?(?:token|cookie|id))?|cookies?|password(?:[_-]?(?:hash|salt))?|totp(?:[_-]?(?:secret|code|seed))?|otp(?:[_-]?code)?|mfa(?:[_-]?(?:code|request[_-]?id))?|verification[_-]?code|client[_-]?secret|(?:x[_-]?)?api[_-]?key|secret|(?:webauthn[_-]?)?private[_-]?key|(?:passkey[_-]?)?assertion|signed[_-]?passkey[_-]?response|authenticator[_-]?data|client[_-]?data[_-]?json|signature|credential[_-]?id|raw[_-]?id|user[_-]?handle|recovery[_-]?(?:key|keys|code|codes|secret)|proxy[_-]?(?:url|authorization|password)|api798[_-]?url|auth(?:orization)?[_-]?code|oauth[_-]?code|code[_-]?(?:verifier|challenge)|oauth[_-]?state|state|nonce)["']?)(\s*[=:]\s*|\s+)(\[[^\]\r\n]*\]|\{[^}\r\n]*\}|"[^"\r\n]*"|'[^'\r\n]*'|[^\s,;]+)`)
	privateKeyPattern = regexp.MustCompile(`(?is)-----BEGIN [^-\r\n]*PRIVATE KEY-----.*?-----END [^-\r\n]*PRIVATE KEY-----`)
	urlPattern        = regexp.MustCompile(`https?://[^\s<>"']+`)
)

var permanentlySensitiveKeys = map[string]struct{}{
	"authorization": {}, "proxyauthorization": {}, "token": {}, "bearertoken": {},
	"accesstoken": {}, "refreshtoken": {}, "idtoken": {}, "session": {}, "sessionid": {}, "sessiontoken": {}, "sessioncookie": {},
	"cookie": {}, "cookies": {}, "setcookie": {}, "password": {}, "passwordhash": {}, "passwordsalt": {}, "proxypassword": {},
	"totp": {}, "totpsecret": {}, "totpcode": {}, "totpseed": {}, "otp": {}, "otpcode": {}, "mfacode": {}, "mfarequestid": {}, "verificationcode": {},
	"secret": {}, "clientsecret": {}, "apikey": {}, "xapikey": {},
	"privatekey": {}, "privatekeypkcs8": {}, "webauthnprivatekey": {}, "assertion": {}, "passkeyassertion": {}, "signedpasskeyresponse": {},
	"authenticatordata": {}, "clientdatajson": {}, "signature": {},
	"credentialid": {}, "rawid": {}, "userhandle": {},
	"recoverykey": {}, "recoverykeys": {}, "recoverycode": {}, "recoverycodes": {}, "recoverysecret": {}, "accountrecoverycode": {},
	"proxyurl": {}, "api798url": {}, "authcode": {}, "authorizationcode": {}, "oauthcode": {},
	"codeverifier": {}, "codechallenge": {}, "oauthstate": {}, "state": {}, "nonce": {},
}

var sensitiveQueryKeys = map[string]struct{}{
	"authorization": {}, "authcode": {}, "authorizationcode": {}, "oauthcode": {}, "code": {}, "state": {},
	"token": {}, "accesstoken": {}, "refreshtoken": {}, "idtoken": {}, "sessiontoken": {},
	"key": {}, "apikey": {}, "xapikey": {}, "password": {}, "secret": {}, "clientsecret": {}, "credential": {}, "credentialid": {},
	"otp": {}, "otpcode": {}, "totp": {}, "totpcode": {}, "mfacode": {}, "verificationcode": {},
	"codeverifier": {}, "codechallenge": {}, "oauthstate": {}, "nonce": {},
	"sig": {}, "signature": {}, "xamzsignature": {}, "xgoogsignature": {},
	"xmssignature": {}, "se": {}, "sp": {}, "sv": {}, "spr": {}, "srt": {}, "ss": {},
}

var signedURLQueryKeys = map[string]struct{}{
	"sig": {}, "signature": {}, "xamzsignature": {}, "xgoogsignature": {}, "xmssignature": {},
	"xamzalgorithm": {}, "xamzcredential": {}, "xamzdate": {}, "xamzexpires": {}, "xamzsignedheaders": {},
	"xgoogalgorithm": {}, "xgoogcredential": {}, "xgoogdate": {}, "xgoogexpires": {}, "xgoogsignedheaders": {},
	"keypairid": {}, "policy": {},
	"sv": {}, "se": {}, "sp": {}, "spr": {}, "sr": {}, "srt": {}, "ss": {},
}

// NormalizeDetailLevel returns the effective diagnostics detail level.
func NormalizeDetailLevel(level string) string {
	if strings.EqualFold(strings.TrimSpace(level), DetailLevelFull) {
		return DetailLevelFull
	}
	return DetailLevelSafe
}

// ProcessText removes credential material and applies the requested management detail policy.
func ProcessText(value, level string, maxBytes int) (string, bool) {
	level = NormalizeDetailLevel(level)
	value = strings.Map(func(r rune) rune {
		if r == '\n' || r == '\t' || !unicode.IsControl(r) {
			return r
		}
		return -1
	}, strings.ToValidUTF8(strings.TrimSpace(value), "\uFFFD"))
	value = urlPattern.ReplaceAllStringFunc(value, func(raw string) string { return ProcessURL(raw, level) })
	value = redactPermanentSecrets(value)
	if level == DetailLevelSafe {
		value = emailPattern.ReplaceAllString(value, "<redacted-email>")
	}
	return truncateUTF8(value, maxBytes)
}

// ProcessResponseBody preserves diagnostic content while permanently removing credential material.
func ProcessResponseBody(raw, level string, maxBytes int) (string, bool) {
	valid := strings.ToValidUTF8(raw, "\uFFFD")
	var decoded any
	if json.Unmarshal([]byte(valid), &decoded) == nil {
		decoded = processJSONValue(decoded, NormalizeDetailLevel(level))
		if encoded, errMarshal := json.Marshal(decoded); errMarshal == nil {
			processed, truncated := truncateUTF8(string(encoded), maxBytes)
			if len(raw) > maxBytes && maxBytes > 0 {
				truncated = true
			}
			return processed, truncated
		}
	}
	processed, truncated := ProcessText(valid, level, maxBytes)
	if len(raw) > maxBytes && maxBytes > 0 {
		truncated = true
	}
	return processed, truncated
}

// ProcessURL keeps ordinary query parameters only in full mode. Credential and signature material is always removed.
func ProcessURL(raw, level string) string {
	trimmed := strings.TrimRight(raw, ".,);]")
	suffix := raw[len(trimmed):]
	parsed, errParse := url.Parse(trimmed)
	if errParse != nil || parsed.Host == "" {
		return "<redacted-url>" + suffix
	}
	parsed.User = nil
	parsed.Fragment = ""
	if NormalizeDetailLevel(level) == DetailLevelSafe {
		parsed.RawQuery = ""
		return parsed.String() + suffix
	}
	query := parsed.Query()
	if isSignedOrAPI798URL(parsed, query) {
		parsed.RawQuery = "redacted"
		return parsed.String() + suffix
	}
	for name := range query {
		if isSensitiveQueryKey(name) {
			query.Set(name, "<redacted>")
		}
	}
	parsed.RawQuery = encodeStableQuery(query)
	return parsed.String() + suffix
}

func processJSONValue(value any, level string) any {
	switch typed := value.(type) {
	case map[string]any:
		for name, item := range typed {
			if isPermanentlySensitiveKey(name) {
				typed[name] = "<redacted>"
				continue
			}
			typed[name] = processJSONValue(item, level)
		}
		return typed
	case []any:
		for index := range typed {
			typed[index] = processJSONValue(typed[index], level)
		}
		return typed
	case string:
		processed, _ := ProcessText(typed, level, 0)
		return processed
	default:
		return typed
	}
}

func redactPermanentSecrets(value string) string {
	value = privateKeyPattern.ReplaceAllString(value, "<redacted-private-key>")
	value = cookieHeader.ReplaceAllString(value, `${1}: <redacted>`)
	value = bearerPattern.ReplaceAllString(value, "Bearer <redacted-token>")
	value = assignmentPattern.ReplaceAllString(value, `${1}${3}<redacted>`)
	value = jwtPattern.ReplaceAllString(value, "<redacted-token>")
	return apiKeyPattern.ReplaceAllString(value, "<redacted-key>")
}

func normalizeKey(value string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}
		return -1
	}, value)
}

func isPermanentlySensitiveKey(name string) bool {
	_, sensitive := permanentlySensitiveKeys[normalizeKey(name)]
	return sensitive
}

func isSensitiveQueryKey(name string) bool {
	_, sensitive := sensitiveQueryKeys[normalizeKey(name)]
	return sensitive
}

func isSignedOrAPI798URL(parsed *url.URL, query url.Values) bool {
	host := strings.ToLower(parsed.Hostname())
	if host == "api798.com" && strings.EqualFold(strings.TrimSuffix(parsed.Path, "/"), "/get_code") {
		return true
	}
	for name := range query {
		if _, signed := signedURLQueryKeys[normalizeKey(name)]; signed {
			return true
		}
	}
	return false
}

func encodeStableQuery(query url.Values) string {
	if len(query) == 0 {
		return ""
	}
	names := make([]string, 0, len(query))
	for name := range query {
		names = append(names, name)
	}
	sort.Strings(names)
	ordered := make(url.Values, len(query))
	for _, name := range names {
		ordered[name] = append([]string(nil), query[name]...)
	}
	return ordered.Encode()
}

func truncateUTF8(value string, maxBytes int) (string, bool) {
	if maxBytes <= 0 || len(value) <= maxBytes {
		return value, false
	}
	value = value[:maxBytes]
	for len(value) > 0 && !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value, true
}

// ProcessValue is a convenience wrapper for structured logging fields.
func ProcessValue(value any, level string, maxBytes int) (string, bool) {
	return ProcessText(fmt.Sprint(value), level, maxBytes)
}
