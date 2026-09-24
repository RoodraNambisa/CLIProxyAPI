package auth

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"
)

// AccessTokenExpirationTime reads expiration evidence for the access token. It
// does not verify its signature, authorization, revocation, or refresh capability.
func (a *Auth) AccessTokenExpirationTime() (time.Time, bool) {
	if a == nil || authAccessToken(a) == "" {
		return time.Time{}, false
	}
	if expires, ok := parseJWTExpiration(authAccessToken(a)); ok {
		return expires, true
	}
	if expires, ok := expirationFromMap(a.Metadata); ok {
		return expires, true
	}
	if strings.EqualFold(strings.TrimSpace(a.Provider), "chatgpt-web") && authMetadataString(a, "expired") != "" {
		return time.Time{}, true
	}
	return time.Time{}, false
}

func codexAccessTokenExpiration(auth *Auth) (time.Time, bool) {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return time.Time{}, false
	}
	if kind, _ := auth.AccountInfo(); kind == "api_key" {
		return time.Time{}, false
	}
	return auth.AccessTokenExpirationTime()
}

func parseJWTExpiration(token string) (time.Time, bool) {
	if len(token) > 64<<10 {
		return time.Time{}, false
	}
	_, rest, ok := strings.Cut(strings.TrimSpace(token), ".")
	if !ok {
		return time.Time{}, false
	}
	payload, signature, ok := strings.Cut(rest, ".")
	if !ok || payload == "" || strings.Contains(signature, ".") {
		return time.Time{}, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(payload, "="))
	if err != nil {
		return time.Time{}, false
	}
	var claims struct {
		Exp json.RawMessage `json:"exp"`
	}
	if json.Unmarshal(raw, &claims) != nil || len(claims.Exp) == 0 {
		return time.Time{}, false
	}
	numeric := string(claims.Exp)
	if claims.Exp[0] == '"' {
		if json.Unmarshal(claims.Exp, &numeric) != nil {
			return time.Time{}, false
		}
		numeric = strings.TrimSpace(numeric)
	}
	seconds, err := strconv.ParseFloat(numeric, 64)
	// JWT NumericDate is seconds, including zero and fractional seconds. Do not
	// reinterpret large claims as milliseconds or let conversion overflow.
	if err != nil || math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < -62135596800 || seconds >= 253402300800 {
		return time.Time{}, false
	}
	whole, fraction := math.Modf(seconds)
	return time.Unix(int64(whole), int64(fraction*float64(time.Second))).UTC(), true
}
