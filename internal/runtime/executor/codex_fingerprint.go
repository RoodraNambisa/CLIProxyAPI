package executor

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type codexFingerprintPersona struct {
	name           string
	tlsProfile     string
	chromeMajor    string
	userAgent      string
	acceptLanguage string
	platform       string
}

type codexTLSFingerprintSpec struct {
	name        string
	chromeMajor string
}

type codexMacDisplayPreset struct {
	label string
}

type codexMacHardwarePreset struct {
	label string
}

type codexMacTimezonePreset struct {
	label string
}

var codexTLSFingerprintSpecs = []codexTLSFingerprintSpec{
	{name: "chrome_133", chromeMajor: "133"},
	{name: "chrome_131", chromeMajor: "131"},
	{name: "chrome_120_pq", chromeMajor: "120"},
	{name: "chrome_120", chromeMajor: "120"},
}

var codexMacDisplayPresets = []codexMacDisplayPreset{
	{label: "1440x900"},
	{label: "1470x956"},
	{label: "1512x982"},
	{label: "1680x1050"},
	{label: "1728x1117"},
	{label: "1792x1120"},
	{label: "1920x1200"},
	{label: "2048x1280"},
}

var codexMacHardwarePresets = []codexMacHardwarePreset{
	{label: "hc8"},
	{label: "hc10"},
	{label: "hc16"},
}

var codexMacTimezonePresets = []codexMacTimezonePreset{
	{label: "us_eastern"},
	{label: "us_central"},
	{label: "us_mountain"},
	{label: "us_pacific"},
}

var codexFingerprintPersonas = buildCodexFingerprintPersonas()

type codexFingerprintPersonaContextKey struct{}

func buildCodexFingerprintPersonas() []codexFingerprintPersona {
	personas := make([]codexFingerprintPersona, 0, len(codexTLSFingerprintSpecs)*len(codexMacDisplayPresets)*len(codexMacHardwarePresets)*len(codexMacTimezonePresets))
	for _, tlsSpec := range codexTLSFingerprintSpecs {
		userAgent := fmt.Sprintf(
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/%s.0.0.0 Safari/537.36",
			tlsSpec.chromeMajor,
		)
		for _, display := range codexMacDisplayPresets {
			for _, hardware := range codexMacHardwarePresets {
				for _, zone := range codexMacTimezonePresets {
					personas = append(personas, codexFingerprintPersona{
						name:           fmt.Sprintf("%s_mac_%s_%s_%s", tlsSpec.name, display.label, zone.label, hardware.label),
						tlsProfile:     tlsSpec.name,
						chromeMajor:    tlsSpec.chromeMajor,
						userAgent:      userAgent,
						acceptLanguage: "en-US,en;q=0.9",
						platform:       `"macOS"`,
					})
				}
			}
		}
	}
	return personas
}

func codexFingerprintJA3Enabled(cfg *config.Config) bool {
	return cfg != nil && cfg.CodexFingerprint.JA3
}

func codexFingerprintBrowserHeadersEnabled(cfg *config.Config) bool {
	return cfg != nil && cfg.CodexFingerprint.BrowserHeaders
}

func codexFingerprintStabilizePerAccount(cfg *config.Config) bool {
	if cfg == nil || cfg.CodexFingerprint.StabilizePerAccount == nil {
		return true
	}
	return *cfg.CodexFingerprint.StabilizePerAccount
}

func codexFingerprintPersonaForAuth(cfg *config.Config, auth *cliproxyauth.Auth) codexFingerprintPersona {
	if len(codexFingerprintPersonas) == 0 {
		return codexFingerprintPersona{}
	}
	if !codexFingerprintStabilizePerAccount(cfg) {
		idx := time.Now().UnixNano() % int64(len(codexFingerprintPersonas))
		if idx < 0 {
			idx = -idx
		}
		return codexFingerprintPersonas[idx]
	}
	seed := codexFingerprintSeed(auth)
	sum := sha256.Sum256([]byte(seed))
	idx := binary.BigEndian.Uint64(sum[:8]) % uint64(len(codexFingerprintPersonas))
	return codexFingerprintPersonas[idx]
}

func contextWithCodexFingerprintPersona(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth) context.Context {
	if !codexFingerprintJA3Enabled(cfg) && !codexFingerprintBrowserHeadersEnabled(cfg) {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Value(codexFingerprintPersonaContextKey{}).(codexFingerprintPersona); ok {
		return ctx
	}
	return context.WithValue(ctx, codexFingerprintPersonaContextKey{}, codexFingerprintPersonaForAuth(cfg, auth))
}

func codexFingerprintPersonaFromContext(ctx context.Context, cfg *config.Config, auth *cliproxyauth.Auth) codexFingerprintPersona {
	if ctx != nil {
		if persona, ok := ctx.Value(codexFingerprintPersonaContextKey{}).(codexFingerprintPersona); ok {
			return persona
		}
	}
	return codexFingerprintPersonaForAuth(cfg, auth)
}

func codexFingerprintSeed(auth *cliproxyauth.Auth) string {
	if auth == nil {
		return "codex:anonymous"
	}
	if accountType, accountValue := auth.AccountInfo(); strings.TrimSpace(accountValue) != "" {
		if strings.EqualFold(accountType, "oauth") {
			accountValue = strings.ToLower(strings.TrimSpace(accountValue))
		}
		return strings.TrimSpace(accountType) + ":" + strings.TrimSpace(accountValue)
	}
	if auth.Metadata != nil {
		for _, key := range []string{"email", "account_id", "organization_id"} {
			if value, ok := auth.Metadata[key].(string); ok {
				if value = strings.TrimSpace(value); value != "" {
					return strings.ToLower(key) + ":" + value
				}
			}
		}
	}
	if auth.ID != "" {
		return "id:" + strings.TrimSpace(auth.ID)
	}
	if auth.Label != "" {
		return "label:" + strings.TrimSpace(auth.Label)
	}
	return "codex:anonymous"
}

func applyCodexBrowserFingerprintHeaders(ctx context.Context, headers http.Header, cfg *config.Config, auth *cliproxyauth.Auth, target *url.URL, websocket bool) {
	if headers == nil || !codexFingerprintBrowserHeadersEnabled(cfg) {
		return
	}
	persona := codexFingerprintPersonaFromContext(ctx, cfg, auth)
	if strings.TrimSpace(persona.userAgent) == "" {
		return
	}

	cfgUserAgent, _ := codexHeaderDefaults(cfg, auth)
	if strings.TrimSpace(cfgUserAgent) == "" {
		headers.Set("User-Agent", persona.userAgent)
	}
	userAgent := strings.TrimSpace(headers.Get("User-Agent"))
	if userAgent == "" {
		userAgent = persona.userAgent
		headers.Set("User-Agent", userAgent)
	}

	headers.Set("Sec-CH-UA", codexSecCHUAForUserAgent(userAgent, persona.chromeMajor))
	headers.Set("Sec-CH-UA-Mobile", "?0")
	headers.Set("Sec-CH-UA-Platform", codexSecCHUAPlatformForUserAgent(userAgent, persona.platform))
	headers.Set("Accept-Language", persona.acceptLanguage)
	headers.Set("DNT", "1")
	headers.Set("Origin", codexBrowserOriginForURL(target))
	headers.Set("Referer", strings.TrimRight(codexBrowserOriginForURL(target), "/")+"/codex")
	headers.Set("Sec-Fetch-Site", "same-origin")
	if websocket {
		headers.Set("Sec-Fetch-Mode", "websocket")
		headers.Set("Sec-Fetch-Dest", "websocket")
		return
	}
	headers.Set("Sec-Fetch-Mode", "cors")
	headers.Set("Sec-Fetch-Dest", "empty")
}

func codexSecCHUAForUserAgent(userAgent string, fallbackMajor string) string {
	major := codexChromeMajorVersionFromUserAgent(userAgent)
	if major == "" {
		major = strings.TrimSpace(fallbackMajor)
	}
	if major == "" {
		major = "133"
	}
	return fmt.Sprintf(`"Google Chrome";v="%s", "Chromium";v="%s", "Not.A/Brand";v="24"`, major, major)
}

func codexChromeMajorVersionFromUserAgent(userAgent string) string {
	userAgent = strings.TrimSpace(userAgent)
	marker := "Chrome/"
	idx := strings.Index(userAgent, marker)
	if idx < 0 {
		return ""
	}
	version := userAgent[idx+len(marker):]
	for i, char := range version {
		if char < '0' || char > '9' {
			return version[:i]
		}
	}
	return version
}

func codexSecCHUAPlatformForUserAgent(userAgent string, fallback string) string {
	switch {
	case strings.Contains(userAgent, "Mac OS X"), strings.Contains(userAgent, "Macintosh"):
		return `"macOS"`
	case strings.Contains(userAgent, "Linux"):
		return `"Linux"`
	case strings.Contains(userAgent, "Windows"):
		return `"Windows"`
	default:
		if strings.TrimSpace(fallback) != "" {
			return fallback
		}
		return `"macOS"`
	}
}

func codexBrowserOriginForURL(target *url.URL) string {
	if target == nil || strings.TrimSpace(target.Host) == "" {
		return "https://chatgpt.com"
	}
	switch target.Scheme {
	case "wss":
		return "https://" + target.Host
	case "ws":
		return "http://" + target.Host
	case "http", "https":
		return target.Scheme + "://" + target.Host
	default:
		return "https://" + target.Host
	}
}

func parsedURLOrNil(raw string) *url.URL {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil
	}
	return parsed
}
