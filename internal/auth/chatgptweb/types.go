package chatgptweb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type LifecycleState string
type RefreshStrategy string
type QuotaState string

const (
	QuotaStateUnknown   QuotaState = "unknown"
	QuotaStateAvailable QuotaState = "available"
	QuotaStateExhausted QuotaState = "exhausted"
)

type Persona struct {
	CatalogVersion      string `json:"catalog_version,omitempty"`
	CatalogID           string `json:"catalog_id,omitempty"`
	Profile             string `json:"profile"`
	UserAgent           string `json:"user_agent"`
	AcceptLanguage      string `json:"accept_language"`
	Language            string `json:"language"`
	Platform            string `json:"platform"`
	ScreenWidth         int    `json:"screen_width"`
	ScreenHeight        int    `json:"screen_height"`
	HardwareConcurrency int    `json:"hardware_concurrency"`
}

func DefaultPersona() Persona {
	return Persona{
		Profile:             "chrome_146",
		UserAgent:           "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
		AcceptLanguage:      "en-US,en;q=0.9",
		Language:            "en-US",
		Platform:            "MacIntel",
		ScreenWidth:         1920,
		ScreenHeight:        1080,
		HardwareConcurrency: 8,
	}
}

func normalizePersona(persona Persona) Persona {
	if entry, ok := personaCatalogEntryForPersona(persona); ok {
		return entry.persona
	}
	return normalizeLegacyPersona(persona)
}

func normalizeLegacyPersona(persona Persona) Persona {
	defaults := DefaultPersona()
	persona.CatalogVersion = ""
	persona.CatalogID = ""
	if strings.TrimSpace(persona.Profile) == "" {
		persona.Profile = defaults.Profile
	}
	if strings.TrimSpace(persona.UserAgent) == "" {
		persona.UserAgent = defaults.UserAgent
	}
	if strings.TrimSpace(persona.AcceptLanguage) == "" {
		persona.AcceptLanguage = defaults.AcceptLanguage
	}
	if strings.TrimSpace(persona.Language) == "" {
		persona.Language = defaults.Language
	}
	if strings.TrimSpace(persona.Platform) == "" {
		persona.Platform = defaults.Platform
	}
	if persona.ScreenWidth <= 0 {
		persona.ScreenWidth = defaults.ScreenWidth
	}
	if persona.ScreenHeight <= 0 {
		persona.ScreenHeight = defaults.ScreenHeight
	}
	if persona.HardwareConcurrency <= 0 {
		persona.HardwareConcurrency = defaults.HardwareConcurrency
	}
	return persona
}

type Cookie struct {
	Name       string `json:"name"`
	Value      string `json:"value"`
	Path       string `json:"path"`
	Domain     string `json:"domain"`
	Host       string `json:"host"`
	Expires    string `json:"expires"`
	RawExpires string `json:"raw_expires"`
	MaxAge     int    `json:"max_age"`
	Secure     bool   `json:"secure"`
	HTTPOnly   bool   `json:"http_only"`
	SameSite   int    `json:"same_site"`
}

type Credential struct {
	CredentialSchemaVersion  int                         `json:"credential_schema_version,omitempty"`
	WebAuthn                 *WebAuthnCredential         `json:"webauthn,omitempty"`
	LoginMethod              LoginMethod                 `json:"login_method,omitempty"`
	API798URL                string                      `json:"api798_url,omitempty"`
	Type                     string                      `json:"type"`
	CredentialUID            string                      `json:"credential_uid,omitempty"`
	CredentialMode           string                      `json:"credential_mode,omitempty"`
	RefreshStrategy          RefreshStrategy             `json:"refresh_strategy,omitempty"`
	SourceAuthID             string                      `json:"source_auth_id,omitempty"`
	SourceCredentialUID      string                      `json:"source_credential_uid,omitempty"`
	SourceIdentity           string                      `json:"source_identity,omitempty"`
	SourceProxyURL           string                      `json:"source_proxy_url,omitempty"`
	Email                    string                      `json:"email"`
	AccountID                string                      `json:"account_id,omitempty"`
	UserID                   string                      `json:"user_id,omitempty"`
	PlanType                 string                      `json:"plan_type,omitempty"`
	ProfileUpdatedAt         string                      `json:"profile_updated_at,omitempty"`
	ImageQuotaRemaining      *int                        `json:"image_quota_remaining,omitempty"`
	ImageQuotaResetAt        string                      `json:"image_quota_reset_at,omitempty"`
	QuotaState               QuotaState                  `json:"quota_state,omitempty"`
	QuotaUpdatedAt           string                      `json:"quota_updated_at,omitempty"`
	QuotaStale               bool                        `json:"quota_stale,omitempty"`
	QuotaLastError           string                      `json:"quota_last_error,omitempty"`
	Password                 string                      `json:"password"`
	TOTPSecret               string                      `json:"totp_secret"`
	AccessToken              string                      `json:"access_token"`
	RefreshToken             string                      `json:"refresh_token"`
	IDToken                  string                      `json:"id_token"`
	SessionToken             string                      `json:"session_token,omitempty"`
	Expired                  string                      `json:"expired"`
	Cookies                  []Cookie                    `json:"cookies"`
	Persona                  Persona                     `json:"persona"`
	BrowserEnvironment       *BrowserEnvironmentIdentity `json:"browser_environment,omitempty"`
	DeviceID                 string                      `json:"device_id,omitempty"`
	SessionID                string                      `json:"session_id,omitempty"`
	LifecycleState           LifecycleState              `json:"lifecycle_state"`
	LifecycleReason          string                      `json:"lifecycle_reason"`
	LifecycleUpdatedAt       string                      `json:"lifecycle_updated_at"`
	LastLoginAt              string                      `json:"last_login_at"`
	LastRefreshAt            string                      `json:"last_refresh_at"`
	LastReloginAt            string                      `json:"last_relogin_at"`
	ImportSessionPending     bool                        `json:"import_session_refresh_pending,omitempty"`
	ImportModelsPending      bool                        `json:"import_model_validation_pending,omitempty"`
	ImportAccountInfoPending bool                        `json:"import_account_info_refresh_pending,omitempty"`
}

// CredentialRuntimeIdentityReader derives stable runtime identity bytes for
// credentials created before device and session IDs were persisted.
func CredentialRuntimeIdentityReader(authID string, credential *Credential) io.Reader {
	email := ""
	if credential != nil {
		email = strings.ToLower(strings.TrimSpace(credential.Email))
	}
	authID = strings.TrimSpace(authID)
	deviceSeed := sha256.Sum256([]byte("chatgpt-web-device:" + authID + ":" + email))
	sessionSeed := sha256.Sum256([]byte("chatgpt-web-session:" + authID + ":" + email))
	seed := make([]byte, 0, len(deviceSeed)+len(sessionSeed))
	seed = append(seed, deviceSeed[:]...)
	seed = append(seed, sessionSeed[:]...)
	return bytes.NewReader(seed)
}

func DecodeCredential(data []byte) (*Credential, error) {
	var credential Credential
	if err := json.Unmarshal(data, &credential); err != nil {
		return nil, fmt.Errorf("decode chatgpt web credential: %w", err)
	}
	if credential.Type != "" && credential.Type != Provider {
		return nil, fmt.Errorf("credential type %q is not %q", credential.Type, Provider)
	}
	credential.Type = Provider
	if credential.CredentialSchemaVersion == CredentialSchemaVersionWebAuthn {
		if err := validateCredentialWebAuthnJSON(data); err != nil {
			return nil, err
		}
	}
	if err := ValidateCredentialWebAuthn(&credential); err != nil {
		return nil, err
	}
	loginMethod, errLoginMethod := NormalizeLoginMethod(credential.LoginMethod)
	if errLoginMethod != nil {
		return nil, errLoginMethod
	}
	credential.LoginMethod = loginMethod
	if credential.API798URL != "" {
		if errAPI798 := ValidateAPI798URL(credential.API798URL, credential.Email); errAPI798 != nil {
			return nil, errAPI798
		}
	}
	credential.Cookies = scopeUnscopedCookiesForURL(credential.Cookies, SessionBaseURL)
	credential.Cookies, credential.SessionToken = normalizeSessionCookies(credential.Cookies)
	strategy, errStrategy := NormalizeRefreshStrategy(credential.RefreshStrategy, &credential)
	if errStrategy != nil {
		return nil, errStrategy
	}
	credential.RefreshStrategy = strategy
	credential.CredentialMode = credentialModeForStrategy(strategy)
	resolveCredentialPersona(&credential, "")
	resolveCredentialBrowserEnvironment(&credential, "")
	if credential.Cookies == nil {
		credential.Cookies = []Cookie{}
	}
	credential.LifecycleState = normalizedCredentialLifecycleState(&credential)
	credential.LifecycleReason = SafeLifecycleReason(credential.LifecycleReason)
	credential.QuotaState = NormalizeQuotaState(credential.QuotaState, credential.ImageQuotaRemaining)
	credential.QuotaLastError = SafeQuotaError(credential.QuotaLastError)
	return &credential, nil
}

func ParseCredential(metadata map[string]any) (*Credential, error) {
	data, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("encode chatgpt web credential metadata: %w", err)
	}
	return DecodeCredential(data)
}

func (credential *Credential) ApplyToMetadata(metadata map[string]any) {
	if credential == nil || metadata == nil {
		return
	}
	metadata["type"] = Provider
	if credential.CredentialSchemaVersion == 0 {
		delete(metadata, "credential_schema_version")
	} else {
		metadata["credential_schema_version"] = credential.CredentialSchemaVersion
	}
	if credential.WebAuthn == nil {
		delete(metadata, "webauthn")
	} else {
		metadata["webauthn"] = cloneWebAuthnCredential(credential.WebAuthn)
	}
	loginMethod, errLoginMethod := NormalizeLoginMethod(credential.LoginMethod)
	if errLoginMethod != nil {
		loginMethod = LoginMethodAuto
	}
	metadata["login_method"] = string(loginMethod)
	if credential.API798URL == "" {
		delete(metadata, "api798_url")
	} else {
		metadata["api798_url"] = credential.API798URL
	}
	metadata["credential_uid"] = strings.TrimSpace(credential.CredentialUID)
	metadata["credential_mode"] = credentialModeForStrategy(credential.RefreshStrategy)
	metadata["refresh_strategy"] = string(credential.RefreshStrategy)
	metadata["source_auth_id"] = strings.TrimSpace(credential.SourceAuthID)
	metadata["source_credential_uid"] = strings.TrimSpace(credential.SourceCredentialUID)
	metadata["source_identity"] = strings.TrimSpace(credential.SourceIdentity)
	metadata["source_proxy_url"] = strings.TrimSpace(credential.SourceProxyURL)
	metadata["email"] = credential.Email
	metadata["account_id"] = credential.AccountID
	metadata["user_id"] = strings.TrimSpace(credential.UserID)
	metadata["plan_type"] = strings.TrimSpace(credential.PlanType)
	metadata["profile_updated_at"] = strings.TrimSpace(credential.ProfileUpdatedAt)
	if credential.ImageQuotaRemaining == nil {
		delete(metadata, "image_quota_remaining")
	} else {
		metadata["image_quota_remaining"] = *credential.ImageQuotaRemaining
	}
	metadata["image_quota_reset_at"] = strings.TrimSpace(credential.ImageQuotaResetAt)
	metadata["quota_state"] = string(NormalizeQuotaState(credential.QuotaState, credential.ImageQuotaRemaining))
	metadata["quota_updated_at"] = strings.TrimSpace(credential.QuotaUpdatedAt)
	metadata["quota_stale"] = credential.QuotaStale
	metadata["quota_last_error"] = SafeQuotaError(credential.QuotaLastError)
	metadata["password"] = credential.Password
	metadata["totp_secret"] = credential.TOTPSecret
	metadata["access_token"] = credential.AccessToken
	metadata["refresh_token"] = credential.RefreshToken
	metadata["id_token"] = credential.IDToken
	normalizedCookies, sessionToken := normalizeSessionCookies(credential.Cookies)
	if sessionToken == "" {
		delete(metadata, "session_token")
	} else {
		metadata["session_token"] = sessionToken
	}
	metadata["expired"] = credential.Expired
	metadata["cookies"] = normalizedCookies
	resolveCredentialPersona(credential, "")
	metadata["persona"] = credential.Persona
	resolveCredentialBrowserEnvironment(credential, "")
	if credential.BrowserEnvironment == nil {
		delete(metadata, "browser_environment")
	} else {
		metadata["browser_environment"] = *credential.BrowserEnvironment
	}
	metadata["device_id"] = credential.DeviceID
	metadata["session_id"] = credential.SessionID
	metadata["lifecycle_state"] = string(normalizedCredentialLifecycleState(credential))
	metadata["lifecycle_reason"] = SafeLifecycleReason(credential.LifecycleReason)
	metadata["lifecycle_updated_at"] = credential.LifecycleUpdatedAt
	metadata["last_login_at"] = credential.LastLoginAt
	metadata["last_refresh_at"] = credential.LastRefreshAt
	metadata["last_relogin_at"] = credential.LastReloginAt
	if credential.ImportSessionPending {
		metadata["import_session_refresh_pending"] = true
	} else {
		delete(metadata, "import_session_refresh_pending")
	}
	if credential.ImportModelsPending {
		metadata["import_model_validation_pending"] = true
	} else {
		delete(metadata, "import_model_validation_pending")
	}
	if credential.ImportAccountInfoPending {
		metadata["import_account_info_refresh_pending"] = true
	} else {
		delete(metadata, "import_account_info_refresh_pending")
	}
}

// NormalizeQuotaState derives a safe quota state while preserving unknown data.
func NormalizeQuotaState(state QuotaState, remaining *int) QuotaState {
	if remaining != nil {
		if *remaining > 0 {
			return QuotaStateAvailable
		}
		return QuotaStateExhausted
	}
	state = QuotaState(strings.ToLower(strings.TrimSpace(string(state))))
	switch state {
	case QuotaStateUnknown, QuotaStateAvailable, QuotaStateExhausted:
		return state
	}
	return QuotaStateUnknown
}

// SafeQuotaError returns a bounded non-sensitive quota refresh error category.
func SafeQuotaError(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "unauthorized", "rate_limited", "upstream_unavailable", "network_error",
		"invalid_response", "identity_mismatch", "credential_unavailable", "canceled":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return "refresh_failed"
	}
}

// NormalizeRefreshStrategy validates an explicit strategy or infers one for
// credentials created before refresh strategies were persisted.
func NormalizeRefreshStrategy(strategy RefreshStrategy, credential *Credential) (RefreshStrategy, error) {
	strategy = RefreshStrategy(strings.ToLower(strings.TrimSpace(string(strategy))))
	if strategy == "" {
		switch {
		case credential != nil && strings.TrimSpace(credential.RefreshToken) != "":
			strategy = RefreshStrategyWebOAuthRT
		case credential != nil && strings.TrimSpace(credential.SourceAuthID) != "" && strings.TrimSpace(credential.SourceCredentialUID) != "":
			strategy = RefreshStrategyCodexSource
		case credential != nil && HasSessionCookie(credential.Cookies):
			strategy = RefreshStrategyChatGPTSession
		case credential != nil && strings.TrimSpace(credential.Password) != "":
			// Legacy password credentials relied on terminal refresh failures to
			// enter the existing automatic re-login path.
			strategy = RefreshStrategyWebOAuthRT
		default:
			strategy = RefreshStrategyTokenOnly
		}
	}
	switch strategy {
	case RefreshStrategyWebOAuthRT, RefreshStrategyChatGPTSession, RefreshStrategyCodexSource, RefreshStrategyTokenOnly:
		return strategy, nil
	default:
		return "", fmt.Errorf("unsupported chatgpt web refresh strategy %q", strategy)
	}
}

// HasSessionCookie reports whether cookies contain an explicit ChatGPT
// NextAuth/Auth.js session cookie.
func HasSessionCookie(cookies []Cookie) bool {
	return hasSessionCookieForURLAt(cookies, SessionBaseURL, time.Now())
}

func hasSessionCookieAt(cookies []Cookie, now time.Time) bool {
	return hasSessionCookieForURLAt(cookies, SessionBaseURL, now)
}

// HasSessionCookieForURL reports whether the cookie set can authenticate the
// configured ChatGPT session endpoint.
func HasSessionCookieForURL(cookies []Cookie, rawURL string) bool {
	return hasSessionCookieForURLAt(cookies, rawURL, time.Now())
}

func scopeUnscopedCookiesForURL(cookies []Cookie, rawURL string) []Cookie {
	target, errParse := url.Parse(strings.TrimSpace(rawURL))
	if errParse != nil || strings.TrimSpace(target.Hostname()) == "" {
		return append([]Cookie(nil), cookies...)
	}
	host := strings.ToLower(strings.TrimSpace(target.Hostname()))
	secure := strings.EqualFold(target.Scheme, "https")
	scoped := append([]Cookie(nil), cookies...)
	for index := range scoped {
		if strings.TrimSpace(scoped[index].Domain) != "" || strings.TrimSpace(scoped[index].Host) != "" {
			continue
		}
		scoped[index].Domain = host
		scoped[index].Host = host
		if strings.TrimSpace(scoped[index].Path) == "" {
			scoped[index].Path = "/"
		}
		if secure {
			scoped[index].Secure = true
		}
	}
	return scoped
}

func hasSessionCookieForURLAt(cookies []Cookie, rawURL string, now time.Time) bool {
	target, errParse := url.Parse(strings.TrimSpace(rawURL))
	if errParse != nil || strings.TrimSpace(target.Hostname()) == "" {
		return false
	}
	targetHost := strings.ToLower(strings.TrimSpace(target.Hostname()))
	targetPath := strings.TrimRight(target.EscapedPath(), "/") + "/api/auth/session"
	type chunkSet struct {
		values map[int]struct{}
		max    int
	}
	chunks := make(map[string]*chunkSet)
	for i := range cookies {
		name := strings.ToLower(strings.TrimSpace(cookies[i].Name))
		chunkIndex := -1
		if separator := strings.LastIndexByte(name, '.'); separator > 0 && decimalSuffix(name[separator+1:]) {
			parsed, errParse := strconv.Atoi(name[separator+1:])
			if errParse != nil {
				continue
			}
			chunkIndex = parsed
			name = name[:separator]
		}
		switch name {
		case "__secure-next-auth.session-token", "next-auth.session-token", "__secure-authjs.session-token", "authjs.session-token":
			if !chatGPTWebSessionCookieUsable(cookies[i], targetHost, targetPath, now) {
				continue
			}
			if chunkIndex < 0 {
				return true
			}
			key := name + "\x00" + strings.ToLower(strings.TrimSpace(cookies[i].Domain)) + "\x00" + strings.ToLower(strings.TrimSpace(cookies[i].Host)) + "\x00" + strings.TrimSpace(cookies[i].Path)
			set := chunks[key]
			if set == nil {
				set = &chunkSet{values: make(map[int]struct{}), max: chunkIndex}
				chunks[key] = set
			}
			set.values[chunkIndex] = struct{}{}
			if chunkIndex > set.max {
				set.max = chunkIndex
			}
		}
	}
	for _, set := range chunks {
		if set.max < 1 || len(set.values) != set.max+1 {
			continue
		}
		complete := true
		for index := 0; index <= set.max; index++ {
			if _, exists := set.values[index]; !exists {
				complete = false
				break
			}
		}
		if complete {
			return true
		}
	}
	return false
}

func chatGPTWebSessionCookieUsable(cookie Cookie, targetHost, targetPath string, now time.Time) bool {
	// net/http uses zero for an unspecified Max-Age and negative values for deletion.
	if strings.TrimSpace(cookie.Value) == "" || cookie.MaxAge < 0 {
		return false
	}
	domain := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(cookie.Domain)), ".")
	if domain == "" {
		domain = normalizeChatGPTWebCookieHost(cookie.Host)
	}
	if domain == "" {
		return false
	}
	if domain != "" && domain != targetHost && !strings.HasSuffix(targetHost, "."+domain) {
		return false
	}
	path := strings.TrimSpace(cookie.Path)
	if path == "" {
		path = "/"
	}
	if path != "/" && targetPath != path && !strings.HasPrefix(targetPath, strings.TrimSuffix(path, "/")+"/") {
		return false
	}
	expiresAt := time.Time{}
	if value := strings.TrimSpace(cookie.Expires); value != "" {
		parsed, errParse := time.Parse(time.RFC3339Nano, value)
		if errParse != nil {
			return false
		}
		expiresAt = parsed
	} else if value := strings.TrimSpace(cookie.RawExpires); value != "" {
		parsed, errParse := http.ParseTime(value)
		if errParse != nil {
			return false
		}
		expiresAt = parsed
	}
	if expiresAt.IsZero() && cookie.MaxAge > 0 {
		return false
	}
	return expiresAt.IsZero() || expiresAt.After(now)
}

func normalizeChatGPTWebCookieHost(rawHost string) string {
	host := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(rawHost)), ".")
	if parsed, errParse := url.Parse(host); errParse == nil && parsed.Hostname() != "" {
		return strings.ToLower(parsed.Hostname())
	}
	if parsed, errParse := url.Parse("//" + host); errParse == nil && parsed.Hostname() != "" {
		return strings.ToLower(parsed.Hostname())
	}
	if name, _, errSplit := net.SplitHostPort(host); errSplit == nil {
		return strings.ToLower(strings.Trim(name, "[]"))
	}
	return strings.Trim(host, "[]")
}

func decimalSuffix(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}

func credentialModeForStrategy(strategy RefreshStrategy) string {
	switch strategy {
	case RefreshStrategyCodexSource:
		return CredentialModeLinkedCodex
	case RefreshStrategyTokenOnly:
		return CredentialModeTokenOnly
	default:
		return CredentialModeNative
	}
}

func normalizedCredentialLifecycleState(credential *Credential) LifecycleState {
	if credential == nil {
		return LifecycleLoginPending
	}
	if strings.TrimSpace(string(credential.LifecycleState)) == "" {
		if strings.TrimSpace(credential.AccessToken) != "" {
			return LifecycleActive
		}
		return LifecycleLoginPending
	}
	return SafeLifecycleState(string(credential.LifecycleState))
}

type LoginInput struct {
	Email                       string
	Password                    string
	TOTPSecret                  string
	ProxyURL                    string
	LoginProxy                  LoginProxyConfig
	PersistWebAuthn             func(context.Context, WebAuthnCredential) (WebAuthnCredential, error)
	RetryInvalidPasskeyResponse bool
	// LoginProxyResolved keeps one runtime configuration snapshot fixed for the whole flow.
	LoginProxyResolved bool
	Credential         *Credential
	Relogin            bool
}

// LoginProxyConfig configures the dynamic proxy used only during interactive login.
type LoginProxyConfig struct {
	Enabled            bool
	URLTemplate        string
	PlaceholderCharset string
	RotateOnRetry      bool
	RequestAttempts    int
	FlowAttempts       int
	RetryDelay         time.Duration
	AcquisitionTimeout time.Duration
}

type Options struct {
	AuthBaseURL        string
	SessionBaseURL     string
	SentinelBaseURL    string
	RedirectURL        string
	ClientID           string
	Audience           string
	AcquisitionTimeout time.Duration
	Rand               io.Reader
	Now                func() time.Time
	Persona            Persona
}
