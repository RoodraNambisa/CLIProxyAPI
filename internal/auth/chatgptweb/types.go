package chatgptweb

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

type LifecycleState string

type Persona struct {
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
	defaults := DefaultPersona()
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
	Type               string         `json:"type"`
	Email              string         `json:"email"`
	Password           string         `json:"password"`
	TOTPSecret         string         `json:"totp_secret"`
	AccessToken        string         `json:"access_token"`
	RefreshToken       string         `json:"refresh_token"`
	IDToken            string         `json:"id_token"`
	Expired            string         `json:"expired"`
	Cookies            []Cookie       `json:"cookies"`
	Persona            Persona        `json:"persona"`
	DeviceID           string         `json:"device_id,omitempty"`
	SessionID          string         `json:"session_id,omitempty"`
	LifecycleState     LifecycleState `json:"lifecycle_state"`
	LifecycleReason    string         `json:"lifecycle_reason"`
	LifecycleUpdatedAt string         `json:"lifecycle_updated_at"`
	LastLoginAt        string         `json:"last_login_at"`
	LastRefreshAt      string         `json:"last_refresh_at"`
	LastReloginAt      string         `json:"last_relogin_at"`
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
	credential.Persona = normalizePersona(credential.Persona)
	if credential.Cookies == nil {
		credential.Cookies = []Cookie{}
	}
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
	metadata["email"] = credential.Email
	metadata["password"] = credential.Password
	metadata["totp_secret"] = credential.TOTPSecret
	metadata["access_token"] = credential.AccessToken
	metadata["refresh_token"] = credential.RefreshToken
	metadata["id_token"] = credential.IDToken
	metadata["expired"] = credential.Expired
	metadata["cookies"] = credential.Cookies
	metadata["persona"] = normalizePersona(credential.Persona)
	metadata["device_id"] = credential.DeviceID
	metadata["session_id"] = credential.SessionID
	metadata["lifecycle_state"] = string(credential.LifecycleState)
	metadata["lifecycle_reason"] = credential.LifecycleReason
	metadata["lifecycle_updated_at"] = credential.LifecycleUpdatedAt
	metadata["last_login_at"] = credential.LastLoginAt
	metadata["last_refresh_at"] = credential.LastRefreshAt
	metadata["last_relogin_at"] = credential.LastReloginAt
}

type LoginInput struct {
	Email      string
	Password   string
	TOTPSecret string
	ProxyURL   string
	Credential *Credential
	Relogin    bool
}

type Options struct {
	AuthBaseURL        string
	SentinelBaseURL    string
	RedirectURL        string
	ClientID           string
	Audience           string
	AcquisitionTimeout time.Duration
	Rand               io.Reader
	Now                func() time.Time
	Persona            Persona
}
