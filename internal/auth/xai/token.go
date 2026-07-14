package xai

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/misc"
)

// TokenStorage stores xAI OAuth credentials on disk.
type TokenStorage struct {
	Type          string `json:"type"`
	AccessToken   string `json:"access_token"`
	RefreshToken  string `json:"refresh_token"`
	IDToken       string `json:"id_token,omitempty"`
	TokenType     string `json:"token_type,omitempty"`
	ExpiresIn     int    `json:"expires_in,omitempty"`
	Expire        string `json:"expired,omitempty"`
	LastRefresh   string `json:"last_refresh,omitempty"`
	Email         string `json:"email,omitempty"`
	Subject       string `json:"sub,omitempty"`
	BaseURL       string `json:"base_url,omitempty"`
	RedirectURI   string `json:"redirect_uri,omitempty"`
	TokenEndpoint string `json:"token_endpoint,omitempty"`
	AuthKind      string `json:"auth_kind,omitempty"`
	UsingAPI      bool   `json:"using_api"`
	Websockets    bool   `json:"websockets"`

	Metadata map[string]any `json:"-"`
}

// SetMetadata allows the token store to merge status fields before saving.
func (ts *TokenStorage) SetMetadata(meta map[string]any) {
	ts.Metadata = meta
}

// MetadataSnapshot returns a copy of the currently injected metadata.
func (ts *TokenStorage) MetadataSnapshot() map[string]any {
	if ts == nil || ts.Metadata == nil {
		return nil
	}
	snapshot := make(map[string]any, len(ts.Metadata))
	for key, value := range ts.Metadata {
		snapshot[key] = value
	}
	return snapshot
}

// SaveTokenToFile writes xAI credentials to a JSON auth file.
func (ts *TokenStorage) SaveTokenToFile(authFilePath string) error {
	misc.LogSavingCredentials(authFilePath)
	if errMkdirAll := os.MkdirAll(filepath.Dir(authFilePath), 0o700); errMkdirAll != nil {
		return fmt.Errorf("xai token storage: create directory: %w", errMkdirAll)
	}
	raw, errMarshal := ts.MarshalTokenData()
	if errMarshal != nil {
		return errMarshal
	}
	if errWrite := os.WriteFile(authFilePath, raw, 0o600); errWrite != nil {
		return fmt.Errorf("xai token storage: write token file: %w", errWrite)
	}
	return nil
}

// MarshalTokenData serializes the credential without performing filesystem I/O.
func (ts *TokenStorage) MarshalTokenData() ([]byte, error) {
	ts.Type = "xai"
	ts.AuthKind = "oauth"
	data, errMerge := misc.MergeMetadata(ts, ts.Metadata)
	if errMerge != nil {
		return nil, fmt.Errorf("xai token storage: merge metadata: %w", errMerge)
	}
	raw, errMarshal := json.MarshalIndent(data, "", "  ")
	if errMarshal != nil {
		return nil, fmt.Errorf("xai token storage: marshal token: %w", errMarshal)
	}
	return append(raw, '\n'), nil
}
