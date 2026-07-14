// Package claude provides authentication and token management functionality
// for Anthropic's Claude AI services. It handles OAuth2 token storage, serialization,
// and retrieval for maintaining authenticated sessions with the Claude API.
package claude

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/misc"
)

// ClaudeTokenStorage stores OAuth2 token information for Anthropic Claude API authentication.
// It maintains compatibility with the existing auth system while adding Claude-specific fields
// for managing access tokens, refresh tokens, and user account information.
type ClaudeTokenStorage struct {
	// IDToken is the JWT ID token containing user claims and identity information.
	IDToken string `json:"id_token"`

	// AccessToken is the OAuth2 access token used for authenticating API requests.
	AccessToken string `json:"access_token"`

	// RefreshToken is used to obtain new access tokens when the current one expires.
	RefreshToken string `json:"refresh_token"`

	// LastRefresh is the timestamp of the last token refresh operation.
	LastRefresh string `json:"last_refresh"`

	// Email is the Anthropic account email address associated with this token.
	Email string `json:"email"`

	// Type indicates the authentication provider type, always "claude" for this storage.
	Type string `json:"type"`

	// Expire is the timestamp when the current access token expires.
	Expire string `json:"expired"`

	// Metadata holds arbitrary key-value pairs injected via hooks.
	// It is not exported to JSON directly to allow flattening during serialization.
	Metadata map[string]any `json:"-"`
}

// SetMetadata allows external callers to inject metadata into the storage before saving.
func (ts *ClaudeTokenStorage) SetMetadata(meta map[string]any) {
	ts.Metadata = meta
}

// MetadataSnapshot returns a copy of the currently injected metadata.
func (ts *ClaudeTokenStorage) MetadataSnapshot() map[string]any {
	if ts == nil || ts.Metadata == nil {
		return nil
	}
	snapshot := make(map[string]any, len(ts.Metadata))
	for key, value := range ts.Metadata {
		snapshot[key] = value
	}
	return snapshot
}

// SaveTokenToFile serializes the Claude token storage to a JSON file.
// This method creates the necessary directory structure and writes the token
// data in JSON format to the specified file path for persistent storage.
// It merges any injected metadata into the top-level JSON object.
//
// Parameters:
//   - authFilePath: The full path where the token file should be saved
//
// Returns:
//   - error: An error if the operation fails, nil otherwise
func (ts *ClaudeTokenStorage) SaveTokenToFile(authFilePath string) error {
	misc.LogSavingCredentials(authFilePath)
	if err := os.MkdirAll(filepath.Dir(authFilePath), 0700); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}
	raw, errMarshal := ts.MarshalTokenData()
	if errMarshal != nil {
		return errMarshal
	}
	if errWrite := os.WriteFile(authFilePath, raw, 0o600); errWrite != nil {
		return fmt.Errorf("failed to write token to file: %w", errWrite)
	}
	return nil
}

// MarshalTokenData serializes the credential without performing filesystem I/O.
func (ts *ClaudeTokenStorage) MarshalTokenData() ([]byte, error) {
	ts.Type = "claude"
	data, errMerge := misc.MergeMetadata(ts, ts.Metadata)
	if errMerge != nil {
		return nil, fmt.Errorf("failed to merge metadata: %w", errMerge)
	}
	raw, errMarshal := json.Marshal(data)
	if errMarshal != nil {
		return nil, fmt.Errorf("failed to marshal token: %w", errMarshal)
	}
	return append(raw, '\n'), nil
}
