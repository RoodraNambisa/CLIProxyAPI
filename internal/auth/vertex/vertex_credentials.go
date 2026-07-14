// Package vertex provides token storage for Google Vertex AI Gemini via service account credentials.
// It serialises service account JSON into an auth file that is consumed by the runtime executor.
package vertex

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/misc"
)

// VertexCredentialStorage stores the service account JSON for Vertex AI access.
// The content is persisted verbatim under the "service_account" key, together with
// helper fields for project, location and email to improve logging and discovery.
type VertexCredentialStorage struct {
	// ServiceAccount holds the parsed service account JSON content.
	ServiceAccount map[string]any `json:"service_account"`

	// ProjectID is derived from the service account JSON (project_id).
	ProjectID string `json:"project_id"`

	// Email is the client_email from the service account JSON.
	Email string `json:"email"`

	// Location optionally sets a default region (e.g., us-central1) for Vertex endpoints.
	Location string `json:"location,omitempty"`

	// Type is the provider identifier stored alongside credentials. Always "vertex".
	Type string `json:"type"`

	// Prefix optionally namespaces models for this credential (e.g., "teamA").
	// This results in model names like "teamA/gemini-2.0-flash".
	Prefix string `json:"prefix,omitempty"`

	// Metadata holds extra top-level fields that should be merged into the saved
	// credential payload.
	Metadata map[string]any `json:"-"`
}

// SetMetadata allows callers to inject metadata before saving the credential file.
func (s *VertexCredentialStorage) SetMetadata(meta map[string]any) {
	s.Metadata = meta
}

// MetadataSnapshot returns a copy of the currently injected metadata.
func (s *VertexCredentialStorage) MetadataSnapshot() map[string]any {
	if s == nil || s.Metadata == nil {
		return nil
	}
	snapshot := make(map[string]any, len(s.Metadata))
	for key, value := range s.Metadata {
		snapshot[key] = value
	}
	return snapshot
}

// SaveTokenToFile writes the credential payload to the given file path in JSON format.
// It ensures the parent directory exists and logs the operation for transparency.
func (s *VertexCredentialStorage) SaveTokenToFile(authFilePath string) error {
	misc.LogSavingCredentials(authFilePath)
	raw, errMarshal := s.MarshalTokenData()
	if errMarshal != nil {
		return errMarshal
	}
	if err := os.MkdirAll(filepath.Dir(authFilePath), 0o700); err != nil {
		return fmt.Errorf("vertex credential: create directory failed: %w", err)
	}
	if errWrite := os.WriteFile(authFilePath, raw, 0o600); errWrite != nil {
		return fmt.Errorf("vertex credential: write file failed: %w", errWrite)
	}
	return nil
}

// MarshalTokenData serializes the credential without performing filesystem I/O.
func (s *VertexCredentialStorage) MarshalTokenData() ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("vertex credential: storage is nil")
	}
	if s.ServiceAccount == nil {
		return nil, fmt.Errorf("vertex credential: service account content is empty")
	}
	s.Type = "vertex"
	data, errMerge := misc.MergeMetadata(s, s.Metadata)
	if errMerge != nil {
		return nil, fmt.Errorf("vertex credential: merge metadata failed: %w", errMerge)
	}
	raw, errMarshal := json.MarshalIndent(data, "", "  ")
	if errMarshal != nil {
		return nil, fmt.Errorf("vertex credential: marshal failed: %w", errMarshal)
	}
	return append(raw, '\n'), nil
}
