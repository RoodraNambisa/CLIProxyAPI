package vertex

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestVertexCredentialStorageSaveTokenToFile_InvalidStorageDoesNotCreateDirectory(t *testing.T) {
	tests := []struct {
		name    string
		storage *VertexCredentialStorage
	}{
		{name: "nil storage"},
		{name: "empty service account", storage: &VertexCredentialStorage{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parent := filepath.Join(t.TempDir(), "missing")
			path := filepath.Join(parent, "vertex.json")
			if errSave := test.storage.SaveTokenToFile(path); errSave == nil {
				t.Fatal("SaveTokenToFile() accepted invalid storage")
			}
			if _, errStat := os.Stat(parent); !errors.Is(errStat, os.ErrNotExist) {
				t.Fatalf("invalid storage created parent directory: %v", errStat)
			}
		})
	}
}

func TestVertexCredentialStorageSaveTokenToFile_MergesMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vertex.json")
	storage := &VertexCredentialStorage{
		ServiceAccount: map[string]any{
			"type":         "service_account",
			"project_id":   "vertex-project",
			"client_email": "vertex@example.com",
		},
		ProjectID: "vertex-project",
		Email:     "vertex@example.com",
		Location:  "us-central1",
		Prefix:    "team-a",
		Metadata: map[string]any{
			"label":                "vertex-label",
			"tool_prefix_disabled": true,
		},
	}

	if err := storage.SaveTokenToFile(path); err != nil {
		t.Fatalf("SaveTokenToFile() error = %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}

	if got, ok := payload["label"].(string); !ok || got != "vertex-label" {
		t.Fatalf("label = %#v, want %q", payload["label"], "vertex-label")
	}
	if got, ok := payload["tool_prefix_disabled"].(bool); !ok || !got {
		t.Fatalf("tool_prefix_disabled = %#v, want true", payload["tool_prefix_disabled"])
	}
	if got, ok := payload["type"].(string); !ok || got != "vertex" {
		t.Fatalf("type = %#v, want %q", payload["type"], "vertex")
	}
}
