package store

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestObjectTokenStoreReadAuthFileSetsCanonicalSourceHash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")
	data := []byte(`{"type":"claude","email":"reader@example.com"}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	store := &ObjectTokenStore{authDir: dir}
	auth, err := store.readAuthFile(path, dir)
	if err != nil {
		t.Fatalf("readAuthFile returned error: %v", err)
	}
	if auth == nil {
		t.Fatal("expected auth to be loaded")
	}
	wantRaw, err := cliproxyauth.CanonicalMetadataBytes(auth)
	if err != nil {
		t.Fatalf("CanonicalMetadataBytes() error = %v", err)
	}
	if got, want := auth.Attributes[cliproxyauth.SourceHashAttributeKey], cliproxyauth.SourceHashFromBytes(wantRaw); got != want {
		t.Fatalf("source hash = %q, want %q", got, want)
	}
	if rawHash := cliproxyauth.SourceHashFromBytes(data); rawHash == auth.Attributes[cliproxyauth.SourceHashAttributeKey] {
		t.Fatal("expected canonical source hash to differ from raw file hash")
	}
}

func TestObjectTokenStoreReadAuthFilePreservesDisabledState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")
	if err := os.WriteFile(path, []byte(`{"type":"claude","email":"reader@example.com","disabled":true}`), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	store := &ObjectTokenStore{authDir: dir}
	auth, err := store.readAuthFile(path, dir)
	if err != nil {
		t.Fatalf("readAuthFile returned error: %v", err)
	}
	if auth == nil {
		t.Fatal("expected auth to be loaded")
	}
	if !auth.Disabled {
		t.Fatal("expected auth to remain disabled")
	}
	if auth.Status != cliproxyauth.StatusDisabled {
		t.Fatalf("status = %q, want %q", auth.Status, cliproxyauth.StatusDisabled)
	}
}

func TestObjectTokenStoreSaveStorageBackedAuthSetsCanonicalSourceHash(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.RawQuery == "location=":
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
		case r.Method == http.MethodPut:
			w.Header().Set("ETag", `"etag"`)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store, err := NewObjectTokenStore(ObjectStoreConfig{
		Endpoint:  strings.TrimPrefix(server.URL, "http://"),
		Bucket:    "test-bucket",
		AccessKey: "test-access",
		SecretKey: "test-secret",
		LocalRoot: t.TempDir(),
		PathStyle: true,
	})
	if err != nil {
		t.Fatalf("NewObjectTokenStore() error = %v", err)
	}

	auth := &cliproxyauth.Auth{
		ID:       "auth.json",
		FileName: "auth.json",
		Provider: "gemini",
		Storage:  &testTokenStorage{},
		Metadata: map[string]any{
			"type":                 "gemini",
			"email":                "writer@example.com",
			"tool_prefix_disabled": true,
		},
	}

	path, err := store.Save(context.Background(), auth)
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	if got, ok := auth.Metadata["access_token"].(string); !ok || got != "tok-storage" {
		t.Fatalf("metadata access_token = %#v, want %q", auth.Metadata["access_token"], "tok-storage")
	}
	if got, ok := auth.Metadata["refresh_token"].(string); !ok || got != "refresh-storage" {
		t.Fatalf("metadata refresh_token = %#v, want %q", auth.Metadata["refresh_token"], "refresh-storage")
	}
	if got, ok := auth.Metadata["tool_prefix_disabled"].(bool); !ok || !got {
		t.Fatalf("metadata tool_prefix_disabled = %#v, want true", auth.Metadata["tool_prefix_disabled"])
	}

	rawFile, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	wantRaw, err := cliproxyauth.CanonicalMetadataBytes(auth)
	if err != nil {
		t.Fatalf("CanonicalMetadataBytes() error = %v", err)
	}
	wantHash := cliproxyauth.SourceHashFromBytes(wantRaw)
	if got := auth.Attributes[cliproxyauth.SourceHashAttributeKey]; got != wantHash {
		t.Fatalf("source hash = %q, want %q", got, wantHash)
	}
	if got, ok := auth.Metadata["disabled"].(bool); !ok || got {
		t.Fatalf("metadata disabled = %#v, want false", auth.Metadata["disabled"])
	}
	if rawHash := cliproxyauth.SourceHashFromBytes(rawFile); rawHash != wantHash {
		t.Fatalf("raw storage file hash = %q, want %q", rawHash, wantHash)
	}
}
