package auth

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestApplyFileAuthProjectionMapsPersistedMetadata(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "nested", "codex.json")
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	auth := &Auth{
		ID:       "nested/codex.json",
		Provider: "codex",
		Attributes: map[string]string{
			SourceHashAttributeKey: "persisted-source-hash",
		},
		Metadata: map[string]any{
			"type":            "codex",
			"label":           "Primary account",
			"email":           "person@example.com",
			"prefix":          "/workspace/",
			"proxy_url":       "http://proxy.example",
			"priority":        float64(7),
			"note":            " operator note ",
			"plan_type":       "plus",
			"excluded_models": []any{"local-model", "SHARED-MODEL"},
			"headers": map[string]any{
				"X-Account": " account-value ",
			},
		},
	}
	cfg := &internalconfig.Config{
		OAuthExcludedModels: map[string][]string{
			"codex": {"global-model", "shared-model"},
		},
	}

	if err := ApplyFileAuthProjection(auth, FileAuthProjectionOptions{
		Config:  cfg,
		AuthDir: authDir,
		Path:    path,
		Now:     now,
	}); err != nil {
		t.Fatalf("ApplyFileAuthProjection() error = %v", err)
	}

	if auth.FileName != "nested/codex.json" || auth.Label != "Primary account" {
		t.Fatalf("identity fields = filename %q label %q", auth.FileName, auth.Label)
	}
	if auth.Prefix != "workspace" || auth.ProxyURL != "http://proxy.example" {
		t.Fatalf("routing fields = prefix %q proxy %q", auth.Prefix, auth.ProxyURL)
	}
	wantAttributes := map[string]string{
		SourceHashAttributeKey: "persisted-source-hash",
		"source":               path,
		"path":                 path,
		"email":                "person@example.com",
		"priority":             "7",
		"note":                 "operator note",
		"header:X-Account":     "account-value",
		"excluded_models":      "global-model,local-model,shared-model",
		"auth_kind":            "oauth",
		"plan_type":            "plus",
	}
	for key, want := range wantAttributes {
		if got := auth.Attributes[key]; got != want {
			t.Errorf("attribute %q = %q, want %q", key, got, want)
		}
	}
	if auth.Attributes["excluded_models_hash"] == "" {
		t.Error("excluded_models_hash is empty")
	}
	if !auth.CreatedAt.Equal(now) || !auth.UpdatedAt.Equal(now) {
		t.Fatalf("timestamps = %s %s, want %s", auth.CreatedAt, auth.UpdatedAt, now)
	}
}

func TestApplyFileAuthProjectionUsesProjectIDLabelFallback(t *testing.T) {
	auth := &Auth{
		Provider: "antigravity",
		Metadata: map[string]any{
			"type":       "antigravity",
			"project_id": "project-from-runtime",
		},
	}
	if err := ApplyFileAuthProjection(auth, FileAuthProjectionOptions{}); err != nil {
		t.Fatalf("ApplyFileAuthProjection() error = %v", err)
	}
	if auth.Label != "project-from-runtime" {
		t.Fatalf("label = %q, want project ID fallback", auth.Label)
	}
}

func TestManagerLoadUsesFileAuthProjection(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "codex.json")
	stored := &Auth{
		ID:       "codex.json",
		Provider: "codex",
		FileName: "codex.json",
		Attributes: map[string]string{
			"path":                 path,
			SourceHashAttributeKey: "store-source-hash",
		},
		Metadata: map[string]any{
			"type":      "codex",
			"prefix":    "tenant",
			"priority":  "9",
			"note":      "loaded from store",
			"plan_type": "pro",
		},
	}
	storedSnapshot := stored.Clone()
	store := &fileProjectionTestStore{auths: []*Auth{stored}}
	manager := NewManager(store, nil, nil)
	manager.SetConfig(&internalconfig.Config{
		AuthDir: authDir,
		OAuthExcludedModels: map[string][]string{
			"codex": {"excluded-by-config"},
		},
	})

	if _, err := manager.LoadWithReport(context.Background()); err != nil {
		t.Fatalf("LoadWithReport() error = %v", err)
	}
	got, ok := manager.GetByID("codex.json")
	if !ok {
		t.Fatal("loaded auth not found")
	}
	if got.Prefix != "tenant" || got.Attributes["priority"] != "9" || got.Attributes["note"] != "loaded from store" {
		t.Fatalf("manager projection = prefix %q attributes %#v", got.Prefix, got.Attributes)
	}
	if got.Attributes["excluded_models"] != "excluded-by-config" || got.Attributes["plan_type"] != "pro" {
		t.Fatalf("manager model projection = %#v", got.Attributes)
	}
	if got.Attributes[SourceHashAttributeKey] != "store-source-hash" {
		t.Fatalf("source hash = %q, want store hash", got.Attributes[SourceHashAttributeKey])
	}
	if !reflect.DeepEqual(store.auths[0], storedSnapshot) {
		t.Fatal("manager mutated the store record")
	}
}

type fileProjectionTestStore struct {
	auths []*Auth
}

func (store *fileProjectionTestStore) List(context.Context) ([]*Auth, error) {
	out := make([]*Auth, 0, len(store.auths))
	for _, auth := range store.auths {
		out = append(out, auth.Clone())
	}
	return out, nil
}

func (*fileProjectionTestStore) Save(context.Context, *Auth) (string, error) {
	return "", nil
}

func (*fileProjectionTestStore) Delete(context.Context, string) error {
	return nil
}
