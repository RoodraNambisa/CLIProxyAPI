package auth

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCredentialWeightFileStoreRejectsInvalidReplacement(t *testing.T) {
	store := NewFileTokenStore()
	store.SetBaseDir(t.TempDir())
	a := &coreauth.Auth{ID: "weight.json", FileName: "weight.json", Provider: "codex", Metadata: map[string]any{"type": "codex", "weight": 0}}
	path, err := store.Save(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{true, 1.5, "9223372036854775808"} {
		next := a.Clone()
		next.Metadata["weight"] = value
		if _, err := store.Save(t.Context(), next); err == nil {
			t.Fatal("invalid weight replaced the credential file")
		}
		after, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(before, after) {
			t.Fatal("rejected save modified credential material")
		}
	}
	reloaded, err := store.List(t.Context())
	if err != nil || len(reloaded) != 1 || reloaded[0].Metadata["weight"] != float64(0) {
		t.Fatal("explicit zero did not survive credential file reload")
	}
	manager := coreauth.NewManager(store, nil, nil)
	if err = manager.Load(t.Context()); err != nil {
		t.Fatal(err)
	}
	installed, ok := manager.GetByID(a.ID)
	if !ok || installed.Attributes[coreauth.AttributeWeight] != "0" {
		t.Fatal("manager load did not project the persisted weight")
	}
}

func TestCredentialWeightFileStoreValidatesRawReadBeforeCoercion(t *testing.T) {
	store := NewFileTokenStore()
	dir := t.TempDir()
	store.SetBaseDir(dir)
	for _, weight := range []string{"-9223372036854775809", "1.5", "null", "0,\"weight\":2"} {
		path := filepath.Join(dir, "invalid.json")
		if err := os.WriteFile(path, []byte(`{"type":"codex","weight":`+weight+`}`), 0600); err != nil {
			t.Fatal(err)
		}
		loaded, err := store.List(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if len(loaded) != 0 {
			t.Fatal("raw invalid weight became a valid runtime credential")
		}
	}
}
