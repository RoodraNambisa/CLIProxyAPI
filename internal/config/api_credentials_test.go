package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestAPICredentialIndexIsolatedByClone(t *testing.T) {
	cfg := &Config{CodexKey: []CodexKey{{APIManagement: APIManagement{CredentialID: "id"}, APIKey: "old", BaseURL: "https://example.invalid"}}}
	cfg.IndexAPICredentials()
	copy, err := Clone(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ref, ok := copy.APICredentialByID("id")
	if !ok {
		t.Fatal("index lost")
	}
	ref.Value.(*CodexKey).APIKey = "new"
	original, _ := cfg.APICredentialByID("id")
	if original.Value.(*CodexKey).APIKey != "old" {
		t.Fatal("index retained mutable source")
	}
}

func BenchmarkAPICredentialLookup(b *testing.B) {
	for _, n := range []int{10000, 100000} {
		b.Run(fmt.Sprintf("n_%d", n), func(b *testing.B) {
			cfg := &Config{CodexKey: make([]CodexKey, n)}
			for i := range cfg.CodexKey {
				cfg.CodexKey[i].CredentialID = fmt.Sprintf("credential-%d", i)
			}
			cfg.IndexAPICredentials()
			id := cfg.CodexKey[n-1].CredentialID
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := cfg.APICredentialByID(id); !ok {
					b.Fatal("missing credential")
				}
			}
		})
	}
}

func TestManagedCredentialYAMLRetainsExtensionsOnRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	original := "codex-api-key:\n  - credential-id: stable-id\n    api-key: old-key # credential comment\n    base-url: https://old.invalid\n    channel-name: Original\n    future-option: keep-me\n"
	if err := os.WriteFile(path, []byte(original), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.CodexKey[0].APIKey = "new-key"
	cfg.CodexKey[0].BaseURL = "https://new.invalid"
	cfg.CodexKey[0].ChannelName = "New name"
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"future-option: keep-me", "# credential comment", "stable-id", "new-key"} {
		if !strings.Contains(string(raw), expected) {
			t.Fatalf("lost %s", expected)
		}
	}
}
