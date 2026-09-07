package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

type savedCredentialIdentityRow struct {
	Key    string                       `yaml:"api-key"`
	Name   string                       `yaml:"name"`
	Extra  string                       `yaml:"future-option"`
	Models []savedCredentialIdentityRow `yaml:"models"`
	Keys   []savedCredentialIdentityRow `yaml:"api-key-entries"`
}

func loadSavedCredentialIdentityRows(t *testing.T, path, family string) []savedCredentialIdentityRow {
	t.Helper()
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]yaml.Node
	if err := yaml.Unmarshal(saved, &raw); err != nil {
		t.Fatal(err)
	}
	sequence := raw[family]
	var rows []savedCredentialIdentityRow
	if err := sequence.Decode(&rows); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestCredentialYAMLIdentityIgnoresUnknownNames(t *testing.T) {
	for _, family := range []struct{ yaml, field string }{
		{"gemini-api-key", "GeminiKey"}, {"interactions-api-key", "InteractionsKey"}, {"claude-api-key", "ClaudeKey"}, {"codex-api-key", "CodexKey"}, {"vertex-api-key", "VertexCompatAPIKey"},
	} {
		t.Run(family.yaml, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			body := family.yaml + ":\n"
			for _, key := range []string{"first", "second"} {
				body += fmt.Sprintf("  - {api-key: %s, base-url: 'https://example.test', weight: 4, name: custom-name, id: custom-id, alias: custom-alias, future-option: %s}\n", key, key)
			}
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			if err != nil {
				t.Fatal(err)
			}
			entries := reflect.ValueOf(cfg).Elem().FieldByName(family.field)
			reordered := reflect.MakeSlice(entries.Type(), 2, 2)
			reordered.Index(0).Set(entries.Index(1))
			reordered.Index(1).Set(entries.Index(0))
			entries.Set(reordered)
			zero := 0
			entries.Index(0).FieldByName("Weight").Set(reflect.ValueOf(&zero))
			if err := SaveConfigPreserveComments(path, cfg); err != nil {
				t.Fatal(err)
			}
			rows := loadSavedCredentialIdentityRows(t, path, family.yaml)
			if len(rows) != 2 || rows[0].Key != "second" || rows[0].Extra != "second" || rows[1].Extra != "first" || rows[0].Name != "custom-name" {
				t.Fatal("unknown identity-like fields detached extensions from their credential")
			}
		})
	}
}

func TestCredentialYAMLIdentityDistinguishesSameKeyDifferentTargets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := "codex-api-key:\n  - {api-key: shared, base-url: 'https://first.test', future-option: first}\n  - {api-key: shared, base-url: 'https://second.test', future-option: second}\nopenai-compatibility:\n  - name: compat\n    base-url: https://example.test\n    api-key-entries:\n      - {api-key: shared, proxy-url: 'http://first.test', future-option: first}\n      - {api-key: shared, proxy-url: 'http://second.test', future-option: second}\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.CodexKey[0], cfg.CodexKey[1] = cfg.CodexKey[1], cfg.CodexKey[0]
	keys := cfg.OpenAICompatibility[0].APIKeyEntries
	keys[0], keys[1] = keys[1], keys[0]
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	rows := loadSavedCredentialIdentityRows(t, path, "codex-api-key")
	providers := loadSavedCredentialIdentityRows(t, path, "openai-compatibility")
	if len(rows) != 2 || rows[0].Extra != "second" || rows[1].Extra != "first" || len(providers) != 1 || len(providers[0].Keys) != 2 || providers[0].Keys[0].Extra != "second" || providers[0].Keys[1].Extra != "first" {
		t.Fatal("reordering equal API keys mixed extensions between different targets")
	}
}

func TestCredentialYAMLProviderAndModelIdentityIgnoresUnknownIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := "openai-compatibility:\n  - name: first\n    id: custom\n    base-url: https://example.test\n    future-option: first\n    models:\n      - {name: model-a, alias: a, id: custom, future-option: a}\n      - {name: model-b, alias: b, id: custom, future-option: b}\n  - {name: second, id: custom, base-url: 'https://example.test', future-option: second}\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	models := cfg.OpenAICompatibility[0].Models
	models[0], models[1] = models[1], models[0]
	cfg.OpenAICompatibility[0], cfg.OpenAICompatibility[1] = cfg.OpenAICompatibility[1], cfg.OpenAICompatibility[0]
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	rows := loadSavedCredentialIdentityRows(t, path, "openai-compatibility")
	if len(rows) != 2 || rows[0].Extra != "second" || rows[1].Extra != "first" || len(rows[1].Models) != 2 || rows[1].Models[0].Extra != "b" || rows[1].Models[1].Extra != "a" {
		t.Fatal("unknown IDs replaced provider or model identity during save")
	}
}

func TestCredentialYAMLIdentityAllowsUniqueTargetEdit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := "codex-api-key:\n  - {api-key: test, base-url: 'https://before.test', future-option: retained}\n"
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.CodexKey[0].BaseURL = "https://after.test"
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	rows := loadSavedCredentialIdentityRows(t, path, "codex-api-key")
	if len(rows) != 1 || rows[0].Extra != "retained" {
		t.Fatal("editing the unique credential's target dropped its extensions")
	}
}
