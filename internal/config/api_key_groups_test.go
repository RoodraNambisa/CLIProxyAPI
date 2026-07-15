package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestNormalizeAPIKeyGroups(t *testing.T) {
	groups, err := NormalizeAPIKeyGroups([]APIKeyGroup{
		{APIKey: " key-a ", Providers: []string{" Codex ", "xAI", "codex"}},
		{APIKey: "key-b", Providers: []string{"all", "claude"}},
		{APIKey: "key-c", Providers: []string{"*"}},
	}, []string{"key-a", "key-b", "key-c"})
	if err != nil {
		t.Fatalf("NormalizeAPIKeyGroups() error = %v", err)
	}
	want := []APIKeyGroup{{APIKey: "key-a", Providers: []string{"codex", "xai"}}}
	if !reflect.DeepEqual(groups, want) {
		t.Fatalf("NormalizeAPIKeyGroups() = %#v, want %#v", groups, want)
	}
}

func TestNormalizeAPIKeyGroupsRejectsUnknownKey(t *testing.T) {
	_, err := NormalizeAPIKeyGroups([]APIKeyGroup{{APIKey: "missing", Providers: []string{"codex"}}}, []string{"known"})
	if err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("NormalizeAPIKeyGroups() error = %v, want unknown key error", err)
	}
	groups, errPrune := PruneAPIKeyGroups([]APIKeyGroup{{APIKey: "missing", Providers: []string{"codex"}}}, []string{"known"})
	if errPrune != nil || len(groups) != 0 {
		t.Fatalf("PruneAPIKeyGroups() = %#v, %v; want empty", groups, errPrune)
	}
}

func TestPruneAPIKeyGroupsDropsDuplicateUnknownKeys(t *testing.T) {
	groups, err := PruneAPIKeyGroups([]APIKeyGroup{
		{APIKey: "missing", Providers: []string{"codex"}},
		{APIKey: "missing", Providers: []string{"xai"}},
	}, []string{"known"})
	if err != nil || len(groups) != 0 {
		t.Fatalf("PruneAPIKeyGroups() = %#v, %v; want empty", groups, err)
	}
}

func TestNormalizeAPIKeyGroupsRejectsDuplicateKey(t *testing.T) {
	_, err := NormalizeAPIKeyGroups([]APIKeyGroup{
		{APIKey: "same", Providers: []string{"codex"}},
		{APIKey: " same ", Providers: []string{"xai"}},
	}, []string{"same"})
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("NormalizeAPIKeyGroups() error = %v, want duplicate error", err)
	}
}

func TestLoadConfigNormalizesAPIKeyGroups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := []byte(`api-keys:
  - key-a
api-key-groups:
  - api-key: key-a
    providers: [Codex, XAI, codex]
`)
	if errWrite := os.WriteFile(path, body, 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}

	cfg, errLoad := LoadConfig(path)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	want := []APIKeyGroup{{APIKey: "key-a", Providers: []string{"codex", "xai"}}}
	if !reflect.DeepEqual(cfg.APIKeyGroups, want) {
		t.Fatalf("APIKeyGroups = %#v, want %#v", cfg.APIKeyGroups, want)
	}
}

func TestSaveConfigPreserveCommentsNormalizesAPIKeyGroups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(path, []byte("api-keys: [key-a]\napi-key-groups: []\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg := &Config{SDKConfig: SDKConfig{
		APIKeys: []string{"key-a"},
		APIKeyGroups: []APIKeyGroup{
			{APIKey: " key-a ", Providers: []string{"XAI", "xai"}},
		},
	}}
	if errSave := SaveConfigPreserveComments(path, cfg); errSave != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", errSave)
	}
	loaded, errLoad := LoadConfig(path)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	want := []APIKeyGroup{{APIKey: "key-a", Providers: []string{"xai"}}}
	if !reflect.DeepEqual(loaded.APIKeyGroups, want) {
		t.Fatalf("saved APIKeyGroups = %#v, want %#v", loaded.APIKeyGroups, want)
	}
}

func TestLoadConfigRejectsUnknownAPIKeyGroup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	body := []byte(`api-keys: [known]
api-key-groups:
  - api-key: missing
    providers: [codex]
`)
	if errWrite := os.WriteFile(path, body, 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	if _, errLoad := LoadConfig(path); errLoad == nil || !strings.Contains(errLoad.Error(), "unknown") {
		t.Fatalf("LoadConfig() error = %v, want unknown key error", errLoad)
	}
	if _, errLoad := LoadConfigOptional(path, true); errLoad == nil || !strings.Contains(errLoad.Error(), "unknown") {
		t.Fatalf("LoadConfigOptional(optional=true) error = %v, want unknown key error", errLoad)
	}
}
