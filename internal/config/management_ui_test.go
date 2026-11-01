package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManagementUIDisplayPreferencesRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("# shared settings\nport: 8317\nunknown-setting: preserve\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ManagementUI.APIKeyTraffic != nil || cfg.ManagementUI.CredentialTraffic != nil {
		t.Fatal("omitted settings must inherit visible defaults")
	}
	hidden, shown := false, true
	cfg.ManagementUI.APIKeyTraffic = &hidden
	cfg.ManagementUI.CredentialTraffic = &shown
	if err := SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	reloaded, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.ManagementUI.APIKeyTraffic == nil || *reloaded.ManagementUI.APIKeyTraffic ||
		reloaded.ManagementUI.CredentialTraffic == nil || !*reloaded.ManagementUI.CredentialTraffic {
		t.Fatal("shared display preferences did not survive reload")
	}
	body, err := json.Marshal(reloaded)
	if err != nil || !strings.Contains(string(body), `"management-ui":{"api-key-traffic":false,"credential-traffic":true}`) {
		t.Fatalf("management response omitted explicit preferences: %v", err)
	}
	saved, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(saved), "# shared settings") || !strings.Contains(string(saved), "unknown-setting: preserve") {
		t.Fatal("saving preferences changed unrelated YAML")
	}
}
