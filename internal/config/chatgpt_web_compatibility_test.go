package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelcompat"
)

func TestSentinelCompatibilitySaveReload(t *testing.T) {
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte("port: 8317\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ChatGPTWeb.Sentinel.GoVMCompatibility = sentinelcompat.Config{Enabled: true, EnvironmentProperties: []sentinelcompat.Property{
		{Path: "window.__flag", Type: "boolean", Value: false}, {Path: "window.__count", Type: "number", Value: 0}, {Path: "window.__none", Type: "null"}, {Path: "window.__missing", Type: "undefined"},
	}}
	before, err := sentinelcompat.Compile(cfg.ChatGPTWeb.Sentinel.GoVMCompatibility)
	if err != nil {
		t.Fatal(err)
	}
	if err = SaveConfigPreserveComments(p, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	after, err := sentinelcompat.Compile(loaded.ChatGPTWeb.Sentinel.GoVMCompatibility)
	if err != nil || before.Version() != after.Version() {
		t.Fatalf("save altered rules: %v", err)
	}
	loaded.ChatGPTWeb.Sentinel.GoVMCompatibility.EnvironmentProperties = []sentinelcompat.Property{}
	loaded.ChatGPTWeb.Sentinel.GoVMCompatibility.Enabled = false
	if err = SaveConfigPreserveComments(p, loaded); err != nil {
		t.Fatal(err)
	}
	loaded, err = LoadConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ChatGPTWeb.Sentinel.GoVMCompatibility.Enabled || len(loaded.ChatGPTWeb.Sentinel.GoVMCompatibility.EnvironmentProperties) != 0 {
		t.Fatal("explicit clear/disable lost")
	}
}
