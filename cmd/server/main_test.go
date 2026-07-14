package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveDefaultConfigPath(t *testing.T) {
	t.Run("prefers new data path when both exist", func(t *testing.T) {
		dir := t.TempDir()
		newPath := filepath.Join(dir, "data", "config.yaml")
		oldPath := filepath.Join(dir, "config.yaml")
		if err := os.MkdirAll(filepath.Dir(newPath), 0o755); err != nil {
			t.Fatalf("mkdir data dir: %v", err)
		}
		if err := os.WriteFile(newPath, []byte("port: 8317\n"), 0o600); err != nil {
			t.Fatalf("write new config: %v", err)
		}
		if err := os.WriteFile(oldPath, []byte("port: 8318\n"), 0o600); err != nil {
			t.Fatalf("write old config: %v", err)
		}

		if got := resolveDefaultConfigPath(dir); got != newPath {
			t.Fatalf("resolveDefaultConfigPath() = %s, want %s", got, newPath)
		}
	})

	t.Run("falls back to legacy path when only old exists", func(t *testing.T) {
		dir := t.TempDir()
		oldPath := filepath.Join(dir, "config.yaml")
		if err := os.WriteFile(oldPath, []byte("port: 8317\n"), 0o600); err != nil {
			t.Fatalf("write old config: %v", err)
		}

		if got := resolveDefaultConfigPath(dir); got != oldPath {
			t.Fatalf("resolveDefaultConfigPath() = %s, want %s", got, oldPath)
		}
	})

	t.Run("defaults to new data path when none exists", func(t *testing.T) {
		dir := t.TempDir()
		want := filepath.Join(dir, "data", "config.yaml")
		if got := resolveDefaultConfigPath(dir); got != want {
			t.Fatalf("resolveDefaultConfigPath() = %s, want %s", got, want)
		}
	})
}

func TestDeprecatedGeminiCLIFlagsError(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantError bool
	}{
		{name: "no retired flags"},
		{name: "retired login", args: []string{"--login"}, wantError: true},
		{name: "explicitly disabled retired login", args: []string{"--login=false"}},
		{name: "retired project", args: []string{"--project_id=legacy-project"}, wantError: true},
		{name: "empty retired project", args: []string{"--project_id="}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flagSet := flag.NewFlagSet(tt.name, flag.ContinueOnError)
			flagSet.Bool("login", false, "")
			flagSet.String("project_id", "", "")
			if errParse := flagSet.Parse(tt.args); errParse != nil {
				t.Fatalf("Parse() error = %v", errParse)
			}
			if got := deprecatedGeminiCLIFlagsError(flagSet); (got != nil) != tt.wantError {
				t.Fatalf("deprecatedGeminiCLIFlagsError() error = %v, wantError %t", got, tt.wantError)
			}
		})
	}
}
