package config

import (
	"os"
	"strings"
	"sync"
	"testing"
)

func TestContainsDeprecatedAmpConfig(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want bool
	}{
		{name: "ampcode block", yaml: "ampcode:\n  upstream-url: https://ampcode.com\n", want: true},
		{name: "legacy key", yaml: "amp-upstream-api-key: secret\n", want: true},
		{name: "merged block", yaml: "defaults: &defaults\n  ampcode:\n    upstream-url: https://ampcode.com\n<<: *defaults\n", want: true},
		{name: "unrelated text value", yaml: "proxy-url: https://ampcode.com\n", want: false},
		{name: "invalid yaml", yaml: "ampcode: [\n", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := containsDeprecatedAmpConfig([]byte(test.yaml)); got != test.want {
				t.Fatalf("containsDeprecatedAmpConfig() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestWarnDeprecatedAmpConfigOnce(t *testing.T) {
	var once sync.Once
	warnings := 0
	warn := func() { warnings++ }
	data := []byte("ampcode:\n  upstream-url: https://ampcode.com\n")
	warnDeprecatedAmpConfigOnce(data, &once, warn)
	warnDeprecatedAmpConfigOnce(data, &once, warn)
	if warnings != 1 {
		t.Fatalf("warnings = %d, want 1", warnings)
	}
}

func TestLoadConfigIgnoresDeprecatedAmpConfig(t *testing.T) {
	path := writeTempConfig(t, `
port: 8317
ampcode:
  upstream-url: https://ampcode.com
`)

	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	if cfg.Port != 8317 {
		t.Fatalf("Port = %d, want 8317", cfg.Port)
	}
}

func TestSaveConfigPreservesIgnoredAmpConfig(t *testing.T) {
	path := writeTempConfig(t, `
port: 8317
defaults: &defaults
  ampcode:
    upstream-url: https://nested.example
    upstream-api-key: nested-sentinel
<<: *defaults
amp-upstream-url: https://flat.example
amp-upstream-api-key: flat-sentinel
amp-restrict-management-to-localhost: true
amp-model-mappings:
  - from: old-model
    to: new-model
`)
	cfg, err := LoadConfigOptional(path, false)
	if err != nil {
		t.Fatalf("LoadConfigOptional() error = %v", err)
	}
	cfg.Port = 9000
	if err = SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatalf("SaveConfigPreserveComments() error = %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	saved := string(data)
	for _, value := range []string{
		"ampcode:",
		"https://nested.example",
		"nested-sentinel",
		"<<: *defaults",
		"amp-upstream-url: https://flat.example",
		"amp-upstream-api-key: flat-sentinel",
		"amp-restrict-management-to-localhost: true",
		"from: old-model",
		"to: new-model",
	} {
		if !strings.Contains(saved, value) {
			t.Fatalf("saved config removed ignored Amp value %q:\n%s", value, saved)
		}
	}
}
