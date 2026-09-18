package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResponseModelRewriteConfigRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("# fixture\nresponse-model-rewrite:\n  enabled: true\n  rules:\n    - providers: [codex]\n      auth-priorities: [0, 3]\n      credential-ids: [abc123]\n      request-models: [gpt-6-astra]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.ResponseModelRewrite.Enabled || len(cfg.ResponseModelRewrite.Rules) != 1 {
		t.Fatal("configuration lost")
	}
	cloned, err := Clone(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cloned.ResponseModelRewrite.Rules[0].Providers[0] = "xai"
	if cfg.ResponseModelRewrite.Rules[0].Providers[0] != "codex" {
		t.Fatal("clone aliases source")
	}
	cfg.ResponseModelRewrite = ResponseModelRewriteConfig{}
	if err = SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ResponseModelRewrite.Enabled || len(loaded.ResponseModelRewrite.Rules) != 0 {
		t.Fatal("removed configuration survived")
	}
	cfg.ResponseModelRewrite.Rules = []ResponseModelRewriteRule{{RequestModels: []string{"bad\nmodel"}}}
	if cfg.ValidateResponseModelRewrite() == nil {
		t.Fatal("invalid matcher accepted")
	}
}
