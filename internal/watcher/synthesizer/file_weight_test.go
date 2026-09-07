package synthesizer

import (
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestFileSynthesizerWeightPrecisionAndDefaults(t *testing.T) {
	ctx := &SynthesisContext{Config: &config.Config{}, AuthDir: t.TempDir(), IDGenerator: NewStableIDGenerator()}
	path := filepath.Join(ctx.AuthDir, "weight.json")
	for _, weight := range []string{"1.5", "1e2", "true", "null", "1000001", "-9223372036854775809", "0,\"weight\":2"} {
		if got := synthesizeFileAuths(ctx, path, []byte(`{"type":"codex","weight":`+weight+`}`)); len(got) != 0 {
			t.Fatal("watcher accepted invalid or duplicate weight before numeric coercion")
		}
	}
	for _, tc := range []struct {
		body, want string
		present    bool
	}{
		{`{"type":"codex"}`, "", false},
		{`{"type":"codex","weight":0}`, "0", true},
		{`{"type":"codex","weight":-2}`, "0", true},
		{`{"type":"codex","weight":"12"}`, "12", true},
	} {
		got := synthesizeFileAuths(ctx, path, []byte(tc.body))
		if len(got) != 1 {
			t.Fatal("valid file was not synthesized")
		}
		value, present := got[0].Attributes["weight"]
		if value != tc.want || present != tc.present {
			t.Fatal("watcher lost weight or default inheritance")
		}
	}
}
