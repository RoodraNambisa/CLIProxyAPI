package helps

import (
	"bytes"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/tidwall/gjson"
)

func TestCodexServiceTierPolicyAndAdminRules(t *testing.T) {
	models := []config.PayloadModelRule{{Name: "client-alias", Protocol: "codex"}}
	rule := config.PayloadRule{Models: models, Params: map[string]any{"service_tier": "flex"}}
	rawRule := config.PayloadRule{Models: models, Params: map[string]any{"service_tier": `"flex"`}}
	for _, tc := range []struct {
		name   string
		allow  bool
		policy config.PayloadConfig
		want   string
	}{
		{name: "default-off"},
		{name: "allow", allow: true, want: "priority"},
		{name: "admin-default", policy: config.PayloadConfig{Default: []config.PayloadRule{rule}}, want: "flex"},
		{name: "admin-default-allowed-client", allow: true, policy: config.PayloadConfig{Default: []config.PayloadRule{rule}}, want: "priority"},
		{name: "admin-default-raw", policy: config.PayloadConfig{DefaultRaw: []config.PayloadRule{rawRule}}, want: "flex"},
		{name: "admin-override", policy: config.PayloadConfig{Override: []config.PayloadRule{rule}}, want: "flex"},
		{name: "admin-override-allowed-client", allow: true, policy: config.PayloadConfig{Override: []config.PayloadRule{rule}}, want: "flex"},
		{name: "admin-override-raw", allow: true, policy: config.PayloadConfig{OverrideRaw: []config.PayloadRule{rawRule}}, want: "flex"},
		{name: "admin-filter", allow: true, policy: config.PayloadConfig{Filter: []config.PayloadFilterRule{{Models: models, Params: []string{"service_tier"}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"service_tier":"priority","parallel_tool_calls":false,"include":["output.encrypted_content"]}`)
			original := bytes.Clone(body)
			out := ApplyCodexPayloadConfig(&config.Config{Payload: tc.policy}, tc.allow, "upstream-model", "codex", body, original, "client-alias")
			if got := gjson.GetBytes(out, "service_tier").String(); got != tc.want {
				t.Fatalf("service_tier=%q, want %q", got, tc.want)
			}
			if !bytes.Equal(body, original) {
				t.Fatal("policy mutated the original payload")
			}
			if gjson.GetBytes(out, "parallel_tool_calls").Raw != "false" || gjson.GetBytes(out, "include.0").String() != "output.encrypted_content" {
				t.Fatal("unrelated request controls changed")
			}
		})
	}
}

func TestCodexServiceTierPolicyRemovesDuplicateAndEscapedKeys(t *testing.T) {
	for _, body := range []string{
		`{"service_tier":"priority","service_tier":"flex","input":[]}`,
		`{"service_tier":null,"service\u005ftier":"priority","input":[]}`,
		`{"service\u005ftier":"priority","service_tier":"flex","input":[]}`,
	} {
		out := ApplyCodexPayloadConfig(nil, false, "", "", []byte(body), nil, "")
		if !gjson.ValidBytes(out) || gjson.GetBytes(out, "service_tier").Exists() || !gjson.GetBytes(out, "input").IsArray() {
			t.Fatalf("tier policy bypass or malformed output: %s", out)
		}
	}
}
