package helps

import (
	"bytes"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestPayloadThinkingAuthorityUsesActualFieldRole(t *testing.T) {
	for _, tc := range []struct {
		name, protocol, body, path string
		value                      any
		filter, want               bool
	}{
		{"same-effort", "openai", `{"reasoning_effort":"low"}`, "reasoning_effort", "low", false, true},
		{"empty-filter", "openai", `{}`, "reasoning_effort", nil, true, true},
		{"parent-replace", "openai-response", `{"reasoning":{"effort":"low"}}`, "reasoning", map[string]any{"summary": "auto"}, false, true},
		{"empty-parent-filter", "openai-response", `{}`, "reasoning", nil, true, true},
		{"summary-only", "openai-response", `{"reasoning":{"effort":"low"}}`, "reasoning.summary", "auto", false, false},
		{"business-json", "openai", `{}`, "tools.0.function.parameters.reasoning_effort", "low", false, false},
		{"escaped-name", "openai", `{"reasoning_effort":"low"}`, `reasoning_\effort`, "low", false, true},
		{"forced-key", "openai-response", `{"reasoning":{"effort":"low"}}`, ":reasoning.:effort", "low", false, true},
		{"literal-dot", "openai-response", `{"reasoning":{"effort":"low"},"reasoning.effort":"low"}`, `reasoning\.effort`, "low", false, false},
		{"wildcard-effort-first", "openai", `{"reasoning_effort":"low","reasoning_extra":"low"}`, "reasoning_*", "low", false, true},
		{"wildcard-business-first", "openai", `{"reasoning_extra":"low","reasoning_effort":"low"}`, "reasoning_*", "low", false, false},
		{"wildcard-nested", "openai-response", `{"reasoning":{"effort":"low"}}`, "reason*.effort", "low", false, true},
		{"absent-child-filter", "openai-response", `{"reasoning":{"effort":"low"}}`, "reasoning.effort.absent", nil, true, false},
		{"changed-child", "openai-response", `{"reasoning":{"effort":{"value":"low"}}}`, "reasoning.effort.value", "high", false, true},
		{"same-child", "openai-response", `{"reasoning":{"effort":{"value":"low"}}}`, "reasoning.effort.value", "low", false, true},
		{"unknown-protocol", "other", `{"reasoning_effort":"low"}`, "reasoning_effort", "high", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{}
			models := []config.PayloadModelRule{{Name: "upstream"}}
			if tc.filter {
				cfg.Payload.Filter = []config.PayloadFilterRule{{Models: models, Params: []string{tc.path}}}
			} else {
				cfg.Payload.Override = []config.PayloadRule{{Models: models, Params: map[string]any{tc.path: tc.value}}}
			}
			body := []byte(tc.body)
			out, got := ApplyPayloadConfigWithThinkingAuthority(cfg, "upstream", tc.protocol, "", body, body, "")
			if got != tc.want || !bytes.Equal(out, ApplyPayloadConfigWithRoot(cfg, "upstream", tc.protocol, "", body, body, "")) {
				t.Fatalf("thinking authority = %t, want %t", got, tc.want)
			}
		})
	}
}
