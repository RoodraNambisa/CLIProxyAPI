package auth

import "testing"

func TestCodexAlphaSearchCredentialCapability(t *testing.T) {
	for _, tc := range []struct {
		name string
		auth *Auth
		want bool
	}{
		{"nil", nil, false},
		{"OAuth access", &Auth{Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}}, true},
		{"OAuth refresh", &Auth{Provider: "codex", Metadata: map[string]any{"refresh_token": "fixture"}}, true},
		{"API key default", &Auth{Provider: "codex", Attributes: map[string]string{"api_key": "fixture"}}, false},
		{"API key opt-in", &Auth{Provider: "codex", Attributes: map[string]string{"api_key": "fixture", CodexAlphaSearchAttributeKey: "true"}}, true},
		{"normalized opt-in", &Auth{Provider: " CODEX ", Attributes: map[string]string{"api_key": "fixture", CodexAlphaSearchAttributeKey: " TRUE "}}, true},
		{"disabled opt-in", &Auth{Provider: "codex", Attributes: map[string]string{"api_key": "fixture", CodexAlphaSearchAttributeKey: "false"}}, false},
		{"invalid opt-in", &Auth{Provider: "codex", Attributes: map[string]string{"api_key": "fixture", CodexAlphaSearchAttributeKey: "1"}}, false},
		{"mixed source", &Auth{Provider: "codex", Attributes: map[string]string{"api_key": "fixture"}, Metadata: map[string]any{"access_token": "fixture", "email": "fixture@example.invalid"}}, false},
		{"blank API source still wins", &Auth{Provider: "codex", Attributes: map[string]string{"api_key": " "}, Metadata: map[string]any{"access_token": "fixture"}}, false},
		{"label only", &Auth{Provider: "codex", Metadata: map[string]any{"email": "fixture@example.invalid", "type": "codex"}}, false},
		{"unknown token format", &Auth{Provider: "codex", Metadata: map[string]any{"access_token": 1, "refresh_token": " "}}, false},
		{"other provider", &Auth{Provider: "xai", Attributes: map[string]string{"api_key": "fixture", CodexAlphaSearchAttributeKey: "true"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := SupportsCodexAlphaSearch(tc.auth); got != tc.want {
				t.Fatalf("capability = %t, want %t", got, tc.want)
			}
		})
	}
}
