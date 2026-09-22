package util

import (
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestMaskSensitiveHeaderValueMasksManagementKey(t *testing.T) {
	secret := "super-secret-management-key"
	got := MaskSensitiveHeaderValue("X-Management-Key", secret)
	if got == secret {
		t.Fatalf("management key was not masked")
	}
	if !strings.Contains(got, "...") {
		t.Fatalf("masked management key %q does not look masked", got)
	}
}

func TestMaskRealtimeCredentialSubprotocolsWithoutChangingNegotiationNames(t *testing.T) {
	for _, key := range []string{"Sec-WebSocket-Protocol", "sec-websocket-protocol", " SEC-WEBSOCKET-PROTOCOL "} {
		value := " realtime, openai-insecure-api-key.fixture-secret ,openai-organization.fixture-org, openai-project.fixture-project, safe-v2 "
		want := " realtime, openai-insecure-api-key.[REDACTED] ,openai-organization.[REDACTED], openai-project.[REDACTED], safe-v2 "
		if got := MaskSensitiveHeaderValue(key, value); got != want {
			t.Fatal("Realtime authentication subprotocol was not fully redacted")
		}
	}
	for _, value := range []string{"", "realtime, safe-v2", " openai-insecure-api-key ", "safe.openai-insecure-api-key.fixture"} {
		if got := MaskSensitiveHeaderValue("Sec-WebSocket-Protocol", value); got != value {
			t.Fatal("non-credential protocol name changed")
		}
	}
	if got := MaskSensitiveHeaderValue("Sec-WebSocket-Protocol", "OpenAI-Insecure-Api-Key.fixture"); got != "OpenAI-Insecure-Api-Key.[REDACTED]" {
		t.Fatal("mixed-case credential was exposed in logs")
	}
}

func TestMaskSensitiveQueryMasksOAuthCallbackParameters(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "code and state",
			raw:  "code=abcd1234wxyz&state=lmno5678pqrs&scope=openid",
			want: "code=abcd...wxyz&state=lmno...pqrs&scope=openid",
		},
		{
			name: "case array suffix and encoded values",
			raw:  "CoDe%5B%5D=abcd%2Fmiddle%2Fwxyz&STATE[]=lmno-middle-pqrs",
			want: "CoDe%5B%5D=abcd...wxyz&STATE[]=lmno...pqrs",
		},
		{
			name: "similar names remain unchanged",
			raw:  "zipcode=postal-code-value&stateful=session-state-value&decode=encoded-value",
			want: "zipcode=postal-code-value&stateful=session-state-value&decode=encoded-value",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := MaskSensitiveQuery(test.raw); got != test.want {
				t.Fatalf("MaskSensitiveQuery(%q) = %q, want %q", test.raw, got, test.want)
			}
		})
	}
}

func TestOpenAICompatibilityAliasSkipsDisabledProviders(t *testing.T) {
	cfg := &config.Config{
		OpenAICompatibility: []config.OpenAICompatibility{
			{
				Name:     "disabled-provider",
				Disabled: true,
				Models:   []config.OpenAICompatibilityModel{{Name: "upstream-model", Alias: "shared-alias"}},
			},
			{
				Name:   "active-provider",
				Models: []config.OpenAICompatibilityModel{{Name: "other-model", Alias: "active-alias"}},
			},
		},
	}

	if IsOpenAICompatibilityAlias("shared-alias", cfg) {
		t.Fatal("disabled provider alias should not be routable")
	}
	if !IsOpenAICompatibilityAlias("active-alias", cfg) {
		t.Fatal("active provider alias should be routable")
	}
	if compat, model := GetOpenAICompatibilityConfig("shared-alias", cfg); compat != nil || model != nil {
		t.Fatalf("disabled provider config should not resolve, got %#v %#v", compat, model)
	}
	if compat, model := GetOpenAICompatibilityConfig("active-alias", cfg); compat == nil || model == nil || compat.Name != "active-provider" {
		t.Fatalf("active provider config did not resolve: %#v %#v", compat, model)
	}
}

func TestManagedSessionHeadersAreNeverPersistedInLogs(t *testing.T) {
	for _, key := range []string{"Cookie", "Set-Cookie", "cookie", "X-Codex-Turn-State"} {
		if got := MaskSensitiveHeaderValue(key, "secret-value"); got != "[REDACTED]" {
			t.Fatalf("%s leaked: %s", key, got)
		}
	}
}
