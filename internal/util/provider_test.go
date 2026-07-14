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
