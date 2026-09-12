package auth

import (
	"strings"
	"testing"
)

func TestCredentialLogIdentity(t *testing.T) {
	for _, tt := range []struct{ file, label, want string }{
		{"/private/auths/codex-fixture@example.test.json", "account label", "codex-fixture@example.test.json"},
		{`C:\auths\fixture.json`, "", "fixture.json"},
		{"", "My API account", "My API account"},
		{"", "opaque-token-fixture\nnext", "<redacted-key> next"},
		{"", "", ""},
	} {
		a := &Auth{ID: "identity-fixture", Provider: "codex", FileName: tt.file, Label: tt.label, Metadata: map[string]any{"access_token": "opaque-token-fixture"}}
		identity := a.LogIdentity()
		if identity.Name != tt.want || identity.Index == "" || identity.Provider != "codex" {
			t.Fatalf("identity = %+v, want name %q", identity, tt.want)
		}
	}
	identity := (&Auth{ID: "bound", Label: strings.Repeat("界", 600)}).LogIdentity()
	if len(identity.Name) > 512 {
		t.Fatal("unbounded identity")
	}
}
