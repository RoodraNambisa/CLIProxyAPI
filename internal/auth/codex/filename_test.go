package codex

import "testing"

func TestCredentialFileNameAccountScopedPlans(t *testing.T) {
	tests := []struct {
		name      string
		plan      string
		accountID string
		want      string
	}{
		{name: "team hash", plan: "team", accountID: "abc12345", want: "codex-abc12345-user@example.com-team.json"},
		{name: "k12 hash", plan: "k12", accountID: "def67890", want: "codex-def67890-user@example.com-k12.json"},
		{name: "empty hash fallback", plan: "team", want: "codex-user@example.com-team.json"},
		{name: "plus hash", plan: "plus", accountID: "abc12345", want: "codex-abc12345-user@example.com-plus.json"},
		{name: "free hash", plan: "free", accountID: "abc12345", want: "codex-abc12345-user@example.com-free.json"},
		{name: "pro hash", plan: "pro", accountID: "abc12345", want: "codex-abc12345-user@example.com-pro.json"},
		{name: "missing plan", accountID: " abc12345 ", want: "codex-abc12345-user@example.com.json"},
		{name: "legacy plus", plan: "plus", want: "codex-user@example.com-plus.json"},
		{name: "legacy no plan", want: "codex-user@example.com.json"},
		{name: "normalized plan", plan: " PRO / Trial ", accountID: "abc12345", want: "codex-abc12345-user@example.com-pro-trial.json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CredentialFileName(" user@example.com ", tt.plan, tt.accountID, true); got != tt.want {
				t.Fatalf("CredentialFileName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCredentialFileNameKeepsPrefixOption(t *testing.T) {
	if got := CredentialFileName("user@example.com", "plus", "account", false); got != "-account-user@example.com-plus.json" {
		t.Fatal("provider prefix option changed")
	}
}
