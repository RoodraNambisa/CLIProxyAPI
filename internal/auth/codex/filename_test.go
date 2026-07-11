package codex

import "testing"

func TestCredentialFileNameTeamScopedPlans(t *testing.T) {
	tests := []struct {
		name      string
		plan      string
		accountID string
		want      string
	}{
		{name: "team hash", plan: "team", accountID: "abc12345", want: "codex-abc12345-user@example.com-team.json"},
		{name: "k12 hash", plan: "k12", accountID: "def67890", want: "codex-def67890-user@example.com-k12.json"},
		{name: "empty hash fallback", plan: "team", want: "codex-user@example.com-team.json"},
		{name: "plus ignores hash", plan: "plus", accountID: "abc12345", want: "codex-user@example.com-plus.json"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CredentialFileName(" user@example.com ", tt.plan, tt.accountID, true); got != tt.want {
				t.Fatalf("CredentialFileName() = %q, want %q", got, tt.want)
			}
		})
	}
}
