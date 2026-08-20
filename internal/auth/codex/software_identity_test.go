package codex

import "testing"

func TestResolveSoftwareIdentity(t *testing.T) {
	tests := []struct {
		name        string
		userAgent   string
		wantAgent   string
		wantOrigin  string
		wantVersion string
	}{
		{
			name:        "default",
			wantAgent:   DefaultUserAgent,
			wantOrigin:  DefaultOriginator,
			wantVersion: "0.146.0",
		},
		{
			name:        "supported custom client",
			userAgent:   "codex-tui/0.148.0-alpha.9 (Mac OS 26.5.0; arm64) Terminal/1.0",
			wantAgent:   "codex-tui/0.148.0-alpha.9 (Mac OS 26.5.0; arm64) Terminal/1.0",
			wantOrigin:  "codex-tui",
			wantVersion: "0.148.0-alpha.9",
		},
		{
			name:        "old version keeps client shape",
			userAgent:   "codex-tui/0.143.9 (Linux; x86_64) tmux/3.5",
			wantAgent:   "codex-tui/0.146.0 (Linux; x86_64) tmux/3.5",
			wantOrigin:  "codex-tui",
			wantVersion: "0.146.0",
		},
		{
			name:        "minimum prerelease is upgraded",
			userAgent:   "codex-tui/0.144.0-alpha.1 (Linux; arm64)",
			wantAgent:   "codex-tui/0.146.0 (Linux; arm64)",
			wantOrigin:  "codex-tui",
			wantVersion: "0.146.0",
		},
		{
			name:        "invalid falls back",
			userAgent:   "custom-client",
			wantAgent:   DefaultUserAgent,
			wantOrigin:  DefaultOriginator,
			wantVersion: "0.146.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveSoftwareIdentity(tt.userAgent)
			if got.UserAgent != tt.wantAgent || got.Originator != tt.wantOrigin || got.Version != tt.wantVersion {
				t.Fatalf("ResolveSoftwareIdentity() = %#v, want agent=%q origin=%q version=%q", got, tt.wantAgent, tt.wantOrigin, tt.wantVersion)
			}
		})
	}
}
