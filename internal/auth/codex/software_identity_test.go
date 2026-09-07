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
			wantVersion: "0.153.4",
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
			wantAgent:   "codex-tui/0.153.4 (Linux; x86_64) tmux/3.5",
			wantOrigin:  "codex-tui",
			wantVersion: "0.153.4",
		},
		{
			name:        "minimum prerelease is upgraded",
			userAgent:   "codex-tui/0.144.0-alpha.1 (Linux; arm64)",
			wantAgent:   "codex-tui/0.153.4 (Linux; arm64)",
			wantOrigin:  "codex-tui",
			wantVersion: "0.153.4",
		},
		{
			name:        "invalid falls back",
			userAgent:   "custom-client",
			wantAgent:   DefaultUserAgent,
			wantOrigin:  DefaultOriginator,
			wantVersion: "0.153.4",
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

func TestResolveSoftwareIdentityForAstraKeepsGenericFloorAndClientSuffix(t *testing.T) {
	old := "my-codex/0.148.0 (Linux; arm64) tmux/3.5"
	if got := ResolveSoftwareIdentityForModel(old, "gpt-5.4"); got.UserAgent != old {
		t.Fatal("generic compatibility floor was raised")
	}
	if got := ResolveSoftwareIdentityForModel(old, "gpt-6-astra"); got.UserAgent != "my-codex/0.153.4 (Linux; arm64) tmux/3.5" || got.Originator != "my-codex" || got.Version != "0.153.4" {
		t.Fatal("Astra identity did not preserve the client shape")
	}
	current := "my-codex/0.153.0 (Linux; arm64) tmux/3.5"
	if got := ResolveSoftwareIdentityForModel(current, "gpt-6-astra"); got.UserAgent != current {
		t.Fatal("supported explicit version was replaced")
	}
}

func TestSoftwareIdentitySupportsModelChecksBothHandshakeVersions(t *testing.T) {
	for _, tc := range []struct {
		userAgentVersion string
		headerVersion    string
		model            string
		want             bool
	}{
		{"0.148.0", "0.148.0", "gpt-5.4", true},
		{"0.148.0", "0.153.4", "gpt-6-astra", false},
		{"0.153.4", "0.148.0", "gpt-6-astra", false},
		{"0.153.4", "", "gpt-6-astra", false},
		{"0.153.4", "0.153.4 suffix", "gpt-6-astra", false},
		{"0.153.4", "0.153.4+", "gpt-6-astra", false},
		{"0.153.0-alpha", "0.153.0", "gpt-6-astra", false},
		{"0.153.0", "0.153.0-alpha", "gpt-6-astra", false},
		{"0.153.0", "0.153.0", "gpt-6-astra", true},
		{"0.154.0-beta", "0.154.0-beta", "gpt-6-astra", true},
	} {
		identity := SoftwareIdentity{UserAgent: "local-codex/" + tc.userAgentVersion + " (Linux) tmux", Version: tc.headerVersion}
		if got := SoftwareIdentitySupportsModel(identity, tc.model); got != tc.want {
			t.Fatalf("versions %q/%q for %s: got %t, want %t", tc.userAgentVersion, tc.headerVersion, tc.model, got, tc.want)
		}
	}
}
