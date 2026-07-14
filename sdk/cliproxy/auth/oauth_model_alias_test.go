package auth

import (
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestResolveOAuthUpstreamModel_SuffixPreservation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		aliases map[string][]internalconfig.OAuthModelAlias
		channel string
		input   string
		want    string
	}{
		{
			name: "numeric suffix preserved",
			aliases: map[string][]internalconfig.OAuthModelAlias{
				"antigravity": {{Name: "gemini-2.5-pro-exp-03-25", Alias: "gemini-2.5-pro"}},
			},
			channel: "antigravity",
			input:   "gemini-2.5-pro(8192)",
			want:    "gemini-2.5-pro-exp-03-25(8192)",
		},
		{
			name: "level suffix preserved",
			aliases: map[string][]internalconfig.OAuthModelAlias{
				"claude": {{Name: "claude-sonnet-4-5-20250514", Alias: "claude-sonnet-4-5"}},
			},
			channel: "claude",
			input:   "claude-sonnet-4-5(high)",
			want:    "claude-sonnet-4-5-20250514(high)",
		},
		{
			name: "no suffix unchanged",
			aliases: map[string][]internalconfig.OAuthModelAlias{
				"antigravity": {{Name: "gemini-2.5-pro-exp-03-25", Alias: "gemini-2.5-pro"}},
			},
			channel: "antigravity",
			input:   "gemini-2.5-pro",
			want:    "gemini-2.5-pro-exp-03-25",
		},
		{
			name: "config suffix takes priority",
			aliases: map[string][]internalconfig.OAuthModelAlias{
				"claude": {{Name: "claude-sonnet-4-5-20250514(low)", Alias: "claude-sonnet-4-5"}},
			},
			channel: "claude",
			input:   "claude-sonnet-4-5(high)",
			want:    "claude-sonnet-4-5-20250514(low)",
		},
		{
			name: "auto suffix preserved",
			aliases: map[string][]internalconfig.OAuthModelAlias{
				"antigravity": {{Name: "gemini-2.5-pro-exp-03-25", Alias: "gemini-2.5-pro"}},
			},
			channel: "antigravity",
			input:   "gemini-2.5-pro(auto)",
			want:    "gemini-2.5-pro-exp-03-25(auto)",
		},
		{
			name: "none suffix preserved",
			aliases: map[string][]internalconfig.OAuthModelAlias{
				"antigravity": {{Name: "gemini-2.5-pro-exp-03-25", Alias: "gemini-2.5-pro"}},
			},
			channel: "antigravity",
			input:   "gemini-2.5-pro(none)",
			want:    "gemini-2.5-pro-exp-03-25(none)",
		},
		{
			name: "kimi suffix preserved",
			aliases: map[string][]internalconfig.OAuthModelAlias{
				"kimi": {{Name: "kimi-k2.5", Alias: "k2.5"}},
			},
			channel: "kimi",
			input:   "k2.5(high)",
			want:    "kimi-k2.5(high)",
		},
		{
			name: "case insensitive alias lookup with suffix",
			aliases: map[string][]internalconfig.OAuthModelAlias{
				"antigravity": {{Name: "gemini-2.5-pro-exp-03-25", Alias: "Gemini-2.5-Pro"}},
			},
			channel: "antigravity",
			input:   "gemini-2.5-pro(high)",
			want:    "gemini-2.5-pro-exp-03-25(high)",
		},
		{
			name: "no alias returns empty",
			aliases: map[string][]internalconfig.OAuthModelAlias{
				"antigravity": {{Name: "gemini-2.5-pro-exp-03-25", Alias: "gemini-2.5-pro"}},
			},
			channel: "antigravity",
			input:   "unknown-model(high)",
			want:    "",
		},
		{
			name: "wrong channel returns empty",
			aliases: map[string][]internalconfig.OAuthModelAlias{
				"antigravity": {{Name: "gemini-2.5-pro-exp-03-25", Alias: "gemini-2.5-pro"}},
			},
			channel: "claude",
			input:   "gemini-2.5-pro(high)",
			want:    "",
		},
		{
			name: "empty suffix filtered out",
			aliases: map[string][]internalconfig.OAuthModelAlias{
				"antigravity": {{Name: "gemini-2.5-pro-exp-03-25", Alias: "gemini-2.5-pro"}},
			},
			channel: "antigravity",
			input:   "gemini-2.5-pro()",
			want:    "gemini-2.5-pro-exp-03-25",
		},
		{
			name: "incomplete suffix treated as no suffix",
			aliases: map[string][]internalconfig.OAuthModelAlias{
				"antigravity": {{Name: "gemini-2.5-pro-exp-03-25", Alias: "gemini-2.5-pro(high"}},
			},
			channel: "antigravity",
			input:   "gemini-2.5-pro(high",
			want:    "gemini-2.5-pro-exp-03-25",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mgr := NewManager(nil, nil, nil)
			mgr.SetConfig(&internalconfig.Config{})
			mgr.SetOAuthModelAlias(tt.aliases)

			auth := createAuthForChannel(tt.channel)
			got := mgr.resolveOAuthUpstreamModel(auth, tt.input)
			if got != tt.want {
				t.Errorf("resolveOAuthUpstreamModel(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func createAuthForChannel(channel string) *Auth {
	switch channel {
	case "antigravity":
		return &Auth{Provider: "antigravity", Attributes: map[string]string{"auth_kind": "oauth"}}
	case "claude":
		return &Auth{Provider: "claude", Attributes: map[string]string{"auth_kind": "oauth"}}
	case "vertex":
		return &Auth{Provider: "vertex", Attributes: map[string]string{"auth_kind": "oauth"}}
	case "codex":
		return &Auth{Provider: "codex", Attributes: map[string]string{"auth_kind": "oauth"}}
	case "aistudio":
		return &Auth{Provider: "aistudio"}
	case "kimi":
		return &Auth{Provider: "kimi"}
	default:
		return &Auth{Provider: channel}
	}
}

func TestOAuthModelAliasChannel_APIKeyOnlyProviderUnsupported(t *testing.T) {
	t.Parallel()

	if got := OAuthModelAliasChannel("gemini", "oauth"); got != "" {
		t.Fatalf("OAuthModelAliasChannel() = %q, want empty channel for API-key-only provider", got)
	}
}

func TestOAuthModelAliasChannel_Kimi(t *testing.T) {
	t.Parallel()

	if got := OAuthModelAliasChannel("kimi", "oauth"); got != "kimi" {
		t.Fatalf("OAuthModelAliasChannel() = %q, want %q", got, "kimi")
	}
}

func TestOAuthModelAliasChannel_XAIAPIKeyUnsupported(t *testing.T) {
	t.Parallel()

	if got := OAuthModelAliasChannel("xai", "apikey"); got != "" {
		t.Fatalf("OAuthModelAliasChannel() = %q, want empty channel for xAI API key", got)
	}
	if got := OAuthModelAliasChannel("xai", "oauth"); got != "xai" {
		t.Fatalf("OAuthModelAliasChannel() = %q, want xai OAuth channel", got)
	}
}

func TestApplyOAuthModelAlias_SuffixPreservation(t *testing.T) {
	t.Parallel()

	aliases := map[string][]internalconfig.OAuthModelAlias{
		"antigravity": {{Name: "gemini-2.5-pro-exp-03-25", Alias: "gemini-2.5-pro"}},
	}

	mgr := NewManager(nil, nil, nil)
	mgr.SetConfig(&internalconfig.Config{})
	mgr.SetOAuthModelAlias(aliases)

	auth := &Auth{ID: "test-auth-id", Provider: "antigravity"}

	resolvedModel := mgr.applyOAuthModelAlias(auth, "gemini-2.5-pro(8192)")
	if resolvedModel != "gemini-2.5-pro-exp-03-25(8192)" {
		t.Errorf("applyOAuthModelAlias() model = %q, want %q", resolvedModel, "gemini-2.5-pro-exp-03-25(8192)")
	}
}

func TestApplyOAuthModelAliasWithResult_ForceMapping(t *testing.T) {
	t.Parallel()

	aliases := map[string][]internalconfig.OAuthModelAlias{
		"codex": {{Name: "gpt-5.5-upstream", Alias: "gpt-5.5-public", ForceMapping: true}},
	}
	mgr := NewManager(nil, nil, nil)
	mgr.SetConfig(&internalconfig.Config{})
	mgr.SetOAuthModelAlias(aliases)

	result := mgr.applyOAuthModelAliasWithResult(createAuthForChannel("codex"), "gpt-5.5-public")
	if result.UpstreamModel != "gpt-5.5-upstream" {
		t.Fatalf("UpstreamModel = %q, want gpt-5.5-upstream", result.UpstreamModel)
	}
	if !result.ForceMapping {
		t.Fatalf("ForceMapping = false, want true")
	}
	if result.OriginalAlias != "gpt-5.5-public" {
		t.Fatalf("OriginalAlias = %q, want gpt-5.5-public", result.OriginalAlias)
	}
}
