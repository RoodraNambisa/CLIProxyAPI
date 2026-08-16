package executor

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestCodexTurnStateOriginTrackerAppliesPolicies(t *testing.T) {
	tracker := newCodexTurnStateOriginTracker(8, time.Hour)
	now := time.Unix(100, 0)
	tracker.note("account:a", "state-a", now)

	tests := []struct {
		name      string
		policy    config.CodexTurnStatePolicy
		owner     string
		state     string
		wantStrip bool
	}{
		{name: "passthrough other", policy: config.CodexTurnStatePolicyPassthrough, owner: "account:b", state: "state-a"},
		{name: "guard same", policy: config.CodexTurnStatePolicyGuardCrossAccount, owner: "account:a", state: "state-a"},
		{name: "guard other", policy: config.CodexTurnStatePolicyGuardCrossAccount, owner: "account:b", state: "state-a", wantStrip: true},
		{name: "guard unknown", policy: config.CodexTurnStatePolicyGuardCrossAccount, owner: "account:b", state: "state-unknown"},
		{name: "same only same", policy: config.CodexTurnStatePolicySameAccountOnly, owner: "account:a", state: "state-a"},
		{name: "same only other", policy: config.CodexTurnStatePolicySameAccountOnly, owner: "account:b", state: "state-a", wantStrip: true},
		{name: "same only unknown", policy: config.CodexTurnStatePolicySameAccountOnly, owner: "account:a", state: "state-unknown", wantStrip: true},
		{name: "strip same", policy: config.CodexTurnStatePolicyStrip, owner: "account:a", state: "state-a", wantStrip: true},
		{name: "strip unknown", policy: config.CodexTurnStatePolicyStrip, owner: "account:a", state: "state-unknown", wantStrip: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			headers := http.Header{codexTurnStateHeader: []string{test.state}}
			if got := tracker.apply(test.policy, test.owner, headers, now.Add(time.Minute)); got != test.wantStrip {
				t.Fatalf("apply() = %v, want %v", got, test.wantStrip)
			}
			if got := headers.Get(codexTurnStateHeader); (got == "") != test.wantStrip {
				t.Fatalf("turn state = %q after apply, wantStrip=%v", got, test.wantStrip)
			}
		})
	}
}

func TestCodexTurnStateStripRemovesEmptyHeader(t *testing.T) {
	tracker := newCodexTurnStateOriginTracker(1, time.Hour)
	headers := http.Header{codexTurnStateHeader: []string{""}}
	tracker.apply(config.CodexTurnStatePolicyStrip, "account:a", headers, time.Now())
	if _, exists := headers[codexTurnStateHeader]; exists {
		t.Fatal("strip policy left an empty turn-state header")
	}
}

func TestCodexTurnStateOriginTrackerExpiresAndBoundsEntries(t *testing.T) {
	tracker := newCodexTurnStateOriginTracker(2, time.Minute)
	now := time.Unix(100, 0)
	tracker.note("account:a", "expired", now)
	expired := http.Header{codexTurnStateHeader: []string{"expired"}}
	if tracker.apply(config.CodexTurnStatePolicyGuardCrossAccount, "account:b", expired, now.Add(time.Minute)) || expired.Get(codexTurnStateHeader) == "" {
		t.Fatal("expired turn state provenance should not strip the header")
	}
	strictExpired := http.Header{codexTurnStateHeader: []string{"expired"}}
	if !tracker.apply(config.CodexTurnStatePolicySameAccountOnly, "account:a", strictExpired, now.Add(time.Minute)) || strictExpired.Get(codexTurnStateHeader) != "" {
		t.Fatal("same-account-only should strip expired provenance")
	}

	for i := 0; i < 4; i++ {
		tracker.note("account:a", fmt.Sprintf("state-%d", i), now.Add(time.Duration(i+2)*time.Minute))
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if got := len(tracker.origins); got > 2 {
		t.Fatalf("origin count = %d, want <= 2", got)
	}
}

func TestGuardCodexTurnStateHeaderUsesRuntimePolicyAndSkipsAPIKeys(t *testing.T) {
	oauth := &cliproxyauth.Auth{ID: "oauth", Provider: "codex"}
	stripCfg := &config.Config{Codex: config.CodexConfig{TurnStatePolicy: config.CodexTurnStatePolicyStrip}}
	oauthHeaders := http.Header{codexTurnStateHeader: []string{"client-state"}}
	guardCodexTurnStateHeader(stripCfg, oauth, oauthHeaders)
	if got := oauthHeaders.Get(codexTurnStateHeader); got != "" {
		t.Fatalf("OAuth turn state = %q, want stripped", got)
	}

	apiKey := &cliproxyauth.Auth{ID: "key", Provider: "codex", Attributes: map[string]string{"api_key": "secret"}}
	apiKeyHeaders := http.Header{codexTurnStateHeader: []string{"client-state"}}
	guardCodexTurnStateHeader(stripCfg, apiKey, apiKeyHeaders)
	if got := apiKeyHeaders.Get(codexTurnStateHeader); got != "client-state" {
		t.Fatalf("API-key turn state = %q, want passthrough", got)
	}
}

func TestApplyCodexHTTPSessionIdentityAppliesTurnStatePolicy(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{TurnStatePolicy: config.CodexTurnStatePolicyStrip}})
	auth := &cliproxyauth.Auth{
		ID:       "oauth",
		Provider: "codex",
		Metadata: map[string]any{codexauth.FingerprintModeMetadataKey: string(codexauth.FingerprintModeOff)},
	}
	httpReq, err := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	httpReq.Header.Set(codexTurnStateHeader, "client-state")
	if _, err = executor.applyCodexHTTPSessionIdentity(
		t.Context(), auth, cliproxyexecutor.Request{}, cliproxyexecutor.Options{}, httpReq, []byte(`{}`), &codexIdentityConfuseState{},
	); err != nil {
		t.Fatalf("applyCodexHTTPSessionIdentity() error = %v", err)
	}
	if got := httpReq.Header.Get(codexTurnStateHeader); got != "" {
		t.Fatalf("HTTP turn state = %q, want stripped", got)
	}
}

func TestCodexTurnStateOwnerUsesAccountIdentityAndSkipsAPIKeys(t *testing.T) {
	first := &cliproxyauth.Auth{ID: "file-a", Provider: "codex", Metadata: map[string]any{"account_id": "account-1"}}
	second := &cliproxyauth.Auth{ID: "file-b", Provider: "codex", Metadata: map[string]any{"account_id": "account-1"}}
	if got, want := codexTurnStateOwner(first), codexTurnStateOwner(second); got == "" || got != want {
		t.Fatalf("owners = %q and %q, want same non-empty account owner", got, want)
	}
	apiKey := &cliproxyauth.Auth{ID: "key", Provider: "codex", Attributes: map[string]string{"api_key": "secret"}}
	if got := codexTurnStateOwner(apiKey); got != "" {
		t.Fatalf("API-key owner = %q, want empty", got)
	}
}

func TestEnsureCodexTurnStateHeaderKeepsCredentialOverride(t *testing.T) {
	target := http.Header{codexTurnStateHeader: []string{"credential-state"}}
	source := http.Header{codexTurnStateHeader: []string{"client-state"}}
	ensureCodexTurnStateHeader(target, source)
	if got := target.Get(codexTurnStateHeader); got != "credential-state" {
		t.Fatalf("turn state = %q, want credential-state", got)
	}

	target = make(http.Header)
	ensureCodexTurnStateHeader(target, source)
	if got := target.Get(codexTurnStateHeader); got != "client-state" {
		t.Fatalf("turn state = %q, want client-state", got)
	}
}

func TestApplyCodexHeadersForwardsClientTurnState(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	clientHeaders := http.Header{codexTurnStateHeader: []string{"client-state"}}
	if err = applyCodexHeadersFromSources(req, nil, "oauth-token", true, nil, clientHeaders); err != nil {
		t.Fatalf("applyCodexHeadersFromSources() error = %v", err)
	}
	if got := req.Header.Get(codexTurnStateHeader); got != "client-state" {
		t.Fatalf("turn state = %q, want client-state", got)
	}
}
