package executor

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCodexTurnStateOriginTrackerGuardsOnlyKnownOtherAccount(t *testing.T) {
	tracker := newCodexTurnStateOriginTracker(8, time.Hour)
	now := time.Unix(100, 0)
	tracker.note("account:a", "state-a", now)

	same := http.Header{codexTurnStateHeader: []string{"state-a"}}
	if tracker.guard("account:a", same, now.Add(time.Minute)) || same.Get(codexTurnStateHeader) == "" {
		t.Fatal("same-account turn state was stripped")
	}

	other := http.Header{codexTurnStateHeader: []string{"state-a"}}
	if !tracker.guard("account:b", other, now.Add(time.Minute)) || other.Get(codexTurnStateHeader) != "" {
		t.Fatal("known cross-account turn state was not stripped")
	}

	unknown := http.Header{codexTurnStateHeader: []string{"state-unknown"}}
	if tracker.guard("account:b", unknown, now.Add(time.Minute)) || unknown.Get(codexTurnStateHeader) == "" {
		t.Fatal("unknown turn state was stripped")
	}
}

func TestCodexTurnStateOriginTrackerExpiresAndBoundsEntries(t *testing.T) {
	tracker := newCodexTurnStateOriginTracker(2, time.Minute)
	now := time.Unix(100, 0)
	tracker.note("account:a", "expired", now)
	expired := http.Header{codexTurnStateHeader: []string{"expired"}}
	if tracker.guard("account:b", expired, now.Add(time.Minute)) || expired.Get(codexTurnStateHeader) == "" {
		t.Fatal("expired turn state provenance should not strip the header")
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
