package auth

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type antigravityCreditsFallbackExecutor struct {
	streamCreditsRequested []bool
}

type antigravityCreditsBudgetExecutor struct {
	antigravityCreditsFallbackExecutor
	executeAuthIDs []string
	streamAuthIDs  []string
}

func (e *antigravityCreditsBudgetExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.executeAuthIDs = append(e.executeAuthIDs, auth.ID)
	return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusInternalServerError, Message: "credits execution failed"}
}

func (e *antigravityCreditsBudgetExecutor) ExecuteStream(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.streamAuthIDs = append(e.streamAuthIDs, auth.ID)
	return nil, &Error{HTTPStatus: http.StatusInternalServerError, Message: "credits stream execution failed"}
}

func (e *antigravityCreditsFallbackExecutor) Identifier() string { return "antigravity" }

func (e *antigravityCreditsFallbackExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusNotImplemented, Message: "Execute not implemented"}
}

func (e *antigravityCreditsFallbackExecutor) ExecuteStream(ctx context.Context, _ *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	creditsRequested := AntigravityCreditsRequested(ctx)
	e.streamCreditsRequested = append(e.streamCreditsRequested, creditsRequested)
	ch := make(chan cliproxyexecutor.StreamChunk, 1)
	if !creditsRequested {
		ch <- cliproxyexecutor.StreamChunk{Err: &Error{HTTPStatus: http.StatusTooManyRequests, Message: "quota exhausted"}}
		close(ch)
		return &cliproxyexecutor.StreamResult{Headers: http.Header{"X-Initial": {req.Model}}, Chunks: ch}, nil
	}
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("credits fallback")}
	close(ch)
	return &cliproxyexecutor.StreamResult{Headers: http.Header{"X-Credits": {req.Model}}, Chunks: ch}, nil
}

func (e *antigravityCreditsFallbackExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (e *antigravityCreditsFallbackExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusNotImplemented, Message: "CountTokens not implemented"}
}

func (e *antigravityCreditsFallbackExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, &Error{HTTPStatus: http.StatusNotImplemented, Message: "HttpRequest not implemented"}
}

func TestManagerExecuteStream_AntigravityCreditsFallbackAfterBootstrap429(t *testing.T) {
	const model = "claude-opus-4-6-thinking"
	executor := &antigravityCreditsFallbackExecutor{}
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(&internalconfig.Config{
		QuotaExceeded: internalconfig.QuotaExceeded{AntigravityCredits: true},
	})
	manager.RegisterExecutor(executor)
	registry.GetGlobalRegistry().RegisterClient("ag-credits", "antigravity", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient("ag-credits") })
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: "ag-credits", Provider: "antigravity"}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	streamResult, errExecute := manager.ExecuteStream(context.Background(), []string{"antigravity"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errExecute != nil {
		t.Fatalf("execute stream: %v", errExecute)
	}

	var payload []byte
	for chunk := range streamResult.Chunks {
		if chunk.Err != nil {
			t.Fatalf("unexpected stream error: %v", chunk.Err)
		}
		payload = append(payload, chunk.Payload...)
	}
	if string(payload) != "credits fallback" {
		t.Fatalf("payload = %q, want %q", string(payload), "credits fallback")
	}
	if got := streamResult.Headers.Get("X-Credits"); got != model {
		t.Fatalf("X-Credits header = %q, want routed model", got)
	}
	if len(executor.streamCreditsRequested) != 2 {
		t.Fatalf("stream calls = %d, want 2", len(executor.streamCreditsRequested))
	}
	if executor.streamCreditsRequested[0] || !executor.streamCreditsRequested[1] {
		t.Fatalf("credits flags = %v, want [false true]", executor.streamCreditsRequested)
	}
}

func TestFindAllAntigravityCreditsCandidateAuths_SkipsSessionCleanup(t *testing.T) {
	executor := &antigravityCreditsFallbackExecutor{}
	manager := NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	if _, errRegister := manager.Register(t.Context(), &Auth{ID: "ag-cleanup", Provider: "antigravity"}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	manager.beginSessionCleanup("ag-cleanup")
	t.Cleanup(func() { manager.endSessionCleanup("ag-cleanup") })
	if candidates := manager.findAllAntigravityCreditsCandidateAuths("claude-opus-4-6-thinking", cliproxyexecutor.Options{}); len(candidates) != 0 {
		t.Fatalf("cleanup candidates = %#v, want none", candidates)
	}
}

func TestFindAllAntigravityCreditsCandidateAuthsTreatsStaleHintAsUnknown(t *testing.T) {
	executor := &antigravityCreditsFallbackExecutor{}
	manager := NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auths := []*Auth{
		{ID: "ag-stale-negative-hint", Provider: "antigravity"},
		{ID: "ag-fresh-negative-hint", Provider: "antigravity"},
	}
	registeredAuths := make([]*Auth, len(auths))
	for index, auth := range auths {
		registered, errRegister := manager.Register(t.Context(), auth)
		if errRegister != nil {
			t.Fatalf("register %s: %v", auth.ID, errRegister)
		}
		registeredAuths[index] = registered
	}
	SetAntigravityCreditsHint(auths[0].ID, AntigravityCreditsHint{
		Known:         true,
		Available:     false,
		CredentialKey: AntigravityCreditsCredentialKey(registeredAuths[0]),
		UpdatedAt:     time.Now().Add(-AntigravityCreditsHintRefreshInterval - time.Second),
	})
	SetAntigravityCreditsHint(auths[1].ID, AntigravityCreditsHint{
		Known:         true,
		Available:     false,
		CredentialKey: AntigravityCreditsCredentialKey(registeredAuths[1]),
		UpdatedAt:     time.Now(),
	})

	candidates := manager.findAllAntigravityCreditsCandidateAuths("claude-opus-4-6-thinking", cliproxyexecutor.Options{})
	if len(candidates) != 1 || candidates[0].auth.ID != auths[0].ID {
		t.Fatalf("candidates = %#v, want stale negative hint only", candidates)
	}
}

func TestFindAllAntigravityCreditsCandidateAuthsHonorsActiveNegativeHint(t *testing.T) {
	executor := &antigravityCreditsFallbackExecutor{}
	manager := NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	authID := "ag-active-negative-hint"
	registered, errRegister := manager.Register(t.Context(), &Auth{
		ID:       authID,
		Provider: "antigravity",
		Metadata: map[string]any{"access_token": "current-token"},
	})
	if errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	SetAntigravityCreditsHint(authID, AntigravityCreditsHint{
		Known:            true,
		Available:        false,
		CredentialKey:    AntigravityCreditsCredentialKey(registered),
		UnavailableUntil: time.Now().Add(time.Hour),
		UpdatedAt:        time.Now().Add(-AntigravityCreditsHintRefreshInterval - time.Second),
	})

	if candidates := manager.findAllAntigravityCreditsCandidateAuths("claude-opus-4-6-thinking", cliproxyexecutor.Options{}); len(candidates) != 0 {
		t.Fatalf("candidates = %#v, want active negative hint excluded", candidates)
	}
}

func TestFindAllAntigravityCreditsCandidateAuthsIgnoresDifferentCredentialHint(t *testing.T) {
	executor := &antigravityCreditsFallbackExecutor{}
	manager := NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	authID := "ag-replaced-credential-hint"
	if _, errRegister := manager.Register(t.Context(), &Auth{
		ID:       authID,
		Provider: "antigravity",
		Metadata: map[string]any{"access_token": "current-token"},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	oldAuth := &Auth{ID: authID, Metadata: map[string]any{"access_token": "old-token"}}
	SetAntigravityCreditsHint(authID, AntigravityCreditsHint{
		Known:                  true,
		Available:              false,
		CredentialKey:          AntigravityCreditsCredentialKey(oldAuth),
		PermanentlyUnavailable: true,
		UpdatedAt:              time.Now(),
	})

	candidates := manager.findAllAntigravityCreditsCandidateAuths("claude-opus-4-6-thinking", cliproxyexecutor.Options{})
	if len(candidates) != 1 || candidates[0].auth.ID != authID {
		t.Fatalf("candidates = %#v, want replacement credential included", candidates)
	}
}

func TestAntigravityCreditsHintWithoutCredentialKeyDoesNotMatch(t *testing.T) {
	auth := &Auth{ID: "ag-empty-credential-key", Provider: "antigravity"}
	if (AntigravityCreditsHint{Known: true, Available: false}).MatchesAuth(auth) {
		t.Fatal("hint without CredentialKey matched a credential generation")
	}
}

func TestPickAntigravityCreditsCandidatePreservesPriorityBeforeKnownHint(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	executor := &antigravityCreditsFallbackExecutor{}
	manager.RegisterExecutor(executor)
	high, errHigh := manager.Register(t.Context(), &Auth{
		ID:         "ag-priority-high-unknown",
		Provider:   "antigravity",
		Attributes: map[string]string{"priority": "1"},
	})
	if errHigh != nil {
		t.Fatalf("register high priority auth: %v", errHigh)
	}
	low, errLow := manager.Register(t.Context(), &Auth{
		ID:         "ag-priority-low-known",
		Provider:   "antigravity",
		Attributes: map[string]string{"priority": "0"},
	})
	if errLow != nil {
		t.Fatalf("register low priority auth: %v", errLow)
	}
	SetAntigravityCreditsHint(low.ID, AntigravityCreditsHint{
		Known:         true,
		Available:     true,
		CredentialKey: AntigravityCreditsCredentialKey(low),
		UpdatedAt:     time.Now(),
	})

	selected, errPick := manager.pickAntigravityCreditsCandidate(t.Context(), "claude-opus-4-6-thinking", cliproxyexecutor.Options{}, newRequestRoundState(), 0)
	if errPick != nil {
		t.Fatalf("pick credits candidate: %v", errPick)
	}
	if selected == nil || selected.auth.ID != high.ID {
		t.Fatalf("selected = %#v, want high priority unknown auth", selected)
	}
}

func TestPickAntigravityCreditsCandidatePrefersKnownWithinPriority(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	executor := &antigravityCreditsFallbackExecutor{}
	manager.RegisterExecutor(executor)
	unknown, errUnknown := manager.Register(t.Context(), &Auth{ID: "ag-known-order-a-unknown", Provider: "antigravity"})
	if errUnknown != nil {
		t.Fatalf("register unknown auth: %v", errUnknown)
	}
	known, errKnown := manager.Register(t.Context(), &Auth{ID: "ag-known-order-z-known", Provider: "antigravity"})
	if errKnown != nil {
		t.Fatalf("register known auth: %v", errKnown)
	}
	SetAntigravityCreditsHint(known.ID, AntigravityCreditsHint{
		Known:         true,
		Available:     true,
		CredentialKey: AntigravityCreditsCredentialKey(known),
		UpdatedAt:     time.Now(),
	})

	selected, errPick := manager.pickAntigravityCreditsCandidate(t.Context(), "claude-opus-4-6-thinking", cliproxyexecutor.Options{}, newRequestRoundState(), 0)
	if errPick != nil {
		t.Fatalf("pick credits candidate: %v", errPick)
	}
	if selected == nil || selected.auth.ID != known.ID {
		t.Fatalf("selected = %#v, want known auth before %s", selected, unknown.ID)
	}
}

func TestFindAllAntigravityCreditsCandidateAuthsOnlyBypassesQuotaCooldown(t *testing.T) {
	const model = "claude-opus-4-6-thinking"
	manager := NewManager(nil, nil, nil)
	executor := &antigravityCreditsFallbackExecutor{}
	manager.RegisterExecutor(executor)
	now := time.Now()
	fixedCooldown := &Auth{
		ID:             "ag-fixed-error-cooldown",
		Provider:       "antigravity",
		Unavailable:    true,
		CooldownScope:  cooldownScopeAuth,
		NextRetryAfter: now.Add(time.Hour),
	}
	quotaCooldown := &Auth{
		ID:             "ag-quota-cooldown",
		Provider:       "antigravity",
		Unavailable:    true,
		CooldownScope:  cooldownScopeAuth,
		NextRetryAfter: now.Add(time.Hour),
		Quota:          QuotaState{Exceeded: true, NextRecoverAt: now.Add(time.Hour)},
	}
	fixedModelCooldown := &Auth{
		ID:       "ag-fixed-model-cooldown",
		Provider: "antigravity",
		ModelStates: map[string]*ModelState{
			model: {Unavailable: true, NextRetryAfter: now.Add(time.Hour)},
		},
	}
	quotaModelCooldown := &Auth{
		ID:       "ag-quota-model-cooldown",
		Provider: "antigravity",
		ModelStates: map[string]*ModelState{
			model: {Unavailable: true, NextRetryAfter: now.Add(time.Hour), Quota: QuotaState{Exceeded: true, NextRecoverAt: now.Add(time.Hour)}},
		},
	}
	combinedQuotaAndFixedCooldown := &Auth{
		ID:             "ag-auth-quota-model-fixed-cooldown",
		Provider:       "antigravity",
		Unavailable:    true,
		CooldownScope:  cooldownScopeAuth,
		NextRetryAfter: now.Add(time.Hour),
		Quota:          QuotaState{Exceeded: true, NextRecoverAt: now.Add(time.Hour)},
		ModelStates: map[string]*ModelState{
			model: {Unavailable: true, NextRetryAfter: now.Add(time.Hour)},
		},
	}
	if _, errRegister := manager.Register(t.Context(), fixedCooldown); errRegister != nil {
		t.Fatalf("register fixed cooldown auth: %v", errRegister)
	}
	if _, errRegister := manager.Register(t.Context(), quotaCooldown); errRegister != nil {
		t.Fatalf("register quota cooldown auth: %v", errRegister)
	}
	if _, errRegister := manager.Register(t.Context(), fixedModelCooldown); errRegister != nil {
		t.Fatalf("register fixed model cooldown auth: %v", errRegister)
	}
	if _, errRegister := manager.Register(t.Context(), quotaModelCooldown); errRegister != nil {
		t.Fatalf("register quota model cooldown auth: %v", errRegister)
	}
	if _, errRegister := manager.Register(t.Context(), combinedQuotaAndFixedCooldown); errRegister != nil {
		t.Fatalf("register combined cooldown auth: %v", errRegister)
	}

	candidates := manager.findAllAntigravityCreditsCandidateAuths(model, cliproxyexecutor.Options{})
	got := make(map[string]bool, len(candidates))
	for _, candidate := range candidates {
		got[candidate.auth.ID] = true
	}
	if len(candidates) != 2 || !got[quotaCooldown.ID] || !got[quotaModelCooldown.ID] {
		t.Fatalf("candidate IDs = %#v, want only auth/model quota cooldowns", got)
	}
}

func TestPickAntigravityCreditsCandidateAppliesPerPriorityCredentialBudget(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	executor := &antigravityCreditsFallbackExecutor{}
	manager.RegisterExecutor(executor)
	for _, auth := range []*Auth{
		{ID: "ag-budget-high-a", Provider: "antigravity", Attributes: map[string]string{"priority": "1"}},
		{ID: "ag-budget-high-b", Provider: "antigravity", Attributes: map[string]string{"priority": "1"}},
		{ID: "ag-budget-low", Provider: "antigravity", Attributes: map[string]string{"priority": "0"}},
	} {
		if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
			t.Fatalf("register %s: %v", auth.ID, errRegister)
		}
	}
	roundState := newRequestRoundState()
	first, errFirst := manager.pickAntigravityCreditsCandidate(t.Context(), "claude-opus-4-6-thinking", cliproxyexecutor.Options{}, roundState, 1)
	if errFirst != nil || first == nil || authPriority(first.auth) != 1 {
		t.Fatalf("first candidate = %#v, err=%v, want priority 1", first, errFirst)
	}
	roundState.tried[first.auth.ID] = struct{}{}
	roundState.markAttempted(first.auth)

	second, errSecond := manager.pickAntigravityCreditsCandidate(t.Context(), "claude-opus-4-6-thinking", cliproxyexecutor.Options{}, roundState, 1)
	if errSecond != nil || second == nil || authPriority(second.auth) != 0 {
		t.Fatalf("second candidate = %#v, err=%v, want priority 0 after high-priority budget", second, errSecond)
	}
	roundState.tried[second.auth.ID] = struct{}{}
	roundState.markAttempted(second.auth)

	third, errThird := manager.pickAntigravityCreditsCandidate(t.Context(), "claude-opus-4-6-thinking", cliproxyexecutor.Options{}, roundState, 1)
	if errThird != nil || third != nil {
		t.Fatalf("third candidate = %#v, err=%v, want exhausted budget", third, errThird)
	}
}

func TestPickAntigravityCreditsCandidateFillFirstRangeKeepsStableGroups(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{Range: 2}, nil)
	executor := &antigravityCreditsFallbackExecutor{}
	manager.RegisterExecutor(executor)
	registered := make(map[string]*Auth)
	for _, authID := range []string{"ag-range-a", "ag-range-b", "ag-range-c", "ag-range-d"} {
		auth, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "antigravity"})
		if errRegister != nil {
			t.Fatalf("register %s: %v", authID, errRegister)
		}
		registered[authID] = auth
	}
	roundState := newRequestRoundState()
	roundState.tried["ag-range-a"] = struct{}{}
	roundState.markAttempted(registered["ag-range-a"])

	selected, errPick := manager.pickAntigravityCreditsCandidate(t.Context(), "claude-opus-4-6-thinking", cliproxyexecutor.Options{}, roundState, 0)
	if errPick != nil {
		t.Fatalf("pick credits candidate: %v", errPick)
	}
	if selected == nil || selected.auth.ID != "ag-range-b" {
		t.Fatalf("selected = %#v, want remaining member of the first fixed group", selected)
	}
}

func TestPickAntigravityCreditsCandidateFillFirstRPMUsesNextAuth(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{FillFirstPerAuthRPM: 1}})
	executor := &antigravityCreditsFallbackExecutor{}
	manager.RegisterExecutor(executor)
	for _, authID := range []string{"ag-rpm-a", "ag-rpm-b"} {
		if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "antigravity"}); errRegister != nil {
			t.Fatalf("register %s: %v", authID, errRegister)
		}
	}

	first, errFirst := manager.pickAntigravityCreditsCandidate(t.Context(), "claude-opus-4-6-thinking", cliproxyexecutor.Options{}, newRequestRoundState(), 0)
	if errFirst != nil || first == nil || first.auth.ID != "ag-rpm-a" {
		t.Fatalf("first candidate = %#v, err=%v, want ag-rpm-a", first, errFirst)
	}
	second, errSecond := manager.pickAntigravityCreditsCandidate(t.Context(), "claude-opus-4-6-thinking", cliproxyexecutor.Options{}, newRequestRoundState(), 0)
	if errSecond != nil || second == nil || second.auth.ID != "ag-rpm-b" {
		t.Fatalf("second candidate = %#v, err=%v, want ag-rpm-b after RPM limit", second, errSecond)
	}
}

func TestPickAntigravityCreditsCandidateRandomPreservesPriority(t *testing.T) {
	manager := NewManager(nil, &RandomSelector{}, nil)
	executor := &antigravityCreditsFallbackExecutor{}
	manager.RegisterExecutor(executor)
	for _, auth := range []*Auth{
		{ID: "ag-random-high-a", Provider: "antigravity", Attributes: map[string]string{"priority": "1"}},
		{ID: "ag-random-high-b", Provider: "antigravity", Attributes: map[string]string{"priority": "1"}},
		{ID: "ag-random-low", Provider: "antigravity", Attributes: map[string]string{"priority": "0"}},
	} {
		if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
			t.Fatalf("register %s: %v", auth.ID, errRegister)
		}
	}

	selected, errPick := manager.pickAntigravityCreditsCandidate(t.Context(), "claude-opus-4-6-thinking", cliproxyexecutor.Options{}, newRequestRoundState(), 0)
	if errPick != nil || selected == nil || authPriority(selected.auth) != 1 {
		t.Fatalf("random candidate = %#v, err=%v, want highest priority", selected, errPick)
	}
}

func TestTryAntigravityCreditsExecuteStopsAtCredentialBudget(t *testing.T) {
	const model = "claude-credits-budget"
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	executor := &antigravityCreditsBudgetExecutor{}
	manager.RegisterExecutor(executor)
	manager.SetRetryConfig(0, 0, 1)
	for _, authID := range []string{"ag-execute-budget-a", "ag-execute-budget-b", "ag-execute-budget-c"} {
		registry.GetGlobalRegistry().RegisterClient(authID, "antigravity", []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
		if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "antigravity"}); errRegister != nil {
			t.Fatalf("register %s: %v", authID, errRegister)
		}
	}

	_, ok, errCredits := manager.tryAntigravityCreditsExecute(t.Context(), cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if ok {
		t.Fatal("credits fallback unexpectedly succeeded")
	}
	if errCredits != nil {
		t.Fatalf("credits fallback error = %v, want original request error to remain authoritative", errCredits)
	}
	if len(executor.executeAuthIDs) != 1 {
		t.Fatalf("credits execute auth IDs = %v, want one credential attempt", executor.executeAuthIDs)
	}
}

func TestTryAntigravityCreditsExecuteStreamStopsAtCredentialBudget(t *testing.T) {
	const model = "claude-credits-stream-budget"
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	executor := &antigravityCreditsBudgetExecutor{}
	manager.RegisterExecutor(executor)
	manager.SetRetryConfig(0, 0, 1)
	for _, authID := range []string{"ag-stream-budget-a", "ag-stream-budget-b", "ag-stream-budget-c"} {
		registry.GetGlobalRegistry().RegisterClient(authID, "antigravity", []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
		if _, errRegister := manager.Register(t.Context(), &Auth{ID: authID, Provider: "antigravity"}); errRegister != nil {
			t.Fatalf("register %s: %v", authID, errRegister)
		}
	}

	_, ok, errCredits := manager.tryAntigravityCreditsExecuteStream(t.Context(), cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if ok {
		t.Fatal("credits stream fallback unexpectedly succeeded")
	}
	if errCredits != nil {
		t.Fatalf("credits stream fallback error = %v, want original request error to remain authoritative", errCredits)
	}
	if len(executor.streamAuthIDs) != 1 {
		t.Fatalf("credits stream auth IDs = %v, want one credential attempt", executor.streamAuthIDs)
	}
}

func TestStatusCodeFromError_UnwrapsStreamBootstrap429(t *testing.T) {
	bootstrapErr := newStreamBootstrapError(&Error{HTTPStatus: http.StatusTooManyRequests, Message: "quota exhausted"}, nil)
	wrappedErr := fmt.Errorf("conductor stream failed: %w", bootstrapErr)

	if status := statusCodeFromError(wrappedErr); status != http.StatusTooManyRequests {
		t.Fatalf("statusCodeFromError() = %d, want %d", status, http.StatusTooManyRequests)
	}
}
