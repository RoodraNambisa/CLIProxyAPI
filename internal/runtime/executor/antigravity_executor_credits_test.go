package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func resetAntigravityCreditsRetryState() {
	antigravityCreditsHintRefreshByID.Range(func(_, value any) bool {
		if state, ok := value.(*antigravityCreditsHintRefreshState); ok && state != nil {
			state.mu.Lock()
			invalidateAntigravityCreditsRefreshLocked(state)
			state.mu.Unlock()
		}
		return true
	})
	antigravityCreditsFailureByAuth = sync.Map{}
	antigravityPreferCreditsByModel = sync.Map{}
	antigravityCreditsHintRefreshByID = sync.Map{}
	antigravityShortCooldownByAuth = sync.Map{}
}

type creditsRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f creditsRoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func setFreshAntigravityCreditsHint(auth *cliproxyauth.Auth, available bool) {
	if auth == nil {
		return
	}
	cliproxyauth.SetAntigravityCreditsHint(auth.ID, cliproxyauth.AntigravityCreditsHint{
		Known:         true,
		Available:     available,
		CredentialKey: cliproxyauth.AntigravityCreditsCredentialKey(auth),
		UpdatedAt:     time.Now(),
	})
}

func TestUpdateAntigravityCreditsBalance(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	const configuredUserAgent = "antigravity/hub/1.23.2 windows/amd64 google-api-nodejs-client/10.3.0"
	auth := &cliproxyauth.Auth{
		ID:         "auth-load-code-assist-balance",
		Attributes: map[string]string{"user_agent": configuredUserAgent},
	}
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", creditsRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if got := req.Method; got != http.MethodPost {
			t.Fatalf("method = %q, want POST", got)
		}
		if got := req.URL.String(); got != "https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist" {
			t.Fatalf("URL = %q", got)
		}
		if got := req.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("Authorization = %q", got)
		}
		if got := req.Header.Get("Accept"); got != "*/*" {
			t.Fatalf("Accept = %q", got)
		}
		if got := req.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("Content-Type = %q", got)
		}
		if got := req.Header.Get("User-Agent"); got != "antigravity/hub/1.23.2 windows/amd64" {
			t.Fatalf("User-Agent = %q", got)
		}
		if got := req.Header.Get("X-Goog-Api-Client"); got != "" {
			t.Fatalf("X-Goog-Api-Client = %q, want empty", got)
		}
		body, errRead := io.ReadAll(req.Body)
		if errRead != nil {
			t.Fatalf("read body: %v", errRead)
		}
		if got := string(body); got != `{"metadata":{"ideType":"ANTIGRAVITY"}}` {
			t.Fatalf("body = %s", got)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body: io.NopCloser(strings.NewReader(
				`{"paidTier":{"id":"tier-1","availableCredits":[{"creditType":"GOOGLE_ONE_AI","creditAmount":"25000","minimumCreditAmountForUsage":"50"}]}}`,
			)),
		}, nil
	}))

	exec := NewAntigravityExecutor(&config.Config{})
	hint, ok := exec.fetchAntigravityCreditsHint(ctx, auth, "token")
	if !ok {
		t.Fatal("credits hint not returned")
	}
	if !hint.Known || !hint.Available {
		t.Fatalf("hint = %#v, want known available", hint)
	}
	if hint.CreditAmount != 25000 || hint.MinCreditAmount != 50 || hint.PaidTierID != "tier-1" {
		t.Fatalf("hint = %#v", hint)
	}
}

func TestEnsureAccessTokenThrottlesConcurrentCreditsHintRefresh(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWorker := func() { releaseOnce.Do(func() { close(release) }) }
	var startedOnce sync.Once
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", creditsRoundTripperFunc(func(_ *http.Request) (*http.Response, error) {
		calls.Add(1)
		startedOnce.Do(func() { close(started) })
		<-release
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body: io.NopCloser(strings.NewReader(
				`{"paidTier":{"id":"tier-1","availableCredits":[{"creditType":"GOOGLE_ONE_AI","creditAmount":"100","minimumCreditAmountForUsage":"10"}]}}`,
			)),
		}, nil
	}))
	auth := &cliproxyauth.Auth{
		ID: "auth-concurrent-credits-hint",
		Metadata: map[string]any{
			"access_token": "token",
			"expired":      time.Now().Add(time.Hour).Format(time.RFC3339),
		},
	}
	t.Cleanup(func() {
		releaseWorker()
		waitForAntigravityCreditsRefreshIdle(t, auth.ID)
	})
	cliproxyauth.SetAntigravityCreditsHint(auth.ID, cliproxyauth.AntigravityCreditsHint{})
	exec := NewAntigravityExecutor(&config.Config{
		QuotaExceeded: config.QuotaExceeded{AntigravityCredits: true},
	})

	if _, _, err := exec.ensureAccessToken(ctx, auth); err != nil {
		t.Fatalf("first ensureAccessToken: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("credits refresh did not start")
	}
	if _, _, err := exec.ensureAccessToken(ctx, auth); err != nil {
		t.Fatalf("second ensureAccessToken: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loadCodeAssist calls = %d, want 1", got)
	}
	releaseWorker()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if hint, ok := cliproxyauth.GetAntigravityCreditsHint(auth.ID); ok && hint.Known {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if hint, ok := cliproxyauth.GetAntigravityCreditsHint(auth.ID); !ok || !hint.Available {
		t.Fatalf("hint = %#v, ok = %v", hint, ok)
	}
	waitForAntigravityCreditsRefreshIdle(t, auth.ID)
	if _, _, err := exec.ensureAccessToken(ctx, auth); err != nil {
		t.Fatalf("third ensureAccessToken: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("loadCodeAssist calls after cached hint = %d, want 1", got)
	}
}

func TestEnsureAccessTokenRefreshesStaleCreditsHint(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	var calls atomic.Int32
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", creditsRoundTripperFunc(func(_ *http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body: io.NopCloser(strings.NewReader(
				`{"paidTier":{"id":"tier-1","availableCredits":[{"creditType":"GOOGLE_ONE_AI","creditAmount":"100","minimumCreditAmountForUsage":"10"}]}}`,
			)),
		}, nil
	}))
	auth := &cliproxyauth.Auth{
		ID: "auth-stale-credits-hint",
		Metadata: map[string]any{
			"access_token": "token",
			"expired":      time.Now().Add(time.Hour).Format(time.RFC3339),
		},
	}
	cliproxyauth.SetAntigravityCreditsHint(auth.ID, cliproxyauth.AntigravityCreditsHint{
		Known:     true,
		Available: false,
		UpdatedAt: time.Now().Add(-antigravityCreditsHintRefreshInterval - time.Second),
	})
	exec := NewAntigravityExecutor(&config.Config{
		QuotaExceeded: config.QuotaExceeded{AntigravityCredits: true},
	})

	if _, _, err := exec.ensureAccessToken(ctx, auth); err != nil {
		t.Fatalf("ensureAccessToken: %v", err)
	}
	waitForAntigravityCreditsRefreshIdle(t, auth.ID)
	if got := calls.Load(); got != 1 {
		t.Fatalf("loadCodeAssist calls = %d, want 1", got)
	}
	if hint, ok := cliproxyauth.GetAntigravityCreditsHint(auth.ID); !ok || !hint.Available {
		t.Fatalf("hint = %#v, ok = %v", hint, ok)
	}
}

func TestAntigravityCreditsRefreshDoesNotOverrideNewerPermanentDisable(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWorker := func() { releaseOnce.Do(func() { close(release) }) }
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", creditsRoundTripperFunc(func(_ *http.Request) (*http.Response, error) {
		close(started)
		<-release
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body: io.NopCloser(strings.NewReader(
				`{"paidTier":{"id":"tier-1","availableCredits":[{"creditType":"GOOGLE_ONE_AI","creditAmount":"100","minimumCreditAmountForUsage":"10"}]}}`,
			)),
		}, nil
	}))
	auth := &cliproxyauth.Auth{
		ID: "auth-stale-credits-response",
		Metadata: map[string]any{
			"access_token": "token",
			"expired":      time.Now().Add(time.Hour).Format(time.RFC3339),
		},
	}
	t.Cleanup(func() {
		releaseWorker()
		waitForAntigravityCreditsRefreshIdle(t, auth.ID)
	})
	cliproxyauth.SetAntigravityCreditsHint(auth.ID, cliproxyauth.AntigravityCreditsHint{})
	exec := NewAntigravityExecutor(&config.Config{
		QuotaExceeded: config.QuotaExceeded{AntigravityCredits: true},
	})

	if _, _, err := exec.ensureAccessToken(ctx, auth); err != nil {
		t.Fatalf("ensureAccessToken: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("credits refresh did not start")
	}
	markAntigravityCreditsPermanentlyDisabled(auth)
	releaseWorker()
	waitForAntigravityCreditsRefreshIdle(t, auth.ID)

	hint, ok := cliproxyauth.GetAntigravityCreditsHint(auth.ID)
	if !ok || !hint.Known || hint.Available {
		t.Fatalf("hint = %#v, ok = %v", hint, ok)
	}
	_, failure, ok := antigravityCreditsFailureStateForAuth(auth)
	if !ok || !failure.PermanentlyDisabled {
		t.Fatalf("failure state = %#v, ok = %v", failure, ok)
	}
}

func TestAntigravityCreditsRefreshDoesNotOverrideNewerTemporaryDisable(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWorker := func() { releaseOnce.Do(func() { close(release) }) }
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", creditsRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if _, hasDeadline := req.Context().Deadline(); hasDeadline {
			t.Error("credits hint request unexpectedly has a deadline")
		}
		close(started)
		<-release
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body: io.NopCloser(strings.NewReader(
				`{"paidTier":{"id":"tier-1","availableCredits":[{"creditType":"GOOGLE_ONE_AI","creditAmount":"100","minimumCreditAmountForUsage":"10"}]}}`,
			)),
		}, nil
	}))
	auth := &cliproxyauth.Auth{
		ID: "auth-stale-credits-temporary-disable",
		Metadata: map[string]any{
			"access_token": "token",
			"expired":      time.Now().Add(time.Hour).Format(time.RFC3339),
		},
	}
	t.Cleanup(func() {
		releaseWorker()
		waitForAntigravityCreditsRefreshIdle(t, auth.ID)
	})
	cliproxyauth.SetAntigravityCreditsHint(auth.ID, cliproxyauth.AntigravityCreditsHint{})
	exec := NewAntigravityExecutor(&config.Config{
		QuotaExceeded: config.QuotaExceeded{AntigravityCredits: true},
	})

	if _, _, errToken := exec.ensureAccessToken(ctx, auth); errToken != nil {
		t.Fatalf("ensureAccessToken() error: %v", errToken)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("credits refresh did not start")
	}
	recordAntigravityCreditsFailure(auth, time.Now())
	releaseWorker()
	waitForAntigravityCreditsRefreshIdle(t, auth.ID)

	hint, ok := cliproxyauth.GetAntigravityCreditsHint(auth.ID)
	if !ok || !hint.Known || hint.Available {
		t.Fatalf("hint = %#v, ok = %v", hint, ok)
	}
	_, failure, ok := antigravityCreditsFailureStateForAuth(auth)
	if !ok || !failure.DisabledUntil.After(time.Now()) || failure.PermanentlyDisabled {
		t.Fatalf("failure state = %#v, ok = %v", failure, ok)
	}
}

func TestEnsureAccessTokenDoesNotRefreshActiveNegativeHint(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	var calls atomic.Int32
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", creditsRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("unexpected credits request")
	}))
	auth := &cliproxyauth.Auth{
		ID: "auth-active-negative-hint",
		Metadata: map[string]any{
			"access_token": "token",
			"expired":      time.Now().Add(time.Hour).Format(time.RFC3339),
		},
	}
	recordAntigravityCreditsFailure(auth, time.Now())
	hint, _ := cliproxyauth.GetAntigravityCreditsHint(auth.ID)
	hint.UpdatedAt = time.Now().Add(-antigravityCreditsHintRefreshInterval - time.Second)
	cliproxyauth.SetAntigravityCreditsHint(auth.ID, hint)

	exec := NewAntigravityExecutor(&config.Config{QuotaExceeded: config.QuotaExceeded{AntigravityCredits: true}})
	if _, _, errToken := exec.ensureAccessToken(ctx, auth); errToken != nil {
		t.Fatalf("ensureAccessToken() error: %v", errToken)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("loadCodeAssist calls = %d, want 0 while disabled", got)
	}
}

func TestAntigravityCreditsRefreshDoesNotCrossCredentialGeneration(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	oldStarted := make(chan struct{})
	oldRelease := make(chan struct{})
	oldDone := make(chan struct{})
	var releaseOnce sync.Once
	releaseOld := func() { releaseOnce.Do(func() { close(oldRelease) }) }
	t.Cleanup(releaseOld)
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", creditsRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		switch req.Header.Get("Authorization") {
		case "Bearer old-token":
			close(oldStarted)
			select {
			case <-oldRelease:
				close(oldDone)
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(`{"paidTier":{"id":"old-tier","availableCredits":[{"creditType":"GOOGLE_ONE_AI","creditAmount":"100","minimumCreditAmountForUsage":"10"}]}}`)),
				}, nil
			case <-req.Context().Done():
				close(oldDone)
				return nil, req.Context().Err()
			}
		case "Bearer new-token":
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"paidTier":{"id":"new-tier","availableCredits":[]}}`)),
			}, nil
		default:
			return nil, fmt.Errorf("unexpected authorization %q", req.Header.Get("Authorization"))
		}
	}))
	oldAuth := &cliproxyauth.Auth{ID: "auth-credential-generation", Metadata: map[string]any{"access_token": "old-token", "expired": time.Now().Add(time.Hour).Format(time.RFC3339)}}
	newAuth := &cliproxyauth.Auth{ID: oldAuth.ID, Metadata: map[string]any{"access_token": "new-token", "expired": time.Now().Add(time.Hour).Format(time.RFC3339)}}
	exec := NewAntigravityExecutor(&config.Config{QuotaExceeded: config.QuotaExceeded{AntigravityCredits: true}})

	if _, _, errToken := exec.ensureAccessToken(ctx, oldAuth); errToken != nil {
		t.Fatalf("old ensureAccessToken() error: %v", errToken)
	}
	select {
	case <-oldStarted:
	case <-time.After(time.Second):
		t.Fatal("old credits refresh did not start")
	}
	if _, _, errToken := exec.ensureAccessToken(ctx, newAuth); errToken != nil {
		t.Fatalf("new ensureAccessToken() error: %v", errToken)
	}
	select {
	case <-oldDone:
	case <-time.After(time.Second):
		t.Fatal("old credential refresh was not canceled")
	}
	waitForAntigravityCreditsRefreshIdle(t, newAuth.ID)
	newKey := cliproxyauth.AntigravityCreditsCredentialKey(newAuth)
	if hint, ok := cliproxyauth.GetAntigravityCreditsHint(newAuth.ID); !ok || hint.Available || hint.CredentialKey != newKey {
		t.Fatalf("new credential hint = %#v, ok = %v", hint, ok)
	}

	if hint, ok := cliproxyauth.GetAntigravityCreditsHint(newAuth.ID); !ok || hint.Available || hint.CredentialKey != newKey {
		t.Fatalf("old refresh overwrote new credential hint: %#v, ok = %v", hint, ok)
	}
}

func TestAntigravityExecutorCloseCancelsManagedCreditsRefresh(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	started := make(chan struct{})
	canceled := make(chan error, 1)
	var startedOnce sync.Once
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", creditsRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		startedOnce.Do(func() { close(started) })
		<-req.Context().Done()
		canceled <- req.Context().Err()
		return nil, req.Context().Err()
	}))
	exec := NewAntigravityExecutor(&config.Config{QuotaExceeded: config.QuotaExceeded{AntigravityCredits: true}})
	manager := cliproxyauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(exec)
	authID := "auth-credits-executor-close"
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), &cliproxyauth.Auth{
		ID:       authID,
		Provider: "antigravity",
		Metadata: map[string]any{
			"access_token": "managed-token",
			"expired":      time.Now().Add(time.Hour).Format(time.RFC3339),
		},
	}); errRegister != nil {
		t.Fatalf("Register() error: %v", errRegister)
	}
	managedAuth, ok := manager.GetByID(authID)
	if !ok {
		t.Fatal("managed auth missing")
	}

	if _, _, errToken := exec.ensureAccessToken(ctx, managedAuth); errToken != nil {
		t.Fatalf("ensureAccessToken() error: %v", errToken)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("managed credits refresh did not start")
	}
	if errClose := manager.CloseExecutors(); errClose != nil {
		t.Fatalf("CloseExecutors() error: %v", errClose)
	}
	select {
	case errCanceled := <-canceled:
		if !errors.Is(errCanceled, context.Canceled) {
			t.Fatalf("credits refresh error = %v, want context.Canceled", errCanceled)
		}
	case <-time.After(time.Second):
		t.Fatal("executor close did not cancel credits refresh")
	}
	waitForAntigravityCreditsRefreshIdle(t, authID)
}

func TestFetchAntigravityCreditsHintValidatesResponseShape(t *testing.T) {
	testCases := []struct {
		name          string
		body          string
		wantOK        bool
		wantKnown     bool
		wantAvailable bool
	}{
		{name: "empty credits", body: `{"paidTier":{"id":"tier-1","availableCredits":[]}}`, wantOK: true, wantKnown: true},
		{name: "other credit type", body: `{"paidTier":{"id":"tier-1","availableCredits":[{"creditType":"OTHER","creditAmount":"100","minimumCreditAmountForUsage":"10"}]}}`, wantOK: true, wantKnown: true},
		{name: "missing paid tier", body: `{}`, wantOK: false},
		{name: "missing credits", body: `{"paidTier":{"id":"tier-1"}}`, wantOK: false},
		{name: "non array credits", body: `{"paidTier":{"availableCredits":{}}}`, wantOK: false},
		{name: "malformed target amount", body: `{"paidTier":{"availableCredits":[{"creditType":"GOOGLE_ONE_AI","creditAmount":"bad","minimumCreditAmountForUsage":"10"}]}}`, wantOK: false},
		{name: "nan target amount", body: `{"paidTier":{"availableCredits":[{"creditType":"GOOGLE_ONE_AI","creditAmount":"NaN","minimumCreditAmountForUsage":"10"}]}}`, wantOK: false},
		{name: "infinite target minimum", body: `{"paidTier":{"availableCredits":[{"creditType":"GOOGLE_ONE_AI","creditAmount":"100","minimumCreditAmountForUsage":"Inf"}]}}`, wantOK: false},
		{name: "invalid json", body: `{`, wantOK: false},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", creditsRoundTripperFunc(func(_ *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(testCase.body)),
				}, nil
			}))
			exec := NewAntigravityExecutor(&config.Config{})
			hint, ok := exec.fetchAntigravityCreditsHint(ctx, &cliproxyauth.Auth{ID: "auth-shape-" + testCase.name}, "token")
			if ok != testCase.wantOK || hint.Known != testCase.wantKnown || hint.Available != testCase.wantAvailable {
				t.Fatalf("hint = %#v, ok = %v", hint, ok)
			}
		})
	}
}

func TestMarkAntigravityCreditsPermanentlyDisabledClearsAvailableHint(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	auth := &cliproxyauth.Auth{ID: "auth-credits-permanently-disabled"}
	cliproxyauth.SetAntigravityCreditsHint(auth.ID, cliproxyauth.AntigravityCreditsHint{
		Known:        true,
		Available:    true,
		CreditAmount: 100,
	})
	markAntigravityCreditsPermanentlyDisabled(auth)

	hint, ok := cliproxyauth.GetAntigravityCreditsHint(auth.ID)
	if !ok || !hint.Known || hint.Available {
		t.Fatalf("hint = %#v, ok = %v", hint, ok)
	}
}

func TestMarkAntigravityCreditsAvailableClearsMatchingNegativeState(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	auth := &cliproxyauth.Auth{
		ID:       "auth-credits-available",
		Metadata: map[string]any{"access_token": "available-token"},
	}
	now := time.Now()
	recordAntigravityCreditsFailure(auth, now)
	markAntigravityCreditsAvailable(auth, now.Add(time.Second))

	if _, ok := antigravityCreditsFailureByAuth.Load(auth.ID); ok {
		t.Fatal("matching credits failure state was not cleared")
	}
	hint, ok := cliproxyauth.GetAntigravityCreditsHint(auth.ID)
	if !ok || !hint.Known || !hint.Available || hint.BlocksRouting(time.Now()) {
		t.Fatalf("available hint = %#v, ok = %v", hint, ok)
	}
	if hint.CredentialKey != cliproxyauth.AntigravityCreditsCredentialKey(auth) {
		t.Fatalf("hint credential key = %q", hint.CredentialKey)
	}
}

func TestMarkAntigravityCreditsAvailableDoesNotClearPermanentState(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	auth := &cliproxyauth.Auth{
		ID:       "auth-credits-late-success",
		Metadata: map[string]any{"access_token": "permanent-token"},
	}
	markAntigravityCreditsPermanentlyDisabled(auth)
	markAntigravityCreditsAvailable(auth, time.Now().Add(time.Second))

	_, failure, ok := antigravityCreditsFailureStateForAuth(auth)
	if !ok || !failure.PermanentlyDisabled {
		t.Fatalf("failure state = %#v, ok = %v", failure, ok)
	}
	hint, ok := cliproxyauth.GetAntigravityCreditsHint(auth.ID)
	if !ok || !hint.PermanentlyUnavailable || hint.Available {
		t.Fatalf("permanent hint = %#v, ok = %v", hint, ok)
	}
}

func TestStaleAntigravityCreditsReadDoesNotDeleteCurrentCredentialState(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	current := &cliproxyauth.Auth{ID: "auth-credits-rotated", Metadata: map[string]any{"access_token": "current-token"}}
	stale := &cliproxyauth.Auth{ID: current.ID, Metadata: map[string]any{"access_token": "stale-token"}}
	markAntigravityCreditsPermanentlyDisabled(current)

	if antigravityCreditsDisabled(stale, time.Now()) {
		t.Fatal("stale credential inherited current credential credits state")
	}
	_, failure, ok := antigravityCreditsFailureStateForAuth(current)
	if !ok || !failure.PermanentlyDisabled {
		t.Fatalf("current failure state = %#v, ok = %v", failure, ok)
	}
}

func TestRecordAntigravityCreditsFailureDoesNotDowngradePermanentDisable(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	auth := &cliproxyauth.Auth{
		ID:       "auth-credits-permanent-wins",
		Metadata: map[string]any{"access_token": "permanent-token"},
	}
	markAntigravityCreditsPermanentlyDisabled(auth)
	recordAntigravityCreditsFailure(auth, time.Now())

	_, failure, ok := antigravityCreditsFailureStateForAuth(auth)
	if !ok || !failure.PermanentlyDisabled || !failure.ExplicitBalanceExhausted {
		t.Fatalf("failure state = %#v, ok = %v", failure, ok)
	}
	hint, ok := cliproxyauth.GetAntigravityCreditsHint(auth.ID)
	if !ok || !hint.PermanentlyUnavailable || hint.Available {
		t.Fatalf("permanent hint = %#v, ok = %v", hint, ok)
	}
}

func waitForAntigravityCreditsRefreshIdle(t *testing.T, authID string) {
	t.Helper()
	state := antigravityCreditsRefreshState(authID)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		state.mu.Lock()
		inFlight := state.inFlight
		state.mu.Unlock()
		if !inFlight {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("credits refresh for %q did not become idle", authID)
}

func TestClassifyAntigravity429(t *testing.T) {
	t.Run("quota exhausted", func(t *testing.T) {
		body := []byte(`{"error":{"status":"RESOURCE_EXHAUSTED","message":"QUOTA_EXHAUSTED"}}`)
		if got := classifyAntigravity429(body); got != antigravity429QuotaExhausted {
			t.Fatalf("classifyAntigravity429() = %q, want %q", got, antigravity429QuotaExhausted)
		}
	})

	t.Run("structured rate limit", func(t *testing.T) {
		body := []byte(`{
			"error": {
				"status": "RESOURCE_EXHAUSTED",
				"details": [
					{"@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": "RATE_LIMIT_EXCEEDED"},
					{"@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "0.5s"}
				]
			}
		}`)
		if got := classifyAntigravity429(body); got != antigravity429RateLimited {
			t.Fatalf("classifyAntigravity429() = %q, want %q", got, antigravity429RateLimited)
		}
	})

	t.Run("structured quota exhausted", func(t *testing.T) {
		body := []byte(`{
			"error": {
				"status": "RESOURCE_EXHAUSTED",
				"details": [
					{"@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": "QUOTA_EXHAUSTED"}
				]
			}
		}`)
		if got := classifyAntigravity429(body); got != antigravity429QuotaExhausted {
			t.Fatalf("classifyAntigravity429() = %q, want %q", got, antigravity429QuotaExhausted)
		}
	})

	t.Run("unstructured 429 defaults to soft rate limit", func(t *testing.T) {
		body := []byte(`{"error":{"message":"too many requests"}}`)
		if got := classifyAntigravity429(body); got != antigravity429SoftRateLimit {
			t.Fatalf("classifyAntigravity429() = %q, want %q", got, antigravity429SoftRateLimit)
		}
	})
}

func TestInjectEnabledCreditTypes(t *testing.T) {
	body := []byte(`{"model":"gemini-2.5-flash","request":{}}`)
	got := injectEnabledCreditTypes(body)
	if got == nil {
		t.Fatal("injectEnabledCreditTypes() returned nil")
	}
	if !strings.Contains(string(got), `"enabledCreditTypes":["GOOGLE_ONE_AI"]`) {
		t.Fatalf("injectEnabledCreditTypes() = %s, want enabledCreditTypes", string(got))
	}

	if got := injectEnabledCreditTypes([]byte(`not json`)); got != nil {
		t.Fatalf("injectEnabledCreditTypes() for invalid json = %s, want nil", string(got))
	}
}

func TestShouldMarkAntigravityCreditsExhausted(t *testing.T) {
	t.Run("credit errors are marked", func(t *testing.T) {
		for _, body := range [][]byte{
			[]byte(`{"error":{"message":"Insufficient GOOGLE_ONE_AI credits"}}`),
			[]byte(`{"error":{"message":"minimumCreditAmountForUsage requirement not met"}}`),
		} {
			if !shouldMarkAntigravityCreditsExhausted(http.StatusForbidden, body, nil) {
				t.Fatalf("shouldMarkAntigravityCreditsExhausted(%s) = false, want true", string(body))
			}
		}
	})

	t.Run("transient 429 resource exhausted is not marked", func(t *testing.T) {
		body := []byte(`{"error":{"code":429,"message":"Resource has been exhausted (e.g. check quota).","status":"RESOURCE_EXHAUSTED"}}`)
		if shouldMarkAntigravityCreditsExhausted(http.StatusTooManyRequests, body, nil) {
			t.Fatalf("shouldMarkAntigravityCreditsExhausted(%s) = true, want false", string(body))
		}
	})

	t.Run("resource exhausted with quota metadata is still marked", func(t *testing.T) {
		body := []byte(`{"error":{"code":429,"message":"Resource has been exhausted","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","metadata":{"quotaResetDelay":"1h","model":"claude-sonnet-4-6"}}]}}`)
		if !shouldMarkAntigravityCreditsExhausted(http.StatusTooManyRequests, body, nil) {
			t.Fatalf("shouldMarkAntigravityCreditsExhausted(%s) = false, want true", string(body))
		}
	})

	if shouldMarkAntigravityCreditsExhausted(http.StatusServiceUnavailable, []byte(`{"error":{"message":"credits exhausted"}}`), nil) {
		t.Fatal("shouldMarkAntigravityCreditsExhausted() = true for 5xx, want false")
	}
}

func TestAntigravityExecute_RetriesTransient429ResourceExhausted(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		switch requestCount {
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"code":429,"message":"Resource has been exhausted (e.g. check quota).","status":"RESOURCE_EXHAUSTED"}}`))
		case 2:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]}}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}}`))
		default:
			t.Fatalf("unexpected request count %d", requestCount)
		}
	}))
	defer server.Close()

	exec := NewAntigravityExecutor(&config.Config{RequestRetry: 1})
	auth := &cliproxyauth.Auth{
		ID: "auth-transient-429",
		Attributes: map[string]string{
			"base_url": server.URL,
		},
		Metadata: map[string]any{
			"access_token": "token",
			"project_id":   "project-1",
			"expired":      time.Now().Add(1 * time.Hour).Format(time.RFC3339),
		},
	}
	setFreshAntigravityCreditsHint(auth, true)

	resp, err := exec.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatAntigravity,
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(resp.Payload) == 0 {
		t.Fatal("Execute() returned empty payload")
	}
	if requestCount != 2 {
		t.Fatalf("request count = %d, want 2", requestCount)
	}
}

func TestAntigravityExecute_RetriesQuotaExhaustedWithCredits(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	var (
		mu            sync.Mutex
		requestBodies []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()

		mu.Lock()
		requestBodies = append(requestBodies, string(body))
		reqNum := len(requestBodies)
		mu.Unlock()

		if reqNum == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"status":"RESOURCE_EXHAUSTED","message":"QUOTA_EXHAUSTED"}}`))
			return
		}

		if !strings.Contains(string(body), `"enabledCreditTypes":["GOOGLE_ONE_AI"]`) {
			t.Fatalf("second request body missing enabledCreditTypes: %s", string(body))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]}}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}}`))
	}))
	defer server.Close()

	exec := NewAntigravityExecutor(&config.Config{
		QuotaExceeded: config.QuotaExceeded{AntigravityCredits: true},
	})
	auth := &cliproxyauth.Auth{
		ID: "auth-credits-ok",
		Attributes: map[string]string{
			"base_url": server.URL,
		},
		Metadata: map[string]any{
			"access_token": "token",
			"project_id":   "project-1",
			"expired":      time.Now().Add(1 * time.Hour).Format(time.RFC3339),
		},
	}
	setFreshAntigravityCreditsHint(auth, true)

	resp, err := exec.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatAntigravity,
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(resp.Payload) == 0 {
		t.Fatal("Execute() returned empty payload")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requestBodies) != 2 {
		t.Fatalf("request count = %d, want 2", len(requestBodies))
	}
}

func TestAntigravityExecute_SkipsCreditsRetryWhenAlreadyExhausted(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"status":"RESOURCE_EXHAUSTED","message":"QUOTA_EXHAUSTED"}}`))
	}))
	defer server.Close()

	exec := NewAntigravityExecutor(&config.Config{
		QuotaExceeded: config.QuotaExceeded{AntigravityCredits: true},
	})
	auth := &cliproxyauth.Auth{
		ID: "auth-credits-exhausted",
		Attributes: map[string]string{
			"base_url": server.URL,
		},
		Metadata: map[string]any{
			"access_token": "token",
			"project_id":   "project-1",
			"expired":      time.Now().Add(1 * time.Hour).Format(time.RFC3339),
		},
	}
	setFreshAntigravityCreditsHint(auth, false)
	recordAntigravityCreditsFailure(auth, time.Now())

	_, err := exec.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatAntigravity,
	})
	if err == nil {
		t.Fatal("Execute() error = nil, want 429")
	}
	sErr, ok := err.(statusErr)
	if !ok {
		t.Fatalf("Execute() error type = %T, want statusErr", err)
	}
	if got := sErr.StatusCode(); got != http.StatusTooManyRequests {
		t.Fatalf("Execute() status code = %d, want %d", got, http.StatusTooManyRequests)
	}
	if requestCount != 1 {
		t.Fatalf("request count = %d, want 1", requestCount)
	}
}

func TestAntigravityExecute_SkipsDirectCreditsWhenAlreadyDisabled(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	var requestBodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		requestBodies = append(requestBodies, string(body))
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"status":"RESOURCE_EXHAUSTED","message":"QUOTA_EXHAUSTED"}}`))
	}))
	defer server.Close()

	exec := NewAntigravityExecutor(&config.Config{
		QuotaExceeded: config.QuotaExceeded{AntigravityCredits: true},
	})
	auth := &cliproxyauth.Auth{
		ID: "auth-direct-credits-disabled",
		Attributes: map[string]string{
			"base_url": server.URL,
		},
		Metadata: map[string]any{
			"access_token": "token",
			"project_id":   "project-1",
			"expired":      time.Now().Add(1 * time.Hour).Format(time.RFC3339),
		},
	}
	setFreshAntigravityCreditsHint(auth, false)
	recordAntigravityCreditsFailure(auth, time.Now())

	ctx := cliproxyauth.WithAntigravityCredits(context.Background())
	_, err := exec.Execute(ctx, auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatAntigravity,
	})
	if err == nil {
		t.Fatal("Execute() error = nil, want 429")
	}
	if len(requestBodies) != 1 {
		t.Fatalf("request count = %d, want 1", len(requestBodies))
	}
	if strings.Contains(requestBodies[0], `"enabledCreditTypes":["GOOGLE_ONE_AI"]`) {
		t.Fatalf("request unexpectedly used disabled credits: %s", requestBodies[0])
	}
}

func TestAntigravityShouldUseCreditsDirectRespectsDisabledAuth(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	cfg := &config.Config{QuotaExceeded: config.QuotaExceeded{AntigravityCredits: true}}
	auth := &cliproxyauth.Auth{ID: "auth-direct-helper-disabled"}
	now := time.Now()
	markAntigravityPreferCredits(auth, "gemini-2.5-flash", now, nil)
	recordAntigravityCreditsFailure(auth, now)

	if antigravityShouldUseCreditsDirect(cfg, auth, "gemini-2.5-flash", true, now) {
		t.Fatal("antigravityShouldUseCreditsDirect() = true for requested disabled credits, want false")
	}
	if antigravityShouldUseCreditsDirect(cfg, auth, "gemini-2.5-flash", false, now) {
		t.Fatal("antigravityShouldUseCreditsDirect() = true for preferred disabled credits, want false")
	}
}

func TestAntigravityExecute_PrefersCreditsAfterSuccessfulFallback(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	var (
		mu            sync.Mutex
		requestBodies []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()

		mu.Lock()
		requestBodies = append(requestBodies, string(body))
		reqNum := len(requestBodies)
		mu.Unlock()

		switch reqNum {
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED"},{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"10s"}]}}`))
		case 2, 3:
			if !strings.Contains(string(body), `"enabledCreditTypes":["GOOGLE_ONE_AI"]`) {
				t.Fatalf("request %d body missing enabledCreditTypes: %s", reqNum, string(body))
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"OK"}]}}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}}`))
		default:
			t.Fatalf("unexpected request count %d", reqNum)
		}
	}))
	defer server.Close()

	exec := NewAntigravityExecutor(&config.Config{
		QuotaExceeded: config.QuotaExceeded{AntigravityCredits: true},
	})
	auth := &cliproxyauth.Auth{
		ID: "auth-prefer-credits",
		Attributes: map[string]string{
			"base_url": server.URL,
		},
		Metadata: map[string]any{
			"access_token": "token",
			"project_id":   "project-1",
			"expired":      time.Now().Add(1 * time.Hour).Format(time.RFC3339),
		},
	}
	setFreshAntigravityCreditsHint(auth, true)

	request := cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatAntigravity}

	if _, err := exec.Execute(context.Background(), auth, request, opts); err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	if _, err := exec.Execute(context.Background(), auth, request, opts); err != nil {
		t.Fatalf("second Execute() error = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(requestBodies) != 3 {
		t.Fatalf("request count = %d, want 3", len(requestBodies))
	}
	if strings.Contains(requestBodies[0], `"enabledCreditTypes":["GOOGLE_ONE_AI"]`) {
		t.Fatalf("first request unexpectedly used credits: %s", requestBodies[0])
	}
	if !strings.Contains(requestBodies[1], `"enabledCreditTypes":["GOOGLE_ONE_AI"]`) {
		t.Fatalf("fallback request missing credits: %s", requestBodies[1])
	}
	if !strings.Contains(requestBodies[2], `"enabledCreditTypes":["GOOGLE_ONE_AI"]`) {
		t.Fatalf("preferred request missing credits: %s", requestBodies[2])
	}
}

func TestAntigravityExecute_DirectCreditsSuccessClearsConcurrentTemporaryFailure(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	var auth *cliproxyauth.Auth
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if !strings.Contains(string(body), `"enabledCreditTypes":["GOOGLE_ONE_AI"]`) {
			t.Fatalf("direct request missing enabledCreditTypes: %s", string(body))
		}
		recordAntigravityCreditsFailure(auth, time.Now())
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"OK"}]}}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}}`))
	}))
	defer server.Close()

	exec := NewAntigravityExecutor(&config.Config{QuotaExceeded: config.QuotaExceeded{AntigravityCredits: true}})
	auth = &cliproxyauth.Auth{
		ID:         "auth-direct-credits-success",
		Attributes: map[string]string{"base_url": server.URL},
		Metadata: map[string]any{
			"access_token": "token",
			"project_id":   "project-1",
			"expired":      time.Now().Add(time.Hour).Format(time.RFC3339),
		},
	}
	setFreshAntigravityCreditsHint(auth, true)
	ctx := cliproxyauth.WithAntigravityCredits(t.Context())
	_, errExecute := exec.Execute(ctx, auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatAntigravity})
	if errExecute != nil {
		t.Fatalf("Execute() error: %v", errExecute)
	}
	if _, ok := antigravityCreditsFailureByAuth.Load(auth.ID); ok {
		t.Fatal("successful direct credits request did not clear temporary failure")
	}
	hint, ok := cliproxyauth.GetAntigravityCreditsHint(auth.ID)
	if !ok || !hint.Available || hint.PermanentlyUnavailable {
		t.Fatalf("credits hint = %#v, ok = %v", hint, ok)
	}
}

func TestAntigravityDirectCreditsSuccessStateAcrossStreamPaths(t *testing.T) {
	paths := []struct {
		name  string
		model string
		run   func(context.Context, *AntigravityExecutor, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) error
	}{
		{
			name:  "claude non stream",
			model: "claude-sonnet-4-5",
			run: func(ctx context.Context, exec *AntigravityExecutor, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) error {
				_, errExecute := exec.Execute(ctx, auth, req, opts)
				return errExecute
			},
		},
		{
			name:  "execute stream",
			model: "gemini-2.5-flash",
			run: func(ctx context.Context, exec *AntigravityExecutor, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) error {
				result, errStream := exec.ExecuteStream(ctx, auth, req, opts)
				if errStream != nil {
					return errStream
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						return chunk.Err
					}
				}
				return nil
			},
		},
	}

	for _, path := range paths {
		for _, permanent := range []bool{false, true} {
			stateName := "temporary"
			if permanent {
				stateName = "permanent"
			}
			t.Run(path.name+"/"+stateName, func(t *testing.T) {
				resetAntigravityCreditsRetryState()
				t.Cleanup(resetAntigravityCreditsRetryState)

				var auth *cliproxyauth.Auth
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, _ := io.ReadAll(r.Body)
					_ = r.Body.Close()
					if !strings.Contains(string(body), `"enabledCreditTypes":["GOOGLE_ONE_AI"]`) {
						t.Errorf("direct request missing enabledCreditTypes: %s", string(body))
					}
					if permanent {
						markAntigravityCreditsPermanentlyDisabled(auth)
					} else {
						recordAntigravityCreditsFailure(auth, time.Now())
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"OK\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":1,\"candidatesTokenCount\":1,\"totalTokenCount\":2}}}\n\n")
				}))
				defer server.Close()

				exec := NewAntigravityExecutor(&config.Config{QuotaExceeded: config.QuotaExceeded{AntigravityCredits: true}})
				t.Cleanup(func() { _ = exec.Close() })
				auth = &cliproxyauth.Auth{
					ID:         "auth-direct-stream-state-" + strings.ReplaceAll(path.name, " ", "-") + "-" + stateName,
					Attributes: map[string]string{"base_url": server.URL},
					Metadata: map[string]any{
						"access_token": "token",
						"project_id":   "project-1",
						"expired":      time.Now().Add(time.Hour).Format(time.RFC3339),
					},
				}
				setFreshAntigravityCreditsHint(auth, true)
				ctx := cliproxyauth.WithAntigravityCredits(t.Context())
				errRun := path.run(ctx, exec, auth, cliproxyexecutor.Request{
					Model:   path.model,
					Payload: []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`),
				}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatAntigravity})
				if errRun != nil {
					t.Fatalf("request error: %v", errRun)
				}

				_, failure, ok := antigravityCreditsFailureStateForAuth(auth)
				hint, hasHint := cliproxyauth.GetAntigravityCreditsHint(auth.ID)
				if permanent {
					if !ok || !failure.PermanentlyDisabled || !hasHint || !hint.PermanentlyUnavailable || hint.Available {
						t.Fatalf("permanent state = %#v, hint = %#v, ok = %v, hasHint = %v", failure, hint, ok, hasHint)
					}
				} else if failure.Count != 0 || !hasHint || !hint.Available || hint.PermanentlyUnavailable {
					t.Fatalf("temporary state = %#v, hint = %#v, hasHint = %v", failure, hint, hasHint)
				}
			})
		}
	}
}

func TestAntigravityExecute_PreservesBaseURLFallbackAfterCreditsRetryFailure(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	var (
		mu          sync.Mutex
		firstCount  int
		secondCount int
	)

	firstServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()

		mu.Lock()
		firstCount++
		reqNum := firstCount
		mu.Unlock()

		switch reqNum {
		case 1:
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED"}]}}`))
		case 2:
			if !strings.Contains(string(body), `"enabledCreditTypes":["GOOGLE_ONE_AI"]`) {
				t.Fatalf("credits retry missing enabledCreditTypes: %s", string(body))
			}
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":{"message":"permission denied"}}`))
		default:
			t.Fatalf("unexpected first server request count %d", reqNum)
		}
	}))
	defer firstServer.Close()

	secondServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		secondCount++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]}}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}}`))
	}))
	defer secondServer.Close()

	exec := NewAntigravityExecutor(&config.Config{
		QuotaExceeded: config.QuotaExceeded{AntigravityCredits: true},
	})
	auth := &cliproxyauth.Auth{
		ID: "auth-baseurl-fallback",
		Attributes: map[string]string{
			"base_url": firstServer.URL,
		},
		Metadata: map[string]any{
			"access_token": "token",
			"project_id":   "project-1",
			"expired":      time.Now().Add(1 * time.Hour).Format(time.RFC3339),
		},
	}
	setFreshAntigravityCreditsHint(auth, true)

	originalOrder := antigravityBaseURLFallbackOrder
	defer func() { antigravityBaseURLFallbackOrder = originalOrder }()
	antigravityBaseURLFallbackOrder = func(auth *cliproxyauth.Auth) []string {
		return []string{firstServer.URL, secondServer.URL}
	}

	resp, err := exec.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatAntigravity,
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(resp.Payload) == 0 {
		t.Fatal("Execute() returned empty payload")
	}
	if firstCount != 2 {
		t.Fatalf("first server request count = %d, want 2", firstCount)
	}
	if secondCount != 1 {
		t.Fatalf("second server request count = %d, want 1", secondCount)
	}
}

func TestAntigravityExecute_DoesNotDirectInjectCreditsWhenFlagDisabled(t *testing.T) {
	resetAntigravityCreditsRetryState()
	t.Cleanup(resetAntigravityCreditsRetryState)

	var requestBodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		requestBodies = append(requestBodies, string(body))
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"status":"RESOURCE_EXHAUSTED","message":"QUOTA_EXHAUSTED"}}`))
	}))
	defer server.Close()

	exec := NewAntigravityExecutor(&config.Config{
		QuotaExceeded: config.QuotaExceeded{AntigravityCredits: false},
	})
	auth := &cliproxyauth.Auth{
		ID: "auth-flag-disabled",
		Attributes: map[string]string{
			"base_url": server.URL,
		},
		Metadata: map[string]any{
			"access_token": "token",
			"project_id":   "project-1",
			"expired":      time.Now().Add(1 * time.Hour).Format(time.RFC3339),
		},
	}
	markAntigravityPreferCredits(auth, "gemini-2.5-flash", time.Now(), nil)

	_, err := exec.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatAntigravity,
	})
	if err == nil {
		t.Fatal("Execute() error = nil, want 429")
	}
	if len(requestBodies) != 1 {
		t.Fatalf("request count = %d, want 1", len(requestBodies))
	}
	if strings.Contains(requestBodies[0], `"enabledCreditTypes":["GOOGLE_ONE_AI"]`) {
		t.Fatalf("request unexpectedly used enabledCreditTypes with flag disabled: %s", requestBodies[0])
	}
}
