package auth

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

type testRefreshEvaluator struct{}

func (testRefreshEvaluator) ShouldRefresh(time.Time, *Auth) bool { return false }

type autoRefreshShutdownExecutor struct {
	schedulerProviderTestExecutor
	started chan struct{}
	stopped chan struct{}
}

type importRefreshPolicyHook struct {
	NoopHook
	mu       sync.Mutex
	policies []ChatGPTWebImportPolicy
	imports  []bool
}

func (hook *importRefreshPolicyHook) OnAuthUpdated(ctx context.Context, _ *Auth) {
	policy, imported := ChatGPTWebImportPolicyFromContext(ctx)
	hook.mu.Lock()
	hook.policies = append(hook.policies, policy)
	hook.imports = append(hook.imports, imported)
	hook.mu.Unlock()
}

func (hook *importRefreshPolicyHook) last() (ChatGPTWebImportPolicy, bool) {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	if len(hook.policies) == 0 {
		return ChatGPTWebImportPolicy{}, false
	}
	index := len(hook.policies) - 1
	return hook.policies[index], hook.imports[index]
}

func (e *autoRefreshShutdownExecutor) Refresh(ctx context.Context, _ *Auth) (*Auth, error) {
	close(e.started)
	<-ctx.Done()
	close(e.stopped)
	return nil, ctx.Err()
}

func TestStopAutoRefreshWaitsForWorkers(t *testing.T) {
	const provider = "auto-refresh-shutdown"
	manager := NewManager(nil, nil, nil)
	executor := &autoRefreshShutdownExecutor{
		schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: provider},
		started:                       make(chan struct{}),
		stopped:                       make(chan struct{}),
	}
	manager.RegisterExecutor(executor)
	registered, errRegister := manager.Register(t.Context(), &Auth{ID: "shutdown-auth", Provider: provider, Status: StatusActive})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	job, marked := manager.markRefreshPending(registered.ID, time.Now())
	if !marked {
		t.Fatal("markRefreshPending() = false")
	}
	ctx, cancel := context.WithCancel(context.Background())
	loop := newAuthAutoRefreshLoop(manager, time.Hour, 1)
	manager.mu.Lock()
	manager.refreshCancel = cancel
	manager.refreshLoop = loop
	manager.mu.Unlock()
	loop.jobs <- job
	go loop.run(ctx)
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("refresh worker did not start")
	}

	manager.StopAutoRefresh()
	select {
	case <-executor.stopped:
	default:
		t.Fatal("StopAutoRefresh() returned before refresh worker exited")
	}
	if !loop.wait(time.Millisecond) {
		t.Fatal("StopAutoRefresh() returned before refresh loop exited")
	}
}

func TestChatGPTWebImportRefreshCarriesModelValidationPolicy(t *testing.T) {
	for _, testCase := range []struct {
		name           string
		validateModels bool
	}{
		{name: "disabled", validateModels: false},
		{name: "enabled", validateModels: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			hook := &importRefreshPolicyHook{}
			manager := NewManager(nil, nil, hook)
			manager.RegisterExecutor(schedulerProviderTestExecutor{provider: "chatgpt-web"})
			metadata := map[string]any{
				"access_token":                "access",
				"lifecycle_state":             LifecycleStateActive,
				ChatGPTWebImportSessionIntent: true,
				ChatGPTWebImportModelsIntent:  testCase.validateModels,
			}
			registered, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{
				ID:       "import-refresh-policy-" + testCase.name,
				Provider: "chatgpt-web",
				Status:   StatusActive,
				Metadata: metadata,
			})
			if errRegister != nil {
				t.Fatalf("Register() error = %v", errRegister)
			}

			manager.mu.RLock()
			expected := manager.auths[registered.ID]
			manager.mu.RUnlock()
			manager.refreshAuthExpected(WithSkipPersist(t.Context()), registered.ID, expected, time.Time{})

			policy, imported := hook.last()
			if !imported {
				t.Fatal("refresh hook did not receive the import policy")
			}
			if policy.ValidateModels != testCase.validateModels {
				t.Fatalf("ValidateModels = %v, want %v", policy.ValidateModels, testCase.validateModels)
			}
		})
	}
}

func setRefreshLeadFactory(t *testing.T, provider string, factory func() *time.Duration) {
	t.Helper()
	key := strings.ToLower(strings.TrimSpace(provider))
	refreshLeadMu.Lock()
	prev, hadPrev := refreshLeadFactories[key]
	if factory == nil {
		delete(refreshLeadFactories, key)
	} else {
		refreshLeadFactories[key] = factory
	}
	refreshLeadMu.Unlock()
	t.Cleanup(func() {
		refreshLeadMu.Lock()
		if hadPrev {
			refreshLeadFactories[key] = prev
		} else {
			delete(refreshLeadFactories, key)
		}
		refreshLeadMu.Unlock()
	})
}

func TestNextRefreshCheckAt_DisabledUnschedule(t *testing.T) {
	now := time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC)
	auth := &Auth{ID: "a1", Provider: "test", Disabled: true}
	if _, ok := nextRefreshCheckAt(now, auth, 15*time.Minute); ok {
		t.Fatalf("nextRefreshCheckAt() ok = true, want false")
	}
}

func TestNextRefreshCheckAt_APIKeyUnschedule(t *testing.T) {
	now := time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC)
	auth := &Auth{ID: "a1", Provider: "test", Attributes: map[string]string{"api_key": "k"}}
	if _, ok := nextRefreshCheckAt(now, auth, 15*time.Minute); ok {
		t.Fatalf("nextRefreshCheckAt() ok = true, want false")
	}
}

func TestNextRefreshCheckAt_NextRefreshAfterGate(t *testing.T) {
	now := time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC)
	nextAfter := now.Add(30 * time.Minute)
	auth := &Auth{
		ID:               "a1",
		Provider:         "test",
		NextRefreshAfter: nextAfter,
		Metadata:         map[string]any{"email": "x@example.com"},
	}
	got, ok := nextRefreshCheckAt(now, auth, 15*time.Minute)
	if !ok {
		t.Fatalf("nextRefreshCheckAt() ok = false, want true")
	}
	if !got.Equal(nextAfter) {
		t.Fatalf("nextRefreshCheckAt() = %s, want %s", got, nextAfter)
	}
}

func TestNextRefreshCheckAt_PreferredInterval_PicksEarliestCandidate(t *testing.T) {
	now := time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC)
	expiry := now.Add(20 * time.Minute)
	auth := &Auth{
		ID:              "a1",
		Provider:        "test",
		LastRefreshedAt: now,
		Metadata: map[string]any{
			"email":                    "x@example.com",
			"expires_at":               expiry.Format(time.RFC3339),
			"refresh_interval_seconds": 900, // 15m
		},
	}
	got, ok := nextRefreshCheckAt(now, auth, 15*time.Minute)
	if !ok {
		t.Fatalf("nextRefreshCheckAt() ok = false, want true")
	}
	want := expiry.Add(-15 * time.Minute)
	if !got.Equal(want) {
		t.Fatalf("nextRefreshCheckAt() = %s, want %s", got, want)
	}
}

func TestNextRefreshCheckAt_ProviderLead_Expiry(t *testing.T) {
	now := time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC)
	expiry := now.Add(time.Hour)
	lead := 10 * time.Minute
	setRefreshLeadFactory(t, "provider-lead-expiry", func() *time.Duration {
		d := lead
		return &d
	})

	auth := &Auth{
		ID:       "a1",
		Provider: "provider-lead-expiry",
		Metadata: map[string]any{
			"email":      "x@example.com",
			"expires_at": expiry.Format(time.RFC3339),
		},
	}

	got, ok := nextRefreshCheckAt(now, auth, 15*time.Minute)
	if !ok {
		t.Fatalf("nextRefreshCheckAt() ok = false, want true")
	}
	want := expiry.Add(-lead)
	if !got.Equal(want) {
		t.Fatalf("nextRefreshCheckAt() = %s, want %s", got, want)
	}
}

func TestNextRefreshCheckAt_RefreshEvaluatorFallback(t *testing.T) {
	now := time.Date(2026, 4, 12, 0, 0, 0, 0, time.UTC)
	interval := 15 * time.Minute
	auth := &Auth{
		ID:       "a1",
		Provider: "test",
		Metadata: map[string]any{"email": "x@example.com"},
		Runtime:  testRefreshEvaluator{},
	}
	got, ok := nextRefreshCheckAt(now, auth, interval)
	if !ok {
		t.Fatalf("nextRefreshCheckAt() ok = false, want true")
	}
	want := now.Add(interval)
	if !got.Equal(want) {
		t.Fatalf("nextRefreshCheckAt() = %s, want %s", got, want)
	}
}
