package auth

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type schedulerTestExecutor struct{}

func (schedulerTestExecutor) Identifier() string { return "test" }

func (schedulerTestExecutor) Execute(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (schedulerTestExecutor) ExecuteStream(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, nil
}

func (schedulerTestExecutor) Refresh(ctx context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (schedulerTestExecutor) CountTokens(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (schedulerTestExecutor) HttpRequest(ctx context.Context, auth *Auth, req *http.Request) (*http.Response, error) {
	return nil, nil
}

type trackingSelector struct {
	calls      int
	lastAuthID []string
}

func (s *trackingSelector) Pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	s.calls++
	s.lastAuthID = s.lastAuthID[:0]
	for _, auth := range auths {
		s.lastAuthID = append(s.lastAuthID, auth.ID)
	}
	if len(auths) == 0 {
		return nil, nil
	}
	return auths[len(auths)-1], nil
}

func newSchedulerForTest(selector Selector, auths ...*Auth) *authScheduler {
	scheduler := newAuthScheduler(selector)
	scheduler.rebuild(auths)
	return scheduler
}

func registerSchedulerModels(t *testing.T, provider string, model string, authIDs ...string) {
	t.Helper()
	reg := registry.GetGlobalRegistry()
	for _, authID := range authIDs {
		reg.RegisterClient(authID, provider, []*registry.ModelInfo{{ID: model}})
	}
	t.Cleanup(func() {
		for _, authID := range authIDs {
			reg.UnregisterClient(authID)
		}
	})
}

func schedulerTestID(t *testing.T, prefix string) string {
	t.Helper()
	name := strings.NewReplacer("/", "-", " ", "-", "_", "-").Replace(t.Name())
	return prefix + "-" + name
}

func TestSchedulerBlockAuthQuarantinesConcurrentUpsert(t *testing.T) {
	scheduler := newSchedulerForTest(&RoundRobinSelector{})
	auth := &Auth{ID: "auth-1", Provider: "codex"}
	scheduler.blockAuth(auth.ID)
	scheduler.upsertAuth(auth)
	if selected, errPick := scheduler.pickSingle(t.Context(), auth.Provider, "", cliproxyexecutor.Options{}, nil); errPick == nil || selected != nil {
		t.Fatalf("pick while blocked = (%#v, %v), want unavailable", selected, errPick)
	}
	scheduler.unblockAuth(auth.ID, auth)
	if selected, errPick := scheduler.pickSingle(t.Context(), auth.Provider, "", cliproxyexecutor.Options{}, nil); errPick != nil || selected == nil || selected.ID != auth.ID {
		t.Fatalf("pick after unblock = (%#v, %v), want %s", selected, errPick, auth.ID)
	}
}

func TestSchedulerPick_PriorityStrategyOverrideBeatsGlobalStrategy(t *testing.T) {
	t.Parallel()

	scheduler := newSchedulerForTest(
		&RoundRobinSelector{},
		&Auth{ID: "priority-a", Provider: "claude", Attributes: map[string]string{"priority": "0"}},
		&Auth{ID: "priority-b", Provider: "claude", Attributes: map[string]string{"priority": "0"}},
	)
	scheduler.setRoutingPriorityOverrides([]internalconfig.RoutingPriorityOverride{
		{Priority: 0, Strategy: "fill-first"},
	})

	for index := 0; index < 2; index++ {
		got, errPick := scheduler.pickSingle(context.Background(), "claude", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickSingle() #%d error = %v", index, errPick)
		}
		if got == nil || got.ID != "priority-a" {
			t.Fatalf("pickSingle() #%d auth = %v, want priority-a", index, got)
		}
	}
}

func TestSchedulerPick_MixedPriorityStrategyOverrideBeatsGlobalStrategy(t *testing.T) {
	t.Parallel()

	scheduler := newSchedulerForTest(
		&RoundRobinSelector{},
		&Auth{ID: "claude-a", Provider: "claude", Attributes: map[string]string{"priority": "0"}},
		&Auth{ID: "gemini-a", Provider: "gemini", Attributes: map[string]string{"priority": "0"}},
	)
	scheduler.setRoutingPriorityOverrides([]internalconfig.RoutingPriorityOverride{
		{Priority: 0, Strategy: "fill-first"},
	})

	for index := 0; index < 2; index++ {
		got, provider, errPick := scheduler.pickMixed(context.Background(), []string{"gemini", "claude"}, "", cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickMixed() #%d error = %v", index, errPick)
		}
		if provider != "claude" {
			t.Fatalf("pickMixed() #%d provider = %q, want claude", index, provider)
		}
		if got == nil || got.ID != "claude-a" {
			t.Fatalf("pickMixed() #%d auth = %v, want claude-a", index, got)
		}
	}
}

func TestSchedulerPick_MixedFillFirstRangeGroupsAcrossProviders(t *testing.T) {
	t.Parallel()

	scheduler := newSchedulerForTest(
		&FillFirstSelector{Range: 2},
		&Auth{ID: "a-claude", Provider: "claude", Attributes: map[string]string{"priority": "0"}},
		&Auth{ID: "b-gemini", Provider: "gemini", Attributes: map[string]string{"priority": "0"}},
		&Auth{ID: "c-gemini", Provider: "gemini", Attributes: map[string]string{"priority": "0"}},
	)

	for index := 0; index < 40; index++ {
		got, provider, errPick := scheduler.pickMixed(context.Background(), []string{"gemini", "claude"}, "", cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickMixed() #%d error = %v", index, errPick)
		}
		if got == nil {
			t.Fatalf("pickMixed() #%d auth = nil", index)
		}
		if got.ID != "a-claude" && got.ID != "b-gemini" {
			t.Fatalf("pickMixed() #%d auth.ID = %q provider = %q, want first cross-provider fill group", index, got.ID, provider)
		}
	}
}

func TestSchedulerPick_RoundRobinHighestPriority(t *testing.T) {
	t.Parallel()

	scheduler := newSchedulerForTest(
		&RoundRobinSelector{},
		&Auth{ID: "low", Provider: "gemini", Attributes: map[string]string{"priority": "0"}},
		&Auth{ID: "high-b", Provider: "gemini", Attributes: map[string]string{"priority": "10"}},
		&Auth{ID: "high-a", Provider: "gemini", Attributes: map[string]string{"priority": "10"}},
	)

	want := []string{"high-a", "high-b", "high-a"}
	for index, wantID := range want {
		got, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickSingle() #%d error = %v", index, errPick)
		}
		if got == nil {
			t.Fatalf("pickSingle() #%d auth = nil", index)
		}
		if got.ID != wantID {
			t.Fatalf("pickSingle() #%d auth.ID = %q, want %q", index, got.ID, wantID)
		}
	}
}

func TestSchedulerPick_FillFirstSticksToFirstReady(t *testing.T) {
	t.Parallel()

	scheduler := newSchedulerForTest(
		&FillFirstSelector{},
		&Auth{ID: "b", Provider: "gemini"},
		&Auth{ID: "a", Provider: "gemini"},
		&Auth{ID: "c", Provider: "gemini"},
	)

	for index := 0; index < 3; index++ {
		got, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickSingle() #%d error = %v", index, errPick)
		}
		if got == nil {
			t.Fatalf("pickSingle() #%d auth = nil", index)
		}
		if got.ID != "a" {
			t.Fatalf("pickSingle() #%d auth.ID = %q, want %q", index, got.ID, "a")
		}
	}
}

func TestSchedulerPick_FillFirstRangeUsesFirstGroup(t *testing.T) {
	t.Parallel()

	scheduler := newSchedulerForTest(
		&FillFirstSelector{Range: 5},
		&Auth{ID: "a", Provider: "gemini"},
		&Auth{ID: "b", Provider: "gemini"},
		&Auth{ID: "c", Provider: "gemini"},
		&Auth{ID: "d", Provider: "gemini"},
		&Auth{ID: "e", Provider: "gemini"},
		&Auth{ID: "f", Provider: "gemini"},
	)

	for index := 0; index < 40; index++ {
		got, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickSingle() #%d error = %v", index, errPick)
		}
		if got == nil {
			t.Fatalf("pickSingle() #%d auth = nil", index)
		}
		if got.ID < "a" || got.ID > "e" {
			t.Fatalf("pickSingle() #%d auth.ID = %q, want first fill range a-e", index, got.ID)
		}
	}
}

func TestSchedulerPick_FillFirstRangeKeepsPartialCoolingGroup(t *testing.T) {
	t.Parallel()

	model := schedulerTestID(t, "model")
	aID := schedulerTestID(t, "a")
	bID := schedulerTestID(t, "b")
	cID := schedulerTestID(t, "c")
	dID := schedulerTestID(t, "d")
	registerSchedulerModels(t, "gemini", model, aID, bID, cID, dID)
	scheduler := newSchedulerForTest(
		&FillFirstSelector{Range: 3},
		&Auth{
			ID:       aID,
			Provider: "gemini",
			ModelStates: map[string]*ModelState{
				model: {
					Status:         StatusError,
					Unavailable:    true,
					NextRetryAfter: time.Now().Add(time.Hour),
				},
			},
		},
		&Auth{ID: bID, Provider: "gemini"},
		&Auth{ID: cID, Provider: "gemini"},
		&Auth{ID: dID, Provider: "gemini"},
	)

	for index := 0; index < 40; index++ {
		got, errPick := scheduler.pickSingle(context.Background(), "gemini", model, cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickSingle() #%d error = %v", index, errPick)
		}
		if got == nil {
			t.Fatalf("pickSingle() #%d auth = nil", index)
		}
		if got.ID != bID && got.ID != cID {
			t.Fatalf("pickSingle() #%d auth.ID = %q, want remaining ready auth from first group", index, got.ID)
		}
	}
}

func TestSchedulerPick_FillFirstRangeAdvancesWhenGroupUnavailable(t *testing.T) {
	t.Parallel()

	model := schedulerTestID(t, "model")
	aID := schedulerTestID(t, "a")
	bID := schedulerTestID(t, "b")
	cID := schedulerTestID(t, "c")
	dID := schedulerTestID(t, "d")
	registerSchedulerModels(t, "gemini", model, aID, bID, cID, dID)
	coolingState := func() map[string]*ModelState {
		return map[string]*ModelState{
			model: {
				Status:         StatusError,
				Unavailable:    true,
				NextRetryAfter: time.Now().Add(time.Hour),
			},
		}
	}
	scheduler := newSchedulerForTest(
		&FillFirstSelector{Range: 3},
		&Auth{ID: aID, Provider: "gemini", ModelStates: coolingState()},
		&Auth{ID: bID, Provider: "gemini", ModelStates: coolingState()},
		&Auth{ID: cID, Provider: "gemini", ModelStates: coolingState()},
		&Auth{ID: dID, Provider: "gemini"},
	)

	got, errPick := scheduler.pickSingle(context.Background(), "gemini", model, cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("pickSingle() error = %v", errPick)
	}
	if got == nil || got.ID != dID {
		t.Fatalf("pickSingle() auth = %v, want %s from second fill group", got, dID)
	}
}

func TestSchedulerPick_FillFirstRangePriorityOverrideBeatsGlobal(t *testing.T) {
	t.Parallel()

	overrideRange := 2
	scheduler := newSchedulerForTest(
		&FillFirstSelector{Range: 5},
		&Auth{ID: "a", Provider: "gemini", Attributes: map[string]string{"priority": "0"}},
		&Auth{ID: "b", Provider: "gemini", Attributes: map[string]string{"priority": "0"}},
		&Auth{ID: "c", Provider: "gemini", Attributes: map[string]string{"priority": "0"}},
	)
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{
		FillFirstRange: 5,
		PriorityOverrides: []internalconfig.RoutingPriorityOverride{
			{Priority: 0, FillFirstRange: &overrideRange},
		},
	})

	for index := 0; index < 40; index++ {
		got, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickSingle() #%d error = %v", index, errPick)
		}
		if got == nil {
			t.Fatalf("pickSingle() #%d auth = nil", index)
		}
		if got.ID != "a" && got.ID != "b" {
			t.Fatalf("pickSingle() #%d auth.ID = %q, want override range a-b", index, got.ID)
		}
	}
}

func TestSchedulerPick_FillFirstRangeDoesNotAffectRoundRobin(t *testing.T) {
	t.Parallel()

	scheduler := newSchedulerForTest(
		&RoundRobinSelector{},
		&Auth{ID: "a", Provider: "gemini"},
		&Auth{ID: "b", Provider: "gemini"},
	)
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{FillFirstRange: 2})

	want := []string{"a", "b", "a"}
	for index, wantID := range want {
		got, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickSingle() #%d error = %v", index, errPick)
		}
		if got == nil || got.ID != wantID {
			t.Fatalf("pickSingle() #%d auth = %v, want %s", index, got, wantID)
		}
	}
}

func TestSchedulerPick_FillFirstPerAuthRPMAdvancesAfterLimit(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 8, 19, 12, 0, 10, 0, time.UTC)
	scheduler := newSchedulerForTest(
		&FillFirstSelector{},
		&Auth{ID: "a", Provider: "gemini"},
		&Auth{ID: "b", Provider: "gemini"},
	)
	scheduler.fillFirstLimiter.now = func() time.Time { return fixed }
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{FillFirstPerAuthRPM: 2})

	want := []string{"a", "a", "b"}
	for index, wantID := range want {
		got, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickSingle() #%d error = %v", index, errPick)
		}
		if got == nil || got.ID != wantID {
			t.Fatalf("pickSingle() #%d auth = %v, want %s", index, got, wantID)
		}
	}
}

func TestSchedulerPick_FillFirstPerAuthRPMResetsNextMinute(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 8, 19, 12, 0, 10, 0, time.UTC)
	scheduler := newSchedulerForTest(
		&FillFirstSelector{},
		&Auth{ID: "a", Provider: "gemini"},
		&Auth{ID: "b", Provider: "gemini"},
	)
	scheduler.fillFirstLimiter.now = func() time.Time { return fixed }
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{FillFirstPerAuthRPM: 1})

	for _, wantID := range []string{"a", "b"} {
		got, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickSingle() error = %v", errPick)
		}
		if got == nil || got.ID != wantID {
			t.Fatalf("pickSingle() auth = %v, want %s", got, wantID)
		}
	}
	fixed = fixed.Add(time.Minute)
	got, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("pickSingle() after minute error = %v", errPick)
	}
	if got == nil || got.ID != "a" {
		t.Fatalf("pickSingle() after minute auth = %v, want a", got)
	}
}

func TestSchedulerPick_FillFirstPerAuthRPMSkipsCoolingFirstAuth(t *testing.T) {
	t.Parallel()

	model := schedulerTestID(t, "model")
	aID := schedulerTestID(t, "a")
	bID := schedulerTestID(t, "b")
	registerSchedulerModels(t, "gemini", model, aID, bID)
	fixed := time.Date(2026, 8, 19, 12, 0, 10, 0, time.UTC)
	scheduler := newSchedulerForTest(
		&FillFirstSelector{},
		&Auth{
			ID:       aID,
			Provider: "gemini",
			ModelStates: map[string]*ModelState{
				model: {
					Status:         StatusError,
					Unavailable:    true,
					NextRetryAfter: fixed.Add(time.Hour),
				},
			},
		},
		&Auth{ID: bID, Provider: "gemini"},
	)
	scheduler.fillFirstLimiter.now = func() time.Time { return fixed }
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{FillFirstPerAuthRPM: 1})

	got, errPick := scheduler.pickSingle(context.Background(), "gemini", model, cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("pickSingle() error = %v", errPick)
	}
	if got == nil || got.ID != bID {
		t.Fatalf("pickSingle() auth = %v, want %s", got, bID)
	}
}

func TestSchedulerPick_FillFirstPerAuthRPMReturnsRetryAfterWhenFull(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 6, 19, 12, 0, 10, 0, time.UTC)
	scheduler := newSchedulerForTest(
		&FillFirstSelector{},
		&Auth{ID: "a", Provider: "gemini"},
		&Auth{ID: "b", Provider: "gemini"},
	)
	scheduler.fillFirstLimiter.now = func() time.Time { return fixed }
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{FillFirstPerAuthRPM: 1})

	for _, wantID := range []string{"a", "b"} {
		got, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickSingle() error = %v", errPick)
		}
		if got == nil || got.ID != wantID {
			t.Fatalf("pickSingle() auth = %v, want %s", got, wantID)
		}
	}
	_, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
	rpmErr, ok := errPick.(*authRPMLimitedError)
	if !ok {
		t.Fatalf("pickSingle() error = %T %v, want *authRPMLimitedError", errPick, errPick)
	}
	if rpmErr.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("StatusCode() = %d, want %d", rpmErr.StatusCode(), http.StatusTooManyRequests)
	}
	if got := rpmErr.Headers().Get("Retry-After"); got != "50" {
		t.Fatalf("Retry-After = %q, want 50", got)
	}
}

func TestSchedulerPick_FillFirstPerAuthRPMReturnsCooldownWhenSooner(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 8, 19, 12, 0, 10, 0, time.UTC)
	scheduler := newSchedulerForTest(
		&FillFirstSelector{},
		&Auth{ID: "a", Provider: "gemini"},
		&Auth{
			ID:             "b",
			Provider:       "gemini",
			Unavailable:    true,
			NextRetryAfter: fixed.Add(10 * time.Second),
			Quota:          QuotaState{Exceeded: true},
		},
	)
	scheduler.fillFirstLimiter.now = func() time.Time { return fixed }
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{FillFirstPerAuthRPM: 1})

	got, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("pickSingle() #1 error = %v", errPick)
	}
	if got == nil || got.ID != "a" {
		t.Fatalf("pickSingle() #1 auth = %v, want a", got)
	}
	_, errPick = scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
	if _, ok := errPick.(*modelCooldownError); !ok {
		t.Fatalf("pickSingle() #2 error = %T %v, want *modelCooldownError", errPick, errPick)
	}
}

func TestSchedulerPick_FillFirstPerAuthRPMFallsBackToHTTPWhenWebsocketFull(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 6, 19, 12, 0, 10, 0, time.UTC)
	scheduler := newSchedulerForTest(
		&FillFirstSelector{},
		&Auth{ID: "codex-http", Provider: "codex"},
		&Auth{ID: "codex-ws", Provider: "codex", Attributes: map[string]string{"websockets": "true"}},
	)
	scheduler.fillFirstLimiter.now = func() time.Time { return fixed }
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{FillFirstPerAuthRPM: 1})
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())

	for index, wantID := range []string{"codex-ws", "codex-http"} {
		got, errPick := scheduler.pickSingle(ctx, "codex", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickSingle() #%d error = %v", index, errPick)
		}
		if got == nil || got.ID != wantID {
			t.Fatalf("pickSingle() #%d auth = %v, want %s", index, got, wantID)
		}
	}
}

func TestSelectorFillFirstPerAuthRPMReturnsCooldownWhenSooner(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 8, 19, 12, 0, 10, 0, time.UTC)
	limiter := newFillFirstMinuteLimiter()
	auths := []*Auth{
		{ID: "a", Provider: "gemini"},
		{
			ID:             "b",
			Provider:       "gemini",
			Unavailable:    true,
			NextRetryAfter: fixed.Add(10 * time.Second),
			Quota:          QuotaState{Exceeded: true},
		},
	}
	rpmForPriority := func(int) int { return 1 }
	rangeForPriority := func(int) int { return 1 }

	got, errPick := selectFillFirstAuthsForAttemptWithPolicy(auths, "gemini", "", fixed, 0, rangeForPriority, rpmForPriority, limiter, nil, nil)
	if errPick != nil {
		t.Fatalf("selectFillFirstAuthsForAttemptWithPolicy() #1 error = %v", errPick)
	}
	if got == nil || got.ID != "a" {
		t.Fatalf("selectFillFirstAuthsForAttemptWithPolicy() #1 auth = %v, want a", got)
	}
	_, errPick = selectFillFirstAuthsForAttemptWithPolicy(auths, "gemini", "", fixed, 0, rangeForPriority, rpmForPriority, limiter, nil, nil)
	if _, ok := errPick.(*modelCooldownError); !ok {
		t.Fatalf("selectFillFirstAuthsForAttemptWithPolicy() #2 error = %T %v, want *modelCooldownError", errPick, errPick)
	}
}

func TestSelectorFillFirstPerAuthRPMFallsBackToHTTPWhenWebsocketRPMFullAndCooldownSooner(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 8, 19, 12, 0, 10, 0, time.UTC)
	limiter := newFillFirstMinuteLimiter()
	auths := []*Auth{
		{ID: "codex-http", Provider: "codex"},
		{ID: "codex-ws-a", Provider: "codex", Attributes: map[string]string{"websockets": "true"}},
		{
			ID:             "codex-ws-b",
			Provider:       "codex",
			Attributes:     map[string]string{"websockets": "true"},
			Unavailable:    true,
			NextRetryAfter: fixed.Add(10 * time.Second),
			Quota:          QuotaState{Exceeded: true},
		},
	}
	rpmForPriority := func(int) int { return 1 }
	rangeForPriority := func(int) int { return 1 }
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())

	got, errPick := selectFillFirstAuthsForContextWithPolicy(ctx, auths, "codex", "", fixed, 0, rangeForPriority, rpmForPriority, limiter, nil, nil)
	if errPick != nil {
		t.Fatalf("selectFillFirstAuthsForContextWithPolicy() #1 error = %v", errPick)
	}
	if got == nil || got.ID != "codex-ws-a" {
		t.Fatalf("selectFillFirstAuthsForContextWithPolicy() #1 auth = %v, want codex-ws-a", got)
	}
	got, errPick = selectFillFirstAuthsForContextWithPolicy(ctx, auths, "codex", "", fixed, 0, rangeForPriority, rpmForPriority, limiter, nil, nil)
	if errPick != nil {
		t.Fatalf("selectFillFirstAuthsForContextWithPolicy() #2 error = %v", errPick)
	}
	if got == nil || got.ID != "codex-http" {
		t.Fatalf("selectFillFirstAuthsForContextWithPolicy() #2 auth = %v, want codex-http", got)
	}
}

func TestSchedulerPick_MixedFillFirstPerAuthRPMAdvancesAcrossProviders(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 6, 19, 12, 0, 10, 0, time.UTC)
	scheduler := newSchedulerForTest(
		&FillFirstSelector{},
		&Auth{ID: "a-claude", Provider: "claude", Attributes: map[string]string{"priority": "0"}},
		&Auth{ID: "b-gemini", Provider: "gemini", Attributes: map[string]string{"priority": "0"}},
	)
	scheduler.fillFirstLimiter.now = func() time.Time { return fixed }
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{FillFirstPerAuthRPM: 1})

	got, provider, errPick := scheduler.pickMixed(context.Background(), []string{"gemini", "claude"}, "", cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("pickMixed() #1 error = %v", errPick)
	}
	if got == nil || got.ID != "a-claude" || provider != "claude" {
		t.Fatalf("pickMixed() #1 auth/provider = %v/%q, want a-claude/claude", got, provider)
	}
	got, provider, errPick = scheduler.pickMixed(context.Background(), []string{"gemini", "claude"}, "", cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("pickMixed() #2 error = %v", errPick)
	}
	if got == nil || got.ID != "b-gemini" || provider != "gemini" {
		t.Fatalf("pickMixed() #2 auth/provider = %v/%q, want b-gemini/gemini", got, provider)
	}
	_, _, errPick = scheduler.pickMixed(context.Background(), []string{"gemini", "claude"}, "", cliproxyexecutor.Options{}, nil)
	if _, ok := errPick.(*authRPMLimitedError); !ok {
		t.Fatalf("pickMixed() #3 error = %T %v, want *authRPMLimitedError", errPick, errPick)
	}
}

func TestSchedulerPick_MixedFillFirstPerAuthRPMReturnsCooldownWhenSooner(t *testing.T) {
	t.Parallel()

	fixed := time.Date(2026, 8, 19, 12, 0, 10, 0, time.UTC)
	scheduler := newSchedulerForTest(
		&FillFirstSelector{},
		&Auth{ID: "a-claude", Provider: "claude", Attributes: map[string]string{"priority": "0"}},
		&Auth{
			ID:             "b-gemini",
			Provider:       "gemini",
			Attributes:     map[string]string{"priority": "0"},
			Unavailable:    true,
			NextRetryAfter: fixed.Add(10 * time.Second),
			Quota:          QuotaState{Exceeded: true},
		},
	)
	scheduler.fillFirstLimiter.now = func() time.Time { return fixed }
	scheduler.setRoutingConfig(internalconfig.RoutingConfig{FillFirstPerAuthRPM: 1})

	got, provider, errPick := scheduler.pickMixed(context.Background(), []string{"gemini", "claude"}, "", cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("pickMixed() #1 error = %v", errPick)
	}
	if got == nil || got.ID != "a-claude" || provider != "claude" {
		t.Fatalf("pickMixed() #1 auth/provider = %v/%q, want a-claude/claude", got, provider)
	}
	_, _, errPick = scheduler.pickMixed(context.Background(), []string{"gemini", "claude"}, "", cliproxyexecutor.Options{}, nil)
	if _, ok := errPick.(*modelCooldownError); !ok {
		t.Fatalf("pickMixed() #2 error = %T %v, want *modelCooldownError", errPick, errPick)
	}
}

func TestManagerPickLegacyFillFirstRangeAuth_SessionAffinityUsesPerAuthRPM(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, NewSessionAffinitySelector(&FillFirstSelector{}), nil)
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{FillFirstPerAuthRPM: 1}})
	fixed := time.Date(2026, 6, 19, 12, 0, 10, 0, time.UTC)
	manager.scheduler.fillFirstLimiter.now = func() time.Time { return fixed }
	candidates := []*Auth{
		{ID: "a", Provider: "gemini"},
		{ID: "b", Provider: "gemini"},
	}
	opts := cliproxyexecutor.Options{
		Headers: http.Header{"Session-Id": {"rpm-session"}},
	}

	for index, wantID := range []string{"a", "b"} {
		got, handled, errPick := manager.pickLegacyFillFirstRangeAuth(context.Background(), "gemini", "", opts, candidates, nil)
		if errPick != nil {
			t.Fatalf("pickLegacyFillFirstRangeAuth() #%d error = %v", index, errPick)
		}
		if !handled {
			t.Fatalf("pickLegacyFillFirstRangeAuth() #%d handled = false, want true", index)
		}
		if got == nil || got.ID != wantID {
			t.Fatalf("pickLegacyFillFirstRangeAuth() #%d auth = %v, want %s", index, got, wantID)
		}
	}
}

func TestManagerPickLegacyFillFirstRangeAuth_FallsBackToHTTPWhenWebsocketRPMFull(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{FillFirstPerAuthRPM: 1}})
	fixed := time.Date(2026, 6, 19, 12, 0, 10, 0, time.UTC)
	manager.scheduler.fillFirstLimiter.now = func() time.Time { return fixed }
	candidates := []*Auth{
		{ID: "codex-http", Provider: "codex"},
		{ID: "codex-ws", Provider: "codex", Attributes: map[string]string{"websockets": "true"}},
	}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())

	for index, wantID := range []string{"codex-ws", "codex-http"} {
		got, handled, errPick := manager.pickLegacyFillFirstRangeAuth(ctx, "codex", "", cliproxyexecutor.Options{}, candidates, nil)
		if errPick != nil {
			t.Fatalf("pickLegacyFillFirstRangeAuth() #%d error = %v", index, errPick)
		}
		if !handled {
			t.Fatalf("pickLegacyFillFirstRangeAuth() #%d handled = false, want true", index)
		}
		if got == nil || got.ID != wantID {
			t.Fatalf("pickLegacyFillFirstRangeAuth() #%d auth = %v, want %s", index, got, wantID)
		}
	}
}

func TestManagerPickLegacyFillFirstRangeAuth_UsesPerPriorityRPMOnFallback(t *testing.T) {
	t.Parallel()

	lowerRPM := 0
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetConfig(&internalconfig.Config{
		Routing: internalconfig.RoutingConfig{
			FillFirstPerAuthRPM: 1,
			PriorityOverrides: []internalconfig.RoutingPriorityOverride{
				{Priority: 0, FillFirstPerAuthRPM: &lowerRPM},
			},
		},
	})
	fixed := time.Date(2026, 6, 19, 12, 0, 10, 0, time.UTC)
	manager.scheduler.fillFirstLimiter.now = func() time.Time { return fixed }
	candidates := []*Auth{
		{ID: "high", Provider: "gemini", Attributes: map[string]string{"priority": "1"}},
		{ID: "low", Provider: "gemini", Attributes: map[string]string{"priority": "0"}},
	}

	for index, wantID := range []string{"high", "low", "low"} {
		got, handled, errPick := manager.pickLegacyFillFirstRangeAuth(context.Background(), "gemini", "", cliproxyexecutor.Options{}, candidates, nil)
		if errPick != nil {
			t.Fatalf("pickLegacyFillFirstRangeAuth() #%d error = %v", index, errPick)
		}
		if !handled {
			t.Fatalf("pickLegacyFillFirstRangeAuth() #%d handled = false, want true", index)
		}
		if got == nil || got.ID != wantID {
			t.Fatalf("pickLegacyFillFirstRangeAuth() #%d auth = %v, want %s", index, got, wantID)
		}
	}
}

func TestManagerPrioritySelectorForAvailable_UsesSessionAffinityFallbackFillFirstRangeOverride(t *testing.T) {
	t.Parallel()

	overrideRange := 2
	manager := NewManager(nil, NewSessionAffinitySelector(&FillFirstSelector{}), nil)
	manager.SetConfig(&internalconfig.Config{
		Routing: internalconfig.RoutingConfig{
			PriorityOverrides: []internalconfig.RoutingPriorityOverride{
				{Priority: 0, FillFirstRange: &overrideRange},
			},
		},
	})

	selector, ok := manager.prioritySelectorForAvailable([]*Auth{
		{ID: "a", Attributes: map[string]string{"priority": "0"}},
	})
	if !ok {
		t.Fatalf("prioritySelectorForAvailable() override = false, want true")
	}
	fillFirst, ok := selector.(*FillFirstSelector)
	if !ok {
		t.Fatalf("prioritySelectorForAvailable() selector = %T, want *FillFirstSelector", selector)
	}
	if fillFirst.Range != overrideRange {
		t.Fatalf("FillFirstSelector.Range = %d, want %d", fillFirst.Range, overrideRange)
	}
}

func TestManagerPickLegacyFillFirstRangeAuth_SessionAffinityUsesFixedGroups(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, NewSessionAffinitySelector(&FillFirstSelector{Range: 3}), nil)
	manager.SetConfig(&internalconfig.Config{Routing: internalconfig.RoutingConfig{FillFirstRange: 3}})
	model := schedulerTestID(t, "model")
	candidates := []*Auth{
		{
			ID:       "a",
			Provider: "gemini",
			ModelStates: map[string]*ModelState{
				model: {
					Status:         StatusError,
					Unavailable:    true,
					NextRetryAfter: time.Now().Add(time.Hour),
				},
			},
		},
		{ID: "b", Provider: "gemini"},
		{ID: "c", Provider: "gemini"},
		{ID: "d", Provider: "gemini"},
	}
	opts := cliproxyexecutor.Options{
		Headers: http.Header{"Session-Id": {"range-session"}},
	}

	got, handled, errPick := manager.pickLegacyFillFirstRangeAuth(context.Background(), "gemini", model, opts, candidates, nil)
	if errPick != nil {
		t.Fatalf("pickLegacyFillFirstRangeAuth() error = %v", errPick)
	}
	if !handled {
		t.Fatalf("pickLegacyFillFirstRangeAuth() handled = false, want true")
	}
	if got == nil {
		t.Fatalf("pickLegacyFillFirstRangeAuth() auth = nil")
	}
	if got.ID != "b" && got.ID != "c" {
		t.Fatalf("pickLegacyFillFirstRangeAuth() auth.ID = %q, want remaining ready auth from first group", got.ID)
	}
}

func TestSchedulerPick_RandomRetryFallsBackToLowerPriority(t *testing.T) {
	t.Parallel()

	scheduler := newSchedulerForTest(
		&RandomSelector{},
		&Auth{ID: "low", Provider: "gemini", Attributes: map[string]string{"priority": "0"}},
		&Auth{ID: "high-a", Provider: "gemini", Attributes: map[string]string{"priority": "10"}},
		&Auth{ID: "high-b", Provider: "gemini", Attributes: map[string]string{"priority": "10"}},
	)

	got, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("pickSingle() initial error = %v", errPick)
	}
	if got == nil {
		t.Fatalf("pickSingle() initial auth = nil")
	}
	if got.ID == "low" {
		t.Fatalf("pickSingle() initial selected lower priority auth %q", got.ID)
	}

	retryOpts := cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.SelectionAttemptMetadataKey: 1,
		},
	}
	got, errPick = scheduler.pickSingle(context.Background(), "gemini", "", retryOpts, nil)
	if errPick != nil {
		t.Fatalf("pickSingle() retry error = %v", errPick)
	}
	if got == nil {
		t.Fatalf("pickSingle() retry auth = nil")
	}
	if got.ID != "low" {
		t.Fatalf("pickSingle() retry auth.ID = %q, want %q", got.ID, "low")
	}
}

func TestSchedulerPick_PromotesExpiredCooldownBeforePick(t *testing.T) {
	t.Parallel()

	model := "gemini-2.5-pro"
	registerSchedulerModels(t, "gemini", model, "cooldown-expired")
	scheduler := newSchedulerForTest(
		&RoundRobinSelector{},
		&Auth{
			ID:       "cooldown-expired",
			Provider: "gemini",
			ModelStates: map[string]*ModelState{
				model: {
					Status:         StatusError,
					Unavailable:    true,
					NextRetryAfter: time.Now().Add(-1 * time.Second),
				},
			},
		},
	)

	got, errPick := scheduler.pickSingle(context.Background(), "gemini", model, cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("pickSingle() error = %v", errPick)
	}
	if got == nil {
		t.Fatalf("pickSingle() auth = nil")
	}
	if got.ID != "cooldown-expired" {
		t.Fatalf("pickSingle() auth.ID = %q, want %q", got.ID, "cooldown-expired")
	}
}

func TestSchedulerPickSingle_BlocksRebuildWhileProviderSelectionIsInFlight(t *testing.T) {
	scheduler := newSchedulerForTest(
		&RoundRobinSelector{},
		&Auth{ID: "old", Provider: "gemini"},
	)

	oldProvider := scheduler.providers["gemini"]
	if oldProvider == nil {
		t.Fatal("expected provider scheduler")
	}
	oldProvider.mu.Lock()

	type pickResult struct {
		auth *Auth
		err  error
	}
	pickDone := make(chan pickResult, 1)
	go func() {
		auth, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
		pickDone <- pickResult{auth: auth, err: errPick}
	}()

	time.Sleep(20 * time.Millisecond)

	rebuildDone := make(chan struct{})
	go func() {
		scheduler.rebuild([]*Auth{{ID: "new", Provider: "gemini"}})
		close(rebuildDone)
	}()

	select {
	case <-rebuildDone:
		oldProvider.mu.Unlock()
		t.Fatal("rebuild completed before in-flight pick released the provider state")
	case <-time.After(50 * time.Millisecond):
	}

	oldProvider.mu.Unlock()

	result := <-pickDone
	if result.err != nil {
		t.Fatalf("pickSingle() error = %v", result.err)
	}
	if result.auth == nil {
		t.Fatal("pickSingle() auth = nil")
	}
	if result.auth.ID != "old" {
		t.Fatalf("pickSingle() auth.ID = %q, want %q", result.auth.ID, "old")
	}

	select {
	case <-rebuildDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("rebuild did not complete after pick released the provider state")
	}

	got, errPick := scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("pickSingle() after rebuild error = %v", errPick)
	}
	if got == nil {
		t.Fatal("pickSingle() after rebuild auth = nil")
	}
	if got.ID != "new" {
		t.Fatalf("pickSingle() after rebuild auth.ID = %q, want %q", got.ID, "new")
	}
}

func TestSchedulerPick_CodexWebsocketPrefersWebsocketEnabledSubset(t *testing.T) {
	t.Parallel()

	scheduler := newSchedulerForTest(
		&RoundRobinSelector{},
		&Auth{ID: "codex-http", Provider: "codex"},
		&Auth{ID: "codex-ws-a", Provider: "codex", Attributes: map[string]string{"websockets": "true"}},
		&Auth{ID: "codex-ws-b", Provider: "codex", Attributes: map[string]string{"websockets": "true"}},
	)

	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	want := []string{"codex-ws-a", "codex-ws-b", "codex-ws-a"}
	for index, wantID := range want {
		got, errPick := scheduler.pickSingle(ctx, "codex", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickSingle() #%d error = %v", index, errPick)
		}
		if got == nil {
			t.Fatalf("pickSingle() #%d auth = nil", index)
		}
		if got.ID != wantID {
			t.Fatalf("pickSingle() #%d auth.ID = %q, want %q", index, got.ID, wantID)
		}
	}
}

func TestSchedulerPick_CodexWebsocketPrefersWebsocketEnabledAcrossPriorities(t *testing.T) {
	t.Parallel()

	scheduler := newSchedulerForTest(
		&RoundRobinSelector{},
		&Auth{ID: "codex-http", Provider: "codex", Attributes: map[string]string{"priority": "10"}},
		&Auth{ID: "codex-ws-a", Provider: "codex", Attributes: map[string]string{"priority": "0", "websockets": "true"}},
		&Auth{ID: "codex-ws-b", Provider: "codex", Attributes: map[string]string{"priority": "0", "websockets": "true"}},
	)

	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	want := []string{"codex-ws-a", "codex-ws-b", "codex-ws-a"}
	for index, wantID := range want {
		got, errPick := scheduler.pickSingle(ctx, "codex", "", cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickSingle() #%d error = %v", index, errPick)
		}
		if got == nil {
			t.Fatalf("pickSingle() #%d auth = nil", index)
		}
		if got.ID != wantID {
			t.Fatalf("pickSingle() #%d auth.ID = %q, want %q", index, got.ID, wantID)
		}
	}
}

func TestSchedulerPick_CodexWebsocketFallsBackWhenOnlyWebsocketCandidatesBlocked(t *testing.T) {
	t.Parallel()

	scheduler := newSchedulerForTest(
		&RoundRobinSelector{},
		&Auth{ID: "codex-http", Provider: "codex", Attributes: map[string]string{"priority": "10"}},
		&Auth{
			ID:             "codex-ws-cooling",
			Provider:       "codex",
			Attributes:     map[string]string{"priority": "0", "websockets": "true"},
			Unavailable:    true,
			NextRetryAfter: time.Now().Add(time.Minute),
		},
	)

	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	got, errPick := scheduler.pickSingle(ctx, "codex", "", cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("pickSingle() error = %v", errPick)
	}
	if got == nil {
		t.Fatal("pickSingle() auth = nil")
	}
	if got.ID != "codex-http" {
		t.Fatalf("pickSingle() auth.ID = %q, want codex-http", got.ID)
	}
}

func TestSchedulerPick_CodexWebsocketFallsBackWhenHigherWebsocketPriorityCooling(t *testing.T) {
	t.Parallel()

	scheduler := newSchedulerForTest(
		&RoundRobinSelector{},
		&Auth{ID: "codex-http", Provider: "codex", Attributes: map[string]string{"priority": "10"}},
		&Auth{
			ID:             "codex-ws-cooling",
			Provider:       "codex",
			Attributes:     map[string]string{"priority": "10", "websockets": "true"},
			Unavailable:    true,
			NextRetryAfter: time.Now().Add(time.Minute),
		},
		&Auth{ID: "codex-ws-ready", Provider: "codex", Attributes: map[string]string{"priority": "0", "websockets": "true"}},
	)

	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	got, errPick := scheduler.pickSingle(ctx, "codex", "", cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("pickSingle() error = %v", errPick)
	}
	if got == nil || got.ID != "codex-ws-ready" {
		t.Fatalf("pickSingle() auth = %v, want codex-ws-ready", got)
	}
}

func TestSchedulerPick_CodexWebsocketFallsBackToHTTPAfterWebsocketTried(t *testing.T) {
	t.Parallel()

	scheduler := newSchedulerForTest(
		&RoundRobinSelector{},
		&Auth{ID: "codex-http", Provider: "codex", Attributes: map[string]string{"priority": "10"}},
		&Auth{ID: "codex-ws", Provider: "codex", Attributes: map[string]string{"priority": "10", "websockets": "true"}},
	)

	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	got, errPick := scheduler.pickSingle(ctx, "codex", "", cliproxyexecutor.Options{}, map[string]struct{}{"codex-ws": {}})
	if errPick != nil {
		t.Fatalf("pickSingle() error = %v", errPick)
	}
	if got == nil || got.ID != "codex-http" {
		t.Fatalf("pickSingle() auth = %v, want codex-http", got)
	}
}

func TestSchedulerPick_CodexWebsocketTriedReadyDoesNotHoldRestrictedPriority(t *testing.T) {
	t.Parallel()

	scheduler := newSchedulerForTest(
		&RoundRobinSelector{},
		&Auth{ID: "codex-http", Provider: "codex", Attributes: map[string]string{"priority": "10"}},
		&Auth{ID: "codex-ws-high", Provider: "codex", Attributes: map[string]string{"priority": "10", "websockets": "true"}},
		&Auth{ID: "codex-ws-low", Provider: "codex", Attributes: map[string]string{"priority": "0", "websockets": "true"}},
	)

	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	got, errPick := scheduler.pickSingle(ctx, "codex", "", cliproxyexecutor.Options{}, map[string]struct{}{"codex-ws-high": {}})
	if errPick != nil {
		t.Fatalf("pickSingle() error = %v", errPick)
	}
	if got == nil || got.ID != "codex-ws-low" {
		t.Fatalf("pickSingle() auth = %v, want codex-ws-low", got)
	}
}

func TestSchedulerPick_MixedProvidersUsesWeightedProviderRotationOverReadyCandidates(t *testing.T) {
	t.Parallel()

	scheduler := newSchedulerForTest(
		&RoundRobinSelector{},
		&Auth{ID: "gemini-a", Provider: "gemini"},
		&Auth{ID: "gemini-b", Provider: "gemini"},
		&Auth{ID: "claude-a", Provider: "claude"},
	)

	wantProviders := []string{"gemini", "gemini", "claude", "gemini"}
	wantIDs := []string{"gemini-a", "gemini-b", "claude-a", "gemini-a"}
	for index := range wantProviders {
		got, provider, errPick := scheduler.pickMixed(context.Background(), []string{"gemini", "claude"}, "", cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickMixed() #%d error = %v", index, errPick)
		}
		if got == nil {
			t.Fatalf("pickMixed() #%d auth = nil", index)
		}
		if provider != wantProviders[index] {
			t.Fatalf("pickMixed() #%d provider = %q, want %q", index, provider, wantProviders[index])
		}
		if got.ID != wantIDs[index] {
			t.Fatalf("pickMixed() #%d auth.ID = %q, want %q", index, got.ID, wantIDs[index])
		}
	}
}

func TestSchedulerPick_MixedRandomRetryFallsBackToLowerPriority(t *testing.T) {
	t.Parallel()

	scheduler := newSchedulerForTest(
		&RandomSelector{},
		&Auth{ID: "gemini-low", Provider: "gemini", Attributes: map[string]string{"priority": "0"}},
		&Auth{ID: "gemini-high", Provider: "gemini", Attributes: map[string]string{"priority": "10"}},
		&Auth{ID: "claude-high", Provider: "claude", Attributes: map[string]string{"priority": "10"}},
	)

	providers := []string{"gemini", "claude"}
	got, provider, errPick := scheduler.pickMixed(context.Background(), providers, "", cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("pickMixed() initial error = %v", errPick)
	}
	if got == nil {
		t.Fatalf("pickMixed() initial auth = nil")
	}
	if got.ID == "gemini-low" {
		t.Fatalf("pickMixed() initial selected lower priority auth %q", got.ID)
	}
	if provider == "" {
		t.Fatalf("pickMixed() initial provider = empty")
	}

	retryOpts := cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.SelectionAttemptMetadataKey: 1,
		},
	}
	got, provider, errPick = scheduler.pickMixed(context.Background(), providers, "", retryOpts, nil)
	if errPick != nil {
		t.Fatalf("pickMixed() retry error = %v", errPick)
	}
	if got == nil {
		t.Fatalf("pickMixed() retry auth = nil")
	}
	if got.ID != "gemini-low" {
		t.Fatalf("pickMixed() retry auth.ID = %q, want %q", got.ID, "gemini-low")
	}
	if provider != "gemini" {
		t.Fatalf("pickMixed() retry provider = %q, want %q", provider, "gemini")
	}
}

func TestSchedulerPick_MixedProvidersPrefersHighestPriorityTier(t *testing.T) {
	t.Parallel()

	model := "gpt-default"
	registerSchedulerModels(t, "provider-low", model, "low")
	registerSchedulerModels(t, "provider-high-a", model, "high-a")
	registerSchedulerModels(t, "provider-high-b", model, "high-b")

	scheduler := newSchedulerForTest(
		&RoundRobinSelector{},
		&Auth{ID: "low", Provider: "provider-low", Attributes: map[string]string{"priority": "4"}},
		&Auth{ID: "high-a", Provider: "provider-high-a", Attributes: map[string]string{"priority": "7"}},
		&Auth{ID: "high-b", Provider: "provider-high-b", Attributes: map[string]string{"priority": "7"}},
	)

	providers := []string{"provider-low", "provider-high-a", "provider-high-b"}
	wantProviders := []string{"provider-high-a", "provider-high-b", "provider-high-a", "provider-high-b"}
	wantIDs := []string{"high-a", "high-b", "high-a", "high-b"}
	for index := range wantProviders {
		got, provider, errPick := scheduler.pickMixed(context.Background(), providers, model, cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickMixed() #%d error = %v", index, errPick)
		}
		if got == nil {
			t.Fatalf("pickMixed() #%d auth = nil", index)
		}
		if provider != wantProviders[index] {
			t.Fatalf("pickMixed() #%d provider = %q, want %q", index, provider, wantProviders[index])
		}
		if got.ID != wantIDs[index] {
			t.Fatalf("pickMixed() #%d auth.ID = %q, want %q", index, got.ID, wantIDs[index])
		}
	}
}

func TestManager_PickNextMixed_UsesWeightedProviderRotationBeforeCredentialRotation(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.executors["gemini"] = schedulerTestExecutor{}
	manager.executors["claude"] = schedulerTestExecutor{}
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: "gemini-a", Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register(gemini-a) error = %v", errRegister)
	}
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: "gemini-b", Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register(gemini-b) error = %v", errRegister)
	}
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: "claude-a", Provider: "claude"}); errRegister != nil {
		t.Fatalf("Register(claude-a) error = %v", errRegister)
	}

	wantProviders := []string{"gemini", "gemini", "claude", "gemini"}
	wantIDs := []string{"gemini-a", "gemini-b", "claude-a", "gemini-a"}
	for index := range wantProviders {
		got, _, provider, errPick := manager.pickNextMixed(context.Background(), []string{"gemini", "claude"}, "", cliproxyexecutor.Options{}, map[string]struct{}{})
		if errPick != nil {
			t.Fatalf("pickNextMixed() #%d error = %v", index, errPick)
		}
		if got == nil {
			t.Fatalf("pickNextMixed() #%d auth = nil", index)
		}
		if provider != wantProviders[index] {
			t.Fatalf("pickNextMixed() #%d provider = %q, want %q", index, provider, wantProviders[index])
		}
		if got.ID != wantIDs[index] {
			t.Fatalf("pickNextMixed() #%d auth.ID = %q, want %q", index, got.ID, wantIDs[index])
		}
	}
}

func TestManagerCustomSelector_FallsBackToLegacyPath(t *testing.T) {
	t.Parallel()

	selector := &trackingSelector{}
	manager := NewManager(nil, selector, nil)
	manager.executors["gemini"] = schedulerTestExecutor{}
	manager.auths["auth-a"] = &Auth{ID: "auth-a", Provider: "gemini"}
	manager.auths["auth-b"] = &Auth{ID: "auth-b", Provider: "gemini"}

	got, _, errPick := manager.pickNext(context.Background(), "gemini", "", cliproxyexecutor.Options{}, map[string]struct{}{})
	if errPick != nil {
		t.Fatalf("pickNext() error = %v", errPick)
	}
	if got == nil {
		t.Fatalf("pickNext() auth = nil")
	}
	if selector.calls != 1 {
		t.Fatalf("selector.calls = %d, want %d", selector.calls, 1)
	}
	if len(selector.lastAuthID) != 2 {
		t.Fatalf("len(selector.lastAuthID) = %d, want %d", len(selector.lastAuthID), 2)
	}
	if got.ID != selector.lastAuthID[len(selector.lastAuthID)-1] {
		t.Fatalf("pickNext() auth.ID = %q, want selector-picked %q", got.ID, selector.lastAuthID[len(selector.lastAuthID)-1])
	}
}

func TestManager_InitializesSchedulerForBuiltInSelector(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	if manager.scheduler == nil {
		t.Fatalf("manager.scheduler = nil")
	}
	if manager.scheduler.strategy != schedulerStrategyRoundRobin {
		t.Fatalf("manager.scheduler.strategy = %v, want %v", manager.scheduler.strategy, schedulerStrategyRoundRobin)
	}

	manager.SetSelector(&FillFirstSelector{})
	if manager.scheduler.strategy != schedulerStrategyFillFirst {
		t.Fatalf("manager.scheduler.strategy = %v, want %v", manager.scheduler.strategy, schedulerStrategyFillFirst)
	}
}

func TestManager_SchedulerTracksRegisterAndUpdate(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: "auth-b", Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register(auth-b) error = %v", errRegister)
	}
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: "auth-a", Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register(auth-a) error = %v", errRegister)
	}

	got, errPick := manager.scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("scheduler.pickSingle() error = %v", errPick)
	}
	if got == nil || got.ID != "auth-a" {
		t.Fatalf("scheduler.pickSingle() auth = %v, want auth-a", got)
	}

	if _, errUpdate := manager.Update(context.Background(), &Auth{ID: "auth-a", Provider: "gemini", Disabled: true}); errUpdate != nil {
		t.Fatalf("Update(auth-a) error = %v", errUpdate)
	}

	got, errPick = manager.scheduler.pickSingle(context.Background(), "gemini", "", cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("scheduler.pickSingle() after update error = %v", errPick)
	}
	if got == nil || got.ID != "auth-b" {
		t.Fatalf("scheduler.pickSingle() after update auth = %v, want auth-b", got)
	}
}

func TestManager_PickNextMixed_UsesSchedulerRotation(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.executors["gemini"] = schedulerTestExecutor{}
	manager.executors["claude"] = schedulerTestExecutor{}
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: "gemini-a", Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register(gemini-a) error = %v", errRegister)
	}
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: "gemini-b", Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register(gemini-b) error = %v", errRegister)
	}
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: "claude-a", Provider: "claude"}); errRegister != nil {
		t.Fatalf("Register(claude-a) error = %v", errRegister)
	}

	wantProviders := []string{"gemini", "gemini", "claude", "gemini"}
	wantIDs := []string{"gemini-a", "gemini-b", "claude-a", "gemini-a"}
	for index := range wantProviders {
		got, _, provider, errPick := manager.pickNextMixed(context.Background(), []string{"gemini", "claude"}, "", cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("pickNextMixed() #%d error = %v", index, errPick)
		}
		if got == nil {
			t.Fatalf("pickNextMixed() #%d auth = nil", index)
		}
		if provider != wantProviders[index] {
			t.Fatalf("pickNextMixed() #%d provider = %q, want %q", index, provider, wantProviders[index])
		}
		if got.ID != wantIDs[index] {
			t.Fatalf("pickNextMixed() #%d auth.ID = %q, want %q", index, got.ID, wantIDs[index])
		}
	}
}

func TestManager_PickNextMixed_SkipsProvidersWithoutExecutors(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.executors["claude"] = schedulerTestExecutor{}
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: "gemini-a", Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register(gemini-a) error = %v", errRegister)
	}
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: "claude-a", Provider: "claude"}); errRegister != nil {
		t.Fatalf("Register(claude-a) error = %v", errRegister)
	}

	got, _, provider, errPick := manager.pickNextMixed(context.Background(), []string{"gemini", "claude"}, "", cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("pickNextMixed() error = %v", errPick)
	}
	if got == nil {
		t.Fatalf("pickNextMixed() auth = nil")
	}
	if provider != "claude" {
		t.Fatalf("pickNextMixed() provider = %q, want %q", provider, "claude")
	}
	if got.ID != "claude-a" {
		t.Fatalf("pickNextMixed() auth.ID = %q, want %q", got.ID, "claude-a")
	}
}

func TestManager_SchedulerTracksMarkResultCooldownAndRecovery(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	model := schedulerTestID(t, "test-model")
	authA := schedulerTestID(t, "auth-a")
	authB := schedulerTestID(t, "auth-b")
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(authA, "gemini", []*registry.ModelInfo{{ID: model}})
	reg.RegisterClient(authB, "gemini", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() {
		reg.UnregisterClient(authA)
		reg.UnregisterClient(authB)
	})
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: authA, Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register(%s) error = %v", authA, errRegister)
	}
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: authB, Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register(%s) error = %v", authB, errRegister)
	}

	manager.MarkResult(context.Background(), Result{
		AuthID:   authA,
		Provider: "gemini",
		Model:    model,
		Success:  false,
		Error:    &Error{HTTPStatus: 429, Message: "quota"},
	})

	got, errPick := manager.scheduler.pickSingle(context.Background(), "gemini", model, cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("scheduler.pickSingle() after cooldown error = %v", errPick)
	}
	if got == nil || got.ID != authB {
		t.Fatalf("scheduler.pickSingle() after cooldown auth = %v, want %s", got, authB)
	}

	manager.MarkResult(context.Background(), Result{
		AuthID:   authA,
		Provider: "gemini",
		Model:    model,
		Success:  true,
	})

	seen := make(map[string]struct{}, 2)
	for index := 0; index < 2; index++ {
		got, errPick = manager.scheduler.pickSingle(context.Background(), "gemini", model, cliproxyexecutor.Options{}, nil)
		if errPick != nil {
			t.Fatalf("scheduler.pickSingle() after recovery #%d error = %v", index, errPick)
		}
		if got == nil {
			t.Fatalf("scheduler.pickSingle() after recovery #%d auth = nil", index)
		}
		seen[got.ID] = struct{}{}
	}
	if len(seen) != 2 {
		t.Fatalf("len(seen) = %d, want %d", len(seen), 2)
	}
}

func TestManager_SchedulerPreservesSupportedModelsAcrossMarkResult(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	model := schedulerTestID(t, "test-model")
	authID := schedulerTestID(t, "auth-a")
	if _, errRegister := manager.Register(context.Background(), &Auth{ID: authID, Provider: "gemini"}); errRegister != nil {
		t.Fatalf("Register(%s) error = %v", authID, errRegister)
	}

	registerSchedulerModels(t, "gemini", model, authID)
	manager.RefreshSchedulerEntry(authID)

	got, errPick := manager.scheduler.pickSingle(context.Background(), "gemini", model, cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("pickSingle() before registry drift error = %v", errPick)
	}
	if got == nil || got.ID != authID {
		t.Fatalf("pickSingle() before registry drift auth = %v, want %s", got, authID)
	}

	registry.GetGlobalRegistry().UnregisterClient(authID)

	manager.MarkResult(context.Background(), Result{
		AuthID:   authID,
		Provider: "gemini",
		Model:    model,
		Success:  true,
	})

	got, errPick = manager.scheduler.pickSingle(context.Background(), "gemini", model, cliproxyexecutor.Options{}, nil)
	if errPick != nil {
		t.Fatalf("pickSingle() after state-only update error = %v", errPick)
	}
	if got == nil || got.ID != authID {
		t.Fatalf("pickSingle() after state-only update auth = %v, want %s", got, authID)
	}
}
