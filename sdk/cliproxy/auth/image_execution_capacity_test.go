package auth

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type imageCapacityTestExecutor struct {
	id       string
	capacity bool
	stream   bool

	mu    sync.Mutex
	calls []string
}

type imagePhaseRecorder struct {
	durations map[string]time.Duration
}

func (recorder *imagePhaseRecorder) ObserveRequestPhase(name string, duration time.Duration) {
	if recorder.durations == nil {
		recorder.durations = make(map[string]time.Duration)
	}
	recorder.durations[name] += duration
}

func (executor *imageCapacityTestExecutor) Identifier() string { return executor.id }

func (executor *imageCapacityTestExecutor) DeferAuthRequestCommitUntilUpstream() bool {
	return executor.capacity
}

func (executor *imageCapacityTestExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	executor.record(auth)
	if executor.capacity {
		return cliproxyexecutor.Response{}, cliproxyexecutor.NewImageExecutionCapacityError("test")
	}
	if opts.AuthRequestSlot != nil {
		opts.AuthRequestSlot.Commit()
	}
	return cliproxyexecutor.Response{Payload: []byte(auth.ID)}, nil
}

func (executor *imageCapacityTestExecutor) ExecuteStream(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	executor.record(auth)
	if executor.capacity {
		return nil, cliproxyexecutor.NewImageExecutionCapacityError("test")
	}
	if opts.AuthRequestSlot != nil {
		opts.AuthRequestSlot.Commit()
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, 2)
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte(auth.ID)}
	chunks <- cliproxyexecutor.SuccessfulStreamTerminalChunk()
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (*imageCapacityTestExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (*imageCapacityTestExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (*imageCapacityTestExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (executor *imageCapacityTestExecutor) record(auth *Auth) {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if auth != nil {
		executor.calls = append(executor.calls, auth.ID)
	}
}

func (executor *imageCapacityTestExecutor) callIDs() []string {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return append([]string(nil), executor.calls...)
}

func newImageCapacityTestManager(t *testing.T, withFallback bool) (*Manager, *imageCapacityTestExecutor, *imageCapacityTestExecutor, string) {
	return newImageCapacityTestManagerWithSelector(t, withFallback, &FillFirstSelector{})
}

func newImageCapacityTestManagerWithSelector(t *testing.T, withFallback bool, selector Selector) (*Manager, *imageCapacityTestExecutor, *imageCapacityTestExecutor, string) {
	t.Helper()
	model := "image-capacity-" + uuid.NewString()
	web := &imageCapacityTestExecutor{id: "chatgpt-web", capacity: true}
	fallback := &imageCapacityTestExecutor{id: "image-capacity-fallback-" + uuid.NewString()}
	manager := NewManager(nil, selector, nil)
	manager.RegisterExecutor(web)
	if withFallback {
		manager.RegisterExecutor(fallback)
	}
	manager.SetConfig(&config.Config{Routing: config.RoutingConfig{
		PerAuthRequestLimit:         1,
		PerAuthRequestWindowMinutes: 5,
	}})
	manager.SetRetryConfig(0, 0, 0)
	auths := []*Auth{
		{ID: "web-a-" + uuid.NewString(), Provider: web.id, Status: StatusActive, Attributes: map[string]string{"priority": "10"}, Metadata: map[string]any{"lifecycle_state": LifecycleStateActive}},
		{ID: "web-b-" + uuid.NewString(), Provider: web.id, Status: StatusActive, Attributes: map[string]string{"priority": "10"}, Metadata: map[string]any{"lifecycle_state": LifecycleStateActive}},
	}
	if withFallback {
		auths = append(auths, &Auth{ID: "fallback-" + uuid.NewString(), Provider: fallback.id, Status: StatusActive})
	}
	for _, auth := range auths {
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
		authID := auth.ID
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
		if _, err := manager.Register(WithSkipPersist(t.Context()), auth); err != nil {
			t.Fatalf("Register(%s) error = %v", auth.ID, err)
		}
	}
	return manager, web, fallback, model
}

func TestImageExecutionCapacityBlocksWebProviderBeforeFallback(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "non_stream", true: "stream"}[stream], func(t *testing.T) {
			manager, web, fallback, model := newImageCapacityTestManager(t, true)
			providers := []string{web.id, fallback.id}
			if stream {
				result, err := manager.ExecuteStream(t.Context(), providers, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				if err != nil {
					t.Fatalf("ExecuteStream() error = %v", err)
				}
				for range result.Chunks {
				}
			} else {
				if _, err := manager.Execute(t.Context(), providers, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{}); err != nil {
					t.Fatalf("Execute() error = %v", err)
				}
			}
			if calls := web.callIDs(); len(calls) != 1 {
				t.Fatalf("web calls = %v, want exactly one credential attempt", calls)
			}
			if calls := fallback.callIDs(); len(calls) != 1 {
				t.Fatalf("fallback calls = %v, want one", calls)
			}
			metrics := manager.RequestExecutionMetrics()
			if metrics.AuthSlotReleased < 1 || metrics.SelectedButNotCommitted < 1 || metrics.UpstreamCommitted != 1 {
				t.Fatalf("request metrics = %+v, want released Web reservation and one fallback commit", metrics)
			}
		})
	}
}

func TestImageExecutionCapacityWebOnlyReturnsOneSafeFailure(t *testing.T) {
	manager, web, _, model := newImageCapacityTestManager(t, false)
	_, err := manager.Execute(t.Context(), []string{web.id}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if !cliproxyexecutor.IsImageExecutionCapacityError(err) {
		t.Fatalf("Execute() error = %T %v, want image capacity", err, err)
	}
	if calls := web.callIDs(); len(calls) != 1 {
		t.Fatalf("web calls = %v, want exactly one", calls)
	}
	metrics := manager.RequestExecutionMetrics()
	if metrics.UpstreamCommitted != 0 || metrics.AuthSlotReleased != 1 || metrics.SelectedButNotCommitted != 1 {
		t.Fatalf("request metrics = %+v, want uncommitted reservation release", metrics)
	}
	for _, auth := range manager.List() {
		if auth.LastError != nil || auth.Unavailable || auth.Status != StatusActive {
			t.Fatalf("capacity failure changed auth state: %#v", auth)
		}
	}
}

func TestImageExecutionCapacityStrictSessionAffinityDoesNotFallback(t *testing.T) {
	failover := false
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback: &FillFirstSelector{},
		TTL:      time.Minute,
		Failover: &failover,
	})
	t.Cleanup(selector.Stop)
	manager, web, fallback, model := newImageCapacityTestManagerWithSelector(t, true, selector)
	manager.SetConfig(&config.Config{Routing: config.RoutingConfig{
		SessionAffinity:             true,
		SessionAffinityFailover:     &failover,
		PerAuthRequestLimit:         1,
		PerAuthRequestWindowMinutes: 5,
	}})

	var boundAuthID string
	for _, auth := range manager.List() {
		if auth.Provider == web.id {
			boundAuthID = auth.ID
			break
		}
	}
	if boundAuthID == "" {
		t.Fatal("web auth not found")
	}
	opts := cliproxyexecutor.Options{Headers: http.Header{"Session-Id": {"image-capacity-strict-session"}}}
	selector.BindSession(t.Context(), affinityProviderKey([]string{web.id, fallback.id}), model, opts, boundAuthID)

	result, err := manager.ExecuteStream(t.Context(), []string{web.id, fallback.id}, cliproxyexecutor.Request{Model: model}, opts)
	if result != nil || !cliproxyexecutor.IsImageExecutionCapacityError(err) {
		t.Fatalf("ExecuteStream() = (%v, %T %v), want image capacity", result, err, err)
	}
	if calls := web.callIDs(); len(calls) != 1 || calls[0] != boundAuthID {
		t.Fatalf("web calls = %v, want only bound auth %q", calls, boundAuthID)
	}
	if calls := fallback.callIDs(); len(calls) != 0 {
		t.Fatalf("fallback calls = %v, want none", calls)
	}
	metrics := manager.RequestExecutionMetrics()
	if metrics.UpstreamCommitted != 0 || metrics.AuthSlotReleased != 1 || metrics.SelectedButNotCommitted != 1 {
		t.Fatalf("request metrics = %+v, want one uncommitted reservation release", metrics)
	}
	diagnostics := manager.RoutingDiagnostics(web.id, model, time.Now())
	if len(diagnostics.Priorities) != 1 || diagnostics.Priorities[0].EligibleNow != 2 || diagnostics.Priorities[0].RequestLimited != 0 {
		t.Fatalf("routing diagnostics after capacity failure = %+v", diagnostics)
	}
	for _, auth := range manager.List() {
		if auth.LastError != nil || auth.Unavailable || auth.Status != StatusActive {
			t.Fatalf("capacity failure changed auth state: %#v", auth)
		}
	}
}

func TestObserveImageRequestSelectionPhasesSeparatesReservationTime(t *testing.T) {
	recorder := &imagePhaseRecorder{}
	metadata := map[string]any{cliproxyexecutor.RequestPhaseObserverMetadataKey: recorder}
	slot := &cliproxyexecutor.AuthRequestSlot{}
	slot.RecordReservationDuration(4 * time.Millisecond)

	started := time.Now().Add(-10 * time.Millisecond)
	observeImageRequestSelectionPhases(metadata, slot, started, 0)
	totalElapsed := time.Since(started)

	if got := recorder.durations[cliproxyexecutor.ImagePhaseRequestSlot]; got != 4*time.Millisecond {
		t.Fatalf("request slot duration = %v, want 4ms", got)
	}
	selection := recorder.durations[cliproxyexecutor.ImagePhaseCredentialSelection]
	if selection < 0 || selection+4*time.Millisecond > totalElapsed {
		t.Fatalf("selection=%v slot=4ms total=%v", selection, totalElapsed)
	}

	previousTotal := slot.ReservationDurationNanos()
	slot.RecordReservationDuration(3 * time.Millisecond)
	retryRecorder := &imagePhaseRecorder{}
	retryMetadata := map[string]any{cliproxyexecutor.RequestPhaseObserverMetadataKey: retryRecorder}
	retryStarted := time.Now().Add(-8 * time.Millisecond)
	observeImageRequestSelectionPhases(retryMetadata, slot, retryStarted, previousTotal)
	retryElapsed := time.Since(retryStarted)
	if got := retryRecorder.durations[cliproxyexecutor.ImagePhaseRequestSlot]; got != 3*time.Millisecond {
		t.Fatalf("retry request slot duration = %v, want only the new 3ms", got)
	}
	if got := retryRecorder.durations[cliproxyexecutor.ImagePhaseCredentialSelection]; got < 0 || got+3*time.Millisecond > retryElapsed {
		t.Fatalf("retry selection=%v slot=3ms total=%v", got, retryElapsed)
	}

	nilRecorder := &imagePhaseRecorder{}
	nilMetadata := map[string]any{cliproxyexecutor.RequestPhaseObserverMetadataKey: nilRecorder}
	nilStarted := time.Now().Add(-time.Millisecond)
	observeImageRequestSelectionPhases(nilMetadata, nil, nilStarted, ^uint64(0))
	if got := nilRecorder.durations[cliproxyexecutor.ImagePhaseRequestSlot]; got != 0 {
		t.Fatalf("nil slot duration = %v, want zero", got)
	}
	if got := nilRecorder.durations[cliproxyexecutor.ImagePhaseCredentialSelection]; got < 0 || got > time.Since(nilStarted) {
		t.Fatalf("nil slot selection duration = %v", got)
	}

	overflowRecorder := &imagePhaseRecorder{}
	overflowMetadata := map[string]any{cliproxyexecutor.RequestPhaseObserverMetadataKey: overflowRecorder}
	observeImageRequestSelectionPhases(overflowMetadata, slot, time.Now(), ^uint64(0))
	if got := overflowRecorder.durations[cliproxyexecutor.ImagePhaseRequestSlot]; got != 0 {
		t.Fatalf("wrapped baseline request slot duration = %v, want zero", got)
	}
}
