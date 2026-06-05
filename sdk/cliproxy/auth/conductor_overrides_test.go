package auth

import (
	"context"
	"errors"
	"fmt"
	rand "math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

const requestScopedNotFoundMessage = "Item with id 'rs_0b5f3eb6f51f175c0169ca74e4a85881998539920821603a74' not found. Items are not persisted when `store` is set to false. Try again with `store` set to true, or remove this item from your input."

func TestManager_RequestRetryOverrideControlsRequestRounds(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetRetryConfig(0, 0, 0)
	m.SetConfig(&internalconfig.Config{NoCooldownStatusCodes: []int{http.StatusInternalServerError}})

	executor := &authFallbackExecutor{
		id:            "claude",
		executeErrors: map[string]error{"auth-1": &Error{HTTPStatus: http.StatusInternalServerError, Message: "boom"}},
	}
	m.RegisterExecutor(executor)
	auth := &Auth{
		ID:       "auth-1",
		Provider: "claude",
		Metadata: map[string]any{
			"request_retry": float64(2),
		},
	}
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })

	if _, errExecute := m.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{}); errExecute == nil {
		t.Fatal("Execute() error = nil, want auth failure")
	}
	if got := executor.ExecuteCalls(); len(got) != 3 {
		t.Fatalf("execute call count = %d, want 3 (initial + 2 request retries)", len(got))
	}
}

func TestManager_RequestRetryOverrideZeroOverridesGlobalDefault(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetRetryConfig(2, 0, 0)
	m.SetConfig(&internalconfig.Config{NoCooldownStatusCodes: []int{http.StatusInternalServerError}})

	authID := "auth-zero-retry-" + uuid.NewString()
	executor := &authFallbackExecutor{
		id:            "claude",
		executeErrors: map[string]error{authID: &Error{HTTPStatus: http.StatusInternalServerError, Message: "boom"}},
	}
	m.RegisterExecutor(executor)
	auth := &Auth{
		ID:       authID,
		Provider: "claude",
		Metadata: map[string]any{
			"request_retry": float64(0),
		},
	}
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })

	if _, errExecute := m.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{}); errExecute == nil {
		t.Fatal("Execute() error = nil, want auth failure")
	}
	if got := executor.ExecuteCalls(); len(got) != 1 {
		t.Fatalf("execute call count = %d, want 1 because request_retry=0 overrides global default", len(got))
	}
}

func TestManagerExecute_WaitsForUpstreamRetryAfter429WithinMaxInterval(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetRetryConfig(1, 50*time.Millisecond, 0)
	m.SetConfig(&internalconfig.Config{NoCooldownStatusCodes: []int{http.StatusTooManyRequests}})

	authID := "auth-retry-after-" + uuid.NewString()
	executor := &authFallbackExecutor{
		id: "claude",
		executeErrors: map[string]error{
			authID: &retryAfterStatusError{
				status:     http.StatusTooManyRequests,
				message:    "quota exhausted",
				retryAfter: time.Millisecond,
			},
		},
	}
	m.RegisterExecutor(executor)
	auth := &Auth{ID: authID, Provider: "claude"}
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })

	if _, errExecute := m.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{}); errExecute == nil {
		t.Fatal("Execute() error = nil, want retry exhaustion error")
	}
	if got := executor.ExecuteCalls(); len(got) != 2 {
		t.Fatalf("execute call count = %d, want 2 after one Retry-After wait", len(got))
	}
}

func TestManager_MaxRequestRetryForProvidersUsesAuthRequestRetryOverride(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetRetryConfig(0, 30*time.Second, 0)

	auth := &Auth{
		ID:       "auth-override-budget",
		Provider: "claude",
		Metadata: map[string]any{
			"request_retry": float64(2),
		},
	}
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	if got := m.maxRequestRetryForProviders([]string{"claude"}); got != 2 {
		t.Fatalf("maxRequestRetryForProviders() = %d, want 2", got)
	}
}

func TestManager_MaxRequestRetryForProvidersPreservesDefaultForUnsetAuths(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetRetryConfig(3, 30*time.Second, 0)

	authWithOverride := &Auth{
		ID:       "auth-zero-override-" + uuid.NewString(),
		Provider: "claude",
		Metadata: map[string]any{
			"request_retry": float64(0),
		},
	}
	authDefault := &Auth{
		ID:       "auth-default-retry-" + uuid.NewString(),
		Provider: "claude",
	}
	if _, errRegister := m.Register(context.Background(), authWithOverride); errRegister != nil {
		t.Fatalf("register override auth: %v", errRegister)
	}
	if _, errRegister := m.Register(context.Background(), authDefault); errRegister != nil {
		t.Fatalf("register default auth: %v", errRegister)
	}

	if got := m.maxRequestRetryForProviders([]string{"claude"}); got != 3 {
		t.Fatalf("maxRequestRetryForProviders() = %d, want 3 from auth without override", got)
	}
}

func TestManager_RequestRetryOverrideDoesNotUseUnrelatedProvider(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetRetryConfig(0, 0, 0)
	m.SetConfig(&internalconfig.Config{NoCooldownStatusCodes: []int{http.StatusInternalServerError}})

	claudeExecutor := &authFallbackExecutor{
		id: "claude",
		executeErrors: map[string]error{
			"claude-budget-target": &Error{HTTPStatus: http.StatusInternalServerError, Message: "boom"},
		},
	}
	geminiExecutor := &authFallbackExecutor{id: "gemini"}
	m.RegisterExecutor(claudeExecutor)
	m.RegisterExecutor(geminiExecutor)

	claudeAuth := &Auth{
		ID:       "claude-budget-target",
		Provider: "claude",
		Metadata: map[string]any{"type": "claude"},
	}
	geminiAuth := &Auth{
		ID:       "gemini-budget-unrelated",
		Provider: "gemini",
		Metadata: map[string]any{
			"type":          "gemini",
			"request_retry": float64(5),
		},
	}
	if _, errRegister := m.Register(context.Background(), claudeAuth); errRegister != nil {
		t.Fatalf("register claude auth: %v", errRegister)
	}
	if _, errRegister := m.Register(context.Background(), geminiAuth); errRegister != nil {
		t.Fatalf("register gemini auth: %v", errRegister)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(claudeAuth.ID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
	reg.RegisterClient(geminiAuth.ID, "gemini", []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		reg.UnregisterClient(claudeAuth.ID)
		reg.UnregisterClient(geminiAuth.ID)
	})

	if _, errExecute := m.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{}); errExecute == nil {
		t.Fatal("Execute() error = nil, want auth failure")
	}
	if got := claudeExecutor.ExecuteCalls(); len(got) != 1 {
		t.Fatalf("claude execute call count = %d, want 1", len(got))
	}
	if got := geminiExecutor.ExecuteCalls(); len(got) != 0 {
		t.Fatalf("gemini execute call count = %d, want 0", len(got))
	}
}

func TestManager_WithRequestRetryBudgetUsesBootstrapRetriesOnly(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetRetryConfig(0, 100*time.Millisecond, 0)

	ctx := m.WithRequestRetryBudget(context.Background(), 2)
	rebuilt := m.withRequestRetryBudget(ctx, []string{"claude"}, 0)
	budget := requestRetryBudgetFromContext(rebuilt)
	if budget == nil {
		t.Fatal("requestRetryBudgetFromContext() = nil, want budget")
	}
	if got := budget.remaining.Load(); got != 2 {
		t.Fatalf("remaining budget = %d, want 2", got)
	}
	if !ConsumeRequestRetryBudget(rebuilt) || !ConsumeRequestRetryBudget(rebuilt) {
		t.Fatal("expected two bootstrap retries to be available")
	}
	if ConsumeRequestRetryBudget(rebuilt) {
		t.Fatal("expected bootstrap retry budget to be exhausted")
	}
}

func TestManager_AvailableAuthsForRouteModelUsesOAuthModelAliasForCooldown(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetRetryConfig(3, 30*time.Second, 0)
	m.SetOAuthModelAlias(map[string][]internalconfig.OAuthModelAlias{
		"kimi": {
			{Name: "deepseek-v3.1", Alias: "pool-model"},
		},
	})

	routeModel := "pool-model"
	upstreamModel := "deepseek-v3.1"
	next := time.Now().Add(5 * time.Second)

	auth := &Auth{
		ID:       "auth-1",
		Provider: "kimi",
		ModelStates: map[string]*ModelState{
			upstreamModel: {
				Unavailable:    true,
				Status:         StatusError,
				NextRetryAfter: next,
				Quota: QuotaState{
					Exceeded:      true,
					Reason:        "quota",
					NextRecoverAt: next,
				},
			},
		},
	}
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	_, errAvailable := m.availableAuthsForRouteModel([]*Auth{auth}, "kimi", routeModel, cliproxyexecutor.Options{}, time.Now())
	if errAvailable == nil {
		t.Fatal("availableAuthsForRouteModel() error = nil, want cooldown error")
	}
	var cooldownErr *modelCooldownError
	if !errors.As(errAvailable, &cooldownErr) {
		t.Fatalf("availableAuthsForRouteModel() error = %T, want *modelCooldownError", errAvailable)
	}
	if cooldownErr.resetIn <= 0 {
		t.Fatalf("cooldown resetIn = %v, want > 0", cooldownErr.resetIn)
	}
}

func TestSetSelectionAttemptMetadata_DoesNotMutateCallerMetadata(t *testing.T) {
	originalMetadata := map[string]any{"email": "user@example.com"}
	opts := cliproxyexecutor.Options{Metadata: originalMetadata}

	updated := setSelectionAttemptMetadata(opts, 2)

	if _, exists := originalMetadata[cliproxyexecutor.SelectionAttemptMetadataKey]; exists {
		t.Fatal("expected original metadata to remain unchanged")
	}
	if got := updated.Metadata[cliproxyexecutor.SelectionAttemptMetadataKey]; got != 2 {
		t.Fatalf("selection_attempt = %v, want 2", got)
	}
	if got := updated.Metadata["email"]; got != "user@example.com" {
		t.Fatalf("email metadata = %v, want user@example.com", got)
	}
}

func TestAvailableAuthsForRouteModel_SortsSelectedPriorityDeterministically(t *testing.T) {
	m := NewManager(nil, &FillFirstSelector{}, nil)
	auths := []*Auth{
		{ID: "b-auth", Provider: "claude", Attributes: map[string]string{"priority": "10"}},
		{ID: "a-auth", Provider: "claude", Attributes: map[string]string{"priority": "10"}},
		{ID: "z-low", Provider: "claude", Attributes: map[string]string{"priority": "1"}},
	}

	available, err := m.availableAuthsForRouteModel(auths, "claude", "", cliproxyexecutor.Options{}, time.Now())
	if err != nil {
		t.Fatalf("availableAuthsForRouteModel() error = %v", err)
	}
	if len(available) != 2 {
		t.Fatalf("availableAuthsForRouteModel() len = %d, want 2", len(available))
	}
	if available[0].ID != "a-auth" || available[1].ID != "b-auth" {
		t.Fatalf("availableAuthsForRouteModel() IDs = [%s %s], want [a-auth b-auth]", available[0].ID, available[1].ID)
	}
}

type credentialRetryLimitExecutor struct {
	id string

	mu    sync.Mutex
	calls int
}

func (e *credentialRetryLimitExecutor) Identifier() string {
	return e.id
}

func (e *credentialRetryLimitExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.recordCall()
	return cliproxyexecutor.Response{}, &Error{HTTPStatus: 500, Message: "boom"}
}

func (e *credentialRetryLimitExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.recordCall()
	return nil, &Error{HTTPStatus: 500, Message: "boom"}
}

func (e *credentialRetryLimitExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (e *credentialRetryLimitExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.recordCall()
	return cliproxyexecutor.Response{}, &Error{HTTPStatus: 500, Message: "boom"}
}

func (e *credentialRetryLimitExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func (e *credentialRetryLimitExecutor) recordCall() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls++
}

func (e *credentialRetryLimitExecutor) Calls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls
}

type authFallbackExecutor struct {
	id string

	mu                sync.Mutex
	executeCalls      []string
	countCalls        []string
	streamCalls       []string
	executeErrors     map[string]error
	countErrors       map[string]error
	streamFirstErrors map[string]error
}

func (e *authFallbackExecutor) Identifier() string {
	return e.id
}

func (e *authFallbackExecutor) Execute(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.mu.Lock()
	e.executeCalls = append(e.executeCalls, auth.ID)
	err := e.executeErrors[auth.ID]
	e.mu.Unlock()
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}
	return cliproxyexecutor.Response{Payload: []byte(auth.ID)}, nil
}

func (e *authFallbackExecutor) ExecuteStream(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.mu.Lock()
	e.streamCalls = append(e.streamCalls, auth.ID)
	err := e.streamFirstErrors[auth.ID]
	e.mu.Unlock()

	ch := make(chan cliproxyexecutor.StreamChunk, 1)
	if err != nil {
		ch <- cliproxyexecutor.StreamChunk{Err: err}
		close(ch)
		return &cliproxyexecutor.StreamResult{Headers: http.Header{"X-Auth": {auth.ID}}, Chunks: ch}, nil
	}
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte(auth.ID)}
	close(ch)
	return &cliproxyexecutor.StreamResult{Headers: http.Header{"X-Auth": {auth.ID}}, Chunks: ch}, nil
}

func (e *authFallbackExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (e *authFallbackExecutor) CountTokens(_ context.Context, auth *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.mu.Lock()
	e.countCalls = append(e.countCalls, auth.ID)
	err := e.countErrors[auth.ID]
	e.mu.Unlock()
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}
	return cliproxyexecutor.Response{Payload: []byte(auth.ID)}, nil
}

func (e *authFallbackExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

type codexHTTPFallbackPayloadExecutor struct {
	mu            sync.Mutex
	calls         []string
	payloads      map[string][]byte
	executeErrors map[string]error
}

func (e *codexHTTPFallbackPayloadExecutor) Identifier() string {
	return "codex"
}

func (e *codexHTTPFallbackPayloadExecutor) Execute(_ context.Context, auth *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.mu.Lock()
	e.calls = append(e.calls, auth.ID)
	if e.payloads == nil {
		e.payloads = make(map[string][]byte)
	}
	e.payloads[auth.ID] = append([]byte(nil), req.Payload...)
	err := e.executeErrors[auth.ID]
	e.mu.Unlock()
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}
	return cliproxyexecutor.Response{Payload: []byte(auth.ID)}, nil
}

func (e *codexHTTPFallbackPayloadExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, &Error{HTTPStatus: http.StatusNotImplemented, Message: "ExecuteStream not implemented"}
}

func (e *codexHTTPFallbackPayloadExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (e *codexHTTPFallbackPayloadExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, &Error{HTTPStatus: http.StatusNotImplemented, Message: "CountTokens not implemented"}
}

func (e *codexHTTPFallbackPayloadExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, &Error{HTTPStatus: http.StatusNotImplemented, Message: "HttpRequest not implemented"}
}

func (e *codexHTTPFallbackPayloadExecutor) Calls() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, len(e.calls))
	copy(out, e.calls)
	return out
}

func (e *codexHTTPFallbackPayloadExecutor) PayloadFor(authID string) []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]byte(nil), e.payloads[authID]...)
}

func (e *authFallbackExecutor) ExecuteCalls() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, len(e.executeCalls))
	copy(out, e.executeCalls)
	return out
}

func (e *authFallbackExecutor) CountCalls() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, len(e.countCalls))
	copy(out, e.countCalls)
	return out
}

func (e *authFallbackExecutor) StreamCalls() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, len(e.streamCalls))
	copy(out, e.streamCalls)
	return out
}

type retryAfterStatusError struct {
	status     int
	message    string
	retryAfter time.Duration
}

func (e *retryAfterStatusError) Error() string {
	if e == nil {
		return ""
	}
	return e.message
}

func (e *retryAfterStatusError) StatusCode() int {
	if e == nil {
		return 0
	}
	return e.status
}

func (e *retryAfterStatusError) RetryAfter() *time.Duration {
	if e == nil {
		return nil
	}
	d := e.retryAfter
	return &d
}

func newCredentialRetryLimitTestManager(t *testing.T, maxRetryCredentials int) (*Manager, *credentialRetryLimitExecutor) {
	return newCredentialRetryLimitTestManagerWithAuthCount(t, maxRetryCredentials, 2)
}

func newCredentialRetryLimitTestManagerWithAuthCount(t *testing.T, maxRetryCredentials int, authCount int) (*Manager, *credentialRetryLimitExecutor) {
	t.Helper()
	if authCount <= 0 {
		authCount = 1
	}

	m := NewManager(nil, nil, nil)
	m.SetRetryConfig(0, 0, maxRetryCredentials)

	executor := &credentialRetryLimitExecutor{id: "claude"}
	m.RegisterExecutor(executor)

	// Auth selection requires that the global model registry knows each credential supports the model.
	reg := registry.GetGlobalRegistry()
	baseID := uuid.NewString()
	authIDs := make([]string, 0, authCount)
	for i := 0; i < authCount; i++ {
		authID := fmt.Sprintf("%s-auth-%d", baseID, i+1)
		authIDs = append(authIDs, authID)
		reg.RegisterClient(authID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
	}
	t.Cleanup(func() {
		for _, authID := range authIDs {
			reg.UnregisterClient(authID)
		}
	})

	for _, authID := range authIDs {
		if _, errRegister := m.Register(context.Background(), &Auth{ID: authID, Provider: "claude"}); errRegister != nil {
			t.Fatalf("register %s: %v", authID, errRegister)
		}
	}

	return m, executor
}

func TestManager_MaxRetryCredentials_LimitsCrossCredentialRetries(t *testing.T) {
	request := cliproxyexecutor.Request{Model: "test-model"}
	testCases := []struct {
		name   string
		invoke func(*Manager) error
	}{
		{
			name: "execute",
			invoke: func(m *Manager) error {
				_, errExecute := m.Execute(context.Background(), []string{"claude"}, request, cliproxyexecutor.Options{})
				return errExecute
			},
		},
		{
			name: "execute_count",
			invoke: func(m *Manager) error {
				_, errExecute := m.ExecuteCount(context.Background(), []string{"claude"}, request, cliproxyexecutor.Options{})
				return errExecute
			},
		},
		{
			name: "execute_stream",
			invoke: func(m *Manager) error {
				_, errExecute := m.ExecuteStream(context.Background(), []string{"claude"}, request, cliproxyexecutor.Options{})
				return errExecute
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			limitedManager, limitedExecutor := newCredentialRetryLimitTestManager(t, 1)
			if errInvoke := tc.invoke(limitedManager); errInvoke == nil {
				t.Fatalf("expected error for limited retry execution")
			}
			if calls := limitedExecutor.Calls(); calls != 1 {
				t.Fatalf("expected 1 call with max-retry-credentials=1, got %d", calls)
			}

			unlimitedManager, unlimitedExecutor := newCredentialRetryLimitTestManager(t, 0)
			if errInvoke := tc.invoke(unlimitedManager); errInvoke == nil {
				t.Fatalf("expected error for unlimited retry execution")
			}
			if calls := unlimitedExecutor.Calls(); calls != 2 {
				t.Fatalf("expected 2 calls with max-retry-credentials=0, got %d", calls)
			}
		})
	}
}

func TestManager_MaxRetryCredentialsZeroKeepsTryingAllAvailableCredentials(t *testing.T) {
	request := cliproxyexecutor.Request{Model: "test-model"}
	manager, executor := newCredentialRetryLimitTestManagerWithAuthCount(t, 0, 6)

	if _, errExecute := manager.Execute(context.Background(), []string{"claude"}, request, cliproxyexecutor.Options{}); errExecute == nil {
		t.Fatalf("expected error for unlimited retry execution")
	}
	if calls := executor.Calls(); calls != 6 {
		t.Fatalf("expected 6 calls with max-retry-credentials=0, got %d", calls)
	}
}

func TestManager_StrictSessionAffinityDoesNotTryAnotherAuthAfterFailure(t *testing.T) {
	failover := false
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback: &RoundRobinSelector{},
		TTL:      time.Minute,
		Failover: &failover,
	})
	t.Cleanup(selector.Stop)
	m := NewManager(nil, selector, nil)
	m.SetConfig(&internalconfig.Config{
		Routing: internalconfig.RoutingConfig{
			SessionAffinity:         true,
			SessionAffinityFailover: &failover,
		},
	})
	m.SetRetryConfig(0, 0, 0)

	executor := &authFallbackExecutor{
		id:            "claude",
		executeErrors: map[string]error{"auth-a": &Error{HTTPStatus: http.StatusInternalServerError, Message: "boom"}},
	}
	m.RegisterExecutor(executor)

	reg := registry.GetGlobalRegistry()
	for _, authID := range []string{"auth-a", "auth-b"} {
		reg.RegisterClient(authID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
		if _, errRegister := m.Register(context.Background(), &Auth{ID: authID, Provider: "claude"}); errRegister != nil {
			t.Fatalf("register %s: %v", authID, errRegister)
		}
	}
	t.Cleanup(func() {
		reg.UnregisterClient("auth-a")
		reg.UnregisterClient("auth-b")
	})

	payload := []byte(`{"metadata":{"user_id":"user_xxx_account__session_manager-strict"}}`)
	_, errExecute := m.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{
		Model:   "test-model",
		Payload: payload,
	}, cliproxyexecutor.Options{OriginalRequest: payload})
	if errExecute == nil {
		t.Fatalf("Execute() error = nil, want auth-a failure")
	}

	executor.mu.Lock()
	defer executor.mu.Unlock()
	if len(executor.executeCalls) != 1 {
		t.Fatalf("execute calls = %v, want only auth-a", executor.executeCalls)
	}
	if executor.executeCalls[0] != "auth-a" {
		t.Fatalf("first execute auth = %q, want auth-a", executor.executeCalls[0])
	}
}

func TestManager_ModelSupportBadRequest_FallsBackAndSuspendsAuth(t *testing.T) {
	m := NewManager(nil, nil, nil)
	executor := &authFallbackExecutor{
		id: "claude",
		executeErrors: map[string]error{
			"aa-bad-auth": &Error{
				HTTPStatus: http.StatusBadRequest,
				Message:    "invalid_request_error: The requested model is not supported.",
			},
		},
	}
	m.RegisterExecutor(executor)

	model := "claude-opus-4-6"
	badAuth := &Auth{ID: "aa-bad-auth", Provider: "claude"}
	goodAuth := &Auth{ID: "bb-good-auth", Provider: "claude"}

	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(badAuth.ID, "claude", []*registry.ModelInfo{{ID: model}})
	reg.RegisterClient(goodAuth.ID, "claude", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() {
		reg.UnregisterClient(badAuth.ID)
		reg.UnregisterClient(goodAuth.ID)
	})

	if _, errRegister := m.Register(context.Background(), badAuth); errRegister != nil {
		t.Fatalf("register bad auth: %v", errRegister)
	}
	if _, errRegister := m.Register(context.Background(), goodAuth); errRegister != nil {
		t.Fatalf("register good auth: %v", errRegister)
	}

	request := cliproxyexecutor.Request{Model: model}
	for i := 0; i < 2; i++ {
		resp, errExecute := m.Execute(context.Background(), []string{"claude"}, request, cliproxyexecutor.Options{})
		if errExecute != nil {
			t.Fatalf("execute %d error = %v, want success", i, errExecute)
		}
		if string(resp.Payload) != goodAuth.ID {
			t.Fatalf("execute %d payload = %q, want %q", i, string(resp.Payload), goodAuth.ID)
		}
	}

	got := executor.ExecuteCalls()
	want := []string{badAuth.ID, goodAuth.ID, goodAuth.ID}
	if len(got) != len(want) {
		t.Fatalf("execute calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("execute call %d auth = %q, want %q", i, got[i], want[i])
		}
	}

	updatedBad, ok := m.GetByID(badAuth.ID)
	if !ok || updatedBad == nil {
		t.Fatalf("expected bad auth to remain registered")
	}
	state := updatedBad.ModelStates[model]
	if state == nil {
		t.Fatalf("expected model state for %q", model)
	}
	if !state.Unavailable {
		t.Fatalf("expected bad auth model state to be unavailable")
	}
	if state.NextRetryAfter.IsZero() {
		t.Fatalf("expected bad auth model state cooldown to be set")
	}
}

func TestManagerExecuteStream_ModelSupportBadRequestFallsBackAndSuspendsAuth(t *testing.T) {
	m := NewManager(nil, nil, nil)
	executor := &authFallbackExecutor{
		id: "claude",
		streamFirstErrors: map[string]error{
			"aa-bad-auth": &Error{
				HTTPStatus: http.StatusBadRequest,
				Message:    "invalid_request_error: The requested model is not supported.",
			},
		},
	}
	m.RegisterExecutor(executor)

	model := "claude-opus-4-6"
	badAuth := &Auth{ID: "aa-bad-auth", Provider: "claude"}
	goodAuth := &Auth{ID: "bb-good-auth", Provider: "claude"}

	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(badAuth.ID, "claude", []*registry.ModelInfo{{ID: model}})
	reg.RegisterClient(goodAuth.ID, "claude", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() {
		reg.UnregisterClient(badAuth.ID)
		reg.UnregisterClient(goodAuth.ID)
	})

	if _, errRegister := m.Register(context.Background(), badAuth); errRegister != nil {
		t.Fatalf("register bad auth: %v", errRegister)
	}
	if _, errRegister := m.Register(context.Background(), goodAuth); errRegister != nil {
		t.Fatalf("register good auth: %v", errRegister)
	}

	request := cliproxyexecutor.Request{Model: model}
	for i := 0; i < 2; i++ {
		streamResult, errExecute := m.ExecuteStream(context.Background(), []string{"claude"}, request, cliproxyexecutor.Options{})
		if errExecute != nil {
			t.Fatalf("execute stream %d error = %v, want success", i, errExecute)
		}
		var payload []byte
		for chunk := range streamResult.Chunks {
			if chunk.Err != nil {
				t.Fatalf("execute stream %d chunk error = %v, want success", i, chunk.Err)
			}
			payload = append(payload, chunk.Payload...)
		}
		if string(payload) != goodAuth.ID {
			t.Fatalf("execute stream %d payload = %q, want %q", i, string(payload), goodAuth.ID)
		}
	}

	got := executor.StreamCalls()
	want := []string{badAuth.ID, goodAuth.ID, goodAuth.ID}
	if len(got) != len(want) {
		t.Fatalf("stream calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stream call %d auth = %q, want %q", i, got[i], want[i])
		}
	}

	updatedBad, ok := m.GetByID(badAuth.ID)
	if !ok || updatedBad == nil {
		t.Fatalf("expected bad auth to remain registered")
	}
	state := updatedBad.ModelStates[model]
	if state == nil {
		t.Fatalf("expected model state for %q", model)
	}
	if !state.Unavailable {
		t.Fatalf("expected bad auth model state to be unavailable")
	}
	if state.NextRetryAfter.IsZero() {
		t.Fatalf("expected bad auth model state cooldown to be set")
	}
}

func TestManagerExecute_RandomRetryDropsToLowerPriority(t *testing.T) {
	m := NewManager(nil, &RandomSelector{}, nil)
	m.SetRetryConfig(1, 0, 1)

	executor := &authFallbackExecutor{
		id: "claude",
		executeErrors: map[string]error{
			"high-a": &Error{HTTPStatus: http.StatusTooManyRequests, Message: "high-a exhausted"},
			"high-b": &Error{HTTPStatus: http.StatusTooManyRequests, Message: "high-b exhausted"},
		},
	}
	m.RegisterExecutor(executor)

	auths := []*Auth{
		{ID: "high-a", Provider: "claude", Attributes: map[string]string{"priority": "10"}},
		{ID: "high-b", Provider: "claude", Attributes: map[string]string{"priority": "10"}},
		{ID: "low", Provider: "claude", Attributes: map[string]string{"priority": "0"}},
	}
	for _, auth := range auths {
		if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register auth %q: %v", auth.ID, errRegister)
		}
	}

	resp, errExecute := m.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if string(resp.Payload) != "low" {
		t.Fatalf("Execute() payload = %q, want %q", string(resp.Payload), "low")
	}

	got := executor.ExecuteCalls()
	if len(got) != 2 {
		t.Fatalf("execute calls = %v, want exactly 2 calls", got)
	}
	if got[0] == "low" {
		t.Fatalf("first execute call = %q, want a highest-priority auth", got[0])
	}
	if got[1] != "low" {
		t.Fatalf("second execute call = %q, want %q", got[1], "low")
	}
}

func TestManagerExecute_RequestRetryDropsPriorityForBuiltInSelectors(t *testing.T) {
	selectors := map[string]Selector{
		"round_robin": &RoundRobinSelector{},
		"fill_first":  &FillFirstSelector{},
		"random":      &RandomSelector{rnd: rand.New(rand.NewPCG(1, 2))},
	}

	for name, selector := range selectors {
		t.Run(name, func(t *testing.T) {
			m := NewManager(nil, selector, nil)
			m.SetRetryConfig(1, 0, 2)
			m.SetConfig(&internalconfig.Config{NoCooldownStatusCodes: []int{http.StatusInternalServerError}})

			executor := &authFallbackExecutor{
				id: "claude",
				executeErrors: map[string]error{
					"high-a": &Error{HTTPStatus: http.StatusInternalServerError, Message: "high-a failed"},
					"high-b": &Error{HTTPStatus: http.StatusInternalServerError, Message: "high-b failed"},
				},
			}
			m.RegisterExecutor(executor)

			auths := []*Auth{
				{ID: "high-a", Provider: "claude", Attributes: map[string]string{"priority": "10"}},
				{ID: "high-b", Provider: "claude", Attributes: map[string]string{"priority": "10"}},
				{ID: "low", Provider: "claude", Attributes: map[string]string{"priority": "0"}},
			}
			for _, auth := range auths {
				if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
					t.Fatalf("register auth %q: %v", auth.ID, errRegister)
				}
			}

			resp, errExecute := m.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
			if errExecute != nil {
				t.Fatalf("Execute() error = %v", errExecute)
			}
			if string(resp.Payload) != "low" {
				t.Fatalf("Execute() payload = %q, want low", string(resp.Payload))
			}

			got := executor.ExecuteCalls()
			if len(got) != 3 {
				t.Fatalf("execute calls = %v, want 3", got)
			}
			if got[0] == "low" || got[1] == "low" {
				t.Fatalf("first request round should stay in highest priority, calls = %v", got)
			}
			if got[2] != "low" {
				t.Fatalf("second request round call = %q, want low", got[2])
			}
		})
	}
}

func TestManagerRequestRetryZeroFallsBackWhenHigherPriorityUnavailableWithinRound(t *testing.T) {
	selectors := map[string]Selector{
		"round_robin": &RoundRobinSelector{},
		"fill_first":  &FillFirstSelector{},
		"random":      &RandomSelector{rnd: rand.New(rand.NewPCG(3, 4))},
	}
	modes := []struct {
		name   string
		invoke func(*Manager) error
		calls  func(*authFallbackExecutor) []string
	}{
		{
			name: "execute",
			invoke: func(m *Manager) error {
				_, errExecute := m.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
				return errExecute
			},
			calls: func(e *authFallbackExecutor) []string { return e.ExecuteCalls() },
		},
		{
			name: "count",
			invoke: func(m *Manager) error {
				_, errExecute := m.ExecuteCount(context.Background(), []string{"claude"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
				return errExecute
			},
			calls: func(e *authFallbackExecutor) []string { return e.CountCalls() },
		},
		{
			name: "stream",
			invoke: func(m *Manager) error {
				result, errExecute := m.ExecuteStream(context.Background(), []string{"claude"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
				if errExecute != nil {
					return errExecute
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						return chunk.Err
					}
				}
				return nil
			},
			calls: func(e *authFallbackExecutor) []string { return e.StreamCalls() },
		},
	}

	for selectorName, selector := range selectors {
		selector := selector
		for _, mode := range modes {
			mode := mode
			t.Run(selectorName+"/"+mode.name, func(t *testing.T) {
				m := NewManager(nil, selector, nil)
				m.SetRetryConfig(0, 0, 0)
				m.SetConfig(&internalconfig.Config{NoCooldownStatusCodes: []int{http.StatusInternalServerError}})

				errByAuth := map[string]error{
					"high-a": &Error{HTTPStatus: http.StatusInternalServerError, Message: "high-a failed"},
					"high-b": &Error{HTTPStatus: http.StatusInternalServerError, Message: "high-b failed"},
					"low":    &Error{HTTPStatus: http.StatusInternalServerError, Message: "low failed"},
				}
				executor := &authFallbackExecutor{
					id:                "claude",
					executeErrors:     errByAuth,
					countErrors:       errByAuth,
					streamFirstErrors: errByAuth,
				}
				m.RegisterExecutor(executor)

				auths := []*Auth{
					{ID: "high-a", Provider: "claude", Attributes: map[string]string{"priority": "10"}},
					{ID: "high-b", Provider: "claude", Attributes: map[string]string{"priority": "10"}},
					{ID: "low", Provider: "claude", Attributes: map[string]string{"priority": "0"}},
				}
				for _, auth := range auths {
					if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
						t.Fatalf("register auth %q: %v", auth.ID, errRegister)
					}
				}

				if errInvoke := mode.invoke(m); errInvoke == nil {
					t.Fatal("Execute path returned nil error, want retry exhaustion failure")
				}
				got := mode.calls(executor)
				if len(got) != 3 {
					t.Fatalf("calls = %v, want high priority credentials followed by low priority credential", got)
				}
				if got[0] == "low" || got[1] == "low" || got[2] != "low" {
					t.Fatalf("calls = %v, want low priority only after higher priority is unavailable", got)
				}
			})
		}
	}
}

func TestManagerExecute_FallsBackWhenHigherPriorityCooling(t *testing.T) {
	m := NewManager(nil, &RoundRobinSelector{}, nil)
	m.SetRetryConfig(1, 100*time.Millisecond, 1)

	executor := &authFallbackExecutor{id: "claude"}
	m.RegisterExecutor(executor)

	next := time.Now().Add(20 * time.Millisecond)
	auths := []*Auth{
		{
			ID:         "high",
			Provider:   "claude",
			Attributes: map[string]string{"priority": "10"},
			ModelStates: map[string]*ModelState{
				"test-model": {
					Status:         StatusError,
					Unavailable:    true,
					NextRetryAfter: next,
				},
			},
		},
		{ID: "low", Provider: "claude", Attributes: map[string]string{"priority": "0"}},
	}
	reg := registry.GetGlobalRegistry()
	for _, auth := range auths {
		reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
		if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register auth %q: %v", auth.ID, errRegister)
		}
	}
	t.Cleanup(func() {
		for _, auth := range auths {
			reg.UnregisterClient(auth.ID)
		}
	})

	resp, errExecute := m.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{})
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if string(resp.Payload) != "low" {
		t.Fatalf("Execute() payload = %q, want low", string(resp.Payload))
	}
	if got := executor.ExecuteCalls(); len(got) != 1 || got[0] != "low" {
		t.Fatalf("execute calls = %v, want [low]", got)
	}
}

func TestManagerFallsBackAfterSameRoundFailureWhenHigherPriorityCooling(t *testing.T) {
	modes := []struct {
		name   string
		invoke func(*Manager) (string, error)
		calls  func(*authFallbackExecutor) []string
	}{
		{
			name: "execute",
			invoke: func(m *Manager) (string, error) {
				resp, errExecute := m.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{})
				return string(resp.Payload), errExecute
			},
			calls: func(e *authFallbackExecutor) []string { return e.ExecuteCalls() },
		},
		{
			name: "count",
			invoke: func(m *Manager) (string, error) {
				resp, errExecute := m.ExecuteCount(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{})
				return string(resp.Payload), errExecute
			},
			calls: func(e *authFallbackExecutor) []string { return e.CountCalls() },
		},
		{
			name: "stream",
			invoke: func(m *Manager) (string, error) {
				result, errExecute := m.ExecuteStream(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{})
				if errExecute != nil {
					return "", errExecute
				}
				var payload []byte
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						return "", chunk.Err
					}
					payload = append(payload, chunk.Payload...)
				}
				return string(payload), nil
			},
			calls: func(e *authFallbackExecutor) []string { return e.StreamCalls() },
		},
	}

	for _, mode := range modes {
		mode := mode
		t.Run(mode.name, func(t *testing.T) {
			m := NewManager(nil, &RoundRobinSelector{}, nil)
			m.SetRetryConfig(1, 100*time.Millisecond, 2)
			m.SetConfig(&internalconfig.Config{NoCooldownStatusCodes: []int{http.StatusInternalServerError}})

			baseID := uuid.NewString()
			highFailID := baseID + "-high-fail"
			highCoolID := baseID + "-high-cool"
			lowID := baseID + "-low"
			errByAuth := map[string]error{
				highFailID: &Error{HTTPStatus: http.StatusInternalServerError, Message: "high failed"},
			}
			executor := &authFallbackExecutor{
				id:                "claude",
				executeErrors:     errByAuth,
				countErrors:       errByAuth,
				streamFirstErrors: errByAuth,
			}
			m.RegisterExecutor(executor)

			next := time.Now().Add(20 * time.Millisecond)
			auths := []*Auth{
				{ID: highFailID, Provider: "claude", Attributes: map[string]string{"priority": "10"}},
				{
					ID:         highCoolID,
					Provider:   "claude",
					Attributes: map[string]string{"priority": "10"},
					ModelStates: map[string]*ModelState{
						"test-model": {
							Status:         StatusError,
							Unavailable:    true,
							NextRetryAfter: next,
						},
					},
				},
				{ID: lowID, Provider: "claude", Attributes: map[string]string{"priority": "0"}},
			}
			reg := registry.GetGlobalRegistry()
			for _, auth := range auths {
				reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
				if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
					t.Fatalf("register auth %q: %v", auth.ID, errRegister)
				}
			}
			t.Cleanup(func() {
				for _, auth := range auths {
					reg.UnregisterClient(auth.ID)
				}
			})

			payload, errExecute := mode.invoke(m)
			if errExecute != nil {
				t.Fatalf("Execute path error = %v", errExecute)
			}
			if payload != lowID {
				t.Fatalf("payload = %q, want %q", payload, lowID)
			}
			if got := mode.calls(executor); len(got) != 2 || got[0] != highFailID || got[1] != lowID {
				t.Fatalf("calls = %v, want [%s %s]", got, highFailID, lowID)
			}
		})
	}
}

func TestManagerExecute_WaitsForMultipleCooldownsInSameRoundWithoutResettingCredentials(t *testing.T) {
	m := NewManager(nil, &RoundRobinSelector{}, nil)
	m.SetRetryConfig(0, 200*time.Millisecond, 3)
	m.SetConfig(&internalconfig.Config{NoCooldownStatusCodes: []int{http.StatusInternalServerError}})

	baseID := uuid.NewString()
	failID := baseID + "-high-fail"
	coolOneID := baseID + "-high-cool-1"
	coolTwoID := baseID + "-high-cool-2"
	errByAuth := map[string]error{
		failID:    &Error{HTTPStatus: http.StatusInternalServerError, Message: "first failed"},
		coolOneID: &Error{HTTPStatus: http.StatusInternalServerError, Message: "second failed"},
	}
	executor := &authFallbackExecutor{
		id:            "claude",
		executeErrors: errByAuth,
	}
	m.RegisterExecutor(executor)

	now := time.Now()
	auths := []*Auth{
		{ID: failID, Provider: "claude", Attributes: map[string]string{"priority": "10"}},
		{
			ID:         coolOneID,
			Provider:   "claude",
			Attributes: map[string]string{"priority": "10"},
			ModelStates: map[string]*ModelState{
				"test-model": {
					Status:         StatusError,
					Unavailable:    true,
					NextRetryAfter: now.Add(20 * time.Millisecond),
				},
			},
		},
		{
			ID:         coolTwoID,
			Provider:   "claude",
			Attributes: map[string]string{"priority": "10"},
			ModelStates: map[string]*ModelState{
				"test-model": {
					Status:         StatusError,
					Unavailable:    true,
					NextRetryAfter: now.Add(60 * time.Millisecond),
				},
			},
		},
	}
	reg := registry.GetGlobalRegistry()
	for _, auth := range auths {
		reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
		if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register auth %q: %v", auth.ID, errRegister)
		}
	}
	t.Cleanup(func() {
		for _, auth := range auths {
			reg.UnregisterClient(auth.ID)
		}
	})

	resp, errExecute := m.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{})
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if string(resp.Payload) != coolTwoID {
		t.Fatalf("Execute() payload = %q, want %q", string(resp.Payload), coolTwoID)
	}
	if got := executor.ExecuteCalls(); len(got) != 3 || got[0] != failID || got[1] != coolOneID || got[2] != coolTwoID {
		t.Fatalf("execute calls = %v, want [%s %s %s]", got, failID, coolOneID, coolTwoID)
	}
}

func TestManagerExecuteStream_RequestRetryDropsPriorityAfterBootstrapFailureAtCredentialLimit(t *testing.T) {
	m := NewManager(nil, &RoundRobinSelector{}, nil)
	m.SetRetryConfig(1, 0, 1)
	m.SetConfig(&internalconfig.Config{NoCooldownStatusCodes: []int{http.StatusInternalServerError}})

	baseID := uuid.NewString()
	highID := baseID + "-high"
	lowID := baseID + "-low"
	executor := &authFallbackExecutor{
		id:                "claude",
		streamFirstErrors: map[string]error{highID: &Error{HTTPStatus: http.StatusInternalServerError, Message: "bootstrap failed"}},
	}
	m.RegisterExecutor(executor)

	auths := []*Auth{
		{ID: highID, Provider: "claude", Attributes: map[string]string{"priority": "10"}},
		{ID: lowID, Provider: "claude", Attributes: map[string]string{"priority": "0"}},
	}
	reg := registry.GetGlobalRegistry()
	for _, auth := range auths {
		reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
		if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register auth %q: %v", auth.ID, errRegister)
		}
	}
	t.Cleanup(func() {
		for _, auth := range auths {
			reg.UnregisterClient(auth.ID)
		}
	})

	result, errExecute := m.ExecuteStream(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{})
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	var payload []byte
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error = %v", chunk.Err)
		}
		payload = append(payload, chunk.Payload...)
	}
	if string(payload) != lowID {
		t.Fatalf("stream payload = %q, want %q", string(payload), lowID)
	}
	if got := executor.StreamCalls(); len(got) != 2 || got[0] != highID || got[1] != lowID {
		t.Fatalf("stream calls = %v, want [%s %s]", got, highID, lowID)
	}
}

func TestManagerExecute_FinalCooldownReturnsAuthUnavailable(t *testing.T) {
	m := NewManager(nil, &RoundRobinSelector{}, nil)
	m.SetRetryConfig(0, 0, 1)

	executor := &authFallbackExecutor{id: "claude"}
	m.RegisterExecutor(executor)

	auth := &Auth{
		ID:         "cooling",
		Provider:   "claude",
		Attributes: map[string]string{"priority": "10"},
		ModelStates: map[string]*ModelState{
			"test-model": {
				Status:         StatusError,
				Unavailable:    true,
				NextRetryAfter: time.Now().Add(time.Minute),
			},
		},
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() {
		reg.UnregisterClient(auth.ID)
	})
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	_, errExecute := m.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{})
	if errExecute == nil {
		t.Fatal("Execute() error = nil, want auth_unavailable")
	}
	var authErr *Error
	if !errors.As(errExecute, &authErr) || authErr.Code != "auth_unavailable" {
		t.Fatalf("Execute() error = %T %[1]v, want auth_unavailable", errExecute)
	}
	var cooldownErr *modelCooldownError
	if errors.As(errExecute, &cooldownErr) {
		t.Fatalf("Execute() returned model cooldown directly: %v", errExecute)
	}
	if got := executor.ExecuteCalls(); len(got) != 0 {
		t.Fatalf("execute calls = %v, want none", got)
	}
}

func TestManagerExecute_SessionAffinityCodexWebsocketPrefersReadyWebsocketAuth(t *testing.T) {
	failover := true
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback: &RoundRobinSelector{},
		TTL:      time.Minute,
		Failover: &failover,
	})
	t.Cleanup(selector.Stop)
	m := NewManager(nil, selector, nil)
	m.SetConfig(&internalconfig.Config{
		Routing: internalconfig.RoutingConfig{
			SessionAffinity:         true,
			SessionAffinityFailover: &failover,
		},
	})

	executor := &authFallbackExecutor{id: "codex"}
	m.RegisterExecutor(executor)

	baseID := uuid.NewString()
	httpID := baseID + "-http"
	wsID := baseID + "-ws"
	auths := []*Auth{
		{ID: httpID, Provider: "codex", Attributes: map[string]string{"priority": "10"}},
		{ID: wsID, Provider: "codex", Attributes: map[string]string{"priority": "0", "websockets": "true"}},
	}
	reg := registry.GetGlobalRegistry()
	for _, auth := range auths {
		reg.RegisterClient(auth.ID, "codex", []*registry.ModelInfo{{ID: "test-model"}})
		if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register auth %q: %v", auth.ID, errRegister)
		}
	}
	t.Cleanup(func() {
		for _, auth := range auths {
			reg.UnregisterClient(auth.ID)
		}
	})

	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	opts := cliproxyexecutor.Options{OriginalRequest: []byte(`{"metadata":{"user_id":"user_xxx_account__session_` + uuid.NewString() + `"}}`)}
	resp, errExecute := m.Execute(ctx, []string{"codex"}, cliproxyexecutor.Request{Model: "test-model"}, opts)
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if string(resp.Payload) != wsID {
		t.Fatalf("Execute() payload = %q, want websocket auth %q", string(resp.Payload), wsID)
	}
	if got := executor.ExecuteCalls(); len(got) != 1 || got[0] != wsID {
		t.Fatalf("execute calls = %v, want [%s]", got, wsID)
	}
}

func TestManagerExecute_SessionAffinityCodexWebsocketFallsBackWhenHigherPriorityCooling(t *testing.T) {
	failover := true
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback: &RoundRobinSelector{},
		TTL:      time.Minute,
		Failover: &failover,
	})
	t.Cleanup(selector.Stop)
	m := NewManager(nil, selector, nil)
	m.SetConfig(&internalconfig.Config{
		Routing: internalconfig.RoutingConfig{
			SessionAffinity:         true,
			SessionAffinityFailover: &failover,
		},
	})

	executor := &authFallbackExecutor{id: "codex"}
	m.RegisterExecutor(executor)

	baseID := uuid.NewString()
	wsCoolingID := baseID + "-ws-cooling"
	wsReadyID := baseID + "-ws-ready"
	next := time.Now().Add(time.Minute)
	auths := []*Auth{
		{
			ID:         wsCoolingID,
			Provider:   "codex",
			Attributes: map[string]string{"priority": "10", "websockets": "true"},
			ModelStates: map[string]*ModelState{
				"test-model": {
					Status:         StatusError,
					Unavailable:    true,
					NextRetryAfter: next,
					Quota:          QuotaState{Exceeded: true},
				},
			},
		},
		{ID: wsReadyID, Provider: "codex", Attributes: map[string]string{"priority": "0", "websockets": "true"}},
	}
	reg := registry.GetGlobalRegistry()
	for _, auth := range auths {
		reg.RegisterClient(auth.ID, "codex", []*registry.ModelInfo{{ID: "test-model"}})
		if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register auth %q: %v", auth.ID, errRegister)
		}
	}
	t.Cleanup(func() {
		for _, auth := range auths {
			reg.UnregisterClient(auth.ID)
		}
	})

	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	opts := cliproxyexecutor.Options{OriginalRequest: []byte(`{"metadata":{"user_id":"user_xxx_account__session_` + uuid.NewString() + `"}}`)}
	resp, errExecute := m.Execute(ctx, []string{"codex"}, cliproxyexecutor.Request{Model: "test-model"}, opts)
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if string(resp.Payload) != wsReadyID {
		t.Fatalf("Execute() payload = %q, want %q", string(resp.Payload), wsReadyID)
	}
	if got := executor.ExecuteCalls(); len(got) != 1 || got[0] != wsReadyID {
		t.Fatalf("execute calls = %v, want [%s]", got, wsReadyID)
	}
}

func TestManagerExecute_CodexWebsocketFallsBackToHTTPAfterWebsocketTried(t *testing.T) {
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback: &RoundRobinSelector{},
		TTL:      time.Minute,
	})
	t.Cleanup(selector.Stop)
	m := NewManager(nil, selector, nil)
	m.SetConfig(&internalconfig.Config{NoCooldownStatusCodes: []int{http.StatusInternalServerError}})

	baseID := uuid.NewString()
	wsID := baseID + "-ws"
	httpID := baseID + "-http"
	executor := &codexHTTPFallbackPayloadExecutor{
		executeErrors: map[string]error{wsID: &Error{HTTPStatus: http.StatusInternalServerError, Message: "websocket failed"}},
	}
	m.RegisterExecutor(executor)

	auths := []*Auth{
		{ID: wsID, Provider: "codex", Attributes: map[string]string{"priority": "10", "websockets": "true"}},
		{ID: httpID, Provider: "codex", Attributes: map[string]string{"priority": "10"}},
	}
	reg := registry.GetGlobalRegistry()
	for _, auth := range auths {
		reg.RegisterClient(auth.ID, "codex", []*registry.ModelInfo{{ID: "test-model"}})
		if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register auth %q: %v", auth.ID, errRegister)
		}
	}
	t.Cleanup(func() {
		for _, auth := range auths {
			reg.UnregisterClient(auth.ID)
		}
	})

	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	req := cliproxyexecutor.Request{
		Model:   "test-model",
		Payload: []byte(`{"model":"test-model","generate":true,"messages":[]}`),
	}
	resp, errExecute := m.Execute(ctx, []string{"codex"}, req, cliproxyexecutor.Options{})
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if string(resp.Payload) != httpID {
		t.Fatalf("Execute() payload = %q, want HTTP auth %q", string(resp.Payload), httpID)
	}
	if got := executor.Calls(); len(got) != 2 || got[0] != wsID || got[1] != httpID {
		t.Fatalf("execute calls = %v, want [%s %s]", got, wsID, httpID)
	}
	if payload := string(executor.PayloadFor(httpID)); strings.Contains(payload, `"generate"`) {
		t.Fatalf("HTTP fallback payload still contains generate: %s", payload)
	}
}

func TestManagerExecute_SessionAffinityBindsSuccessfulAuthAfterFailover(t *testing.T) {
	failover := true
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback: &RoundRobinSelector{},
		TTL:      time.Minute,
		Failover: &failover,
	})
	t.Cleanup(selector.Stop)
	m := NewManager(nil, selector, nil)
	m.SetConfig(&internalconfig.Config{
		NoCooldownStatusCodes: []int{http.StatusInternalServerError},
		Routing: internalconfig.RoutingConfig{
			SessionAffinity:         true,
			SessionAffinityFailover: &failover,
		},
	})
	m.SetRetryConfig(0, 0, 2)

	executor := &authFallbackExecutor{
		id:            "claude",
		executeErrors: map[string]error{"auth-a": &Error{HTTPStatus: http.StatusInternalServerError, Message: "boom"}},
	}
	m.RegisterExecutor(executor)

	auths := []*Auth{
		{ID: "auth-a", Provider: "claude"},
		{ID: "auth-b", Provider: "claude"},
	}
	reg := registry.GetGlobalRegistry()
	for _, auth := range auths {
		reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
		if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register auth %q: %v", auth.ID, errRegister)
		}
	}
	t.Cleanup(func() {
		for _, auth := range auths {
			reg.UnregisterClient(auth.ID)
		}
	})

	opts := cliproxyexecutor.Options{OriginalRequest: []byte(`{"metadata":{"user_id":"user_xxx_account__session_success-rebind"}}`)}
	req := cliproxyexecutor.Request{Model: "test-model"}
	resp, errExecute := m.Execute(context.Background(), []string{"claude"}, req, opts)
	if errExecute != nil {
		t.Fatalf("first Execute() error = %v", errExecute)
	}
	if string(resp.Payload) != "auth-b" {
		t.Fatalf("first Execute() payload = %q, want auth-b", string(resp.Payload))
	}

	resp, errExecute = m.Execute(context.Background(), []string{"claude"}, req, opts)
	if errExecute != nil {
		t.Fatalf("second Execute() error = %v", errExecute)
	}
	if string(resp.Payload) != "auth-b" {
		t.Fatalf("second Execute() payload = %q, want auth-b", string(resp.Payload))
	}
	if got := executor.ExecuteCalls(); len(got) != 3 || got[0] != "auth-a" || got[1] != "auth-b" || got[2] != "auth-b" {
		t.Fatalf("execute calls = %v, want [auth-a auth-b auth-b]", got)
	}
}

func TestManagerExecuteCount_SessionAffinityBindsSuccessfulAuthAfterFailover(t *testing.T) {
	failover := true
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback: &RoundRobinSelector{},
		TTL:      time.Minute,
		Failover: &failover,
	})
	t.Cleanup(selector.Stop)
	m := NewManager(nil, selector, nil)
	m.SetConfig(&internalconfig.Config{
		NoCooldownStatusCodes: []int{http.StatusInternalServerError},
		Routing: internalconfig.RoutingConfig{
			SessionAffinity:         true,
			SessionAffinityFailover: &failover,
		},
	})
	m.SetRetryConfig(0, 0, 2)

	baseID := uuid.NewString()
	authA := baseID + "-auth-a"
	authB := baseID + "-auth-b"
	executor := &authFallbackExecutor{
		id:          "claude",
		countErrors: map[string]error{authA: &Error{HTTPStatus: http.StatusInternalServerError, Message: "boom"}},
	}
	m.RegisterExecutor(executor)

	auths := []*Auth{
		{ID: authA, Provider: "claude"},
		{ID: authB, Provider: "claude"},
	}
	reg := registry.GetGlobalRegistry()
	for _, auth := range auths {
		reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
		if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register auth %q: %v", auth.ID, errRegister)
		}
	}
	t.Cleanup(func() {
		for _, auth := range auths {
			reg.UnregisterClient(auth.ID)
		}
	})

	opts := cliproxyexecutor.Options{OriginalRequest: []byte(`{"metadata":{"user_id":"user_xxx_account__session_` + uuid.NewString() + `"}}`)}
	req := cliproxyexecutor.Request{Model: "test-model"}
	resp, errExecute := m.ExecuteCount(context.Background(), []string{"claude"}, req, opts)
	if errExecute != nil {
		t.Fatalf("first ExecuteCount() error = %v", errExecute)
	}
	if string(resp.Payload) != authB {
		t.Fatalf("first ExecuteCount() payload = %q, want %q", string(resp.Payload), authB)
	}

	resp, errExecute = m.ExecuteCount(context.Background(), []string{"claude"}, req, opts)
	if errExecute != nil {
		t.Fatalf("second ExecuteCount() error = %v", errExecute)
	}
	if string(resp.Payload) != authB {
		t.Fatalf("second ExecuteCount() payload = %q, want %q", string(resp.Payload), authB)
	}
	if got := executor.CountCalls(); len(got) != 3 || got[0] != authA || got[1] != authB || got[2] != authB {
		t.Fatalf("count calls = %v, want [%s %s %s]", got, authA, authB, authB)
	}
}

func TestManagerExecuteCount_StrictSessionAffinityDoesNotEnterNextRequestRound(t *testing.T) {
	failover := false
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback: &RoundRobinSelector{},
		TTL:      time.Minute,
		Failover: &failover,
	})
	t.Cleanup(selector.Stop)
	m := NewManager(nil, selector, nil)
	m.SetConfig(&internalconfig.Config{
		NoCooldownStatusCodes: []int{http.StatusInternalServerError},
		Routing: internalconfig.RoutingConfig{
			SessionAffinity:         true,
			SessionAffinityFailover: &failover,
		},
	})
	m.SetRetryConfig(1, 0, 1)

	authID := "auth-strict-count-" + uuid.NewString()
	executor := &authFallbackExecutor{
		id:          "claude",
		countErrors: map[string]error{authID: &Error{HTTPStatus: http.StatusInternalServerError, Message: "boom"}},
	}
	m.RegisterExecutor(executor)

	auth := &Auth{ID: authID, Provider: "claude"}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	opts := cliproxyexecutor.Options{OriginalRequest: []byte(`{"metadata":{"user_id":"user_xxx_account__session_` + uuid.NewString() + `"}}`)}
	_, errExecute := m.ExecuteCount(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: "test-model"}, opts)
	if errExecute == nil {
		t.Fatal("ExecuteCount() error = nil, want strict session auth failure")
	}
	if got := executor.CountCalls(); len(got) != 1 {
		t.Fatalf("count calls = %v, want one call without request-round retry", got)
	}
}

func TestManagerExecuteStream_SessionAffinityBindsSuccessfulAuthAfterFailover(t *testing.T) {
	failover := true
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback: &RoundRobinSelector{},
		TTL:      time.Minute,
		Failover: &failover,
	})
	t.Cleanup(selector.Stop)
	m := NewManager(nil, selector, nil)
	m.SetConfig(&internalconfig.Config{
		NoCooldownStatusCodes: []int{http.StatusInternalServerError},
		Routing: internalconfig.RoutingConfig{
			SessionAffinity:         true,
			SessionAffinityFailover: &failover,
		},
	})
	m.SetRetryConfig(0, 0, 2)

	baseID := uuid.NewString()
	authA := baseID + "-auth-a"
	authB := baseID + "-auth-b"
	executor := &authFallbackExecutor{
		id:                "claude",
		streamFirstErrors: map[string]error{authA: &Error{HTTPStatus: http.StatusInternalServerError, Message: "boom"}},
	}
	m.RegisterExecutor(executor)

	auths := []*Auth{
		{ID: authA, Provider: "claude"},
		{ID: authB, Provider: "claude"},
	}
	reg := registry.GetGlobalRegistry()
	for _, auth := range auths {
		reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "test-model"}})
		if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register auth %q: %v", auth.ID, errRegister)
		}
	}
	t.Cleanup(func() {
		for _, auth := range auths {
			reg.UnregisterClient(auth.ID)
		}
	})

	drain := func(result *cliproxyexecutor.StreamResult) (string, error) {
		var payload []byte
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				return "", chunk.Err
			}
			payload = append(payload, chunk.Payload...)
		}
		return string(payload), nil
	}

	opts := cliproxyexecutor.Options{OriginalRequest: []byte(`{"metadata":{"user_id":"user_xxx_account__session_` + uuid.NewString() + `"}}`)}
	req := cliproxyexecutor.Request{Model: "test-model"}
	result, errExecute := m.ExecuteStream(context.Background(), []string{"claude"}, req, opts)
	if errExecute != nil {
		t.Fatalf("first ExecuteStream() error = %v", errExecute)
	}
	payload, errDrain := drain(result)
	if errDrain != nil {
		t.Fatalf("first stream error = %v", errDrain)
	}
	if payload != authB {
		t.Fatalf("first ExecuteStream() payload = %q, want %q", payload, authB)
	}

	result, errExecute = m.ExecuteStream(context.Background(), []string{"claude"}, req, opts)
	if errExecute != nil {
		t.Fatalf("second ExecuteStream() error = %v", errExecute)
	}
	payload, errDrain = drain(result)
	if errDrain != nil {
		t.Fatalf("second stream error = %v", errDrain)
	}
	if payload != authB {
		t.Fatalf("second ExecuteStream() payload = %q, want %q", payload, authB)
	}
	if got := executor.StreamCalls(); len(got) != 3 || got[0] != authA || got[1] != authB || got[2] != authB {
		t.Fatalf("stream calls = %v, want [%s %s %s]", got, authA, authB, authB)
	}
}

func TestManagerExecute_RandomRetryDoesNotDropPriorityWhenAuthHasNoExecutableModels(t *testing.T) {
	m := NewManager(nil, &RandomSelector{rnd: rand.New(rand.NewPCG(1, 1))}, nil)
	m.SetRetryConfig(0, 0, 2)
	m.SetOAuthModelAlias(map[string][]internalconfig.OAuthModelAlias{
		"claude": {
			{Name: "upstream-model", Alias: "route-model"},
		},
	})

	executor := &authFallbackExecutor{id: "claude"}
	m.RegisterExecutor(executor)

	blockedUntil := time.Now().Add(5 * time.Minute)
	auths := []*Auth{
		{
			ID:       "high-empty",
			Provider: "claude",
			Attributes: map[string]string{
				"priority": "10",
			},
			ModelStates: map[string]*ModelState{
				"upstream-model": {
					Unavailable:    true,
					Status:         StatusError,
					NextRetryAfter: blockedUntil,
				},
			},
		},
		{
			ID:       "high-good",
			Provider: "claude",
			Attributes: map[string]string{
				"priority": "10",
			},
		},
		{
			ID:       "low",
			Provider: "claude",
			Attributes: map[string]string{
				"priority": "0",
			},
		},
	}
	reg := registry.GetGlobalRegistry()
	for _, auth := range auths {
		if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register auth %q: %v", auth.ID, errRegister)
		}
		reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: "upstream-model"}})
	}
	t.Cleanup(func() {
		for _, auth := range auths {
			reg.UnregisterClient(auth.ID)
		}
	})

	resp, errExecute := m.Execute(context.Background(), []string{"claude"}, cliproxyexecutor.Request{Model: "route-model"}, cliproxyexecutor.Options{})
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if string(resp.Payload) != "high-good" {
		t.Fatalf("Execute() payload = %q, want %q", string(resp.Payload), "high-good")
	}

	got := executor.ExecuteCalls()
	want := []string{"high-good"}
	if len(got) != len(want) {
		t.Fatalf("execute calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("execute call %d = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestManager_MarkResult_RespectsAuthDisableCoolingOverride(t *testing.T) {
	prev := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(prev) })

	m := NewManager(nil, nil, nil)

	auth := &Auth{
		ID:       "auth-1",
		Provider: "claude",
		Metadata: map[string]any{
			"disable_cooling": true,
		},
	}
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	model := "test-model"
	m.MarkResult(context.Background(), Result{
		AuthID:   "auth-1",
		Provider: "claude",
		Model:    model,
		Success:  false,
		Error:    &Error{HTTPStatus: 500, Message: "boom"},
	})

	updated, ok := m.GetByID("auth-1")
	if !ok || updated == nil {
		t.Fatalf("expected auth to be present")
	}
	state := updated.ModelStates[model]
	if state == nil {
		t.Fatalf("expected model state to be present")
	}
	if !state.NextRetryAfter.IsZero() {
		t.Fatalf("expected NextRetryAfter to be zero when disable_cooling=true, got %v", state.NextRetryAfter)
	}
}

func TestManager_MarkResult_RespectsAuthDisableCoolingOverride_On403(t *testing.T) {
	prev := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(prev) })

	m := NewManager(nil, nil, nil)

	auth := &Auth{
		ID:       "auth-403",
		Provider: "claude",
		Metadata: map[string]any{
			"disable_cooling": true,
		},
	}
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	model := "test-model-403"
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })

	m.MarkResult(context.Background(), Result{
		AuthID:   auth.ID,
		Provider: "claude",
		Model:    model,
		Success:  false,
		Error:    &Error{HTTPStatus: http.StatusForbidden, Message: "forbidden"},
	})

	updated, ok := m.GetByID(auth.ID)
	if !ok || updated == nil {
		t.Fatalf("expected auth to be present")
	}
	state := updated.ModelStates[model]
	if state == nil {
		t.Fatalf("expected model state to be present")
	}
	if !state.NextRetryAfter.IsZero() {
		t.Fatalf("expected NextRetryAfter to be zero when disable_cooling=true, got %v", state.NextRetryAfter)
	}

	if count := reg.GetModelCount(model); count <= 0 {
		t.Fatalf("expected model count > 0 when disable_cooling=true, got %d", count)
	}
}

func TestManager_MarkResult_NoCooldownStatusCodeSkipsModelCooldown(t *testing.T) {
	prev := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(prev) })

	m := NewManager(nil, nil, nil)
	m.SetConfig(&internalconfig.Config{NoCooldownStatusCodes: []int{http.StatusUnauthorized}})

	auth := &Auth{
		ID:       "auth-no-cooldown-401",
		Provider: "claude",
	}
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	model := "test-model-no-cooldown-401"
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })

	m.MarkResult(context.Background(), Result{
		AuthID:   auth.ID,
		Provider: "claude",
		Model:    model,
		Success:  false,
		Error:    &Error{HTTPStatus: http.StatusUnauthorized, Message: "unauthorized"},
	})

	updated, ok := m.GetByID(auth.ID)
	if !ok || updated == nil {
		t.Fatalf("expected auth to be present")
	}
	if updated.Unavailable {
		t.Fatal("expected auth to remain available")
	}
	state := updated.ModelStates[model]
	if state == nil {
		t.Fatalf("expected model state to be present")
	}
	if state.Unavailable {
		t.Fatal("expected model state to remain available")
	}
	if !state.NextRetryAfter.IsZero() {
		t.Fatalf("expected NextRetryAfter to be zero, got %v", state.NextRetryAfter)
	}
	if state.Quota.Exceeded {
		t.Fatalf("expected quota state to remain clear, got %#v", state.Quota)
	}
	if state.LastError == nil {
		t.Fatal("expected LastError to be recorded")
	}
	if count := reg.GetModelCount(model); count <= 0 {
		t.Fatalf("expected model count > 0 when cooldown is skipped, got %d", count)
	}
}

func TestManager_MarkResult_NoCooldownStatusCodeSkipsAuthCooldown(t *testing.T) {
	prev := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(prev) })

	m := NewManager(nil, nil, nil)
	m.SetConfig(&internalconfig.Config{NoCooldownStatusCodes: []int{http.StatusTooManyRequests}})

	auth := &Auth{
		ID:       "auth-no-cooldown-429",
		Provider: "claude",
	}
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	retryAfter := 2 * time.Minute
	m.MarkResult(context.Background(), Result{
		AuthID:     auth.ID,
		Provider:   "claude",
		Success:    false,
		RetryAfter: &retryAfter,
		Error:      &Error{HTTPStatus: http.StatusTooManyRequests, Message: "quota exhausted"},
	})

	updated, ok := m.GetByID(auth.ID)
	if !ok || updated == nil {
		t.Fatalf("expected auth to be present")
	}
	if updated.Unavailable {
		t.Fatal("expected auth to remain available")
	}
	if !updated.NextRetryAfter.IsZero() {
		t.Fatalf("expected NextRetryAfter to be zero, got %v", updated.NextRetryAfter)
	}
	if updated.Quota.Exceeded || updated.Quota.StrikeCount != 0 {
		t.Fatalf("expected quota state to remain clear, got %#v", updated.Quota)
	}
	if updated.LastError == nil {
		t.Fatal("expected LastError to be recorded")
	}
}

func TestManager_Execute_DisableCooling_DoesNotBlackoutAfter403(t *testing.T) {
	prev := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(prev) })

	m := NewManager(nil, nil, nil)
	executor := &authFallbackExecutor{
		id: "claude",
		executeErrors: map[string]error{
			"auth-403-exec": &Error{
				HTTPStatus: http.StatusForbidden,
				Message:    "forbidden",
			},
		},
	}
	m.RegisterExecutor(executor)

	auth := &Auth{
		ID:       "auth-403-exec",
		Provider: "claude",
		Metadata: map[string]any{
			"disable_cooling": true,
		},
	}
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	model := "test-model-403-exec"
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })

	req := cliproxyexecutor.Request{Model: model}
	_, errExecute1 := m.Execute(context.Background(), []string{"claude"}, req, cliproxyexecutor.Options{})
	if errExecute1 == nil {
		t.Fatal("expected first execute error")
	}
	if statusCodeFromError(errExecute1) != http.StatusForbidden {
		t.Fatalf("first execute status = %d, want %d", statusCodeFromError(errExecute1), http.StatusForbidden)
	}

	_, errExecute2 := m.Execute(context.Background(), []string{"claude"}, req, cliproxyexecutor.Options{})
	if errExecute2 == nil {
		t.Fatal("expected second execute error")
	}
	if statusCodeFromError(errExecute2) != http.StatusForbidden {
		t.Fatalf("second execute status = %d, want %d", statusCodeFromError(errExecute2), http.StatusForbidden)
	}
}

func TestManager_Execute_DisableCooling_DoesNotBlackoutAfter429RetryAfter(t *testing.T) {
	prev := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(prev) })

	m := NewManager(nil, nil, nil)
	executor := &authFallbackExecutor{
		id: "claude",
		executeErrors: map[string]error{
			"auth-429-exec": &retryAfterStatusError{
				status:     http.StatusTooManyRequests,
				message:    "quota exhausted",
				retryAfter: 2 * time.Minute,
			},
		},
	}
	m.RegisterExecutor(executor)

	auth := &Auth{
		ID:       "auth-429-exec",
		Provider: "claude",
		Metadata: map[string]any{
			"disable_cooling": true,
		},
	}
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	model := "test-model-429-exec"
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })

	req := cliproxyexecutor.Request{Model: model}
	_, errExecute1 := m.Execute(context.Background(), []string{"claude"}, req, cliproxyexecutor.Options{})
	if errExecute1 == nil {
		t.Fatal("expected first execute error")
	}
	if statusCodeFromError(errExecute1) != http.StatusTooManyRequests {
		t.Fatalf("first execute status = %d, want %d", statusCodeFromError(errExecute1), http.StatusTooManyRequests)
	}

	_, errExecute2 := m.Execute(context.Background(), []string{"claude"}, req, cliproxyexecutor.Options{})
	if errExecute2 == nil {
		t.Fatal("expected second execute error")
	}
	if statusCodeFromError(errExecute2) != http.StatusTooManyRequests {
		t.Fatalf("second execute status = %d, want %d", statusCodeFromError(errExecute2), http.StatusTooManyRequests)
	}

	calls := executor.ExecuteCalls()
	if len(calls) != 2 {
		t.Fatalf("execute calls = %d, want 2", len(calls))
	}

	updated, ok := m.GetByID(auth.ID)
	if !ok || updated == nil {
		t.Fatalf("expected auth to be present")
	}
	state := updated.ModelStates[model]
	if state == nil {
		t.Fatalf("expected model state to be present")
	}
	if !state.NextRetryAfter.IsZero() {
		t.Fatalf("expected NextRetryAfter to be zero when disable_cooling=true, got %v", state.NextRetryAfter)
	}
}

func TestManager_Execute_DisableCooling_RetriesAfter429RetryAfter(t *testing.T) {
	prev := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(prev) })

	m := NewManager(nil, nil, nil)
	m.SetRetryConfig(3, 100*time.Millisecond, 0)

	executor := &authFallbackExecutor{
		id: "claude",
		executeErrors: map[string]error{
			"auth-429-retryafter-exec": &retryAfterStatusError{
				status:     http.StatusTooManyRequests,
				message:    "quota exhausted",
				retryAfter: 5 * time.Millisecond,
			},
		},
	}
	m.RegisterExecutor(executor)

	auth := &Auth{
		ID:       "auth-429-retryafter-exec",
		Provider: "claude",
		Metadata: map[string]any{
			"disable_cooling": true,
		},
	}
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	model := "test-model-429-retryafter-exec"
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, "claude", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })

	req := cliproxyexecutor.Request{Model: model}
	_, errExecute := m.Execute(context.Background(), []string{"claude"}, req, cliproxyexecutor.Options{})
	if errExecute == nil {
		t.Fatal("expected execute error")
	}
	if statusCodeFromError(errExecute) != http.StatusTooManyRequests {
		t.Fatalf("execute status = %d, want %d", statusCodeFromError(errExecute), http.StatusTooManyRequests)
	}

	calls := executor.ExecuteCalls()
	if len(calls) != 4 {
		t.Fatalf("execute calls = %d, want 4 (initial + 3 retries)", len(calls))
	}
}

func TestManager_MarkResult_RequestScopedNotFoundDoesNotCooldownAuth(t *testing.T) {
	m := NewManager(nil, nil, nil)

	auth := &Auth{
		ID:       "auth-1",
		Provider: "openai",
	}
	if _, errRegister := m.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	model := "gpt-4.1"
	m.MarkResult(context.Background(), Result{
		AuthID:   auth.ID,
		Provider: auth.Provider,
		Model:    model,
		Success:  false,
		Error: &Error{
			HTTPStatus: http.StatusNotFound,
			Message:    requestScopedNotFoundMessage,
		},
	})

	updated, ok := m.GetByID(auth.ID)
	if !ok || updated == nil {
		t.Fatalf("expected auth to be present")
	}
	if updated.Unavailable {
		t.Fatalf("expected request-scoped 404 to keep auth available")
	}
	if !updated.NextRetryAfter.IsZero() {
		t.Fatalf("expected request-scoped 404 to keep auth cooldown unset, got %v", updated.NextRetryAfter)
	}
	if state := updated.ModelStates[model]; state != nil {
		t.Fatalf("expected request-scoped 404 to avoid model cooldown state, got %#v", state)
	}
}

func TestManager_RequestScopedNotFoundStopsRetryWithoutSuspendingAuth(t *testing.T) {
	m := NewManager(nil, nil, nil)
	executor := &authFallbackExecutor{
		id: "openai",
		executeErrors: map[string]error{
			"aa-bad-auth": &Error{
				HTTPStatus: http.StatusNotFound,
				Message:    requestScopedNotFoundMessage,
			},
		},
	}
	m.RegisterExecutor(executor)

	model := "gpt-4.1"
	badAuth := &Auth{ID: "aa-bad-auth", Provider: "openai"}
	goodAuth := &Auth{ID: "bb-good-auth", Provider: "openai"}

	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(badAuth.ID, "openai", []*registry.ModelInfo{{ID: model}})
	reg.RegisterClient(goodAuth.ID, "openai", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() {
		reg.UnregisterClient(badAuth.ID)
		reg.UnregisterClient(goodAuth.ID)
	})

	if _, errRegister := m.Register(context.Background(), badAuth); errRegister != nil {
		t.Fatalf("register bad auth: %v", errRegister)
	}
	if _, errRegister := m.Register(context.Background(), goodAuth); errRegister != nil {
		t.Fatalf("register good auth: %v", errRegister)
	}

	_, errExecute := m.Execute(context.Background(), []string{"openai"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errExecute == nil {
		t.Fatal("expected request-scoped not-found error")
	}
	errResult, ok := errExecute.(*Error)
	if !ok {
		t.Fatalf("expected *Error, got %T", errExecute)
	}
	if errResult.HTTPStatus != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", errResult.HTTPStatus, http.StatusNotFound)
	}
	if errResult.Message != requestScopedNotFoundMessage {
		t.Fatalf("message = %q, want %q", errResult.Message, requestScopedNotFoundMessage)
	}

	got := executor.ExecuteCalls()
	want := []string{badAuth.ID}
	if len(got) != len(want) {
		t.Fatalf("execute calls = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("execute call %d auth = %q, want %q", i, got[i], want[i])
		}
	}

	updatedBad, ok := m.GetByID(badAuth.ID)
	if !ok || updatedBad == nil {
		t.Fatalf("expected bad auth to remain registered")
	}
	if updatedBad.Unavailable {
		t.Fatalf("expected request-scoped 404 to keep bad auth available")
	}
	if !updatedBad.NextRetryAfter.IsZero() {
		t.Fatalf("expected request-scoped 404 to keep bad auth cooldown unset, got %v", updatedBad.NextRetryAfter)
	}
	if state := updatedBad.ModelStates[model]; state != nil {
		t.Fatalf("expected request-scoped 404 to avoid bad auth model cooldown state, got %#v", state)
	}
}
