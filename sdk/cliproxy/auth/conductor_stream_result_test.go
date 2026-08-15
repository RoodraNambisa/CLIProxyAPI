package auth

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type streamResultTestExecutor struct {
	id                    string
	unauthorizedThenEmpty bool

	mu         sync.Mutex
	calls      []string
	recoveries int
	results    map[string]*cliproxyexecutor.StreamResult
	errors     map[string]error
}

func (e *streamResultTestExecutor) Identifier() string { return e.id }

func (*streamResultTestExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (e *streamResultTestExecutor) ExecuteStream(_ context.Context, _ *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.mu.Lock()
	e.calls = append(e.calls, req.Model)
	callCount := len(e.calls)
	result := e.results[req.Model]
	err := e.errors[req.Model]
	e.mu.Unlock()

	if e.unauthorizedThenEmpty && callCount == 1 {
		return nil, &Error{HTTPStatus: http.StatusUnauthorized, Message: "expired credential"}
	}
	return result, err
}

func (*streamResultTestExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (*streamResultTestExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (*streamResultTestExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *streamResultTestExecutor) ShouldRecoverUnauthorized(_ *Auth, err error) bool {
	return e.unauthorizedThenEmpty && isUnauthorizedError(err)
}

func (e *streamResultTestExecutor) RecoverUnauthorized(_ context.Context, auth *Auth) (*Auth, error) {
	e.mu.Lock()
	e.recoveries++
	e.mu.Unlock()
	updated := auth.Clone()
	if updated.Metadata == nil {
		updated.Metadata = map[string]any{}
	}
	updated.Metadata["recovered"] = true
	return updated, nil
}

func (e *streamResultTestExecutor) snapshot() ([]string, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.calls...), e.recoveries
}

func successfulStreamResult(payload string) *cliproxyexecutor.StreamResult {
	chunks := make(chan cliproxyexecutor.StreamChunk, 2)
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte(payload)}
	chunks <- cliproxyexecutor.SuccessfulStreamTerminalChunk()
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}
}

func TestValidateStreamResult(t *testing.T) {
	originalErr := errors.New("upstream failure")
	valid := successfulStreamResult("ok")
	tests := []struct {
		name     string
		result   *cliproxyexecutor.StreamResult
		err      error
		wantCode string
		wantErr  error
	}{
		{name: "preserves executor error", err: originalErr, wantErr: originalErr},
		{name: "rejects nil result", wantCode: "empty_stream"},
		{name: "rejects nil chunks", result: &cliproxyexecutor.StreamResult{}, wantCode: "empty_stream"},
		{name: "accepts stream source", result: valid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := validateStreamResult(test.result, test.err)
			if result != test.result {
				t.Fatalf("result = %p, want %p", result, test.result)
			}
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("error = %v, want %v", err, test.wantErr)
				}
				return
			}
			if test.wantCode == "" {
				if err != nil {
					t.Fatalf("error = %v, want nil", err)
				}
				return
			}
			var authErr *Error
			if !errors.As(err, &authErr) || authErr.Code != test.wantCode || !authErr.Retryable {
				t.Fatalf("error = %#v, want retryable %s", err, test.wantCode)
			}
		})
	}
}

func TestExecuteStreamWithModelPoolRetriesAfterNilResult(t *testing.T) {
	executor := &streamResultTestExecutor{
		id:      "stream-result-model-pool",
		results: map[string]*cliproxyexecutor.StreamResult{"model-b": successfulStreamResult("ok")},
	}
	manager := NewManager(nil, nil, nil)
	auth := &Auth{ID: "stream-result-auth", Provider: executor.id, Status: StatusActive}

	result, err := manager.executeStreamWithModelPool(
		t.Context(),
		t.Context(),
		executor,
		auth,
		[]string{executor.id},
		executor.id,
		cliproxyexecutor.Request{Model: "route-model"},
		cliproxyexecutor.Options{},
		"route-model",
		[]string{"model-a", "model-b"},
		true,
		OAuthModelAliasResult{},
		func() bool { return false },
	)
	if err != nil {
		t.Fatalf("executeStreamWithModelPool() error = %v", err)
	}
	var payloads []string
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error = %v", chunk.Err)
		}
		if len(chunk.Payload) > 0 {
			payloads = append(payloads, string(chunk.Payload))
		}
	}
	if !reflect.DeepEqual(payloads, []string{"ok"}) {
		t.Fatalf("payloads = %v, want [ok]", payloads)
	}
	calls, _ := executor.snapshot()
	if !reflect.DeepEqual(calls, []string{"model-a", "model-b"}) {
		t.Fatalf("stream calls = %v, want [model-a model-b]", calls)
	}
}

func TestExecuteStreamRejectsEmptyResultAfterUnauthorizedRecovery(t *testing.T) {
	provider := "stream-result-recovery-" + uuid.NewString()
	model := "stream-result-model-" + uuid.NewString()
	authID := "stream-result-auth-" + uuid.NewString()
	executor := &streamResultTestExecutor{id: provider, unauthorizedThenEmpty: true}
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(executor)
	if _, err := manager.Register(t.Context(), &Auth{
		ID:       authID,
		Provider: provider,
		Status:   StatusActive,
		Metadata: map[string]any{"access_token": "stale"},
	}); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, provider, []*registry.ModelInfo{{ID: model}})
	manager.RefreshSchedulerEntry(authID)
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	_, err := manager.ExecuteStream(t.Context(), []string{provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	var authErr *Error
	if !errors.As(err, &authErr) || authErr.Code != "empty_stream" {
		t.Fatalf("ExecuteStream() error = %#v, want empty_stream", err)
	}
	calls, recoveries := executor.snapshot()
	if !reflect.DeepEqual(calls, []string{model, model}) || recoveries != 1 {
		t.Fatalf("stream calls = %v, recoveries = %d, want two calls and one recovery", calls, recoveries)
	}
}
