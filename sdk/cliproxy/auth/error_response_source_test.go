package auth

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type errorResponseSourceTestExecutor struct {
	id                  string
	executeErr          error
	streamErr           error
	streamPayloadBefore bool
	preparedPriority    *int
}

type cancelingErrorResponseSourceTestExecutor struct {
	id      string
	started chan<- struct{}
}

func (e *cancelingErrorResponseSourceTestExecutor) Identifier() string {
	return e.id
}

func (e *cancelingErrorResponseSourceTestExecutor) waitForCancellation(ctx context.Context) error {
	select {
	case e.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

func (e *cancelingErrorResponseSourceTestExecutor) Execute(ctx context.Context, _ *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, e.waitForCancellation(ctx)
}

func (e *cancelingErrorResponseSourceTestExecutor) ExecuteStream(ctx context.Context, _ *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, e.waitForCancellation(ctx)
}

func (e *cancelingErrorResponseSourceTestExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (e *cancelingErrorResponseSourceTestExecutor) CountTokens(ctx context.Context, _ *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, e.waitForCancellation(ctx)
}

func (e *cancelingErrorResponseSourceTestExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (e *errorResponseSourceTestExecutor) Identifier() string {
	return e.id
}

func (e *errorResponseSourceTestExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, e.executeErr
}

func (e *errorResponseSourceTestExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	chunks := make(chan cliproxyexecutor.StreamChunk, 2)
	if e.streamPayloadBefore {
		chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("data")}
	}
	if e.streamErr != nil {
		chunks <- cliproxyexecutor.StreamChunk{Err: e.streamErr}
	}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *errorResponseSourceTestExecutor) ShouldPrepareRequestAuth(*Auth) bool {
	return e.preparedPriority != nil
}

func (e *errorResponseSourceTestExecutor) PrepareRequestAuth(_ context.Context, auth *Auth) (*Auth, error) {
	prepared := auth.Clone()
	if prepared.Attributes == nil {
		prepared.Attributes = make(map[string]string)
	}
	prepared.Attributes["priority"] = strconv.Itoa(*e.preparedPriority)
	return prepared, nil
}

func (e *errorResponseSourceTestExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (e *errorResponseSourceTestExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, e.executeErr
}

func (e *errorResponseSourceTestExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

type errorResponseSourceRecorder struct {
	mu     sync.Mutex
	source cliproxyexecutor.ErrorResponseSourceSnapshot
}

func (r *errorResponseSourceRecorder) store(source cliproxyexecutor.ErrorResponseSourceSnapshot) {
	r.mu.Lock()
	r.source = source
	r.mu.Unlock()
}

func (r *errorResponseSourceRecorder) load() cliproxyexecutor.ErrorResponseSourceSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.source
}

func TestManagerPublishesCredentialErrorSourceWithoutChangingErrorType(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	original := &Error{HTTPStatus: http.StatusBadRequest, Message: "invalid request"}
	manager.RegisterExecutor(&errorResponseSourceTestExecutor{id: "source-test", executeErr: original})
	registerErrorResponseSourceTestAuth(t, manager)

	recorder := &errorResponseSourceRecorder{}
	_, errExecute := manager.Execute(t.Context(), []string{"source-test"}, cliproxyexecutor.Request{Model: "source-test-model"}, cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.SelectedAuthSourceCallbackMetadataKey: recorder.store,
		},
	})
	if errExecute != original {
		t.Fatalf("Execute() error = %T %v, want original *Error", errExecute, errExecute)
	}
	assertCredentialErrorResponseSource(t, recorder.load())
}

func TestManagerPublishesFinalCredentialSourceAfterFallback(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(1, 0, 1)
	manager.SetConfig(&internalconfig.Config{NoCooldownStatusCodes: []int{http.StatusInternalServerError}})
	highErr := &Error{HTTPStatus: http.StatusInternalServerError, Message: "high failed"}
	lowErr := &Error{HTTPStatus: http.StatusInternalServerError, Message: "low failed"}
	executor := &authFallbackExecutor{
		id: "source-fallback-test",
		executeErrors: map[string]error{
			"source-high": highErr,
			"source-low":  lowErr,
		},
	}
	manager.RegisterExecutor(executor)
	for _, auth := range []*Auth{
		{ID: "source-high", Provider: "source-fallback-test", Status: StatusActive, Attributes: map[string]string{"priority": "10"}},
		{ID: "source-low", Provider: "source-fallback-test", Status: StatusActive, Attributes: map[string]string{"priority": "-2"}},
	} {
		if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
			t.Fatalf("register auth %s: %v", auth.ID, errRegister)
		}
	}

	recorder := &errorResponseSourceRecorder{}
	_, errExecute := manager.Execute(t.Context(), []string{"source-fallback-test"}, cliproxyexecutor.Request{}, cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.SelectedAuthSourceCallbackMetadataKey: recorder.store,
		},
	})
	if errExecute != lowErr {
		t.Fatalf("Execute() error = %T %v, want final low-priority error", errExecute, errExecute)
	}
	assertCredentialErrorResponseSourceFor(t, recorder.load(), "source-fallback-test", -2)
	if calls := executor.ExecuteCalls(); len(calls) < 2 || calls[0] != "source-high" || calls[len(calls)-1] != "source-low" {
		t.Fatalf("execute calls = %v, want source-high first and source-low last", calls)
	}
}

func TestManagerPublishesTerminalStreamErrorSourceWithoutChangingChunkError(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	original := &Error{HTTPStatus: http.StatusBadGateway, Message: "terminal stream failure"}
	manager.RegisterExecutor(&errorResponseSourceTestExecutor{id: "source-test", streamErr: original, streamPayloadBefore: true})
	registerErrorResponseSourceTestAuth(t, manager)

	recorder := &errorResponseSourceRecorder{}
	result, errExecute := manager.ExecuteStream(t.Context(), []string{"source-test"}, cliproxyexecutor.Request{Model: "source-test-model"}, cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.SelectedAuthSourceCallbackMetadataKey: recorder.store,
		},
	})
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	var terminalErr error
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			terminalErr = chunk.Err
		}
	}
	if terminalErr != original {
		t.Fatalf("terminal error = %T %v, want original *Error", terminalErr, terminalErr)
	}
	assertCredentialErrorResponseSource(t, recorder.load())
}

func TestManagerPublishesBootstrapStreamErrorSourceWithoutChangingChunkError(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	original := &Error{HTTPStatus: http.StatusBadGateway, Message: "bootstrap stream failure"}
	manager.RegisterExecutor(&errorResponseSourceTestExecutor{id: "source-test", streamErr: original})
	registerErrorResponseSourceTestAuth(t, manager)

	recorder := &errorResponseSourceRecorder{}
	result, errExecute := manager.ExecuteStream(t.Context(), []string{"source-test"}, cliproxyexecutor.Request{Model: "source-test-model"}, cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.SelectedAuthSourceCallbackMetadataKey: recorder.store,
		},
	})
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	chunk, ok := <-result.Chunks
	if !ok || chunk.Err != original {
		t.Fatalf("bootstrap chunk error = %T %v, want original *Error", chunk.Err, chunk.Err)
	}
	assertCredentialErrorResponseSource(t, recorder.load())
}

func TestManagerKeepsSelectedCredentialSourceWhenExecutionIsCanceled(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *Manager, cliproxyexecutor.Options) error
	}{
		{
			name: "execute",
			run: func(ctx context.Context, manager *Manager, opts cliproxyexecutor.Options) error {
				_, errExecute := manager.Execute(ctx, []string{"source-test"}, cliproxyexecutor.Request{Model: "source-test-model"}, opts)
				return errExecute
			},
		},
		{
			name: "count",
			run: func(ctx context.Context, manager *Manager, opts cliproxyexecutor.Options) error {
				_, errCount := manager.ExecuteCount(ctx, []string{"source-test"}, cliproxyexecutor.Request{Model: "source-test-model"}, opts)
				return errCount
			},
		},
		{
			name: "stream",
			run: func(ctx context.Context, manager *Manager, opts cliproxyexecutor.Options) error {
				_, errStream := manager.ExecuteStream(ctx, []string{"source-test"}, cliproxyexecutor.Request{Model: "source-test-model"}, opts)
				return errStream
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			manager := NewManager(nil, &FillFirstSelector{}, nil)
			manager.SetRetryConfig(0, 0, 0)
			started := make(chan struct{}, 1)
			manager.RegisterExecutor(&cancelingErrorResponseSourceTestExecutor{id: "source-test", started: started})
			registerErrorResponseSourceTestAuth(t, manager)

			recorder := &errorResponseSourceRecorder{}
			opts := cliproxyexecutor.Options{Metadata: map[string]any{
				cliproxyexecutor.SelectedAuthSourceCallbackMetadataKey: recorder.store,
			}}
			ctx, cancel := context.WithCancel(t.Context())
			result := make(chan error, 1)
			go func() {
				result <- testCase.run(ctx, manager, opts)
			}()

			select {
			case <-started:
				cancel()
			case <-t.Context().Done():
				cancel()
				t.Fatal("executor did not start")
			}

			errExecute := <-result
			if !errors.Is(errExecute, context.Canceled) {
				t.Fatalf("execution error = %T %v, want context.Canceled", errExecute, errExecute)
			}
			assertCredentialErrorResponseSource(t, recorder.load())
		})
	}
}

func TestManagerPublishesPreparedCredentialPriorityOnSuccess(t *testing.T) {
	tests := []struct {
		name string
		run  func(context.Context, *Manager, cliproxyexecutor.Options) error
	}{
		{
			name: "execute",
			run: func(ctx context.Context, manager *Manager, opts cliproxyexecutor.Options) error {
				_, errExecute := manager.Execute(ctx, []string{"source-test"}, cliproxyexecutor.Request{Model: "source-test-model"}, opts)
				return errExecute
			},
		},
		{
			name: "count",
			run: func(ctx context.Context, manager *Manager, opts cliproxyexecutor.Options) error {
				_, errCount := manager.ExecuteCount(ctx, []string{"source-test"}, cliproxyexecutor.Request{Model: "source-test-model"}, opts)
				return errCount
			},
		},
		{
			name: "stream",
			run: func(ctx context.Context, manager *Manager, opts cliproxyexecutor.Options) error {
				result, errStream := manager.ExecuteStream(ctx, []string{"source-test"}, cliproxyexecutor.Request{Model: "source-test-model"}, opts)
				if errStream != nil {
					return errStream
				}
				for range result.Chunks {
				}
				return nil
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			manager := NewManager(nil, &FillFirstSelector{}, nil)
			manager.SetRetryConfig(0, 0, 0)
			preparedPriority := 7
			manager.RegisterExecutor(&errorResponseSourceTestExecutor{
				id:                  "source-test",
				streamPayloadBefore: true,
				preparedPriority:    &preparedPriority,
			})
			registerErrorResponseSourceTestAuth(t, manager)

			recorder := &errorResponseSourceRecorder{}
			errExecute := testCase.run(t.Context(), manager, cliproxyexecutor.Options{Metadata: map[string]any{
				cliproxyexecutor.SelectedAuthSourceCallbackMetadataKey: recorder.store,
			}})
			if errExecute != nil {
				t.Fatalf("execution error = %v", errExecute)
			}
			assertCredentialErrorResponseSourceFor(t, recorder.load(), "source-test", preparedPriority)
		})
	}
}

func TestWithInheritedErrorResponseSourcePreservesTriggeringCredential(t *testing.T) {
	sourceErr := cliproxyexecutor.WithErrorResponseSource(
		errors.New("upstream unavailable"),
		cliproxyexecutor.CredentialErrorResponseSource("codex", -1),
	)
	errWait := withInheritedErrorResponseSource(context.Canceled, sourceErr)
	source, ok := cliproxyexecutor.ErrorResponseSourceOf(errWait)
	if !ok {
		t.Fatal("inherited error source is missing")
	}
	assertCredentialErrorResponseSourceFor(t, source, "codex", -1)
	if !errors.Is(errWait, context.Canceled) {
		t.Fatalf("inherited error = %T %v, want context.Canceled", errWait, errWait)
	}
}

func TestFinalizeErrorResponseSourceDoesNotOverwriteSelectedCredential(t *testing.T) {
	recorder := &errorResponseSourceRecorder{}
	meta := map[string]any{
		cliproxyexecutor.SelectedAuthSourceCallbackMetadataKey: recorder.store,
	}
	selected := cliproxyexecutor.CredentialErrorResponseSource("chatgpt-web", 4)
	publishErrorResponseSourceMetadata(meta, selected)

	errFinal := finalizeErrorResponseSource(meta, context.Canceled)
	if !errors.Is(errFinal, context.Canceled) {
		t.Fatalf("final error = %T %v, want context.Canceled", errFinal, errFinal)
	}
	assertCredentialErrorResponseSourceFor(t, recorder.load(), "chatgpt-web", 4)
}

func registerErrorResponseSourceTestAuth(t *testing.T, manager *Manager) {
	t.Helper()
	auth := &Auth{
		ID:         "source-test-auth",
		Provider:   "source-test",
		Status:     StatusActive,
		Attributes: map[string]string{"priority": "-2"},
	}
	if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "source-test-model"}})
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })
}

func TestErrorResponseSourceForAuthUsesSchedulerPriorityDefaults(t *testing.T) {
	for _, testCase := range []struct {
		name     string
		auth     *Auth
		provider string
		priority int
		hasAuth  bool
	}{
		{name: "nil auth is local", provider: "local"},
		{name: "missing priority", auth: &Auth{Provider: "codex"}, provider: "codex", hasAuth: true},
		{name: "invalid priority", auth: &Auth{Provider: "codex", Attributes: map[string]string{"priority": "invalid"}}, provider: "codex", hasAuth: true},
		{name: "signed priority", auth: &Auth{Provider: "codex", Attributes: map[string]string{"priority": "-3"}}, provider: "codex", priority: -3, hasAuth: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			source := errorResponseSourceForAuth(testCase.auth, "fallback")
			if source.Provider != testCase.provider || source.AuthPriority != testCase.priority || source.HasAuthPriority != testCase.hasAuth {
				t.Fatalf("source = %#v", source)
			}
		})
	}
}

func assertCredentialErrorResponseSource(t *testing.T, source cliproxyexecutor.ErrorResponseSourceSnapshot) {
	t.Helper()
	assertCredentialErrorResponseSourceFor(t, source, "source-test", -2)
}

func assertCredentialErrorResponseSourceFor(t *testing.T, source cliproxyexecutor.ErrorResponseSourceSnapshot, provider string, priority int) {
	t.Helper()
	if source.Provider != provider || source.AuthPriority != priority || !source.HasAuthPriority {
		t.Fatalf("source = %#v, want provider %s priority %d", source, provider, priority)
	}
}
