package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type chatGPTWebRetryTestExecutor struct {
	*chatGPTWebUnauthorizedRefreshExecutor
	rounds    []int
	bootstrap bool
	before    func()
}

func (e *chatGPTWebRetryTestExecutor) record(opts core.Options) {
	e.rounds = append(e.rounds, selectionAttemptFromMetadata(opts.Metadata))
	if e.before != nil {
		e.before()
	}
}

func (e *chatGPTWebRetryTestExecutor) Execute(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (core.Response, error) {
	e.record(opts)
	return e.chatGPTWebUnauthorizedRefreshExecutor.Execute(ctx, auth, req, opts)
}

func (e *chatGPTWebRetryTestExecutor) CountTokens(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (core.Response, error) {
	e.record(opts)
	return e.chatGPTWebUnauthorizedRefreshExecutor.CountTokens(ctx, auth, req, opts)
}

func (e *chatGPTWebRetryTestExecutor) ExecuteStream(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	e.record(opts)
	result, err := e.chatGPTWebUnauthorizedRefreshExecutor.ExecuteStream(ctx, auth, req, opts)
	if err != nil && e.bootstrap {
		chunks := make(chan core.StreamChunk, 1)
		chunks <- core.StreamChunk{Err: err}
		close(chunks)
		return &core.StreamResult{Headers: http.Header{"X-Fixture": {"unauthorized"}}, Chunks: chunks}, nil
	}
	return result, err
}

func runChatGPTWebRetryOperation(ctx context.Context, manager *Manager, mode, model string, opts core.Options) error {
	req := core.Request{Model: model}
	switch mode {
	case "execute":
		_, err := manager.Execute(ctx, []string{"chatgpt-web"}, req, opts)
		return err
	case "count":
		_, err := manager.ExecuteCount(ctx, []string{"chatgpt-web"}, req, opts)
		return err
	default:
		stream, err := manager.ExecuteStream(ctx, []string{"chatgpt-web"}, req, opts)
		if stream != nil {
			for chunk := range stream.Chunks {
				if chunk.Err != nil {
					err = chunk.Err
				}
			}
		}
		return err
	}
}

func blockChatGPTWebRetryRefresh(t *testing.T, executor *chatGPTWebUnauthorizedRefreshExecutor) <-chan struct{} {
	t.Helper()
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	executor.beforeRefresh = func() {
		once.Do(func() { close(started) })
		<-release
	}
	t.Cleanup(func() { close(release) })
	return started
}

func TestChatGPTWebUnauthorizedRetriesWithoutWaitingForRecovery(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream", "stream-bootstrap"} {
		for _, tc := range []struct {
			name             string
			rounds, maxAuths int
			wantRounds       []int
		}{
			{"same-round", 0, 2, []int{0, 0}},
			{"unlimited-credentials", 0, 0, []int{0, 0}},
			{"additional-round", 1, 1, []int{0, 1}},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				manager, base, primary, backup, model := newChatGPTWebUnauthorizedRefreshFixture(t)
				manager.SetRetryConfig(tc.rounds, 0, tc.maxAuths)
				executor := &chatGPTWebRetryTestExecutor{chatGPTWebUnauthorizedRefreshExecutor: base, bootstrap: mode == "stream-bootstrap"}
				manager.RegisterExecutor(executor)
				started := blockChatGPTWebRetryRefresh(t, base)
				done := make(chan error, 1)
				go func() { done <- runChatGPTWebRetryOperation(t.Context(), manager, mode, model, core.Options{}) }()
				select {
				case err := <-done:
					if err != nil {
						t.Fatalf("request did not fail over: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("request waited for credential recovery")
				}
				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("credential recovery did not start")
				}
				calls := append(append(base.executeCalls, base.countCalls...), base.streamCalls...)
				if !reflect.DeepEqual(calls, []string{primary.ID, backup.ID}) || !reflect.DeepEqual(executor.rounds, tc.wantRounds) {
					t.Fatalf("calls=%v rounds=%v, want primary/backup in %v", calls, executor.rounds, tc.wantRounds)
				}
				if got := chatGPTWebRequestRefreshBlockCount(manager, primary.ID); got != 1 {
					t.Fatalf("failed credential recovery blocks=%d, want 1", got)
				}
			})
		}
	}
}

func TestChatGPTWebUnauthorizedPreparationAllowsConfiguredRetry(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		for _, extraRound := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/extra-round=%t", mode, extraRound), func(t *testing.T) {
				manager, executor, _, backup, model := newChatGPTWebUnauthorizedRefreshFixture(t)
				manager.SetRetryConfig(0, 0, 2)
				if extraRound {
					manager.SetRetryConfig(1, 0, 1)
				}
				executor.prepareErr = &Error{HTTPStatus: http.StatusUnauthorized, Message: "expired access token"}
				blockChatGPTWebRetryRefresh(t, executor)
				if err := runChatGPTWebRetryOperation(t.Context(), manager, mode, model, core.Options{}); err != nil {
					t.Fatalf("preparation failure prevented retry: %v", err)
				}
				calls := append(append(executor.executeCalls, executor.countCalls...), executor.streamCalls...)
				if executor.prepareCalls != 2 || !reflect.DeepEqual(calls, []string{backup.ID}) {
					t.Fatalf("prepare=%d calls=%v, want two preparations and backup only", executor.prepareCalls, calls)
				}
			})
		}
	}
}

func TestChatGPTWebUnauthorizedPreparationKeepsSingleAttempt(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		t.Run(mode, func(t *testing.T) {
			manager, executor, _, _, model := newChatGPTWebUnauthorizedRefreshFixture(t)
			manager.SetRetryConfig(2, 0, 2)
			executor.prepareErr = &Error{HTTPStatus: 401, Message: "expired access token"}
			err := runChatGPTWebRetryOperation(core.WithSingleAttempt(t.Context()), manager, mode, model, core.Options{})
			if statusCodeFromError(err) != 401 || executor.prepareCalls != 1 || executor.refreshCalls != 0 ||
				len(executor.executeCalls)+len(executor.countCalls)+len(executor.streamCalls) != 0 {
				t.Fatalf("single attempt replayed preparation: err=%v prepares=%d refreshes=%d", err, executor.prepareCalls, executor.refreshCalls)
			}
		})
	}
}

func TestChatGPTWebUnauthorizedRetryPreservesStopBoundaries(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream", "stream-bootstrap"} {
		for _, boundary := range []string{"limit", "non-retryable", "stop", "stop-and-cooldown", "single", "pinned", "strict-affinity", "committed", "canceled", "released", "credential-round-limit"} {
			t.Run(mode+"/"+boundary, func(t *testing.T) {
				manager, base, primary, backup, model := newChatGPTWebUnauthorizedRefreshFixture(t)
				manager.SetRetryConfig(2, 0, 2)
				executor := &chatGPTWebRetryTestExecutor{chatGPTWebUnauthorizedRefreshExecutor: base, bootstrap: mode == "stream-bootstrap"}
				manager.RegisterExecutor(executor)
				blockChatGPTWebRetryRefresh(t, base)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				opts := core.Options{}
				switch boundary {
				case "limit":
					manager.SetRetryConfig(0, 0, 1)
				case "non-retryable":
					manager.SetConfig(&config.Config{NonRetryableErrors: []config.NonRetryableErrorRule{{StatusCode: 401}}})
				case "stop", "stop-and-cooldown":
					current, _ := manager.GetByID(primary.ID)
					current.Metadata["request_scoped_errors"] = []config.RequestScopedErrorRule{{Status: 401, Match: []string{"invalid access token"}, Action: boundary}}
					if _, err := manager.Update(WithSkipPersist(t.Context()), current); err != nil {
						t.Fatal(err)
					}
				case "single":
					ctx = core.WithSingleAttempt(ctx)
				case "pinned":
					opts.Metadata = map[string]any{core.PinnedAuthMetadataKey: primary.ID}
				case "strict-affinity":
					failover := false
					selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Failover: &failover, Fallback: &FillFirstSelector{}})
					t.Cleanup(selector.Stop)
					manager.SetConfigAndSelector(&config.Config{Routing: config.RoutingConfig{SessionAffinity: true, SessionAffinityFailover: &failover}}, selector)
					opts.Headers = http.Header{"Session-Id": {"web-401-fixture"}}
				case "committed":
					base.executeErr = chatGPTWebSettledUnauthorizedError{cause: &Error{HTTPStatus: 401, Message: "settle access token rejected"}}
				case "canceled":
					executor.before = cancel
				case "released":
					ctrl := core.NewRequestBodyReleaseController(1, nil)
					executor.before = func() { ctrl.Release() }
					opts.Metadata = map[string]any{core.BodyReleaseControllerMetadataKey: ctrl}
				case "credential-round-limit":
					manager.SetRetryConfig(1, 0, 1)
					executor.before = func() {
						current, _ := manager.GetByID(backup.ID)
						current.Metadata["request_retry"] = 0
						if _, err := manager.Update(WithSkipPersist(t.Context()), current); err != nil {
							t.Fatal(err)
						}
					}
				}
				err := runChatGPTWebRetryOperation(ctx, manager, mode, model, opts)
				if err == nil {
					t.Fatalf("401 bypassed retry boundary: rounds=%v execute=%v count=%v stream=%v", executor.rounds, base.executeCalls, base.countCalls, base.streamCalls)
				}
				if boundary == "canceled" {
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("request lost cancellation: %v", err)
					}
				} else if statusCodeFromError(err) != http.StatusUnauthorized {
					t.Fatalf("request lost upstream 401: %v", err)
				}
				calls := append(append(base.executeCalls, base.countCalls...), base.streamCalls...)
				if !reflect.DeepEqual(calls, []string{primary.ID}) {
					t.Fatalf("boundary %s allowed replay: %v", boundary, calls)
				}
			})
		}
	}
}

func TestChatGPTWebUnauthorizedWithoutRecoveryHonorsRoundLimits(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		for _, rounds := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/rounds=%d", mode, rounds), func(t *testing.T) {
				manager := NewManager(nil, &FillFirstSelector{}, nil)
				manager.SetRetryConfig(rounds, 0, 1)
				failures := map[string]error{
					"a": &Error{HTTPStatus: 401, Message: "token_revoked a"},
					"b": &Error{HTTPStatus: 401, Message: "token_expired b"},
				}
				executor := &authFallbackExecutor{id: "chatgpt-web", executeErrors: failures, countErrors: failures, streamFirstErrors: failures}
				manager.RegisterExecutor(executor)
				t.Cleanup(func() {
					if err := manager.CloseExecutors(); err != nil {
						t.Error(err)
					}
				})
				for _, id := range []string{"a", "b", "c"} {
					registerFallbackAuthForModel(t, manager, &Auth{ID: id, Provider: "chatgpt-web", Attributes: map[string]string{"compat_name": "no-recovery"}}, "web-retry-limits")
				}
				var source core.ErrorResponseSourceSnapshot
				opts := core.Options{Metadata: map[string]any{core.SelectedAuthSourceCallbackMetadataKey: func(current core.ErrorResponseSourceSnapshot) { source = current }}}
				err := runChatGPTWebRetryOperation(t.Context(), manager, mode, "web-retry-limits", opts)
				calls := append(append(executor.ExecuteCalls(), executor.CountCalls()...), executor.StreamCalls()...)
				want := []string{"a", "b"}
				if rounds == 2 {
					want = append(want, "c")
					if err != nil {
						t.Fatalf("second additional round did not reach healthy credential: %v", err)
					}
				} else {
					if !errors.Is(err, failures["b"]) || statusCodeFromError(err) != 401 {
						t.Fatalf("exhausted retries lost last upstream failure: %v", err)
					}
					if source.Provider != "chatgpt-web" || !source.HasAuthPriority {
						t.Fatalf("exhausted retries lost provider source: %#v", source)
					}
				}
				if !reflect.DeepEqual(calls, want) {
					t.Fatalf("calls=%v, want %v", calls, want)
				}
				for _, id := range []string{"a", "b"} {
					current, _ := manager.GetByID(id)
					if !current.Unavailable || current.LastError == nil || current.LastError.HTTPStatus != 401 {
						t.Fatalf("failed credential %s lost its cooldown/error", id)
					}
				}
			})
		}
	}
}
