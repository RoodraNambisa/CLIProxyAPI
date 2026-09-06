package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type upstreamPriorityExecutor struct {
	schedulerProviderTestExecutor
	upstream   error
	local      error
	mark       bool
	streamMode string
	attempts   []string
}

func (e *upstreamPriorityExecutor) ShouldPrepareRequestAuth(auth *Auth) bool {
	return auth.ID == "b-local"
}
func (e *upstreamPriorityExecutor) PrepareRequestAuth(context.Context, *Auth) (*Auth, error) {
	return nil, e.local
}
func (e *upstreamPriorityExecutor) Execute(ctx context.Context, auth *Auth, _ executor.Request, _ executor.Options) (executor.Response, error) {
	e.attempts = append(e.attempts, auth.ID)
	if e.mark {
		executor.MarkUpstreamAttempt(ctx)
	}
	return executor.Response{}, e.upstream
}
func (e *upstreamPriorityExecutor) CountTokens(ctx context.Context, auth *Auth, req executor.Request, opts executor.Options) (executor.Response, error) {
	return e.Execute(ctx, auth, req, opts)
}
func (e *upstreamPriorityExecutor) ExecuteStream(ctx context.Context, auth *Auth, req executor.Request, opts executor.Options) (*executor.StreamResult, error) {
	_, err := e.Execute(ctx, auth, req, opts)
	if e.streamMode != "" {
		if e.streamMode == "nil" {
			return nil, nil
		}
		chunks := make(chan executor.StreamChunk, 1)
		if e.streamMode == "error-chunk" {
			chunks <- executor.StreamChunk{Err: err}
		}
		close(chunks)
		return &executor.StreamResult{Headers: http.Header{"Retry-After": {"12"}}, Chunks: chunks}, nil
	}
	return nil, err
}

func TestManagerPrefersObservedUpstreamErrorOverLaterLocalPreparation(t *testing.T) {
	for _, operation := range []string{"execute", "count", "stream"} {
		for _, mark := range []bool{false, true} {
			manager := NewManager(nil, &FillFirstSelector{}, nil)
			upstream := &Error{HTTPStatus: 503, Message: "original upstream failure"}
			exec := &upstreamPriorityExecutor{schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"}, upstream: upstream, local: errors.New("local preparation unavailable"), mark: mark}
			manager.RegisterExecutor(exec)
			for _, id := range []string{"a-upstream", "b-local"} {
				if _, err := manager.Register(t.Context(), &Auth{ID: id, Provider: "codex"}); err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "priority-model"}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
			}
			var err error
			req := executor.Request{Model: "priority-model"}
			switch operation {
			case "execute":
				_, err = manager.Execute(t.Context(), []string{"codex"}, req, executor.Options{})
			case "count":
				_, err = manager.ExecuteCount(t.Context(), []string{"codex"}, req, executor.Options{})
			case "stream":
				_, err = manager.ExecuteStream(t.Context(), []string{"codex"}, req, executor.Options{})
			}
			if mark {
				if !errors.Is(err, upstream) || statusCodeFromError(err) != 503 {
					t.Fatal("local preparation replaced the observed upstream error")
				}
			} else if !errors.Is(err, exec.local) {
				t.Fatal("unobserved local failure was incorrectly promoted to upstream priority")
			}
			if len(exec.attempts) != 1 {
				t.Fatal("error prioritization changed upstream attempt count")
			}
		}
	}
}

func TestManagerKeepsObservedStreamFailureAndHeadersAfterLocalPreparationFailure(t *testing.T) {
	for _, mode := range []string{"empty", "nil", "error-chunk"} {
		t.Run(mode, func(t *testing.T) {
			manager := NewManager(nil, &FillFirstSelector{}, nil)
			exec := &upstreamPriorityExecutor{schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: "codex"}, upstream: &Error{HTTPStatus: 503, Message: "upstream failed"}, local: errors.New("local preparation unavailable"), mark: true, streamMode: mode}
			manager.RegisterExecutor(exec)
			for _, id := range []string{"a-upstream", "b-local"} {
				if _, err := manager.Register(t.Context(), &Auth{ID: id, Provider: "codex"}); err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "stream-priority-model"}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
			}
			result, err := manager.ExecuteStream(t.Context(), []string{"codex"}, executor.Request{Model: "stream-priority-model"}, executor.Options{})
			var headers http.Header
			if result != nil {
				headers = result.Headers
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						err = chunk.Err
					}
				}
			} else {
				var source interface{ Headers() http.Header }
				if errors.As(err, &source) {
					headers = source.Headers()
				}
			}
			if err == nil || errors.Is(err, exec.local) || !executor.IsUpstreamAttemptError(err) || len(exec.attempts) != 1 {
				t.Fatal("observed stream failure was replaced by local preparation or retried")
			}
			if mode == "error-chunk" && !errors.Is(err, exec.upstream) {
				t.Fatal("stream bootstrap lost its original cause")
			}
			if mode != "error-chunk" && !strings.Contains(err.Error(), "upstream stream") {
				t.Fatal("empty stream cause was lost")
			}
			if mode != "nil" && headers.Get("Retry-After") != "12" {
				t.Fatal("stream bootstrap response headers were lost")
			}
		})
	}
}
