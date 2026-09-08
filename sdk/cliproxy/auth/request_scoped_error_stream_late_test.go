package auth

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type requestScopedLateExecutor struct {
	*authFallbackExecutor
	failure error
	release *core.RequestBodyReleaseController
}

func (e *requestScopedLateExecutor) ExecuteStream(_ context.Context, a *Auth, _ core.Request, _ core.Options) (*core.StreamResult, error) {
	e.mu.Lock()
	e.streamCalls = append(e.streamCalls, a.ID)
	e.mu.Unlock()
	ch := make(chan core.StreamChunk, 2)
	ch <- core.StreamChunk{Payload: []byte(`{"type":"response.output_text.delta","delta":"visible output"}`)}
	if e.release != nil {
		e.release.Release()
	}
	ch <- core.StreamChunk{Err: e.failure}
	close(ch)
	return &core.StreamResult{Chunks: ch}, nil
}

func TestRequestScopedLateStreamActionsDoNotReplayOutput(t *testing.T) {
	for _, released := range []bool{false, true} {
		for _, action := range []string{"stop", "stop-and-cooldown", "continue", "continue-and-cooldown"} {
			t.Run(fmt.Sprintf("released=%t/action=%s", released, action), func(t *testing.T) {
				hook := &requestScopedResultHook{}
				m := NewManager(nil, &FillFirstSelector{}, hook)
				m.SetRetryConfig(3, 0, 0)
				failure := &Error{HTTPStatus: 500, Message: "fixture"}
				e := &requestScopedLateExecutor{authFallbackExecutor: &authFallbackExecutor{id: "claude"}, failure: failure}
				opts := core.Options{}
				if released {
					e.release = core.NewRequestBodyReleaseController(1, nil)
					opts.Metadata = map[string]any{core.BodyReleaseControllerMetadataKey: e.release}
				}
				m.RegisterExecutor(e)
				const model = "scoped-late-model"
				for _, id := range []string{"a", "b"} {
					registerFallbackAuthForModel(t, m, &Auth{ID: id, Provider: "claude", Metadata: map[string]any{"request_scoped_errors": []config.RequestScopedErrorRule{{Status: 500, Match: []string{"fixture"}, Action: action}}}}, model)
				}
				stream, err := m.ExecuteStream(t.Context(), []string{"claude"}, core.Request{Model: model}, opts)
				if err != nil || stream == nil {
					t.Fatalf("stream start: %v", err)
				}
				payloads, failures := 0, 0
				for chunk := range stream.Chunks {
					if len(chunk.Payload) > 0 {
						payloads++
					}
					if chunk.Err != nil {
						failures++
						if !errors.Is(chunk.Err, failure) {
							t.Fatal("late action lost original error")
						}
					}
				}
				if payloads != 1 || failures != 1 || hook.failures.Load() != 1 || len(e.StreamCalls()) != 1 {
					t.Fatal("late error duplicated output, result or upstream request")
				}
				a, _ := m.GetByID("a")
				cooled := false
				if state := a.ModelStates[model]; state != nil {
					cooled = state.NextRetryAfter.After(time.Now())
				}
				if cooled != (action == "stop-and-cooldown" || action == "continue-and-cooldown") {
					t.Fatal("late error ignored configured cooldown action")
				}
			})
		}
	}
}
