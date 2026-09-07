package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type retryRoundLegacySelector struct{ RoundRobinSelector }

func TestCredentialRetryRoundFilteringUsesEachCredentialLimit(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, mixed := range []bool{false, true} {
			for _, mode := range []string{"execute", "count", "stream"} {
				t.Run(fmt.Sprintf("%s/legacy=%t/mixed=%t", mode, legacy, mixed), func(t *testing.T) {
					var selector Selector = &RoundRobinSelector{}
					if legacy {
						selector = &retryRoundLegacySelector{}
					}
					manager := NewManager(nil, selector, nil)
					manager.SetRetryConfig(1, 0, 0)
					manager.SetConfig(&config.Config{NoCooldownStatusCodes: []int{500}})
					providers := []string{"claude"}
					if mixed {
						providers = append(providers, "codex")
					}
					want := map[string]int{}
					executors := []*authFallbackExecutor{}
					for _, provider := range providers {
						errs := map[string]error{}
						executor := &authFallbackExecutor{id: provider, executeErrors: errs, countErrors: errs, streamFirstErrors: errs}
						manager.RegisterExecutor(executor)
						executors = append(executors, executor)
						for _, row := range []struct {
							name     string
							override any
							calls    int
						}{
							{"zero", 0, 1}, {"one", 1, 2}, {"two", 2, 3}, {"inherited", nil, 2}, {"negative", -2, 1},
						} {
							id := provider + "-" + row.name
							auth := &Auth{ID: id, Provider: provider, Status: StatusActive}
							if row.override != nil {
								auth.Metadata = map[string]any{"request_retry": row.override}
							}
							errs[id] = &Error{HTTPStatus: http.StatusInternalServerError, Message: "synthetic upstream failure"}
							want[id] = row.calls
							registerFallbackAuthForModel(t, manager, auth, "retry-round-model")
						}
					}
					request := core.Request{Model: "retry-round-model"}
					var err error
					switch mode {
					case "execute":
						_, err = manager.Execute(t.Context(), providers, request, core.Options{})
					case "count":
						_, err = manager.ExecuteCount(t.Context(), providers, request, core.Options{})
					case "stream":
						var stream *core.StreamResult
						stream, err = manager.ExecuteStream(t.Context(), providers, request, core.Options{})
						if stream != nil {
							for chunk := range stream.Chunks {
								if chunk.Err != nil {
									err = chunk.Err
								}
							}
						}
					}
					if err == nil {
						t.Fatal("expected the synthetic upstream failure")
					}
					got := map[string]int{}
					for _, executor := range executors {
						var calls []string
						switch mode {
						case "execute":
							calls = executor.ExecuteCalls()
						case "count":
							calls = executor.CountCalls()
						case "stream":
							calls = executor.StreamCalls()
						}
						for _, id := range calls {
							got[id]++
						}
					}
					for id, count := range want {
						if got[id] != count {
							t.Errorf("%s: calls=%d want=%d", id, got[id], count)
						}
					}
				})
			}
		}
	}
}

func runCredentialRetryOperation(ctx context.Context, manager *Manager, mode string, request core.Request, opts core.Options) error {
	switch mode {
	case "count":
		_, err := manager.ExecuteCount(ctx, []string{"claude"}, request, opts)
		return err
	case "stream":
		stream, err := manager.ExecuteStream(ctx, []string{"claude"}, request, opts)
		if stream != nil {
			missingHeaders := stream.Headers.Get("X-Auth") == ""
			for chunk := range stream.Chunks {
				if chunk.Err != nil {
					err = chunk.Err
				}
			}
			if missingHeaders {
				return errors.New("stream lost the selected failure's headers")
			}
		}
		return err
	default:
		_, err := manager.Execute(ctx, []string{"claude"}, request, opts)
		return err
	}
}

func TestCredentialRetryRoundExhaustionKeepsFailure(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		for _, pinned := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/pinned=%t", mode, pinned), func(t *testing.T) {
				manager := NewManager(nil, nil, nil)
				manager.SetRetryConfig(0, 0, 0)
				manager.SetConfig(&config.Config{NoCooldownStatusCodes: []int{500}})
				failure := &Error{HTTPStatus: 500, Message: "original selected credential failure"}
				errs := map[string]error{"selected": failure}
				executor := &authFallbackExecutor{id: "claude", executeErrors: errs, countErrors: errs, streamFirstErrors: errs}
				manager.RegisterExecutor(executor)
				registerFallbackAuthForModel(t, manager, &Auth{ID: "selected", Provider: "claude", Metadata: map[string]any{"request_retry": 0}}, "model-a")
				otherModel := "model-b"
				if pinned {
					otherModel = "model-a"
				}
				registerFallbackAuthForModel(t, manager, &Auth{ID: "excluded", Provider: "claude", Metadata: map[string]any{"request_retry": 3}}, otherModel)
				opts := core.Options{}
				if pinned {
					opts.Metadata = map[string]any{core.PinnedAuthMetadataKey: "selected"}
				}
				err := runCredentialRetryOperation(t.Context(), manager, mode, core.Request{Model: "model-a"}, opts)
				if !errors.Is(err, failure) {
					t.Fatalf("an ineligible credential's retry budget replaced the original failure: %v", err)
				}
				if got := len(executor.ExecuteCalls()) + len(executor.CountCalls()) + len(executor.StreamCalls()); got != 1 {
					t.Fatalf("unexpected upstream calls: %d", got)
				}
			})
		}
	}
}

type retryRoundHookExecutor struct {
	*authFallbackExecutor
	once sync.Once
	hook func()
}

func (e *retryRoundHookExecutor) Execute(ctx context.Context, auth *Auth, request core.Request, opts core.Options) (core.Response, error) {
	e.once.Do(e.hook)
	return e.authFallbackExecutor.Execute(ctx, auth, request, opts)
}
func (e *retryRoundHookExecutor) CountTokens(ctx context.Context, auth *Auth, request core.Request, opts core.Options) (core.Response, error) {
	e.once.Do(e.hook)
	return e.authFallbackExecutor.CountTokens(ctx, auth, request, opts)
}
func (e *retryRoundHookExecutor) ExecuteStream(ctx context.Context, auth *Auth, request core.Request, opts core.Options) (*core.StreamResult, error) {
	e.once.Do(e.hook)
	return e.authFallbackExecutor.ExecuteStream(ctx, auth, request, opts)
}

func TestCredentialRetryRoundKeepsDefaultSnapshotAndCancellation(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		for _, cancelRequest := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cancel=%t", mode, cancelRequest), func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				manager := NewManager(nil, nil, nil)
				manager.SetRetryConfig(1, 0, 0)
				manager.SetConfig(&config.Config{NoCooldownStatusCodes: []int{500}})
				errs := map[string]error{"inherited": &Error{HTTPStatus: 500, Message: "synthetic failure"}}
				executor := &retryRoundHookExecutor{authFallbackExecutor: &authFallbackExecutor{id: "claude", executeErrors: errs, countErrors: errs, streamFirstErrors: errs}, hook: func() {
					manager.SetRetryConfig(0, 0, 0)
					if cancelRequest {
						cancel()
					}
				}}
				manager.RegisterExecutor(executor)
				registerFallbackAuthForModel(t, manager, &Auth{ID: "inherited", Provider: "claude"}, "model-a")
				err := runCredentialRetryOperation(ctx, manager, mode, core.Request{Model: "model-a"}, core.Options{})
				if err == nil || (cancelRequest && !errors.Is(err, context.Canceled)) {
					t.Fatalf("unexpected failure: %v", err)
				}
				want := 2
				if cancelRequest {
					want = 1
				}
				before := len(executor.ExecuteCalls()) + len(executor.CountCalls()) + len(executor.StreamCalls())
				if before != want {
					t.Fatalf("in-flight calls=%d want=%d", before, want)
				}
				if cancelRequest {
					return
				}
				_ = runCredentialRetryOperation(t.Context(), manager, mode, core.Request{Model: "model-a"}, core.Options{})
				if after := len(executor.ExecuteCalls()) + len(executor.CountCalls()) + len(executor.StreamCalls()); after-before != 1 {
					t.Fatalf("new request did not use updated default: calls=%d", after-before)
				}
			})
		}
	}
}

func TestCredentialRetryRoundUsesUpdatedCredentialAtNextSelection(t *testing.T) {
	for _, mode := range []string{"execute", "count", "stream"} {
		t.Run(mode, func(t *testing.T) {
			manager := NewManager(nil, &FillFirstSelector{}, nil)
			manager.SetRetryConfig(0, 0, 0)
			manager.SetConfig(&config.Config{NoCooldownStatusCodes: []int{500}})
			errs := map[string]error{"a": &Error{HTTPStatus: 500, Message: "failure-a"}, "b": &Error{HTTPStatus: 500, Message: "failure-b"}}
			executor := &retryRoundHookExecutor{authFallbackExecutor: &authFallbackExecutor{id: "claude", executeErrors: errs, countErrors: errs, streamFirstErrors: errs}, hook: func() {
				updated, _ := manager.GetByID("b")
				updated.Metadata["request_retry"] = 0
				if _, err := manager.Update(WithSkipPersist(t.Context()), updated); err != nil {
					t.Fatal(err)
				}
			}}
			manager.RegisterExecutor(executor)
			registerFallbackAuthForModel(t, manager, &Auth{ID: "a", Provider: "claude", Metadata: map[string]any{"request_retry": 1}}, "model-a")
			registerFallbackAuthForModel(t, manager, &Auth{ID: "b", Provider: "claude", Metadata: map[string]any{"request_retry": 2}}, "model-a")
			if err := runCredentialRetryOperation(t.Context(), manager, mode, core.Request{Model: "model-a"}, core.Options{}); err == nil {
				t.Fatal("missing upstream failure")
			}
			calls := append(executor.ExecuteCalls(), executor.CountCalls()...)
			calls = append(calls, executor.StreamCalls()...)
			got := map[string]int{}
			for _, id := range calls {
				got[id]++
			}
			if got["a"] != 2 || got["b"] != 1 {
				t.Fatalf("updated credential retry counts: %v", got)
			}
		})
	}
}

func TestCredentialRetryRoundKeepsPriorityProgression(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, mode := range []string{"execute", "count", "stream"} {
			t.Run(fmt.Sprintf("%s/legacy=%t", mode, legacy), func(t *testing.T) {
				var selector Selector = &RoundRobinSelector{}
				if legacy {
					selector = &retryRoundLegacySelector{}
				}
				manager := NewManager(nil, selector, nil)
				manager.SetConfig(&config.Config{NoCooldownStatusCodes: []int{500}})
				errs := map[string]error{}
				executor := &authFallbackExecutor{id: "claude", executeErrors: errs, countErrors: errs, streamFirstErrors: errs}
				manager.RegisterExecutor(executor)
				for _, row := range []struct {
					id              string
					priority, retry int
				}{{"high", 10, 0}, {"middle", 5, 1}, {"low", 0, 2}} {
					errs[row.id] = &Error{HTTPStatus: 500, Message: "synthetic failure"}
					registerFallbackAuthForModel(t, manager, &Auth{ID: row.id, Provider: "claude", Attributes: map[string]string{"priority": strconv.Itoa(row.priority)}, Metadata: map[string]any{"request_retry": row.retry}}, "priority-retry-model")
				}
				if err := runCredentialRetryOperation(t.Context(), manager, mode, core.Request{Model: "priority-retry-model"}, core.Options{}); err == nil {
					t.Fatal("missing upstream failure")
				}
				calls := append(executor.ExecuteCalls(), executor.CountCalls()...)
				calls = append(calls, executor.StreamCalls()...)
				// A round can fail over through remaining lower priorities; only
				// its starting tier advances between additional request rounds.
				if !reflect.DeepEqual(calls, []string{"high", "middle", "low", "middle", "low", "low"}) {
					t.Fatalf("retry filtering changed priority progression: %v", calls)
				}
			})
		}
	}
}
