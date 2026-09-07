package auth

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestManagerInitiallyUnavailablePoolRetainsFailureWithoutUpstreamCalls(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, mixed := range []bool{false, true} {
			for _, operation := range []string{"execute", "count", "stream"} {
				t.Run(fmt.Sprintf("legacy=%t/mixed=%t/%s", legacy, mixed, operation), func(t *testing.T) {
					var selector Selector = &RoundRobinSelector{}
					if legacy {
						selector = &trackingSelector{}
					}
					manager := NewManager(nil, selector, nil)
					manager.SetRetryConfig(2, 0, 0)
					providers := []string{"codex"}
					if mixed {
						providers = append(providers, "openai")
					}
					var executors []*upstreamPriorityExecutor
					for _, provider := range providers {
						executor := &upstreamPriorityExecutor{schedulerProviderTestExecutor: schedulerProviderTestExecutor{provider: provider}, upstream: errors.New("unexpected execution"), mark: true}
						executors = append(executors, executor)
						manager.RegisterExecutor(executor)
						id := schedulerTestID(t, provider)
						registerSchedulerModels(t, provider, "initially-unavailable", id)
						if _, err := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: id, Provider: provider}); err != nil {
							t.Fatal(err)
						}
						delay := time.Minute
						manager.MarkResult(WithSkipPersist(t.Context()), Result{AuthID: id, Provider: provider, Model: "initially-unavailable", RetryAfter: &delay, Error: &Error{HTTPStatus: 429, Message: "private previous response", Diagnostic: &ErrorDiagnostic{Stage: "previous"}}})
					}
					request := core.Request{Model: "initially-unavailable"}
					var err error
					switch operation {
					case "execute":
						_, err = manager.Execute(t.Context(), providers, request, core.Options{})
					case "count":
						_, err = manager.ExecuteCount(t.Context(), providers, request, core.Options{})
					case "stream":
						var stream *core.StreamResult
						stream, err = manager.ExecuteStream(t.Context(), providers, request, core.Options{})
						if stream != nil {
							for range stream.Chunks {
							}
						}
					}
					var selection *Error
					var cooldown *modelCooldownError
					if !errors.As(err, &selection) || selection.Code != "auth_unavailable" || errors.As(err, &cooldown) {
						t.Fatal("final availability classification changed")
					}
					failure := StoredAuthFailureOf(err)
					if failure == nil || failure.HTTPStatus != 429 || failure.Diagnostic == nil || failure.Diagnostic.Stage != "previous" {
						t.Fatal("manager lost stored provider details")
					}
					var headers interface{ Headers() http.Header }
					if !errors.As(err, &headers) || headers.Headers().Get("Retry-After") == "" {
						t.Fatal("retry hint was lost")
					}
					if strings.Contains(err.Error(), "private previous") || core.IsUpstreamAttemptError(err) {
						t.Fatal("old failure became public current-request evidence")
					}
					for _, executor := range executors {
						if len(executor.attempts) != 0 {
							t.Fatal("initially unavailable pool invoked upstream")
						}
					}
				})
			}
		}
	}
}
