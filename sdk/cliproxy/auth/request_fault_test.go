package auth

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestKnownRequestFaultPreservesCredentialEvidence(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "policy code", err: &Error{HTTPStatus: 403, Code: "misalignment_policy_violation", Message: "blocked"}, want: true},
		{name: "policy type", err: &Error{HTTPStatus: 401, Message: `{"error":{"type":"misalignment_policy_violation"}}`}, want: true},
		{name: "policy type with auth code", err: &Error{HTTPStatus: 401, Message: `{"error":{"type":"misalignment_policy_violation","code":"token_revoked"}}`}},
		{name: "policy image", err: &Error{HTTPStatus: 403, Message: `{"error":{"type":"image_generation_user_error","code":"misalignment_policy_violation"}}`}, want: true},
		{name: "request scoped 401", err: &Error{HTTPStatus: 401, Message: `{"error":{"type":"invalid_request_error","code":"invalid_value"}}`}, want: true},
		{name: "nested stream fault", err: &Error{HTTPStatus: 500, Message: `{"type":"response.failed","response":{"error":{"code":"cyber_policy"}}}`}, want: true},
		{name: "real authentication", err: &Error{HTTPStatus: 401, Message: `{"error":{"type":"authentication_error","code":"invalid_request_error"}}`}},
		{name: "real payment", err: &Error{HTTPStatus: 402, Message: `{"error":{"type":"invalid_request_error","code":"misalignment_policy_violation"}}`}},
		{name: "real rate limit", err: &Error{HTTPStatus: 429, Message: `{"error":{"type":"invalid_request_error","code":"misalignment_policy_violation"}}`}},
		{name: "permission unknown", err: &Error{HTTPStatus: 403, Message: "request not allowed"}},
		{name: "unstructured policy mention", err: &Error{HTTPStatus: 503, Message: "failed while handling misalignment_policy_violation"}},
		{name: "configurable image fault", err: &Error{HTTPStatus: 400, Message: `{"error":{"type":"image_generation_user_error","code":"invalid_value"}}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := isKnownRequestFault(fmt.Errorf("upstream: %w", tc.err)); got != tc.want {
				t.Fatalf("request fault = %t, want %t", got, tc.want)
			}
		})
	}
}

func TestRequestFaultSurvivesResultWrappingWithoutChangingPublicCode(t *testing.T) {
	original := &Error{HTTPStatus: 403, Code: "misalignment_policy_violation", Message: "rejected"}
	result := executionResultError(nil, fmt.Errorf("transport: %w", original))
	if !isCredentialNeutralFailure(result) {
		t.Fatal("result wrapping lost the explicit request error code")
	}
	if result.Message != "transport: misalignment_policy_violation: rejected" || result.HTTPStatus != 403 {
		t.Fatal("result wrapping changed the source failure")
	}
	result.HTTPStatus = 429
	if isCredentialNeutralFailure(result) {
		t.Fatal("request code overrode the authoritative quota status")
	}
}

func TestManagerPolicyRefusalStopsAllRetryEntrypointsWithoutCooling(t *testing.T) {
	for _, code := range []string{"misalignment_policy_violation", "cyber_policy", "content_policy_violation"} {
		for _, status := range []int{400, 401, 403} {
			for _, operation := range []string{"execute", "count", "stream"} {
				t.Run(fmt.Sprintf("%s/%d/%s", code, status, operation), func(t *testing.T) {
					manager, executor := newCredentialRetryLimitTestManagerWithAuthCount(t, 0, 3)
					manager.SetRetryConfig(3, 0, 0)
					manager.SetConfig(&internalconfig.Config{NonRetryableErrors: []internalconfig.NonRetryableErrorRule{}})
					executor.err = &Error{HTTPStatus: status, Code: code, Message: `{"error":{"code":"` + code + `","message":"request rejected"}}`}
					req := cliproxyexecutor.Request{Model: "test-model"}
					var err error
					switch operation {
					case "execute":
						_, err = manager.Execute(t.Context(), []string{"claude"}, req, cliproxyexecutor.Options{})
					case "count":
						_, err = manager.ExecuteCount(t.Context(), []string{"claude"}, req, cliproxyexecutor.Options{})
					case "stream":
						_, err = manager.ExecuteStream(t.Context(), []string{"claude"}, req, cliproxyexecutor.Options{})
					}
					if !errors.Is(err, executor.err) {
						t.Fatal("original policy error was replaced")
					}
					if executor.Calls() != 1 {
						t.Fatalf("attempt count = %d, want 1", executor.Calls())
					}
					for _, auth := range manager.List() {
						if auth.Unavailable || !auth.NextRetryAfter.IsZero() || auth.Quota.Exceeded || len(auth.ModelStates) > 0 {
							t.Fatal("request refusal changed credential/model availability")
						}
					}
				})
			}
		}
	}
}

func TestManagerRequestFaultDoesNotTriggerUnauthorizedRefresh(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	executor := &antigravityUnauthorizedRefreshExecutor{}
	auth := &Auth{ID: "policy-auth", Provider: "antigravity", Metadata: map[string]any{"refresh_token": "test-refresh"}}
	err := &Error{HTTPStatus: 401, Message: `{"error":{"code":"misalignment_policy_violation"}}`}
	_, attempted, refreshErr := manager.tryRefreshAfterUnauthorized(t.Context(), executor, auth, err, false)
	if attempted || refreshErr != nil || executor.refreshCalls != 0 {
		t.Fatal("request fault triggered credential recovery")
	}
	if deferUnauthorizedStreamResult(auth, err) {
		t.Fatal("request fault deferred stream result for a credential refresh")
	}
}

func TestManagerPolicyRefusalStreamChunksRemainCredentialNeutral(t *testing.T) {
	for _, hasOutput := range []bool{false, true} {
		t.Run(fmt.Sprintf("output=%t", hasOutput), func(t *testing.T) {
			manager, _ := newCredentialRetryLimitTestManagerWithAuthCount(t, 0, 2)
			manager.SetRetryConfig(3, 0, 0)
			upstreamErr := &Error{HTTPStatus: 403, Message: `{"type":"response.failed","response":{"error":{"code":"misalignment_policy_violation"}}}`}
			chunks := make(chan cliproxyexecutor.StreamChunk, 2)
			if hasOutput {
				chunks <- cliproxyexecutor.StreamChunk{Payload: []byte(`data: {"type":"response.output_text.delta","delta":"prefix"}`)}
			}
			chunks <- cliproxyexecutor.StreamChunk{Err: upstreamErr}
			close(chunks)
			executor := &streamResultTestExecutor{id: "claude", results: map[string]*cliproxyexecutor.StreamResult{"test-model": {Chunks: chunks}}}
			manager.RegisterExecutor(executor)
			stream, err := manager.ExecuteStream(t.Context(), []string{"claude"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{Stream: true})
			if err == nil {
				for chunk := range stream.Chunks {
					if chunk.Err != nil {
						err = chunk.Err
					}
				}
			}
			if !errors.Is(err, upstreamErr) {
				t.Fatal("stream lost the original refusal")
			}
			calls, recoveries := executor.snapshot()
			if len(calls) != 1 || recoveries != 0 {
				t.Fatal("stream refusal was retried or recovered")
			}
			for _, auth := range manager.List() {
				if auth.Unavailable || !auth.NextRetryAfter.IsZero() || len(auth.ModelStates) != 0 {
					t.Fatal("stream refusal cooled a credential")
				}
			}
		})
	}
}

func TestManagerRequestFaultBodyDoesNotHideQuotaStatus(t *testing.T) {
	for _, status := range []int{http.StatusPaymentRequired, http.StatusTooManyRequests} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			manager, executor := newCredentialRetryLimitTestManagerWithAuthCount(t, 0, 2)
			executor.err = &Error{HTTPStatus: status, Message: `{"error":{"type":"invalid_request_error","code":"misalignment_policy_violation"}}`}
			_, err := manager.Execute(t.Context(), []string{"claude"}, cliproxyexecutor.Request{Model: "test-model"}, cliproxyexecutor.Options{})
			if err == nil || executor.Calls() != 2 {
				t.Fatal("authoritative quota/payment status did not allow credential fallback")
			}
			for _, auth := range manager.List() {
				state := auth.ModelStates["test-model"]
				if state == nil || !state.Unavailable || state.NextRetryAfter.IsZero() {
					t.Fatal("quota/payment status lost its cooldown")
				}
			}
		})
	}
}
