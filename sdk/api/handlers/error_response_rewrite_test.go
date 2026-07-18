package handlers

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type rewriteTestExecutor struct{}

type rewriteRuntimeError struct{}

type rewriteProtocolStreamExecutor struct {
	closeWithoutError bool
	differentError    bool
}

type rewriteProtocolStreamError struct{}

func (rewriteRuntimeError) Error() string        { return "invalid_request_error: maximum context exceeded" }
func (rewriteRuntimeError) StatusCode() int      { return http.StatusBadRequest }
func (rewriteRuntimeError) SkipAuthResult() bool { return true }

func (rewriteProtocolStreamError) Error() string {
	return "upstream stream protocol error: native protocol failed"
}
func (rewriteProtocolStreamError) StatusCode() int { return http.StatusBadGateway }

func (rewriteTestExecutor) Identifier() string { return "rewrite-test" }

func (rewriteTestExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, rewriteRuntimeError{}
}

func (rewriteTestExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	chunks := make(chan coreexecutor.StreamChunk, 1)
	chunks <- coreexecutor.StreamChunk{Err: rewriteRuntimeError{}}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (rewriteTestExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (rewriteTestExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (rewriteTestExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (rewriteProtocolStreamExecutor) Identifier() string { return "rewrite-protocol-stream" }

func (rewriteProtocolStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (e rewriteProtocolStreamExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	chunks := make(chan coreexecutor.StreamChunk, 2)
	chunks <- coreexecutor.StreamChunk{Payload: []byte("data: {\"type\":\"error\",\"error\":{\"message\":\"native protocol failed\"}}\n\n")}
	if !e.closeWithoutError {
		streamErr := error(rewriteProtocolStreamError{})
		if e.differentError {
			streamErr = errors.New("different runtime failure")
		}
		chunks <- coreexecutor.StreamChunk{Err: streamErr}
	}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (rewriteProtocolStreamExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (rewriteProtocolStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("not implemented")
}

func (rewriteProtocolStreamExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func TestRewriteExecutionErrorResponseUsesFirstMatchingRule(t *testing.T) {
	firstBody := map[string]any{"error": map[string]any{"message": "rewritten", "code": "context_too_large"}}
	secondBody := map[string]any{"error": map[string]any{"message": "wrong rule"}}
	handler := NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{
		{StatusCode: http.StatusTooManyRequests, MessageContains: "MAXIMUM CONTEXT", ResponseStatusCode: http.StatusBadRequest, ResponseBody: &firstBody},
		{StatusCode: http.StatusTooManyRequests, ResponseStatusCode: http.StatusTeapot, ResponseBody: &secondBody},
	}}, nil)
	original := &interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      errors.New("maximum context length exceeded"),
		Addon: http.Header{
			"retry-after":      {"30"},
			"Content-Length":   {"999"},
			"Content-Encoding": {"gzip"},
			"ETag":             {`"old"`},
			"X-Request-Id":     {"request-1"},
		},
	}

	projected := handler.RewriteExecutionErrorResponse(original)
	if projected == original {
		t.Fatal("RewriteExecutionErrorResponse() returned the original message")
	}
	if projected.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", projected.StatusCode, http.StatusBadRequest)
	}
	if projected.Addon.Get("Retry-After") != "" {
		t.Fatalf("Retry-After = %q, want removed", projected.Addon.Get("Retry-After"))
	}
	if projected.Addon.Get("X-Request-Id") != "request-1" {
		t.Fatalf("X-Request-Id = %q", projected.Addon.Get("X-Request-Id"))
	}
	for _, key := range []string{"Content-Length", "Content-Encoding", "ETag"} {
		if projected.Addon.Get(key) != "" {
			t.Fatalf("%s = %q, want removed", key, projected.Addon.Get(key))
		}
	}
	if values := original.Addon["retry-after"]; len(values) != 1 || values[0] != "30" {
		t.Fatal("projection mutated the original headers")
	}
	if OriginalErrorStatusCode(projected) != http.StatusTooManyRequests {
		t.Fatalf("original status = %d", OriginalErrorStatusCode(projected))
	}
	body, ok := RewrittenErrorResponseBody(projected)
	if !ok || string(body) != `{"error":{"code":"context_too_large","message":"rewritten"}}` {
		t.Fatalf("rewritten body = %s, ok = %v", body, ok)
	}
}

func TestRewriteExecutionErrorResponsePreservesOmittedBodyAndRetryHeader(t *testing.T) {
	handler := NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{
		{MessageContains: "quota", ResponseStatusCode: http.StatusServiceUnavailable},
	}}, nil)
	originalErr := errors.New("quota exhausted")
	projected := handler.RewriteExecutionErrorResponse(&interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      originalErr,
		Addon:      http.Header{"Retry-After": {"12"}},
	})

	if projected.StatusCode != http.StatusServiceUnavailable || projected.Error.Error() != originalErr.Error() {
		t.Fatalf("projection = %#v", projected)
	}
	if projected.Addon.Get("Retry-After") != "12" {
		t.Fatalf("Retry-After = %q, want preserved", projected.Addon.Get("Retry-After"))
	}
	if _, ok := RewrittenErrorResponseBody(projected); ok {
		t.Fatal("omitted response body reported as rewritten")
	}
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	handler.WriteErrorResponse(ctx, projected)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("HTTP status = %d", recorder.Code)
	}
	if body := recorder.Body.String(); !strings.Contains(body, `"type":"rate_limit_error"`) || !strings.Contains(body, `"code":"rate_limit_exceeded"`) {
		t.Fatalf("status-only rewrite changed body shape: %s", body)
	}
}

func TestRewriteExecutionErrorResponsePreservesExplicitEmptyBody(t *testing.T) {
	emptyBody := map[string]any{}
	handler := NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{
		{StatusCode: http.StatusBadGateway, ResponseBody: &emptyBody},
	}}, nil)
	projected := handler.RewriteExecutionErrorResponse(&interfaces.ErrorMessage{
		StatusCode: http.StatusBadGateway,
		Error:      errors.New("upstream failed"),
	})
	body, ok := RewrittenErrorResponseBody(projected)
	if !ok || string(body) != `{}` {
		t.Fatalf("body = %q, ok = %v", body, ok)
	}
	if projected.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d", projected.StatusCode)
	}
}

func TestRewriteExecutionErrorResponseUnmatchedAndHTTPWrite(t *testing.T) {
	body := map[string]any{"custom": true}
	handler := NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{
		{StatusCode: http.StatusTooManyRequests, ResponseStatusCode: http.StatusBadRequest, ResponseBody: &body},
	}}, nil)
	unmatched := &interfaces.ErrorMessage{StatusCode: http.StatusUnauthorized, Error: errors.New("unauthorized")}
	if got := handler.RewriteExecutionErrorResponse(unmatched); got != unmatched {
		t.Fatal("unmatched rule changed the error")
	}

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	projected := handler.RewriteExecutionErrorResponse(&interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      errors.New("limited"),
	})
	handler.WriteErrorResponse(ctx, projected)
	if recorder.Code != http.StatusBadRequest || recorder.Body.String() != `{"custom":true}` {
		t.Fatalf("response = status %d body %q", recorder.Code, recorder.Body.String())
	}
}

func TestRewriteExecutionErrorResponseSkipsInvalidSDKRules(t *testing.T) {
	body := map[string]any{"valid": true}
	handler := NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{
		{ResponseStatusCode: http.StatusTeapot},
		{StatusCode: http.StatusTooManyRequests, ResponseStatusCode: http.StatusOK},
		{StatusCode: http.StatusTooManyRequests, ResponseBody: new(map[string]any)},
		{StatusCode: http.StatusTooManyRequests, ResponseStatusCode: http.StatusBadRequest, ResponseBody: &body},
	}}, nil)
	projected := handler.RewriteExecutionErrorResponse(&interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      errors.New("limited"),
	})
	if projected.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want valid rule status %d", projected.StatusCode, http.StatusBadRequest)
	}
	responseBody, ok := RewrittenErrorResponseBody(projected)
	if !ok || string(responseBody) != `{"valid":true}` {
		t.Fatalf("response body = %s, ok = %v", responseBody, ok)
	}
}

func TestExecutionErrorRewriteAppliesOnlyAfterAuthManagerExecution(t *testing.T) {
	body := map[string]any{"error": map[string]any{"code": "context_too_large"}}
	newHandler := func(t *testing.T) *BaseAPIHandler {
		t.Helper()
		manager := coreauth.NewManager(nil, &coreauth.FillFirstSelector{}, nil)
		manager.RegisterExecutor(rewriteTestExecutor{})
		if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
			ID:       "rewrite-auth",
			Provider: "rewrite-test",
			Status:   coreauth.StatusActive,
		}); errRegister != nil {
			t.Fatalf("register auth: %v", errRegister)
		}
		registry.GetGlobalRegistry().RegisterClient("rewrite-auth", "rewrite-test", []*registry.ModelInfo{{ID: "rewrite-model"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient("rewrite-auth") })
		return NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: []config.ErrorResponseRewriteRule{
			{StatusCode: http.StatusBadRequest, ResponseStatusCode: http.StatusUnprocessableEntity, ResponseBody: &body},
			{StatusCode: http.StatusBadGateway, ResponseStatusCode: http.StatusTeapot, ResponseBody: &body},
		}}, manager)
	}

	t.Run("non-stream runtime error", func(t *testing.T) {
		handler := newHandler(t)
		_, _, errMsg := handler.ExecuteWithProviders(t.Context(), []string{"rewrite-test"}, "openai", "rewrite-model", nil, "")
		if errMsg == nil || errMsg.StatusCode != http.StatusUnprocessableEntity || OriginalErrorStatusCode(errMsg) != http.StatusBadRequest {
			t.Fatalf("execution error = %#v", errMsg)
		}
	})

	t.Run("stream runtime error", func(t *testing.T) {
		handler := newHandler(t)
		data, _, errs := handler.ExecuteStreamWithProviders(t.Context(), []string{"rewrite-test"}, "openai", "rewrite-model", nil, "")
		if data != nil {
			for range data {
			}
		}
		if errs == nil {
			t.Fatal("stream error channel is nil")
		}
		errMsg := <-errs
		if errMsg == nil || errMsg.StatusCode != http.StatusUnprocessableEntity || OriginalErrorStatusCode(errMsg) != http.StatusBadRequest {
			t.Fatalf("stream execution error = %#v", errMsg)
		}
	})

	t.Run("local provider error", func(t *testing.T) {
		handler := newHandler(t)
		_, _, errMsg := handler.ExecuteWithProviders(t.Context(), nil, "openai", "rewrite-model", nil, "")
		if errMsg == nil || errMsg.StatusCode != http.StatusBadGateway || errMsg.Error == nil || errMsg.Error.Error() != "no provider configured for model rewrite-model" {
			t.Fatalf("local provider error = %#v", errMsg)
		}
	})
}

func TestStreamProtocolErrorPayloadIsSuppressedOnlyWhenRewritten(t *testing.T) {
	newHandler := func(t *testing.T, executor rewriteProtocolStreamExecutor, rules []config.ErrorResponseRewriteRule) *BaseAPIHandler {
		t.Helper()
		manager := coreauth.NewManager(nil, &coreauth.FillFirstSelector{}, nil)
		manager.RegisterExecutor(executor)
		if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
			ID:       "rewrite-protocol-auth",
			Provider: "rewrite-protocol-stream",
			Status:   coreauth.StatusActive,
		}); errRegister != nil {
			t.Fatalf("register auth: %v", errRegister)
		}
		registry.GetGlobalRegistry().RegisterClient("rewrite-protocol-auth", "rewrite-protocol-stream", []*registry.ModelInfo{{ID: "rewrite-model"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient("rewrite-protocol-auth") })
		return NewBaseAPIHandlers(&config.SDKConfig{ErrorResponseRewrites: rules}, manager)
	}

	collect := func(t *testing.T, handler *BaseAPIHandler) ([][]byte, *interfaces.ErrorMessage) {
		t.Helper()
		data, _, errs := handler.ExecuteStreamWithProviders(t.Context(), []string{"rewrite-protocol-stream"}, "claude", "rewrite-model", nil, "")
		var payloads [][]byte
		if data != nil {
			for payload := range data {
				payloads = append(payloads, payload)
			}
		}
		if errs == nil {
			t.Fatal("stream error channel is nil")
		}
		return payloads, <-errs
	}

	t.Run("matching rewrite suppresses original frame", func(t *testing.T) {
		body := map[string]any{"error": map[string]any{"message": "client-safe"}}
		handler := newHandler(t, rewriteProtocolStreamExecutor{}, []config.ErrorResponseRewriteRule{{
			StatusCode:         http.StatusBadGateway,
			MessageContains:    "claude stream protocol error",
			ResponseStatusCode: http.StatusBadRequest,
			ResponseBody:       &body,
		}})
		payloads, errMsg := collect(t, handler)
		if len(payloads) != 0 {
			t.Fatalf("leaked upstream payloads: %q", payloads)
		}
		if errMsg == nil || errMsg.StatusCode != http.StatusBadRequest || OriginalErrorStatusCode(errMsg) != http.StatusBadGateway {
			t.Fatalf("projected error = %#v", errMsg)
		}
	})

	t.Run("unmatched rewrite preserves original frame", func(t *testing.T) {
		handler := newHandler(t, rewriteProtocolStreamExecutor{}, []config.ErrorResponseRewriteRule{{
			StatusCode:         http.StatusTooManyRequests,
			ResponseStatusCode: http.StatusBadRequest,
		}})
		payloads, errMsg := collect(t, handler)
		if got := string(bytes.Join(payloads, nil)); !strings.Contains(got, "native protocol failed") {
			t.Fatalf("upstream payloads = %q", got)
		}
		if errMsg == nil || errMsg.StatusCode != http.StatusBadGateway || IsErrorResponseRewritten(errMsg) {
			t.Fatalf("original error = %#v", errMsg)
		}
	})

	for _, testCase := range []struct {
		name     string
		executor rewriteProtocolStreamExecutor
	}{
		{name: "matched frame survives different terminal error", executor: rewriteProtocolStreamExecutor{differentError: true}},
		{name: "matched frame at clean EOF remains an error", executor: rewriteProtocolStreamExecutor{closeWithoutError: true}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			handler := newHandler(t, testCase.executor, []config.ErrorResponseRewriteRule{{
				StatusCode:         http.StatusBadGateway,
				MessageContains:    "native protocol failed",
				ResponseStatusCode: http.StatusBadRequest,
			}})
			payloads, errMsg := collect(t, handler)
			if len(payloads) != 0 {
				t.Fatalf("leaked upstream payloads: %q", payloads)
			}
			if errMsg == nil || errMsg.StatusCode != http.StatusBadRequest || OriginalErrorStatusCode(errMsg) != http.StatusBadGateway {
				t.Fatalf("projected error = %#v", errMsg)
			}
		})
	}
}
