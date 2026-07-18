package executor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestParseCodexRetryAfter(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)

	t.Run("resets_in_seconds", func(t *testing.T) {
		body := []byte(`{"error":{"type":"usage_limit_reached","resets_in_seconds":123}}`)
		retryAfter := parseCodexRetryAfter(http.StatusTooManyRequests, body, now)
		if retryAfter == nil {
			t.Fatalf("expected retryAfter, got nil")
		}
		if *retryAfter != 123*time.Second {
			t.Fatalf("retryAfter = %v, want %v", *retryAfter, 123*time.Second)
		}
	})

	t.Run("prefers resets_at", func(t *testing.T) {
		resetAt := now.Add(5 * time.Minute).Unix()
		body := []byte(`{"error":{"type":"usage_limit_reached","resets_at":` + itoa(resetAt) + `,"resets_in_seconds":1}}`)
		retryAfter := parseCodexRetryAfter(http.StatusTooManyRequests, body, now)
		if retryAfter == nil {
			t.Fatalf("expected retryAfter, got nil")
		}
		if *retryAfter != 5*time.Minute {
			t.Fatalf("retryAfter = %v, want %v", *retryAfter, 5*time.Minute)
		}
	})

	t.Run("fallback when resets_at is past", func(t *testing.T) {
		resetAt := now.Add(-1 * time.Minute).Unix()
		body := []byte(`{"error":{"type":"usage_limit_reached","resets_at":` + itoa(resetAt) + `,"resets_in_seconds":77}}`)
		retryAfter := parseCodexRetryAfter(http.StatusTooManyRequests, body, now)
		if retryAfter == nil {
			t.Fatalf("expected retryAfter, got nil")
		}
		if *retryAfter != 77*time.Second {
			t.Fatalf("retryAfter = %v, want %v", *retryAfter, 77*time.Second)
		}
	})

	t.Run("non-429 status code", func(t *testing.T) {
		body := []byte(`{"error":{"type":"usage_limit_reached","resets_in_seconds":30}}`)
		if got := parseCodexRetryAfter(http.StatusBadRequest, body, now); got != nil {
			t.Fatalf("expected nil for non-429, got %v", *got)
		}
	})

	t.Run("non usage_limit_reached error type", func(t *testing.T) {
		body := []byte(`{"error":{"type":"server_error","resets_in_seconds":30}}`)
		if got := parseCodexRetryAfter(http.StatusTooManyRequests, body, now); got != nil {
			t.Fatalf("expected nil for non-usage_limit_reached, got %v", *got)
		}
	})
}

func TestNewCodexStatusErrTreatsCapacityAsRetryableRateLimit(t *testing.T) {
	body := []byte(`{"error":{"message":"Selected model is at capacity. Please try a different model."}}`)

	err := newCodexStatusErr(http.StatusBadRequest, body)

	if got := err.StatusCode(); got != http.StatusTooManyRequests {
		t.Fatalf("status code = %d, want %d", got, http.StatusTooManyRequests)
	}
	if err.RetryAfter() != nil {
		t.Fatalf("expected nil explicit retryAfter for capacity fallback, got %v", *err.RetryAfter())
	}
}

func TestNewCodexStatusErrClassifiesKnownFailures(t *testing.T) {
	testCases := []struct {
		name       string
		statusCode int
		body       []byte
		wantCode   string
		wantType   string
	}{
		{
			name:       "context too large by message",
			statusCode: http.StatusBadRequest,
			body:       []byte(`{"error":{"message":"maximum context length exceeded"}}`),
			wantCode:   "context_too_large",
			wantType:   "invalid_request_error",
		},
		{
			name:       "invalid thinking signature",
			statusCode: http.StatusBadRequest,
			body:       []byte(`{"error":{"message":"invalid signature in thinking block"}}`),
			wantCode:   "thinking_signature_invalid",
			wantType:   "invalid_request_error",
		},
		{
			name:       "previous response missing",
			statusCode: http.StatusBadRequest,
			body:       []byte(`{"error":{"message":"previous_response_id was not found"}}`),
			wantCode:   "previous_response_not_found",
			wantType:   "invalid_request_error",
		},
		{
			name:       "auth unavailable",
			statusCode: http.StatusUnauthorized,
			body:       []byte(`{"error":{"message":"invalid or expired token"}}`),
			wantCode:   "auth_unavailable",
			wantType:   "authentication_error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := newCodexStatusErr(tc.statusCode, tc.body)
			body := []byte(err.Error())
			if got := gjson.GetBytes(body, "error.code").String(); got != tc.wantCode {
				t.Fatalf("error.code = %q, want %q; body=%s", got, tc.wantCode, body)
			}
			if got := gjson.GetBytes(body, "error.type").String(); got != tc.wantType {
				t.Fatalf("error.type = %q, want %q; body=%s", got, tc.wantType, body)
			}
		})
	}
}

func TestNewCodexStatusErrContextTooLargeIsRequestScoped(t *testing.T) {
	testCases := []struct {
		name       string
		statusCode int
		body       []byte
	}{
		{
			name:       "bad request message",
			statusCode: http.StatusBadRequest,
			body:       []byte(`{"error":{"message":"maximum context length exceeded"}}`),
		},
		{
			name:       "payload too large code",
			statusCode: http.StatusRequestEntityTooLarge,
			body:       []byte(`{"error":{"message":"request too large","code":"context_length_exceeded"}}`),
		},
		{
			name:       "rate limit status with request code",
			statusCode: http.StatusTooManyRequests,
			body:       []byte(`{"error":{"message":"maximum context length exceeded","type":"invalid_request_error","code":"context_too_large"}}`),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := newCodexStatusErr(testCase.statusCode, testCase.body)
			if got := err.StatusCode(); got != testCase.statusCode {
				t.Fatalf("status code = %d, want %d", got, testCase.statusCode)
			}
			if !err.SkipAuthResult() {
				t.Fatal("SkipAuthResult = false, want true")
			}
			if err.RetryOtherAuth() {
				t.Fatal("RetryOtherAuth = true, want false")
			}
			if got := gjson.Get(err.Error(), "error.code").String(); got != "context_too_large" {
				t.Fatalf("error.code = %q, want context_too_large; body=%s", got, err.Error())
			}
		})
	}
}

func TestNewCodexStatusErrKeepsOrdinaryTokenRateLimitRetryable(t *testing.T) {
	err := newCodexStatusErr(
		http.StatusTooManyRequests,
		[]byte(`{"error":{"message":"too many tokens per minute","code":"rate_limit_exceeded"}}`),
	)
	if err.SkipAuthResult() {
		t.Fatal("SkipAuthResult = true, want ordinary 429 to remain auth-retryable")
	}
	if got := gjson.Get(err.Error(), "error.code").String(); got != "rate_limit_exceeded" {
		t.Fatalf("error.code = %q, want rate_limit_exceeded; body=%s", got, err.Error())
	}
}

func TestCodexStreamStatusErrContextTooLargeIsRequestScoped(t *testing.T) {
	for _, statusCode := range []int{http.StatusBadRequest, http.StatusRequestEntityTooLarge, http.StatusTooManyRequests} {
		err := codexStreamStatusErr(
			statusCode,
			"maximum context length exceeded",
			"context_length_exceeded",
			"invalid_request_error",
			nil,
		)
		if got := err.StatusCode(); got != statusCode {
			t.Fatalf("status %d error status = %d, want preserved", statusCode, got)
		}
		if !err.SkipAuthResult() {
			t.Fatalf("status %d SkipAuthResult = false, want true", statusCode)
		}
		if err.RetryOtherAuth() {
			t.Fatalf("status %d RetryOtherAuth = true, want false", statusCode)
		}
	}
	if got := codexStreamErrorStatus("context_length_exceeded", http.StatusInternalServerError); got != http.StatusBadRequest {
		t.Fatalf("stream context status = %d, want 400", got)
	}
	terminalErr, ok := codexTerminalStreamError([]byte(`{"type":"error","error":{"message":"maximum context length exceeded","type":"invalid_request_error","code":"context_length_exceeded"}}`))
	if !ok {
		t.Fatal("context stream event was not recognized as terminal error")
	}
	if terminalErr.StatusCode() != http.StatusBadRequest || !terminalErr.SkipAuthResult() {
		t.Fatalf("terminal stream error = %#v, want request-scoped status 400", terminalErr)
	}
}

func TestCodexContextTooLargeStopsCredentialAndRequestRetry(t *testing.T) {
	testCases := []struct {
		name          string
		statusCode    int
		body          string
		contentType   string
		streamOnly    bool
		wantStatus    int
		wantErrorCode string
	}{
		{
			name:          "bad_request",
			statusCode:    http.StatusBadRequest,
			body:          `{"error":{"message":"maximum context length exceeded"}}`,
			wantStatus:    http.StatusBadRequest,
			wantErrorCode: "context_too_large",
		},
		{
			name:          "payload_too_large",
			statusCode:    http.StatusRequestEntityTooLarge,
			body:          `{"error":{"message":"request too large"}}`,
			wantStatus:    http.StatusRequestEntityTooLarge,
			wantErrorCode: "context_too_large",
		},
		{
			name:          "upstream_429",
			statusCode:    http.StatusTooManyRequests,
			body:          `{"error":{"message":"maximum context length exceeded","type":"invalid_request_error","code":"context_too_large"}}`,
			wantStatus:    http.StatusTooManyRequests,
			wantErrorCode: "context_too_large",
		},
		{
			name:          "sse_terminal_error",
			statusCode:    http.StatusOK,
			body:          "data: {\"type\":\"error\",\"error\":{\"message\":\"maximum context length exceeded\",\"type\":\"invalid_request_error\",\"code\":\"context_length_exceeded\"}}\n\n",
			contentType:   "text/event-stream",
			streamOnly:    true,
			wantStatus:    http.StatusBadRequest,
			wantErrorCode: "context_length_exceeded",
		},
	}

	for _, testCase := range testCases {
		streamModes := []bool{false, true}
		if testCase.streamOnly {
			streamModes = []bool{true}
		}
		for _, stream := range streamModes {
			name := testCase.name + "_non_stream"
			if stream {
				name = testCase.name + "_stream"
			}
			t.Run(name, func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					contentType := testCase.contentType
					if contentType == "" {
						contentType = "application/json"
					}
					w.Header().Set("Content-Type", contentType)
					w.WriteHeader(testCase.statusCode)
					_, _ = w.Write([]byte(testCase.body))
				}))
				defer server.Close()

				manager := cliproxyauth.NewManager(nil, nil, nil)
				manager.SetRetryConfig(2, 0, 0)
				manager.RegisterExecutor(NewCodexExecutor(&config.Config{}))
				model := fmt.Sprintf("codex-context-%s-%t", testCase.name, stream)
				authIDs := []string{model + "-a", model + "-b"}
				for _, authID := range authIDs {
					auth := &cliproxyauth.Auth{
						ID:       authID,
						Provider: "codex",
						Status:   cliproxyauth.StatusActive,
						Attributes: map[string]string{
							"api_key":  "test",
							"base_url": server.URL,
						},
					}
					if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
						t.Fatalf("register auth %s: %v", authID, errRegister)
					}
					registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: model}})
				}
				t.Cleanup(func() {
					for _, authID := range authIDs {
						registry.GetGlobalRegistry().UnregisterClient(authID)
					}
				})

				request := cliproxyexecutor.Request{
					Model:   model,
					Payload: []byte(fmt.Sprintf(`{"model":%q,"input":"hello"}`, model)),
				}
				opts := cliproxyexecutor.Options{
					Stream:          stream,
					OriginalRequest: request.Payload,
					SourceFormat:    sdktranslator.FromString("codex"),
				}
				var errExecute error
				if stream {
					_, errExecute = manager.ExecuteStream(context.Background(), []string{"codex"}, request, opts)
				} else {
					_, errExecute = manager.Execute(context.Background(), []string{"codex"}, request, opts)
				}
				if errExecute == nil {
					t.Fatal("execution error = nil, want context error")
				}
				statusError, ok := errExecute.(interface{ StatusCode() int })
				if !ok || statusError.StatusCode() != testCase.wantStatus {
					t.Fatalf("execution error = %T %v, want status %d", errExecute, errExecute, testCase.wantStatus)
				}
				if !strings.Contains(errExecute.Error(), testCase.wantErrorCode) {
					t.Fatalf("execution error = %v, want %s", errExecute, testCase.wantErrorCode)
				}
				if got := calls.Load(); got != 1 {
					t.Fatalf("upstream calls = %d, want 1 without credential or request retry", got)
				}
				for _, authID := range authIDs {
					current, okAuth := manager.GetByID(authID)
					if !okAuth || current == nil {
						t.Fatalf("auth %s not found", authID)
					}
					if current.Unavailable || current.Status == cliproxyauth.StatusError || current.LastError != nil || len(current.ModelStates) != 0 {
						t.Fatalf("auth %s state = %+v, want no recorded failure or cooldown", authID, current)
					}
				}
			})
		}
	}
}

func itoa(v int64) string {
	return strconv.FormatInt(v, 10)
}
