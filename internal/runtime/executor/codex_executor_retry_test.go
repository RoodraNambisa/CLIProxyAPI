package executor

import (
	"net/http"
	"strconv"
	"testing"
	"time"

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

func itoa(v int64) string {
	return strconv.FormatInt(v, 10)
}
