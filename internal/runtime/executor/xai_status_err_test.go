package executor

import (
	"net/http"
	"testing"
	"time"
)

func TestXAIStatusErr_FreeUsageExhaustedSets24hRetryAfter(t *testing.T) {
	body := []byte(`{"code":"subscription:free-usage-exhausted","error":"You've used all the included free usage for model grok-4.5-build-free for now. Usage resets over a rolling 24-hour window — tokens (actual/limit): 1065387/1000000."}`)
	err := xaiStatusErr(http.StatusTooManyRequests, body)
	if err.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", err.StatusCode())
	}
	if err.RetryAfter() == nil {
		t.Fatal("expected RetryAfter for free-usage-exhausted")
	}
	if *err.RetryAfter() != 24*time.Hour {
		t.Fatalf("RetryAfter = %v, want 24h", *err.RetryAfter())
	}
}

func TestXAIStatusErr_Generic429WithoutHeaderHasNoRetryAfter(t *testing.T) {
	body := []byte(`{"code":"rate_limit","error":"too many requests"}`)
	err := xaiStatusErr(http.StatusTooManyRequests, body)
	if err.RetryAfter() != nil {
		t.Fatalf("expected nil RetryAfter for generic 429, got %v", *err.RetryAfter())
	}
}

func TestXAIStatusErrFromResponseUsesRetryAfterHeader(t *testing.T) {
	now := time.Date(2026, time.July, 11, 12, 0, 0, 0, time.UTC)
	body := []byte(`{"code":"rate_limit","error":"too many requests"}`)

	t.Run("delta seconds", func(t *testing.T) {
		err := xaiStatusErrFromResponse(http.StatusTooManyRequests, body, http.Header{"Retry-After": []string{"120"}}, now)
		if err.RetryAfter() == nil || *err.RetryAfter() != 2*time.Minute {
			t.Fatalf("RetryAfter = %v, want 2m", err.RetryAfter())
		}
	})

	t.Run("http date", func(t *testing.T) {
		retryAt := now.Add(5 * time.Minute).Format(http.TimeFormat)
		err := xaiStatusErrFromResponse(http.StatusTooManyRequests, body, http.Header{"Retry-After": []string{retryAt}}, now)
		if err.RetryAfter() == nil || *err.RetryAfter() != 5*time.Minute {
			t.Fatalf("RetryAfter = %v, want 5m", err.RetryAfter())
		}
	})

	t.Run("free usage remains authoritative", func(t *testing.T) {
		freeBody := []byte(`{"code":"subscription:free-usage-exhausted","error":"included free usage exhausted"}`)
		err := xaiStatusErrFromResponse(http.StatusTooManyRequests, freeBody, http.Header{"Retry-After": []string{"60"}}, now)
		if err.RetryAfter() == nil || *err.RetryAfter() != 24*time.Hour {
			t.Fatalf("RetryAfter = %v, want 24h", err.RetryAfter())
		}
	})
}

func TestParseXAIWebsocketErrorUsesRetryAfterHeader(t *testing.T) {
	err, ok := parseXAIWebsocketError([]byte(`{"type":"error","status":429,"error":{"code":"rate_limit","message":"too many requests"},"headers":{"retry-after":"90"}}`))
	if !ok {
		t.Fatal("parseXAIWebsocketError() did not recognize error")
	}
	retryable, ok := err.(interface{ RetryAfter() *time.Duration })
	if !ok {
		t.Fatalf("error type = %T, want RetryAfter support", err)
	}
	if retryable.RetryAfter() == nil || *retryable.RetryAfter() != 90*time.Second {
		t.Fatalf("RetryAfter = %v, want 90s", retryable.RetryAfter())
	}
}

func TestXAITerminalStreamErrorPreservesFreeUsageCooldown(t *testing.T) {
	event := []byte(`{"type":"response.failed","response":{"error":{"code":"subscription:free-usage-exhausted","message":"You've used all the included free usage"}}}`)
	err, ok := xaiTerminalStreamError(event)
	if !ok {
		t.Fatal("xaiTerminalStreamError() did not recognize response.failed")
	}
	if err.StatusCode() != http.StatusTooManyRequests || err.RetryAfter() == nil || *err.RetryAfter() != 24*time.Hour {
		t.Fatalf("terminal error = %#v", err)
	}
}

func TestXAIStatusErr_Non429Unchanged(t *testing.T) {
	body := []byte(`{"error":"nope"}`)
	err := xaiStatusErr(http.StatusBadRequest, body)
	if err.RetryAfter() != nil {
		t.Fatalf("expected nil RetryAfter for 400, got %v", *err.RetryAfter())
	}
}
