package executor

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
)

func TestChatGPTWebImageRequestErrorRequiresExplicitQuotaEvidence(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		body      string
		wantQuota bool
	}{
		{
			name:      "explicit quota message",
			path:      "/backend-api/f/conversation",
			body:      `{"error":{"message":"image quota exhausted"}}`,
			wantQuota: true,
		},
		{
			name:      "explicit quota code",
			path:      "/backend-api/conversation",
			body:      `{"error":{"code":"image_generation_limit_reached"}}`,
			wantQuota: true,
		},
		{
			name: "generic conversation rate limit",
			path: "/backend-api/f/conversation",
			body: `{"error":{"code":"rate_limit_exceeded","message":"Too many requests"}}`,
		},
		{
			name: "requirements rate limit",
			path: "/backend-api/sentinel/chat-requirements",
			body: `{"error":{"message":"image quota exhausted"}}`,
		},
		{
			name: "upload rate limit",
			path: "/backend-api/files",
			body: `{"error":{"message":"Too many requests"}}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upstream := newChatGPTWebStatusError(
				http.StatusTooManyRequests,
				test.path,
				[]byte(test.body),
				fhttp.Header{"Retry-After": {"17"}},
			)
			refreshes := 0
			projected := chatGPTWebImageRequestErrorWithRefresh(upstream, func() {
				refreshes++
			})

			var model interface{ ExecutionResultModel() string }
			if !errors.As(projected, &model) || model.ExecutionResultModel() != chatgptwebauth.ImageModel {
				t.Fatalf("result model projection missing: %v", projected)
			}
			var code interface{ ExecutionResultErrorCode() string }
			hasQuotaCode := errors.As(projected, &code) && code.ExecutionResultErrorCode() == "chatgpt_web_image_quota"
			if hasQuotaCode != test.wantQuota {
				t.Fatalf("quota projection = %v, want %v: %v", hasQuotaCode, test.wantQuota, projected)
			}
			if refreshes != 1 {
				t.Fatalf("account info refreshes = %d, want 1", refreshes)
			}
			status, okStatus := projected.(interface{ StatusCode() int })
			if !okStatus || status.StatusCode() != http.StatusTooManyRequests {
				t.Fatalf("status projection missing: %v", projected)
			}
			headers, okHeaders := projected.(interface{ Headers() http.Header })
			if !okHeaders || headers.Headers().Get("Retry-After") != "17" {
				t.Fatalf("Retry-After was not preserved: %v", projected)
			}
			retryAfter, okRetryAfter := projected.(interface{ RetryAfter() *time.Duration })
			if !okRetryAfter || retryAfter.RetryAfter() == nil || *retryAfter.RetryAfter() != 17*time.Second {
				t.Fatalf("RetryAfter() was not preserved: %v", projected)
			}
		})
	}
}

func TestChatGPTWebGenericImageRateLimitPreservesCommittedMarkerWithoutQuotaCode(t *testing.T) {
	upstream := newChatGPTWebStatusError(
		http.StatusTooManyRequests,
		"/backend-api/f/conversation",
		[]byte(`{"error":{"code":"rate_limit_exceeded","message":"Too many requests"}}`),
		fhttp.Header{"Retry-After": {"23"}},
	)
	projected := chatGPTWebImageRequestError(upstream)
	committed := chatGPTWebCommittedRequestError(context.Background(), projected)

	var marker interface{ RequestCommitted() bool }
	if !errors.As(committed, &marker) || !marker.RequestCommitted() {
		t.Fatalf("committed marker missing: %v", committed)
	}
	var code interface{ ExecutionResultErrorCode() string }
	if errors.As(committed, &code) && code.ExecutionResultErrorCode() != "" {
		t.Fatalf("ordinary rate limit received quota code %q", code.ExecutionResultErrorCode())
	}
	headers, okHeaders := committed.(interface{ Headers() http.Header })
	if !okHeaders || headers.Headers().Get("Retry-After") != "23" {
		t.Fatalf("Retry-After was not preserved: %v", committed)
	}
	status, okStatus := committed.(interface{ StatusCode() int })
	if !okStatus || status.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("status projection missing: %v", committed)
	}
	retryAfter, okRetryAfter := committed.(interface{ RetryAfter() *time.Duration })
	if !okRetryAfter || retryAfter.RetryAfter() == nil || *retryAfter.RetryAfter() != 23*time.Second {
		t.Fatalf("RetryAfter() was not preserved: %v", committed)
	}
}

func TestChatGPTWebTerminalImageFailureProjectsExplicitQuotaEvidence(t *testing.T) {
	projected := chatGPTWebImageRequestError(chatGPTWebImageFailureError(
		"image generation limit reached; no remaining image credits",
	))
	var code interface{ ExecutionResultErrorCode() string }
	if !errors.As(projected, &code) || code.ExecutionResultErrorCode() != "chatgpt_web_image_quota" {
		t.Fatalf("terminal quota failure was not projected: %v", projected)
	}
	var status interface{ StatusCode() int }
	if !errors.As(projected, &status) || status.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("terminal quota status = %v, want 429", projected)
	}

	generic := chatGPTWebImageRequestError(chatGPTWebImageFailureError("image generation failed"))
	if errors.As(generic, &code) {
		t.Fatalf("generic terminal failure received quota projection: %v", generic)
	}
	if !errors.As(generic, &status) || status.StatusCode() != http.StatusBadGateway {
		t.Fatalf("generic terminal status = %v, want 502", generic)
	}
}

func TestChatGPTWebImagePollSlotsAreBoundedAndCancelable(t *testing.T) {
	slots := make(chan struct{}, 1)
	if err := acquireChatGPTWebImagePollSlot(context.Background(), slots); err != nil {
		t.Fatalf("acquire first poll slot: %v", err)
	}

	waitCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := acquireChatGPTWebImagePollSlot(waitCtx, slots); !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked poll acquire error = %v, want context canceled", err)
	}

	releaseChatGPTWebImagePollSlot(slots)
	if err := acquireChatGPTWebImagePollSlot(context.Background(), slots); err != nil {
		t.Fatalf("reacquire poll slot: %v", err)
	}
	releaseChatGPTWebImagePollSlot(slots)
}

func TestChatGPTWebImageResultErrorsForwardAuthRetryClassification(t *testing.T) {
	retryAfter := 9 * time.Second
	cause := statusErr{
		code:           http.StatusTooManyRequests,
		retryAfter:     &retryAfter,
		skipAuthResult: true,
		retryOtherAuth: true,
	}
	for _, projected := range []error{
		&chatGPTWebImageRateLimitResultError{cause: cause},
		&chatGPTWebImageQuotaResultError{cause: cause},
	} {
		skip, okSkip := projected.(interface{ SkipAuthResult() bool })
		if !okSkip || !skip.SkipAuthResult() {
			t.Fatalf("%T SkipAuthResult() was not forwarded", projected)
		}
		retry, okRetry := projected.(interface{ RetryOtherAuth() bool })
		if !okRetry || !retry.RetryOtherAuth() {
			t.Fatalf("%T RetryOtherAuth() was not forwarded", projected)
		}
	}
}
