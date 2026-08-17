package executor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
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
			name:      "chatgpt image limit message",
			path:      "/backend-api/f/conversation",
			body:      `{"error":{"message":"You've hit your limit. Please try again later."}}`,
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
			forcedRefresh := false
			projected := chatGPTWebImageRequestErrorWithRefresh(upstream, func(force bool) {
				refreshes++
				forcedRefresh = force
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
			if projected.Error() != chatGPTWebImageRateLimitClientBody {
				t.Fatalf("client rate limit body = %q", projected.Error())
			}
			if refreshes != 1 {
				t.Fatalf("account info refreshes = %d, want 1", refreshes)
			}
			if forcedRefresh != test.wantQuota {
				t.Fatalf("forced account info refresh = %v, want %v", forcedRefresh, test.wantQuota)
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

func TestChatGPTWebExplicitImageQuotaErrorDoesNotRefreshWhenAutomaticRefreshDisabled(t *testing.T) {
	enabled := false
	executor := &ChatGPTWebExecutor{}
	runtime := newChatGPTWebAccountInfoRuntime(executor, &config.Config{
		ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
			AutoRefreshEnabled: &enabled,
		}},
	})
	executor.accountInfo = runtime
	t.Cleanup(runtime.close)

	projected := executor.handleChatGPTWebImageRequestError(
		"web-limit.json",
		chatGPTWebImageFailureError("You've hit your limit. Please try again later."),
	)
	var code interface{ ExecutionResultErrorCode() string }
	if !errors.As(projected, &code) || code.ExecutionResultErrorCode() != "chatgpt_web_image_quota" {
		t.Fatalf("explicit quota failure was not projected: %v", projected)
	}

	runtime.mu.Lock()
	queued := runtime.queuedWorkLocked()
	scheduled := len(runtime.scheduled)
	pending := len(runtime.pendingTriggers)
	runtime.mu.Unlock()
	if len(queued) != 0 || scheduled != 0 || pending != 0 {
		t.Fatalf("disabled quota refresh work: queue=%+v scheduled=%d pending=%d", queued, scheduled, pending)
	}
}

func TestChatGPTWebExplicitImageQuotaErrorQueuesOneRefreshWhenEnabled(t *testing.T) {
	const authID = "web-limit-queued.json"
	executor := &ChatGPTWebExecutor{}
	runtime := newChatGPTWebAccountInfoRuntime(executor, &config.Config{})
	executor.accountInfo = runtime
	t.Cleanup(runtime.close)

	for range 2 {
		executor.handleChatGPTWebImageRequestError(
			authID,
			chatGPTWebImageFailureError("You've hit your limit. Please try again later."),
		)
	}

	runtime.mu.Lock()
	queued := runtime.queuedWorkLocked()
	runtime.mu.Unlock()
	if len(queued) != 1 || !queued[0].force || !queued[0].automatic || queued[0].target.AuthID != authID {
		t.Fatalf("enabled quota refresh queue = %+v, want one forced automatic work item", queued)
	}
}

func TestChatGPTWebExplicitImageQuotaErrorPromotesScheduledRefreshWithoutDuplicating(t *testing.T) {
	now := time.Now().UTC()
	const authID = "web-limit-scheduled.json"
	executor := &ChatGPTWebExecutor{}
	runtime := newChatGPTWebAccountInfoRuntime(executor, &config.Config{})
	runtime.now = func() time.Time { return now }
	executor.accountInfo = runtime
	t.Cleanup(runtime.close)

	retryAt := now.Add(10 * time.Minute)
	runtime.mu.Lock()
	scheduled := runtime.scheduleLocked("retry:"+authID, retryAt, chatGPTWebAccountInfoWork{
		target:    chatgptwebauth.AccountInfoRefreshTarget{Name: authID, AuthID: authID},
		attempt:   2,
		automatic: true,
	})
	runtime.mu.Unlock()
	if !scheduled {
		t.Fatal("failed to schedule automatic refresh")
	}

	executor.handleChatGPTWebImageRequestError(
		authID,
		chatGPTWebImageFailureError("You've hit your limit. Please try again later."),
	)

	runtime.mu.Lock()
	retry := runtime.scheduled["retry:"+authID]
	queued := runtime.queueLengthLocked()
	scheduledCount := len(runtime.scheduledByTarget[authID])
	runtime.mu.Unlock()
	if retry == nil || !retry.due.Equal(now) || !retry.work.force {
		t.Fatalf("scheduled quota refresh = %+v, want forced refresh due now", retry)
	}
	if queued != 0 || scheduledCount != 1 {
		t.Fatalf("quota refresh was duplicated: queued=%d scheduled=%d", queued, scheduledCount)
	}
}

func TestChatGPTWebExplicitImageQuotaErrorReusesInflightRefresh(t *testing.T) {
	const authID = "web-limit-inflight.json"
	executor := &ChatGPTWebExecutor{}
	runtime := newChatGPTWebAccountInfoRuntime(executor, &config.Config{})
	executor.accountInfo = runtime
	t.Cleanup(runtime.close)

	runtime.mu.Lock()
	runtime.inflight[authID] = 1
	runtime.mu.Unlock()
	executor.handleChatGPTWebImageRequestError(
		authID,
		chatGPTWebImageFailureError("You've hit your limit. Please try again later."),
	)

	runtime.mu.Lock()
	queued := runtime.queueLengthLocked()
	pending := runtime.pendingTriggers[authID]
	scheduled := len(runtime.scheduledByTarget[authID])
	runtime.mu.Unlock()
	if queued != 0 || pending != chatGPTWebAccountInfoTriggerNone || scheduled != 0 {
		t.Fatalf("inflight quota refresh was duplicated: queued=%d pending=%d scheduled=%d", queued, pending, scheduled)
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

func TestChatGPTWebImageFailureStageSurvivesRequestNormalization(t *testing.T) {
	tests := []struct {
		name  string
		stage string
		err   error
	}{
		{
			name:  "download transport",
			stage: "download",
			err: withChatGPTWebFailureStage("download",
				newChatGPTWebAssetTransportError(context.Background(), "download", io.ErrUnexpectedEOF)),
		},
		{
			name:  "settle response",
			stage: "settle",
			err: withChatGPTWebFailureStage("settle", newChatGPTWebImageSettleError(
				newChatGPTWebStatusError(http.StatusBadGateway, "/backend-api/conversation/image-settle", nil, nil))),
		},
		{
			name:  "generic rate limit",
			stage: "settle",
			err: withChatGPTWebFailureStage("settle", newChatGPTWebStatusError(
				http.StatusTooManyRequests,
				"/backend-api/f/conversation",
				[]byte(`{"error":{"code":"rate_limit_exceeded","message":"Too many requests"}}`),
				nil,
			)),
		},
		{
			name:  "image quota",
			stage: "settle",
			err: withChatGPTWebFailureStage("settle", newChatGPTWebStatusError(
				http.StatusTooManyRequests,
				"/backend-api/f/conversation",
				[]byte(`{"error":{"code":"image_generation_limit_reached","message":"image quota exhausted"}}`),
				nil,
			)),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			projected := chatGPTWebImageRequestError(test.err)
			committed := chatGPTWebCommittedRequestError(context.Background(), projected)
			var stageProvider chatGPTWebFailureStageProvider
			if !errors.As(committed, &stageProvider) {
				t.Fatalf("normalized error has no failure stage: %v", committed)
			}
			if got := stageProvider.ChatGPTWebFailureStage(); got != test.stage {
				t.Fatalf("failure stage = %q, want %q", got, test.stage)
			}
		})
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

	limitMessage := "You've hit your limit. Please try again later."
	limit := chatGPTWebImageRequestError(chatGPTWebImageFailureError(limitMessage))
	if !errors.As(limit, &code) || code.ExecutionResultErrorCode() != "chatgpt_web_image_quota" {
		t.Fatalf("ChatGPT limit failure was not projected as image quota: %v", limit)
	}
	if !errors.As(limit, &status) || status.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("ChatGPT limit status = %v, want 429", limit)
	}
	if strings.Contains(limit.Error(), limitMessage) {
		t.Fatalf("projected quota error exposed upstream text: %v", limit)
	}
	var skip interface{ SkipAuthResult() bool }
	if !errors.As(limit, &skip) || skip.SkipAuthResult() {
		t.Fatalf("terminal quota result skipped immediate credential cooldown: %v", limit)
	}
}

func TestChatGPTWebStructuredToolRateLimitReturnsQuotaError(t *testing.T) {
	executor := &ChatGPTWebExecutor{}
	prepared := &chatGPTWebPreparedRequest{
		routeModel: "gpt-image-2",
		request: helps.ChatGPTWebRequest{
			Image: &helps.ChatGPTWebImageRequest{Prompt: "draw"},
		},
	}
	execution := &chatGPTWebImageExecution{response: &fhttp.Response{
		StatusCode: http.StatusOK,
		Body: io.NopCloser(strings.NewReader(
			"data: {\"o\":\"add\",\"v\":{\"message\":{\"author\":{\"role\":\"tool\"},\"content\":{\"content_type\":\"system_error\",\"name\":\"ChatGPTAgentToolRateLimitException\",\"text\":\"localized upstream instruction\"},\"status\":\"finished_successfully\"}}}\n\n" +
				"data: {\"v\":{\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"content_type\":\"text\",\"parts\":[\"localized assistant response\"]},\"status\":\"finished_successfully\",\"end_turn\":true}}}\n\n" +
				"data: {\"type\":\"message_stream_complete\"}\n\n" +
				"data: [DONE]\n\n",
		)),
	}}

	_, err := executor.finishChatGPTWebImage(context.Background(), nil, nil, prepared, execution)
	projected := chatGPTWebImageRequestError(err)
	var code interface{ ExecutionResultErrorCode() string }
	if !errors.As(projected, &code) || code.ExecutionResultErrorCode() != "chatgpt_web_image_quota" {
		t.Fatalf("structured tool rate limit was not projected as quota: %v", projected)
	}
	var status interface{ StatusCode() int }
	if !errors.As(projected, &status) || status.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("structured tool rate limit status = %v, want 429", projected)
	}
	if projected.Error() != chatGPTWebImageRateLimitClientBody || strings.Contains(projected.Error(), "ChatGPTAgentToolRateLimitException") {
		t.Fatalf("structured tool rate limit leaked upstream detail: %v", projected)
	}
}

func TestChatGPTWebStructuredModerationReturnsOpenAIError(t *testing.T) {
	executor := &ChatGPTWebExecutor{}
	prepared := &chatGPTWebPreparedRequest{
		routeModel: "gpt-image-2",
		request: helps.ChatGPTWebRequest{
			Image: &helps.ChatGPTWebImageRequest{Prompt: "draw"},
		},
	}
	execution := &chatGPTWebImageExecution{response: &fhttp.Response{
		StatusCode: http.StatusOK,
		Body: io.NopCloser(strings.NewReader(
			"data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"is_error\":true,\"content\":{\"content_type\":\"text\",\"parts\":[\"We’re so sorry, but the prompt may violate our guardrails around illicit or illegal activities.\"]}}}\n\n" +
				"data: {\"type\":\"message_stream_complete\"}\n\n" +
				"data: [DONE]\n\n",
		)),
	}}

	_, err := executor.finishChatGPTWebImage(context.Background(), nil, nil, prepared, execution)
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusBadRequest {
		t.Fatalf("structured moderation status = %v, want 400", err)
	}
	if err.Error() != helps.OpenAIImageModerationErrorBody {
		t.Fatalf("structured moderation body = %q", err.Error())
	}
}

func TestChatGPTWebTerminalImageModerationFailureReturnsOpenAIError(t *testing.T) {
	err := chatGPTWebCommittedRequestError(context.Background(), newChatGPTWebImageModerationResultError())
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusBadRequest {
		t.Fatalf("moderation status = %v, want 400", err)
	}
	if err.Error() != helps.OpenAIImageModerationErrorBody {
		t.Fatalf("moderation body = %q", err.Error())
	}
	var skip interface{ SkipAuthResult() bool }
	if !errors.As(err, &skip) || !skip.SkipAuthResult() {
		t.Fatalf("moderation failure affected credential health: %v", err)
	}
	var retry interface{ RetryOtherAuth() bool }
	if !errors.As(err, &retry) || retry.RetryOtherAuth() {
		t.Fatalf("moderation failure retried another credential: %v", err)
	}
}

func TestChatGPTWebTerminalImageModerationDoesNotRefreshQuota(t *testing.T) {
	executor := &ChatGPTWebExecutor{}
	runtime := newChatGPTWebAccountInfoRuntime(executor, &config.Config{})
	executor.accountInfo = runtime
	t.Cleanup(runtime.close)

	projected := executor.handleChatGPTWebImageRequestError(
		"web-moderation.json",
		newChatGPTWebImageModerationResultError(),
	)
	var status interface{ StatusCode() int }
	if !errors.As(projected, &status) || status.StatusCode() != http.StatusBadRequest {
		t.Fatalf("moderation status = %v, want 400", projected)
	}
	runtime.mu.Lock()
	queued := runtime.queueLengthLocked()
	_, cooled := runtime.ambiguousImageRecheckAfter["web-moderation.json"]
	runtime.mu.Unlock()
	if queued != 0 || cooled {
		t.Fatalf("moderation scheduled quota work: queued=%d cooled=%t", queued, cooled)
	}
}

func TestChatGPTWebEmptyImageResultKeeps502AndSchedulesCooledQuotaRecheck(t *testing.T) {
	now := time.Now().UTC()
	executor := &ChatGPTWebExecutor{}
	runtime := newChatGPTWebAccountInfoRuntime(executor, &config.Config{})
	runtime.now = func() time.Time { return now }
	executor.accountInfo = runtime
	t.Cleanup(runtime.close)

	projected := executor.handleChatGPTWebImageRequestError(
		"web-empty.json",
		newChatGPTWebImageNoOutputResultError(),
	)
	var status interface{ StatusCode() int }
	if !errors.As(projected, &status) || status.StatusCode() != http.StatusBadGateway {
		t.Fatalf("empty image result status = %v, want 502", projected)
	}
	var code interface{ ExecutionResultErrorCode() string }
	if errors.As(projected, &code) {
		t.Fatalf("empty image result received quota code %q", code.ExecutionResultErrorCode())
	}
	if projected.Error() != chatGPTWebImageNoOutputMessage {
		t.Fatalf("empty image result = %q, want %q", projected.Error(), chatGPTWebImageNoOutputMessage)
	}
	var skip interface{ SkipAuthResult() bool }
	if !errors.As(projected, &skip) || !skip.SkipAuthResult() {
		t.Fatalf("empty image result was allowed to cool the credential directly: %v", projected)
	}

	runtime.mu.Lock()
	queued := runtime.queueLengthLocked()
	deadline := runtime.ambiguousImageRecheckAfter["web-empty.json"]
	runtime.mu.Unlock()
	if queued != 1 {
		t.Fatalf("queued quota rechecks = %d, want 1", queued)
	}
	if want := now.Add(chatGPTWebAmbiguousImageRecheckCooldown); !deadline.Equal(want) {
		t.Fatalf("quota recheck cooldown = %v, want %v", deadline, want)
	}

	executor.handleChatGPTWebImageRequestError("web-empty.json", newChatGPTWebImageNoOutputResultError())
	runtime.mu.Lock()
	queued = runtime.queueLengthLocked()
	secondDeadline := runtime.ambiguousImageRecheckAfter["web-empty.json"]
	runtime.mu.Unlock()
	if queued != 1 || !secondDeadline.Equal(deadline) {
		t.Fatalf("repeated empty result changed queue/deadline: queued=%d deadline=%v", queued, secondDeadline)
	}
}

func TestChatGPTWebImagePollSlotsAreBoundedAndCancelable(t *testing.T) {
	cliproxyexecutor.ConfigureChatGPTWebImageAdmissions(1, 1, 1)
	cliproxyexecutor.ConfigureChatGPTWebImageRuntimeAdmissions(1, 1, 1)
	t.Cleanup(func() {
		cliproxyexecutor.ConfigureChatGPTWebImageAdmissions(
			config.DefaultChatGPTWebImageMaxInFlight,
			config.DefaultChatGPTWebImageAdmissionQueueSize,
			config.DefaultChatGPTWebImageMaxFinalizers,
		)
		cliproxyexecutor.ConfigureChatGPTWebImageRuntimeAdmissions(
			config.DefaultChatGPTWebImageMaxInFlight,
			config.DefaultChatGPTWebImagePollConcurrency,
			config.DefaultChatGPTWebImageMemoryFinalizerConcurrency,
		)
	})
	before := ChatGPTWebImagePollSnapshot()
	releaseFirst, err := acquireChatGPTWebImagePollSlot(context.Background())
	if err != nil {
		t.Fatalf("acquire first poll slot: %v", err)
	}
	waitCtx, cancel := context.WithCancel(context.Background())
	waitResult := make(chan error, 2)
	for range 2 {
		go func() {
			_, errAcquire := acquireChatGPTWebImagePollSlot(waitCtx)
			waitResult <- errAcquire
		}()
	}
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		return cliproxyexecutor.ChatGPTWebImagePollAdmissionSnapshot().Queued == 2
	})
	if _, errFull := acquireChatGPTWebImagePollSlot(context.Background()); errFull == nil {
		t.Fatal("acquire with full poll queue succeeded")
	}
	cancel()
	for range 2 {
		if errCanceled := <-waitResult; !errors.Is(errCanceled, context.Canceled) {
			t.Fatalf("blocked poll acquire error = %v, want context canceled", errCanceled)
		}
	}
	releaseFirst()
	releaseSecond, err := acquireChatGPTWebImagePollSlot(context.Background())
	if err != nil {
		t.Fatalf("reacquire poll slot: %v", err)
	}
	releaseSecond()
	after := ChatGPTWebImagePollSnapshot()
	if after.Limit != 1 || after.QueueLimit != 2 || after.Queued != 0 || after.PeakQueued < 2 {
		t.Fatalf("poll capacity snapshot = %#v", after)
	}
	if after.AcquireAttempts != before.AcquireAttempts+5 || after.Acquired != before.Acquired+2 ||
		after.QueueRejects != before.QueueRejects+1 || after.Canceled != before.Canceled+2 ||
		after.TimedOut != before.TimedOut {
		t.Fatalf("poll accounting before=%#v after=%#v", before, after)
	}
}

func TestChatGPTWebImageResultErrorsPreserveQuotaCredentialUpdate(t *testing.T) {
	retryAfter := 9 * time.Second
	cause := statusErr{
		code:           http.StatusTooManyRequests,
		retryAfter:     &retryAfter,
		skipAuthResult: true,
		retryOtherAuth: true,
	}
	rateLimited := &chatGPTWebImageRateLimitResultError{cause: cause}
	if !rateLimited.SkipAuthResult() {
		t.Fatal("ordinary image rate limit did not preserve SkipAuthResult")
	}
	quotaExhausted := &chatGPTWebImageQuotaResultError{cause: cause}
	if quotaExhausted.SkipAuthResult() {
		t.Fatal("explicit image quota result skipped immediate credential cooldown")
	}
	for _, projected := range []error{rateLimited, quotaExhausted} {
		if projected.Error() != chatGPTWebImageRateLimitClientBody {
			t.Fatalf("%T client error = %q", projected, projected.Error())
		}
		retry, okRetry := projected.(interface{ RetryOtherAuth() bool })
		if !okRetry || !retry.RetryOtherAuth() {
			t.Fatalf("%T RetryOtherAuth() was not forwarded", projected)
		}
	}
}
