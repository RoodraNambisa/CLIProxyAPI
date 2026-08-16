package executor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func newChatGPTWebStreamCleanupTest(t *testing.T) (*ChatGPTWebExecutor, *chatGPTWebPreparedRequest, func(), <-chan struct{}, *atomic.Int32) {
	t.Helper()
	executor := &ChatGPTWebExecutor{streamInitialWait: time.Hour}
	prepared := &chatGPTWebPreparedRequest{routeModel: "gpt-5", responseFormat: sdktranslator.FormatCodex}
	var calls atomic.Int32
	done := make(chan struct{}, 1)
	cleanup := func() {
		calls.Add(1)
		select {
		case done <- struct{}{}:
		default:
		}
	}
	return executor, prepared, cleanup, done, &calls
}

func waitForChatGPTWebStreamCleanup(t *testing.T, done <-chan struct{}, calls *atomic.Int32) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream cleanup did not run")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("stream cleanup calls = %d, want 1", got)
	}
}

func TestChatGPTWebDeferredStreamCleanupWaitsForWorkerAndConsumer(t *testing.T) {
	t.Run("worker completes before consumer", func(t *testing.T) {
		executor, prepared, cleanup, done, calls := newChatGPTWebStreamCleanupTest(t)
		result := executor.streamDeferredChatGPTWebResponse(
			t.Context(), nil, nil, prepared, nil, nil, nil, nil, false, cleanup,
			func() ([]byte, error) {
				return buildChatGPTWebCompletedEvent("gpt-5", chatGPTWebTextResult{Text: "done"}), nil
			},
		)
		first := <-result.Chunks
		if !cliproxyexecutor.IsBootstrapCommitStreamChunk(first) {
			t.Fatalf("first chunk = %#v, want bootstrap", first)
		}
		second, ok := <-result.Chunks
		if !ok || len(second.Payload) == 0 || second.Err != nil {
			t.Fatalf("second chunk = %#v, want translated payload", second)
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("cleanup ran before consumer drained stream: %d", got)
		}
		for range result.Chunks {
		}
		waitForChatGPTWebStreamCleanup(t, done, calls)
	})

	t.Run("consumer cancellation waits for worker", func(t *testing.T) {
		executor, prepared, cleanup, done, calls := newChatGPTWebStreamCleanupTest(t)
		ctx, cancel := context.WithCancel(t.Context())
		workerStarted := make(chan struct{})
		releaseWorker := make(chan struct{})
		result := executor.streamDeferredChatGPTWebResponse(
			ctx, nil, nil, prepared, nil, nil, nil, nil, false, cleanup,
			func() ([]byte, error) {
				close(workerStarted)
				<-releaseWorker
				return buildChatGPTWebCompletedEvent("gpt-5", chatGPTWebTextResult{Text: "done"}), nil
			},
		)
		if first := <-result.Chunks; !cliproxyexecutor.IsBootstrapCommitStreamChunk(first) {
			t.Fatalf("first chunk = %#v, want bootstrap", first)
		}
		<-workerStarted
		cancel()
		for range result.Chunks {
		}
		if got := calls.Load(); got != 0 {
			t.Fatalf("cleanup ran while worker was still active: %d", got)
		}
		close(releaseWorker)
		waitForChatGPTWebStreamCleanup(t, done, calls)
	})

	t.Run("bootstrap delivery failure", func(t *testing.T) {
		executor, prepared, cleanup, done, calls := newChatGPTWebStreamCleanupTest(t)
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		var workCalls atomic.Int32
		result := executor.streamDeferredChatGPTWebResponse(
			ctx, nil, nil, prepared, nil, nil, nil, nil, false, cleanup,
			func() ([]byte, error) {
				workCalls.Add(1)
				return nil, nil
			},
		)
		waitForChatGPTWebStreamCleanup(t, done, calls)
		for range result.Chunks {
		}
		if got := workCalls.Load(); got != 0 {
			t.Fatalf("work calls after bootstrap delivery failure = %d", got)
		}
	})

	t.Run("image lifecycle release", func(t *testing.T) {
		cliproxyexecutor.ConfigureChatGPTWebImageAdmissions(1, 0, 1)
		t.Cleanup(func() { cliproxyexecutor.ConfigureChatGPTWebImageAdmissions(64, 64, 8) })
		releaseLifecycle, err := cliproxyexecutor.AcquireChatGPTWebImageExecution(t.Context(), 0)
		if err != nil {
			t.Fatalf("AcquireChatGPTWebImageExecution() error = %v", err)
		}
		executor, prepared, _, _, _ := newChatGPTWebStreamCleanupTest(t)
		done := make(chan struct{}, 1)
		var calls atomic.Int32
		cleanup := func() {
			calls.Add(1)
			releaseLifecycle()
			select {
			case done <- struct{}{}:
			default:
			}
		}
		result := executor.streamDeferredChatGPTWebResponse(
			t.Context(), nil, nil, prepared, nil, nil, nil, nil, false, cleanup,
			func() ([]byte, error) {
				return buildChatGPTWebCompletedEvent("gpt-image-2", chatGPTWebTextResult{Text: "done"}), nil
			},
		)
		for range result.Chunks {
		}
		waitForChatGPTWebStreamCleanup(t, done, &calls)
		if snapshot := cliproxyexecutor.ChatGPTWebImageExecutionAdmissionSnapshot(); snapshot.Active != 0 || snapshot.Queued != 0 {
			t.Fatalf("stream image lifecycle leaked: %#v", snapshot)
		}
	})
}
