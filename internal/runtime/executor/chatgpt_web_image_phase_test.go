package executor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestChatGPTWebRequirementsObservesSubphases(t *testing.T) {
	for _, withObserver := range []bool{false, true} {
		name := "without observer"
		if withObserver {
			name = "with observer and turnstile"
		}
		t.Run(name, func(t *testing.T) {
			server := newChatGPTWebSentinelRequirementsServer(t, chatGPTWebSentinelRuntimeTestSDK, func(string) map[string]any {
				if withObserver {
					prepare := chatGPTWebSentinelObserverPrepare()
					prepare["turnstile"] = map[string]any{"required": true, "dx": "test-challenge"}
					return prepare
				}
				return map[string]any{"prepare_token": "prepare"}
			}, func(map[string]any) map[string]any { return map[string]any{"token": "requirements"} })
			defer server.Close()
			executor, _ := newChatGPTWebSentinelTestExecutor(server.URL, chatGPTWebSentinelRuntimeTestSDK)
			defer func() { _ = executor.Close() }()
			client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseIdleConnections()
			observer := &chatGPTWebImagePhaseTestObserver{}
			ctx := cliproxyexecutor.WithRequestPhaseObserver(t.Context(), observer)
			if _, err := executor.chatGPTWebRequirements(ctx, client, credential); err != nil {
				t.Fatal(err)
			}
			for _, phase := range []string{
				cliproxyexecutor.ImagePhaseRequirementsBootstrap, cliproxyexecutor.ImagePhaseRequirementsLocal,
				cliproxyexecutor.ImagePhaseRequirementsPrepare, cliproxyexecutor.ImagePhaseRequirementsObserver,
				cliproxyexecutor.ImagePhaseRequirementsFinalize,
			} {
				if got := observer.count(phase); got != 1 {
					t.Errorf("%s count = %d, want 1", phase, got)
				}
			}
			if got := observer.count(cliproxyexecutor.ImagePhaseRequirementsParse); got != 2 {
				t.Errorf("parse count = %d, want 2", got)
			}
			wantOptional := 0
			if withObserver {
				wantOptional = 1
			}
			for _, phase := range []string{
				cliproxyexecutor.ImagePhaseRequirementsTurnstile, cliproxyexecutor.ImagePhaseRequirementsSnapshot,
				cliproxyexecutor.ImagePhaseRequirementsCleanup,
			} {
				if got := observer.count(phase); got != wantOptional {
					t.Errorf("%s count = %d, want %d", phase, got, wantOptional)
				}
			}
			if got := observer.count(cliproxyexecutor.ImagePhaseRequirementsProof); got != 0 {
				t.Errorf("unused proof phase count = %d", got)
			}
		})
	}
}

func TestChatGPTWebRequirementsTracksBlockedBodyAndCancellation(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "<html>")
		w.(http.Flusher).Flush()
		close(entered)
		<-release
	}))
	defer server.Close()
	defer close(release)
	executor, _ := newChatGPTWebSentinelTestExecutor(server.URL, chatGPTWebSentinelRuntimeTestSDK)
	defer func() { _ = executor.Close() }()
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	observer := &chatGPTWebImagePhaseTestObserver{}
	ctx, cancel := context.WithCancel(cliproxyexecutor.WithRequestPhaseObserver(t.Context(), observer))
	defer cancel()
	registry := newChatGPTWebImageTaskRegistry(time.Now)
	ctx, handle := registry.begin(ctx, "test-auth")
	defer handle.finish()
	finished := make(chan error, 1)
	go func() {
		_, err := executor.chatGPTWebRequirements(ctx, client, credential)
		finished <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("bootstrap did not reach response body")
	}
	snapshot := registry.snapshot()
	if len(snapshot.Tasks) != 1 || snapshot.Tasks[0].Stage != cliproxyexecutor.ImagePhaseRequirementsBootstrap {
		t.Fatalf("blocked task stage = %+v", snapshot.Tasks)
	}
	if got := observer.count(cliproxyexecutor.ImagePhaseRequirementsBootstrap); got != 0 {
		t.Fatalf("unfinished HTTP body reported as completed: count %d", got)
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled requirements did not exit")
	}
	if got := observer.count(cliproxyexecutor.ImagePhaseRequirementsBootstrap); got != 1 {
		t.Fatalf("canceled bootstrap count = %d, want 1", got)
	}
	if got := observer.count(cliproxyexecutor.ImagePhaseRequirementsLocal); got != 0 {
		t.Fatalf("unreached local phase count = %d, want 0", got)
	}
}

type chatGPTWebImagePhaseTestObserver struct {
	mu     sync.Mutex
	counts map[string]int
}

func (observer *chatGPTWebImagePhaseTestObserver) ObserveRequestPhase(name string, _ time.Duration) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.counts == nil {
		observer.counts = make(map[string]int)
	}
	observer.counts[name]++
}

func (observer *chatGPTWebImagePhaseTestObserver) count(name string) int {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.counts[name]
}

func TestBeginChatGPTWebImageObservesInputUploadOnceOnEarlyExit(t *testing.T) {
	t.Run("upload failure", func(t *testing.T) {
		observer := &chatGPTWebImagePhaseTestObserver{}
		ctx := cliproxyexecutor.WithRequestPhaseObserver(context.Background(), observer)
		prepared := &chatGPTWebPreparedRequest{request: helps.ChatGPTWebRequest{Image: &helps.ChatGPTWebImageRequest{
			Model: "gpt-image-2", Prompt: "draw", Images: []string{"data:image/png;base64,not-base64"},
		}}}
		if _, err := (&ChatGPTWebExecutor{}).beginChatGPTWebImage(ctx, nil, nil, prepared); err == nil {
			t.Fatal("beginChatGPTWebImage() error = nil")
		}
		if got := observer.count(cliproxyexecutor.ImagePhaseInputUpload); got != 1 {
			t.Fatalf("input upload phase count = %d, want 1", got)
		}
	})

	t.Run("zero inputs", func(t *testing.T) {
		observer := &chatGPTWebImagePhaseTestObserver{}
		ctx, cancel := context.WithCancel(cliproxyexecutor.WithRequestPhaseObserver(context.Background(), observer))
		cancel()
		prepared := &chatGPTWebPreparedRequest{request: helps.ChatGPTWebRequest{Image: &helps.ChatGPTWebImageRequest{
			Model: "gpt-image-2", Prompt: "draw",
		}}}
		executor := &ChatGPTWebExecutor{runtimeBaseURL: "https://chatgpt.com"}
		client, credential, errClient := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
		if errClient != nil {
			t.Fatalf("newRuntimeClient() error = %v", errClient)
		}
		defer client.CloseIdleConnections()
		if _, err := executor.beginChatGPTWebImage(ctx, client, credential, prepared); err == nil {
			t.Fatal("beginChatGPTWebImage() error = nil")
		}
		if got := observer.count(cliproxyexecutor.ImagePhaseInputUpload); got != 1 {
			t.Fatalf("input upload phase count = %d, want 1", got)
		}
	})
}
