package executor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

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
