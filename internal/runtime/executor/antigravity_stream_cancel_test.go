package executor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

type antigravityStreamRoundTripperFunc func(*http.Request) (*http.Response, error)

func (f antigravityStreamRoundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type antigravityCloseSignalBody struct {
	io.Reader
	closed chan struct{}
	once   sync.Once
}

func (b *antigravityCloseSignalBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

type antigravityCancelingErrorBody struct {
	cancel context.CancelFunc
}

func (b *antigravityCancelingErrorBody) Read([]byte) (int, error) {
	b.cancel()
	return 0, context.Canceled
}

func (b *antigravityCancelingErrorBody) Close() error { return nil }

func antigravityStreamTestAuth() *cliproxyauth.Auth {
	return &cliproxyauth.Auth{
		ID: "auth-stream-cancel",
		Metadata: map[string]any{
			"access_token": "token",
			"project_id":   "project-1",
			"expired":      time.Now().Add(time.Hour).Format(time.RFC3339),
		},
	}
}

func antigravityStreamTestRequest(model string) cliproxyexecutor.Request {
	return cliproxyexecutor.Request{
		Model:   model,
		Payload: []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`),
	}
}

func TestAntigravityExecuteStreamCancellationUnblocksProducer(t *testing.T) {
	body := &antigravityCloseSignalBody{
		Reader: strings.NewReader("data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"ok\"}]}}]}}\n\n"),
		closed: make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, "cliproxy.roundtripper", antigravityStreamRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body,
		}, nil
	}))

	exec := NewAntigravityExecutor(nil)
	result, err := exec.ExecuteStream(ctx, antigravityStreamTestAuth(), antigravityStreamTestRequest("gemini-3-flash"), cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatAntigravity,
		Stream:       true,
	})
	if err != nil {
		cancel()
		t.Fatalf("ExecuteStream: %v", err)
	}

	cancel()
	select {
	case <-body.closed:
	case <-time.After(time.Second):
		t.Fatal("stream response body was not closed after cancellation")
	}
	select {
	case _, ok := <-result.Chunks:
		if ok {
			t.Fatal("stream channel remained open after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("stream channel did not close after cancellation")
	}
}

func TestAntigravityInternalStreamPreservesCancellationError(t *testing.T) {
	for i := 0; i < 32; i++ {
		baseCtx, cancel := context.WithCancel(context.Background())
		ctx := context.WithValue(baseCtx, "cliproxy.roundtripper", antigravityStreamRoundTripperFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       &antigravityCancelingErrorBody{cancel: cancel},
			}, nil
		}))

		exec := NewAntigravityExecutor(nil)
		_, err := exec.Execute(ctx, antigravityStreamTestAuth(), antigravityStreamTestRequest("claude-sonnet-4-6"), cliproxyexecutor.Options{
			SourceFormat: sdktranslator.FormatAntigravity,
		})
		cancel()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("iteration %d: Execute error = %v, want context.Canceled", i, err)
		}
	}
}
