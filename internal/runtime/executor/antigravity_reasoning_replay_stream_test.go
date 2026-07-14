package executor

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	internalcache "github.com/router-for-me/CLIProxyAPI/v6/internal/cache"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestAntigravityExecuteStreamCachesReplayOnlyAfterTerminal(t *testing.T) {
	internalcache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(internalcache.ClearAntigravityReasoningReplayCache)

	body := "data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"reasoning\",\"thought\":true,\"thoughtSignature\":\"0123456789abcdef\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":1,\"candidatesTokenCount\":1,\"totalTokenCount\":2}}}\n\n"
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", antigravityStreamRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	}))
	auth := antigravityStreamTestAuth()
	req := antigravityStreamTestRequest("gemini-3-flash-agent")
	req.Payload = []byte(`{"sessionId":"stream-success","request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`)

	result, err := NewAntigravityExecutor(nil).ExecuteStream(ctx, auth, req, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatAntigravity,
		Stream:       true,
	})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error = %v", chunk.Err)
		}
	}
	items, ok := internalcache.GetAntigravityReasoningReplayItems("gemini-3-flash-agent", "session:stream-success")
	if !ok || len(items) != 1 {
		t.Fatalf("cached items = %d ok=%v, want one", len(items), ok)
	}
	if got := gjson.GetBytes(items[0], "thoughtSignature").String(); got != "0123456789abcdef" {
		t.Fatalf("thoughtSignature = %q", got)
	}
}

func TestAntigravityExecuteStreamDoesNotCacheIncompleteReplay(t *testing.T) {
	internalcache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(internalcache.ClearAntigravityReasoningReplayCache)

	body := "data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"partial\",\"thought\":true,\"thoughtSignature\":\"0123456789abcdef\"}]}}]}}\n\n"
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", antigravityStreamRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	}))
	auth := antigravityStreamTestAuth()
	req := antigravityStreamTestRequest("gemini-3-flash-agent")
	req.Payload = []byte(`{"sessionId":"stream-incomplete","request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`)

	result, err := NewAntigravityExecutor(nil).ExecuteStream(ctx, auth, req, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatAntigravity,
		Stream:       true,
	})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	var streamErr error
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	if streamErr == nil {
		t.Fatal("incomplete stream returned no terminal error")
	}
	if _, ok := internalcache.GetAntigravityReasoningReplayItems("gemini-3-flash-agent", "session:stream-incomplete"); ok {
		t.Fatal("incomplete stream cached replay state")
	}
}
