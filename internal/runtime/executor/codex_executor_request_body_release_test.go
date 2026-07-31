package executor

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

type codexBodyReleaseExecuteResult struct {
	response cliproxyexecutor.Response
	err      error
}

func executeCodexBodyReleaseRequest(ctx context.Context, baseURL string, ctrl *cliproxyexecutor.RequestBodyReleaseController) (cliproxyexecutor.Response, error) {
	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"base_url": baseURL,
		"api_key":  "test",
	}}
	return executor.Execute(ctx, auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4",
		Payload: []byte(`{"model":"gpt-5.4","input":"hello"}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Metadata: map[string]any{
			cliproxyexecutor.BodyReleaseControllerMetadataKey: ctrl,
		},
	})
}

func waitForCodexRequestBodyRelease(t *testing.T, ctrl *cliproxyexecutor.RequestBodyReleaseController) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ctrl.Released() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !ctrl.Released() {
		t.Fatal("timed out waiting for request body release")
	}
}

func TestCodexExecutorExecuteReleasesRequestBodyAfterUpstreamStreamEstablished(t *testing.T) {
	headersFlushed := make(chan struct{})
	continueResponse := make(chan struct{}, 1)
	streamForced := make(chan bool, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, errRead := io.ReadAll(r.Body)
		if errRead != nil {
			t.Errorf("read upstream request body: %v", errRead)
			return
		}
		streamForced <- bytes.Contains(body, []byte(`"stream":true`))
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("test response writer does not support flushing")
			return
		}
		flusher.Flush()
		close(headersFlushed)
		select {
		case <-continueResponse:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, `data: {"type":"response.completed","response":{"id":"resp_release","status":"completed","output":[]}}`+"\n\n")
	}))
	defer server.Close()
	defer func() {
		select {
		case continueResponse <- struct{}{}:
		default:
		}
	}()

	ctrl := cliproxyexecutor.NewRequestBodyReleaseController(1024, []byte("<timer release>"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resultCh := make(chan codexBodyReleaseExecuteResult, 1)
	go func() {
		response, err := executeCodexBodyReleaseRequest(ctx, server.URL, ctrl)
		resultCh <- codexBodyReleaseExecuteResult{response: response, err: err}
	}()

	select {
	case <-headersFlushed:
	case <-ctx.Done():
		t.Fatalf("upstream headers were not flushed: %v", ctx.Err())
	}
	if !<-streamForced {
		t.Fatal("non-stream client request was not forced to an upstream stream")
	}
	waitForCodexRequestBodyRelease(t, ctrl)
	if ctrl.Replayable() {
		t.Fatal("request remained replayable after upstream stream establishment")
	}
	if placeholder := string(ctrl.Placeholder()); !strings.Contains(placeholder, "stream established") {
		t.Fatalf("release placeholder = %q, want stream-established reason", placeholder)
	}
	select {
	case result := <-resultCh:
		t.Fatalf("Execute() completed before the upstream stream finished: %v", result.err)
	default:
	}

	continueResponse <- struct{}{}
	select {
	case result := <-resultCh:
		if result.err != nil {
			t.Fatalf("Execute() error = %v", result.err)
		}
		if !bytes.Contains(result.response.Payload, []byte("resp_release")) {
			t.Fatalf("Execute() response = %s, want completed response", result.response.Payload)
		}
	case <-ctx.Done():
		t.Fatalf("Execute() did not finish after completion event: %v", ctx.Err())
	}
}

func TestCodexExecutorExecuteDoesNotReleaseRequestBodyBeforeSuccessfulResponse(t *testing.T) {
	t.Run("non-2xx", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, `{"error":{"message":"retry"}}`)
		}))
		defer server.Close()

		ctrl := cliproxyexecutor.NewRequestBodyReleaseController(1024, []byte("<released>"))
		if _, err := executeCodexBodyReleaseRequest(context.Background(), server.URL, ctrl); err == nil {
			t.Fatal("Execute() error = nil, want non-2xx error")
		}
		if ctrl.Released() || !ctrl.Replayable() {
			t.Fatal("non-2xx response released the replayable request body")
		}
	})

	t.Run("connection failure", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		baseURL := server.URL
		server.Close()

		ctrl := cliproxyexecutor.NewRequestBodyReleaseController(1024, []byte("<released>"))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := executeCodexBodyReleaseRequest(ctx, baseURL, ctrl); err == nil {
			t.Fatal("Execute() error = nil, want connection error")
		}
		if ctrl.Released() || !ctrl.Replayable() {
			t.Fatal("connection failure released the replayable request body")
		}
	})
}

func TestCodexExecutorExecuteTimedReleaseWhileWaitingForUpstreamHeaders(t *testing.T) {
	requestStarted := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(requestStarted)
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctrl := cliproxyexecutor.NewRequestBodyReleaseController(1024, []byte("<timer release>"))
	if !ctrl.StartTimer(50*time.Millisecond, ctx.Done()) {
		t.Fatal("request body release timer did not start")
	}
	resultCh := make(chan error, 1)
	go func() {
		_, err := executeCodexBodyReleaseRequest(ctx, server.URL, ctrl)
		resultCh <- err
	}()

	select {
	case <-requestStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream request did not start")
	}
	waitForCodexRequestBodyRelease(t, ctrl)
	if ctrl.Replayable() {
		t.Fatal("timed release left the request replayable")
	}
	select {
	case err := <-resultCh:
		t.Fatalf("Execute() finished before its context was canceled: %v", err)
	default:
	}

	cancel()
	select {
	case err := <-resultCh:
		if err == nil {
			t.Fatal("Execute() error = nil after context cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Execute() did not stop after context cancellation")
	}
}

func TestCodexStreamBodyRefsRealReleaseKeepsSlimMetadata(t *testing.T) {
	ctrl := cliproxyexecutor.NewRequestBodyReleaseController(1024, []byte("<released>"))
	opts := cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.BodyReleaseControllerMetadataKey: ctrl,
		},
	}

	original := []byte(`{"messages":[{"content":"large prompt"}],"tools":[{"type":"function","function":{"name":"tool_original","description":"large schema"}}]}`)
	body := []byte(`{"input":"large translated prompt","tools":[{"type":"image_generation","model":"gpt-image-1.5"},{"type":"function","function":{"name":"tool_translated"}}]}`)
	releasedOriginal := slimCodexOriginalPayloadForTranslation(sdktranslator.FormatOpenAI, original)
	releasedBody := slimCodexBodyForStreamUsage(body)
	originalRef, bodyRef, unregister := codexStreamBodyRefs(context.Background(), opts, original, body, releasedOriginal, releasedBody)
	defer unregister()

	ctrl.Release()

	gotOriginal := string(originalRef.Bytes())
	if !strings.Contains(gotOriginal, "tool_original") {
		t.Fatalf("original payload after release = %q, want tool metadata", gotOriginal)
	}
	if strings.Contains(gotOriginal, "large prompt") || strings.Contains(gotOriginal, "large schema") {
		t.Fatalf("original payload after release retained large fields: %q", gotOriginal)
	}
	gotBody := string(bodyRef.Bytes())
	if !strings.Contains(gotBody, "image_generation") || !strings.Contains(gotBody, "gpt-image-1.5") {
		t.Fatalf("translated body after release = %q, want image tool metadata", gotBody)
	}
	if strings.Contains(gotBody, "large translated prompt") || strings.Contains(gotBody, "tool_translated") {
		t.Fatalf("translated body after release retained unrelated fields: %q", gotBody)
	}
}

func TestCodexStreamBodyRefsLogOnlyKeepsPayloads(t *testing.T) {
	ctrl := cliproxyexecutor.NewRequestBodyReleaseControllerWithMode(1024, []byte("<released>"), true)
	opts := cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.BodyReleaseControllerMetadataKey: ctrl,
		},
	}

	originalRef, bodyRef, unregister := codexStreamBodyRefs(context.Background(), opts, []byte("original"), []byte("translated"), []byte("slim-original"), []byte("slim-translated"))
	defer unregister()

	ctrl.Release()

	if got := string(originalRef.Bytes()); got != "original" {
		t.Fatalf("original payload after log-only release = %q, want original", got)
	}
	if got := string(bodyRef.Bytes()); got != "translated" {
		t.Fatalf("translated body after log-only release = %q, want translated", got)
	}
}

func TestSlimCodexOriginalPayloadForTranslationKeepsProviderToolNames(t *testing.T) {
	tests := []struct {
		name string
		from sdktranslator.Format
		body []byte
		want string
	}{
		{
			name: "openai",
			from: sdktranslator.FormatOpenAI,
			body: []byte(`{"tools":[{"type":"function","function":{"name":"openai_tool","description":"drop"}}],"input":"drop"}`),
			want: "openai_tool",
		},
		{
			name: "claude",
			from: sdktranslator.FormatClaude,
			body: []byte(`{"tools":[{"name":"claude_tool","description":"drop"}],"messages":[{"content":"drop"}]}`),
			want: "claude_tool",
		},
		{
			name: "gemini",
			from: sdktranslator.FormatGemini,
			body: []byte(`{"tools":[{"functionDeclarations":[{"name":"gemini_tool","description":"drop"}]}],"contents":[{"parts":[{"text":"drop"}]}]}`),
			want: "gemini_tool",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(slimCodexOriginalPayloadForTranslation(tt.from, tt.body))
			if !strings.Contains(got, tt.want) {
				t.Fatalf("slim payload = %q, want %s", got, tt.want)
			}
			if strings.Contains(got, "drop") {
				t.Fatalf("slim payload retained dropped metadata: %q", got)
			}
		})
	}
}
