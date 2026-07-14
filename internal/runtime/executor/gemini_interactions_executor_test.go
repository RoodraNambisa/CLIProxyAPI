package executor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestGeminiInteractionsExecutorIdentityAndFormat(t *testing.T) {
	exec := NewGeminiInteractionsExecutor(&config.Config{})
	if got := exec.Identifier(); got != "gemini-interactions" {
		t.Fatalf("Identifier() = %q, want gemini-interactions", got)
	}
	if got := exec.RequestToFormat(cliproxyexecutor.Request{}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAI}); got != sdktranslator.FormatInteractions {
		t.Fatalf("RequestToFormat() = %q, want interactions", got)
	}
}

func TestGeminiInteractionsExecutorStreamPreservesFramesAndTerminalMarker(t *testing.T) {
	upstream := "event: interaction.created\ndata: {\"event_type\":\"interaction.created\",\"interaction\":{\"id\":\"i1\"}}\n\n" +
		"event: interaction.completed\ndata: {\"event_type\":\"interaction.completed\",\"interaction\":{\"id\":\"i1\",\"status\":\"completed\",\"usage\":{\"total_input_tokens\":2,\"total_output_tokens\":3,\"total_tokens\":5}}}\n\n" +
		"event: done\ndata: [DONE]\n\n"
	var requestBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(upstream))
	}))
	defer server.Close()

	exec := NewGeminiInteractionsExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Provider: "gemini-interactions", Attributes: map[string]string{
		"api_key":  "test-key",
		"base_url": server.URL,
	}}
	result, errExecute := exec.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gemini-3.5-flash",
		Payload: []byte(`{"model":"gemini-3.5-flash","input":"hi"}`),
	}, cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FormatInteractions,
		ResponseFormat: sdktranslator.FormatInteractions,
		Metadata: map[string]any{
			cliproxyexecutor.InteractionsAPIVersionMetadataKey: "v1",
			cliproxyexecutor.StreamTerminalMarkerMetadataKey:   true,
		},
	})
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	var visible bytes.Buffer
	markerCount := 0
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error = %v", chunk.Err)
		}
		if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
			markerCount++
			continue
		}
		visible.Write(chunk.Payload)
	}
	if got := visible.String(); got != upstream {
		t.Fatalf("stream output = %q, want %q", got, upstream)
	}
	if markerCount != 1 {
		t.Fatalf("terminal marker count = %d, want 1", markerCount)
	}
	if !bytes.Contains(requestBody, []byte(`"stream":true`)) {
		t.Fatalf("request body = %s, want stream=true", requestBody)
	}
}

func TestGeminiInteractionsExecutorStreamAcceptsCompletedWithoutDone(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: interaction.completed\ndata: {\"event_type\":\"interaction.completed\",\"interaction\":{\"status\":\"completed\"}}\n\n"))
	}))
	defer server.Close()

	result := executeTestGeminiInteractionsStream(t, server.URL, true)
	markerSeen := false
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error = %v", chunk.Err)
		}
		markerSeen = markerSeen || cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk)
	}
	if !markerSeen {
		t.Fatal("successful terminal marker not emitted")
	}
}

func TestGeminiInteractionsExecutorStreamRejectsIncompleteEOF(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: interaction.created\ndata: {\"event_type\":\"interaction.created\",\"interaction\":{\"id\":\"i1\"}}\n\n"))
	}))
	defer server.Close()

	result := executeTestGeminiInteractionsStream(t, server.URL, true)
	var streamErr error
	markerSeen := false
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
		markerSeen = markerSeen || cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk)
	}
	if streamErr == nil || !bytes.Contains([]byte(streamErr.Error()), []byte("without a successful terminal event")) {
		t.Fatalf("stream error = %v, want incomplete stream error", streamErr)
	}
	if markerSeen {
		t.Fatal("successful terminal marker emitted for incomplete stream")
	}
}

func TestGeminiInteractionsExecutorStreamRejectsFailedCompletion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: interaction.completed\ndata: {\"event_type\":\"interaction.completed\",\"interaction\":{\"status\":\"failed\"},\"error\":{\"message\":\"generation failed\"}}\n\n"))
	}))
	defer server.Close()

	result := executeTestGeminiInteractionsStream(t, server.URL, true)
	var streamErr error
	markerSeen := false
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
		markerSeen = markerSeen || cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk)
	}
	if streamErr == nil || !bytes.Contains([]byte(streamErr.Error()), []byte("generation failed")) {
		t.Fatalf("stream error = %v, want upstream failure", streamErr)
	}
	if markerSeen {
		t.Fatal("successful terminal marker emitted for failed completion")
	}
}

func TestGeminiInteractionsExecutorStreamRejectsReadErrorAfterTerminal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, rw, errHijack := w.(http.Hijacker).Hijack()
		if errHijack != nil {
			t.Errorf("Hijack() error = %v", errHijack)
			return
		}
		body := "event: interaction.completed\ndata: {\"event_type\":\"interaction.completed\",\"interaction\":{\"status\":\"completed\"}}\n\n" +
			"event: step.delta\ndata: {\"event_type\":"
		_, _ = rw.WriteString("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 1000\r\n\r\n" + body)
		_ = rw.Flush()
		_ = conn.Close()
	}))
	defer server.Close()

	result := executeTestGeminiInteractionsStream(t, server.URL, true)
	var visible bytes.Buffer
	var streamErr error
	markerSeen := false
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			streamErr = chunk.Err
			continue
		}
		if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
			markerSeen = true
			continue
		}
		visible.Write(chunk.Payload)
	}
	if streamErr == nil {
		t.Fatal("stream error = nil, want truncated transport error")
	}
	if markerSeen {
		t.Fatal("successful terminal marker emitted after transport error")
	}
	if bytes.Contains(visible.Bytes(), []byte("step.delta")) {
		t.Fatalf("partial frame was emitted: %q", visible.String())
	}
}

func TestGeminiInteractionsExecutorStreamCancellationClosesOutput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: interaction.created\ndata: {\"event_type\":\"interaction.created\"}\n\n"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	exec := NewGeminiInteractionsExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Provider: "gemini-interactions", Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL}}
	result, errExecute := exec.ExecuteStream(ctx, auth, cliproxyexecutor.Request{
		Model: "gemini-3.5-flash", Payload: []byte(`{"model":"gemini-3.5-flash","input":"hi"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatInteractions, ResponseFormat: sdktranslator.FormatInteractions})
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	if _, ok := <-result.Chunks; !ok {
		t.Fatal("stream closed before first frame")
	}
	cancel()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case _, ok := <-result.Chunks:
			if !ok {
				return
			}
		case <-deadline.C:
			t.Fatal("stream output did not close after cancellation")
		}
	}
}

func TestAppendGeminiInteractionsFrameLineEnforcesTotalLimit(t *testing.T) {
	frame, errAppend := appendGeminiInteractionsFrameLine(nil, []byte("123"), 7)
	if errAppend != nil {
		t.Fatalf("first append error = %v", errAppend)
	}
	frame, errAppend = appendGeminiInteractionsFrameLine(frame, []byte("456"), 7)
	if errAppend != nil || string(frame) != "123\n456" {
		t.Fatalf("second append frame = %q, error = %v", frame, errAppend)
	}
	if _, errAppend = appendGeminiInteractionsFrameLine(frame, []byte("x"), 7); errAppend == nil {
		t.Fatal("append beyond frame limit succeeded")
	}
}

func executeTestGeminiInteractionsStream(t *testing.T, baseURL string, marker bool) *cliproxyexecutor.StreamResult {
	t.Helper()
	exec := NewGeminiInteractionsExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Provider: "gemini-interactions", Attributes: map[string]string{
		"api_key":  "test-key",
		"base_url": baseURL,
	}}
	result, errExecute := exec.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gemini-3.5-flash",
		Payload: []byte(`{"model":"gemini-3.5-flash","input":"hi"}`),
	}, cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FormatInteractions,
		ResponseFormat: sdktranslator.FormatInteractions,
		Metadata: map[string]any{
			cliproxyexecutor.InteractionsAPIVersionMetadataKey: "v1",
			cliproxyexecutor.StreamTerminalMarkerMetadataKey:   marker,
		},
	})
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	return result
}

func TestGeminiInteractionsExecutorVersionAndRevision(t *testing.T) {
	tests := []struct {
		name         string
		version      string
		requestValue string
		authValue    string
		wantPath     string
		wantRevision string
	}{
		{name: "stable", version: "v1", wantPath: "/v1/interactions"},
		{name: "beta default", version: "v1beta", wantPath: "/v1beta/interactions", wantRevision: "2026-05-20"},
		{name: "beta request override", version: "v1beta", requestValue: "2026-07-01", wantPath: "/v1beta/interactions", wantRevision: "2026-07-01"},
		{name: "auth override", version: "v1beta", requestValue: "2026-07-01", authValue: "2026-06-01", wantPath: "/v1beta/interactions", wantRevision: "2026-06-01"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotPath, gotRevision string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath = r.URL.Path
				gotRevision = r.Header.Get("Api-Revision")
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"id":"interaction_1","status":"completed","steps":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))
			}))
			defer server.Close()

			auth := &cliproxyauth.Auth{Provider: "gemini-interactions", Attributes: map[string]string{
				"api_key":  "test-key",
				"base_url": server.URL,
			}}
			if tt.authValue != "" {
				auth.Attributes["header:Api-Revision"] = tt.authValue
			}
			headers := make(http.Header)
			if tt.requestValue != "" {
				headers.Set("Api-Revision", tt.requestValue)
			}
			exec := NewGeminiInteractionsExecutor(&config.Config{})
			resp, errExecute := exec.Execute(context.Background(), auth, cliproxyexecutor.Request{
				Model:   "agents/test-agent",
				Payload: []byte(`{"agent":"agents/test-agent","input":"hi"}`),
			}, cliproxyexecutor.Options{
				SourceFormat:   sdktranslator.FormatInteractions,
				ResponseFormat: sdktranslator.FormatInteractions,
				Headers:        headers,
				Metadata: map[string]any{
					cliproxyexecutor.InteractionsAPIVersionMetadataKey: tt.version,
				},
			})
			if errExecute != nil {
				t.Fatalf("Execute() error = %v", errExecute)
			}
			if !bytes.Contains(resp.Payload, []byte(`"id":"interaction_1"`)) {
				t.Fatalf("response payload = %s", resp.Payload)
			}
			if gotPath != tt.wantPath {
				t.Fatalf("path = %q, want %q", gotPath, tt.wantPath)
			}
			if gotRevision != tt.wantRevision {
				t.Fatalf("Api-Revision = %q, want %q", gotRevision, tt.wantRevision)
			}
		})
	}
}

func TestGeminiInteractionsExecutorPreservesStatusWhenErrorBodyIsTruncated(t *testing.T) {
	tests := []struct {
		name   string
		stream bool
	}{
		{name: "non-stream"},
		{name: "stream", stream: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				conn, rw, errHijack := w.(http.Hijacker).Hijack()
				if errHijack != nil {
					t.Errorf("Hijack() error = %v", errHijack)
					return
				}
				_, _ = rw.WriteString("HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"error\":")
				_ = rw.Flush()
				_ = conn.Close()
			}))
			defer server.Close()

			exec := NewGeminiInteractionsExecutor(&config.Config{})
			auth := &cliproxyauth.Auth{Provider: "gemini-interactions", Attributes: map[string]string{
				"api_key":  "test-key",
				"base_url": server.URL,
			}}
			req := cliproxyexecutor.Request{Model: "gemini-3.5-flash", Payload: []byte(`{"model":"gemini-3.5-flash","input":"hi"}`)}
			opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatInteractions, ResponseFormat: sdktranslator.FormatInteractions}
			var errExecute error
			if tt.stream {
				_, errExecute = exec.ExecuteStream(context.Background(), auth, req, opts)
			} else {
				_, errExecute = exec.Execute(context.Background(), auth, req, opts)
			}
			var statusError cliproxyexecutor.StatusError
			if !errors.As(errExecute, &statusError) || statusError.StatusCode() != http.StatusTooManyRequests {
				t.Fatalf("error = %v, want status 429", errExecute)
			}
		})
	}
}
