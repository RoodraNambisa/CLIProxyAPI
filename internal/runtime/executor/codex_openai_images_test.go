package executor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexExecutorOpenAIImageExecuteProxiesGeneration(t *testing.T) {
	var capturedPath string
	var capturedAccept string
	var capturedAuth string
	var capturedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedAccept = r.Header.Get("Accept")
		capturedAuth = r.Header.Get("Authorization")
		var err error
		capturedBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"created":1700000000,"data":[{"b64_json":"aW1n"}],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}`))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}})
	auth := &cliproxyauth.Auth{Provider: "codex", Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL}}
	resp, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-image-2",
		Payload: []byte(`{"model":"gpt-image-2","prompt":"draw","stream":true}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString(codexOpenAIImageSourceFormat),
		Alt:          codexOpenAIImageGenerations,
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if capturedPath != "/images/generations" {
		t.Fatalf("path = %q, want /images/generations", capturedPath)
	}
	if capturedAccept != "application/json" {
		t.Fatalf("Accept = %q, want application/json", capturedAccept)
	}
	if capturedAuth != "Bearer test-key" {
		t.Fatalf("Authorization = %q", capturedAuth)
	}
	if got := gjson.GetBytes(capturedBody, "model").String(); got != "gpt-image-2" {
		t.Fatalf("body model = %q", got)
	}
	if got := gjson.GetBytes(capturedBody, "stream"); got.Exists() {
		t.Fatalf("body stream exists = %s, want absent", got.Raw)
	}
	if got := gjson.GetBytes(resp.Payload, "data.0.b64_json").String(); got != "aW1n" {
		t.Fatalf("response b64_json = %q", got)
	}
}

func TestCodexPrepareOpenAIImageBodyUsesExecutionModel(t *testing.T) {
	body, model, err := codexPrepareOpenAIImageBody(cliproxyexecutor.Request{
		Model:   "gpt-image-1.5",
		Payload: []byte(`{"model":"team/gpt-image-1.5","prompt":"draw"}`),
	}, false)
	if err != nil {
		t.Fatalf("codexPrepareOpenAIImageBody() error = %v", err)
	}
	if model != "gpt-image-1.5" {
		t.Fatalf("model = %q, want gpt-image-1.5", model)
	}
	if got := gjson.GetBytes(body, "model").String(); got != "gpt-image-1.5" {
		t.Fatalf("body model = %q, want gpt-image-1.5", got)
	}
}

func TestCodexExecutorOpenAIImageExecuteStreamProxiesEdits(t *testing.T) {
	var capturedPath string
	var capturedAccept string
	var capturedBody []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedAccept = r.Header.Get("Accept")
		var err error
		capturedBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll request body: %v", err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: image_generation.partial_image\n"))
		_, _ = w.Write([]byte(`data: {"type":"image_generation.partial_image","b64_json":"cGFydA=="}` + "\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}})
	auth := &cliproxyauth.Auth{Provider: "codex", Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL}}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-image-2",
		Payload: []byte(`{"model":"gpt-image-2","prompt":"edit","images":[{"file_id":"file_123"}]}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString(codexOpenAIImageSourceFormat),
		Alt:          codexOpenAIImageEdits,
		Metadata: map[string]any{
			cliproxyexecutor.StreamTerminalMarkerMetadataKey: true,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	var out bytes.Buffer
	bootstrapCount := 0
	terminalCount := 0
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error = %v", chunk.Err)
		}
		if cliproxyexecutor.IsBootstrapCommitStreamChunk(chunk) {
			bootstrapCount++
			continue
		}
		if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
			terminalCount++
			continue
		}
		out.Write(chunk.Payload)
	}
	if bootstrapCount != 0 {
		t.Fatalf("bootstrap marker count = %d, want 0 before semantic image data", bootstrapCount)
	}
	if terminalCount != 1 {
		t.Fatalf("terminal marker count = %d, want 1", terminalCount)
	}
	if capturedPath != "/images/edits" {
		t.Fatalf("path = %q, want /images/edits", capturedPath)
	}
	if capturedAccept != "text/event-stream" {
		t.Fatalf("Accept = %q, want text/event-stream", capturedAccept)
	}
	if got := gjson.GetBytes(capturedBody, "stream").Bool(); !got {
		t.Fatalf("body stream = %v, want true", got)
	}
	if got := gjson.GetBytes(capturedBody, "images.0.file_id").String(); got != "file_123" {
		t.Fatalf("images.0.file_id = %q", got)
	}
	if !strings.Contains(out.String(), "image_generation.partial_image") {
		t.Fatalf("stream output = %q, want partial image event", out.String())
	}
}

func TestCodexExecutorOpenAIImageStreamSupportsCROnlySSE(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: image_generation.partial_image\r"))
		_, _ = w.Write([]byte(`data: {"type":"image_generation.partial_image","b64_json":"cGFydA=="}` + "\r\r"))
		_, _ = w.Write([]byte("data: [DONE]\r\r"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}})
	auth := &cliproxyauth.Auth{Provider: "codex", Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL}}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-image-2",
		Payload: []byte(`{"model":"gpt-image-2","prompt":"draw"}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString(codexOpenAIImageSourceFormat),
		Alt:          codexOpenAIImageGenerations,
		Metadata: map[string]any{
			cliproxyexecutor.StreamTerminalMarkerMetadataKey: true,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	var output bytes.Buffer
	terminalCount := 0
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error = %v", chunk.Err)
		}
		if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
			terminalCount++
			continue
		}
		output.Write(chunk.Payload)
	}
	if terminalCount != 1 {
		t.Fatalf("terminal marker count = %d, want 1", terminalCount)
	}
	if !strings.Contains(output.String(), "image_generation.partial_image") || !strings.Contains(output.String(), "[DONE]") {
		t.Fatalf("stream output = %q", output.String())
	}
}

func TestCodexExecutorOpenAIImageStreamClassifiesAgentTaskErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: error\n"))
		_, _ = w.Write([]byte(`data: {"type":"error","error":{"code":"invalid_task_id","message":"task expired"}}` + "\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}})
	auth := &cliproxyauth.Auth{Provider: "codex", Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL}}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-image-2",
		Payload: []byte(`{"model":"gpt-image-2","prompt":"draw"}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString(codexOpenAIImageSourceFormat),
		Alt:          codexOpenAIImageGenerations,
	})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	chunks := make([]cliproxyexecutor.StreamChunk, 0, 1)
	for chunk := range result.Chunks {
		chunks = append(chunks, chunk)
	}
	if len(chunks) != 1 || chunks[0].Err == nil || len(chunks[0].Payload) != 0 {
		t.Fatalf("chunks = %#v, want one error-only chunk", chunks)
	}
	var status cliproxyexecutor.StatusError
	if !errors.As(chunks[0].Err, &status) || status.StatusCode() != http.StatusUnauthorized || !strings.Contains(chunks[0].Err.Error(), "invalid_task_id") {
		t.Fatalf("stream error = %v, want invalid_task_id 401", chunks[0].Err)
	}
}

func TestReadCodexOpenAIImageResponseBodyEnforcesLimit(t *testing.T) {
	_, err := readCodexOpenAIImageResponseBody(strings.NewReader("12345"), 4)
	var status statusErr
	if !errors.As(err, &status) || status.StatusCode() != http.StatusBadGateway || !status.SkipAuthResult() {
		t.Fatalf("read error = %#v, want non-auth 502", err)
	}
}
