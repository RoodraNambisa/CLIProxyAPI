package executor

import (
	"bytes"
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/translator"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexExecutorExecuteStreamClearsRejectedReasoningReplay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"type":"response.failed","response":{"status":"failed","error":{"code":"invalid_encrypted_content","message":"invalid signature in thinking block"}}}` + "\n\n"))
	}))
	defer server.Close()

	ctx := context.Background()
	auth := &cliproxyauth.Auth{ID: "codex-replay-auth", Provider: "codex", Attributes: map[string]string{
		"base_url": server.URL,
		"api_key":  "test",
	}}
	namespace := helps.ReasoningReplayNamespace(ctx, "codex", auth.ID)
	body := []byte(`{"input":[{"type":"function_call_output","call_id":"call_1","output":"ok"}]}`)
	metadata := map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "session-1"}
	_, scope := helps.ApplyCodexReasoningReplay(ctx, "claude", namespace, "gpt-5.4-mini", nil, body, nil, metadata, nil)
	signatureBytes := make([]byte, 73)
	signatureBytes[0] = 0x80
	signature := base64.RawURLEncoding.EncodeToString(signatureBytes)
	completed := []byte(`{"response":{"output":[{"type":"reasoning","encrypted_content":"` + signature + `"},{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{}"}]}}`)
	if !helps.CacheCodexReasoningReplayFromCompleted(scope, completed) {
		t.Fatal("failed to seed replay cache")
	}

	executor := NewCodexExecutor(&config.Config{})
	result, err := executor.ExecuteStream(ctx, auth, cliproxyexecutor.Request{
		Model:    "gpt-5.4-mini",
		Payload:  []byte(`{"model":"gpt-5.4-mini","messages":[{"role":"user","content":"continue"}]}`),
		Metadata: metadata,
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatClaude, Stream: true, Metadata: metadata})
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
		t.Fatal("stream error = nil")
	}

	replayed, _ := helps.ApplyCodexReasoningReplay(ctx, "claude", namespace, "gpt-5.4-mini", nil, body, nil, metadata, nil)
	if got := gjson.GetBytes(replayed, "input.0.type").String(); got == "reasoning" {
		t.Fatalf("rejected replay remained cached: %s", replayed)
	}
}

func TestCodexExecutorExecute_EmptyStreamCompletionOutputUsesOutputItemDone(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"ok\"}]},\"output_index\":0}\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"created_at\":1775555723,\"status\":\"completed\",\"model\":\"gpt-5.4-mini-2026-03-17\",\"output\":[],\"usage\":{\"input_tokens\":8,\"output_tokens\":28,\"total_tokens\":36}}}\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"base_url": server.URL,
		"api_key":  "test",
	}}

	resp, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","messages":[{"role":"user","content":"Say ok"}]}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai"),
		Stream:       false,
	})
	if err != nil {
		t.Fatalf("Execute error: %v", err)
	}

	gotContent := gjson.GetBytes(resp.Payload, "choices.0.message.content").String()
	if gotContent != "ok" {
		t.Fatalf("choices.0.message.content = %q, want %q; payload=%s", gotContent, "ok", string(resp.Payload))
	}
}

func TestCodexExecutorExecute_ResponseFailedReturnsTerminalError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"type":"response.failed","response":{"id":"resp_1","status":"failed","error":{"code":"server_error","message":"The model failed to generate a response."},"output":[]}}` + "\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"base_url": server.URL,
		"api_key":  "test",
	}}

	_, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"Say ok"}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai-response"),
		Stream:       false,
	})
	if err == nil {
		t.Fatal("Execute error = nil, want response.failed error")
	}
	status, ok := err.(interface{ StatusCode() int })
	if !ok {
		t.Fatalf("Execute error type = %T, want StatusCode", err)
	}
	if got := status.StatusCode(); got != http.StatusInternalServerError {
		t.Fatalf("StatusCode = %d, want %d; err=%v", got, http.StatusInternalServerError, err)
	}
	if strings.Contains(err.Error(), "response.completed") {
		t.Fatalf("error should not mention missing response.completed: %v", err)
	}
	if !strings.Contains(err.Error(), "The model failed to generate a response.") {
		t.Fatalf("error = %v, want upstream failed message", err)
	}
}

func TestCodexExecutorExecute_ResponseIncompleteReturnsTerminalError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"type":"response.incomplete","response":{"id":"resp_1","status":"incomplete","error":null,"incomplete_details":{"reason":"max_tokens"},"output":[]}}` + "\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"base_url": server.URL,
		"api_key":  "test",
	}}

	_, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"Say ok"}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai-response"),
		Stream:       false,
	})
	if err == nil {
		t.Fatal("Execute error = nil, want response.incomplete error")
	}
	status, ok := err.(interface{ StatusCode() int })
	if !ok {
		t.Fatalf("Execute error type = %T, want StatusCode", err)
	}
	if got := status.StatusCode(); got != http.StatusBadGateway {
		t.Fatalf("StatusCode = %d, want %d; err=%v", got, http.StatusBadGateway, err)
	}
	if strings.Contains(err.Error(), "response.completed") {
		t.Fatalf("error should not mention missing response.completed: %v", err)
	}
	if !strings.Contains(err.Error(), "response incomplete: max_tokens") {
		t.Fatalf("error = %v, want incomplete reason", err)
	}
	skipper, ok := err.(interface{ SkipAuthResult() bool })
	if !ok || !skipper.SkipAuthResult() {
		t.Fatalf("SkipAuthResult = %v, want true", ok)
	}
}

func TestCodexExecutorExecute_ErrorEventReturnsTerminalError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"type":"error","error":{"type":"server_error","code":"server_error","message":"upstream server error","param":null},"sequence_number":5}` + "\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"base_url": server.URL,
		"api_key":  "test",
	}}

	_, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"Say ok"}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai-response"),
		Stream:       false,
	})
	if err == nil {
		t.Fatal("Execute error = nil, want error event")
	}
	status, ok := err.(interface{ StatusCode() int })
	if !ok {
		t.Fatalf("Execute error type = %T, want StatusCode", err)
	}
	if got := status.StatusCode(); got != http.StatusInternalServerError {
		t.Fatalf("StatusCode = %d, want %d; err=%v", got, http.StatusInternalServerError, err)
	}
	if strings.Contains(err.Error(), "response.completed") {
		t.Fatalf("error should not mention missing response.completed: %v", err)
	}
	if !strings.Contains(err.Error(), "upstream server error") {
		t.Fatalf("error = %v, want upstream error message", err)
	}
}

func TestCodexExecutorExecuteStream_EmptyStreamCompletionOutputUsesOutputItemDone(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"ok\"}]},\"output_index\":0}\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"created_at\":1775555723,\"status\":\"completed\",\"model\":\"gpt-5.4-mini-2026-03-17\",\"output\":[],\"usage\":{\"input_tokens\":8,\"output_tokens\":28,\"total_tokens\":36}}}\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"base_url": server.URL,
		"api_key":  "test",
	}}

	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"Say ok"}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai-response"),
		Stream:       true,
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}

	var completed []byte
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		payload := bytes.TrimSpace(chunk.Payload)
		if !bytes.HasPrefix(payload, []byte("data:")) {
			continue
		}
		data := bytes.TrimSpace(payload[5:])
		if gjson.GetBytes(data, "type").String() == "response.completed" {
			completed = append([]byte(nil), data...)
		}
	}

	if len(completed) == 0 {
		t.Fatal("missing response.completed chunk")
	}

	gotContent := gjson.GetBytes(completed, "response.output.0.content.0.text").String()
	if gotContent != "ok" {
		t.Fatalf("response.output[0].content[0].text = %q, want %q; completed=%s", gotContent, "ok", string(completed))
	}
}

func TestCodexExecutorExecuteStream_TrustUpstreamSSEBypassesOutputRepair(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_item.done\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"ok\"}]},\"output_index\":0}\n\n"))
		_, _ = w.Write([]byte("event: response.completed\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"created_at\":1775555723,\"status\":\"completed\",\"model\":\"gpt-5.4-mini-2026-03-17\",\"output\":[],\"usage\":{\"input_tokens\":8,\"output_tokens\":28,\"total_tokens\":36}}}\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{
		"base_url": server.URL,
		"api_key":  "test",
	}}

	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"Say ok"}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("openai-response"),
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.TrustUpstreamSSEMetadataKey: true,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}

	var completed []byte
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		payload := bytes.TrimSpace(chunk.Payload)
		if !bytes.Contains(payload, []byte("event: response.completed")) {
			continue
		}
		for _, line := range bytes.Split(payload, []byte("\n")) {
			line = bytes.TrimSpace(line)
			if !bytes.HasPrefix(line, []byte("data:")) {
				continue
			}
			data := bytes.TrimSpace(line[5:])
			if gjson.GetBytes(data, "type").String() == "response.completed" {
				completed = append([]byte(nil), data...)
			}
		}
	}

	if len(completed) == 0 {
		t.Fatal("missing response.completed chunk")
	}

	if got := gjson.GetBytes(completed, "response.output").Raw; got != "[]" {
		t.Fatalf("response.output = %s, want [] when upstream SSE is trusted; completed=%s", got, string(completed))
	}
}
