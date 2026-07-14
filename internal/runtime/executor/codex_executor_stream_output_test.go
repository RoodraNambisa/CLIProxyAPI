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

func TestCodexExecutorExecute_ResponseDoneFailedReturnsTerminalError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"type":"response.done","response":{"id":"resp_1","status":"failed","error":{"code":"server_error","message":"done reported failure"},"output":[]}}` + "\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	_, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"Say ok"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex})
	if err == nil || !strings.Contains(err.Error(), "done reported failure") {
		t.Fatalf("Execute() error = %v, want response.done failure", err)
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

func TestCodexExecutorExecuteStream_MarksTranslatedCompletionTerminal(t *testing.T) {
	for _, completionType := range []string{"response.completed", "response.done"} {
		t.Run(completionType, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte(`data: {"type":"` + completionType + `","response":{"id":"resp_1","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}` + "\n\n"))
				_, _ = w.Write([]byte(`data: {"type":"response.created","response":{"id":"late"}}` + "\n\n"))
			}))
			defer server.Close()

			executor := NewCodexExecutor(&config.Config{})
			auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
			result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
				Model:   "gpt-5.4-mini",
				Payload: []byte(`{"model":"gpt-5.4-mini","input":"Say ok"}`),
			}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, Stream: true, Metadata: map[string]any{
				cliproxyexecutor.StreamTerminalMarkerMetadataKey: true,
			}})
			if err != nil {
				t.Fatalf("ExecuteStream error: %v", err)
			}

			terminalCount := 0
			completionCount := 0
			for chunk := range result.Chunks {
				if chunk.Err != nil {
					t.Fatalf("stream chunk error: %v", chunk.Err)
				}
				if bytes.Contains(chunk.Payload, []byte(`"id":"late"`)) {
					t.Fatalf("received event after completion: %s", chunk.Payload)
				}
				if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
					terminalCount++
					continue
				}
				data := bytes.TrimSpace(bytes.TrimPrefix(bytes.TrimSpace(chunk.Payload), dataTag))
				if got := gjson.GetBytes(data, "type").String(); got == "response.completed" {
					completionCount++
				}
			}
			if terminalCount != 1 || completionCount != 1 {
				t.Fatalf("terminal markers = %d, completion payloads = %d; want 1 each", terminalCount, completionCount)
			}
		})
	}
}

func TestCodexExecutorExecuteStream_ResponseDoneFailedDoesNotMarkSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"type":"response.done","response":{"id":"resp_1","status":"failed","error":{"code":"server_error","message":"done reported failure"},"output":[]}}` + "\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"Say ok"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, Stream: true, Metadata: map[string]any{
		cliproxyexecutor.StreamTerminalMarkerMetadataKey: true,
	}})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}

	var streamErr error
	for chunk := range result.Chunks {
		if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
			t.Fatal("failed response.done emitted successful terminal marker")
		}
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	if streamErr == nil || !strings.Contains(streamErr.Error(), "done reported failure") {
		t.Fatalf("stream error = %v, want response.done failure", streamErr)
	}
}

func TestNormalizeCodexCompletionOnlyNormalizesSuccessfulDone(t *testing.T) {
	tests := []struct {
		name     string
		payload  string
		wantType string
	}{
		{name: "completed", payload: `{"type":"response.done","response":{"status":"completed","error":null}}`, wantType: "response.completed"},
		{name: "failed", payload: `{"type":"response.done","response":{"status":"failed"}}`, wantType: "response.done"},
		{name: "incomplete", payload: `{"type":"response.done","response":{"status":"incomplete"}}`, wantType: "response.done"},
		{name: "cancelled", payload: `{"type":"response.done","response":{"status":"cancelled"}}`, wantType: "response.done"},
		{name: "error", payload: `{"type":"response.done","response":{"status":"completed","error":{"message":"failed"}}}`, wantType: "response.done"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := normalizeCodexCompletion([]byte(test.payload))
			if gotType := gjson.GetBytes(got, "type").String(); gotType != test.wantType {
				t.Fatalf("type = %q, want %q; payload=%s", gotType, test.wantType, got)
			}
		})
	}
}

func TestCodexExecutorExecuteStream_EmptyTranslationEmitsTerminal(t *testing.T) {
	emptyFormat := sdktranslator.FromString("codex-empty-terminal-http-test")
	sdktranslator.Register(emptyFormat, sdktranslator.FormatCodex, nil, sdktranslator.ResponseTransform{
		Stream: func(context.Context, string, []byte, []byte, []byte, *any) [][]byte { return nil },
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"id":"resp_1","output":[]}}` + "\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"Say ok"}`),
	}, cliproxyexecutor.Options{SourceFormat: emptyFormat, Stream: true, Metadata: map[string]any{
		cliproxyexecutor.StreamTerminalMarkerMetadataKey: true,
	}})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}

	chunks := make([]cliproxyexecutor.StreamChunk, 0, 1)
	for chunk := range result.Chunks {
		chunks = append(chunks, chunk)
	}
	if len(chunks) != 1 || !cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunks[0]) {
		t.Fatalf("chunks = %#v, want one successful terminal marker", chunks)
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
			cliproxyexecutor.TrustUpstreamSSEMetadataKey:     true,
			cliproxyexecutor.StreamTerminalMarkerMetadataKey: true,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}

	var completed []byte
	terminalCount := 0
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
			terminalCount++
			continue
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
	if terminalCount != 1 {
		t.Fatalf("terminal chunk count = %d, want 1", terminalCount)
	}

	if got := gjson.GetBytes(completed, "response.output").Raw; got != "[]" {
		t.Fatalf("response.output = %s, want [] when upstream SSE is trusted; completed=%s", got, string(completed))
	}
}

func TestCodexExecutorExecuteStream_TrustUpstreamSSERequiresCompletionData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.done\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"Say ok"}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.TrustUpstreamSSEMetadataKey:     true,
			cliproxyexecutor.StreamTerminalMarkerMetadataKey: true,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}

	var streamErr error
	for chunk := range result.Chunks {
		if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
			t.Fatal("event-only frame emitted successful terminal marker")
		}
		if bytes.Contains(chunk.Payload, []byte("event: response.completed")) {
			t.Fatalf("event-only frame was normalized as successful: %s", chunk.Payload)
		}
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	if streamErr == nil || !strings.Contains(streamErr.Error(), "successful terminal event") {
		t.Fatalf("stream error = %v, want incomplete terminal error", streamErr)
	}
}

func TestCodexExecutorExecuteStream_TrustUpstreamSSENormalizesSuccessfulDoneFrame(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.done\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.done","response":{"id":"resp_1","status":"completed","output":[]}}` + "\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"Say ok"}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.TrustUpstreamSSEMetadataKey:     true,
			cliproxyexecutor.StreamTerminalMarkerMetadataKey: true,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}

	var payload []byte
	terminalCount := 0
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
			terminalCount++
			continue
		}
		payload = append(payload, chunk.Payload...)
	}
	if terminalCount != 1 {
		t.Fatalf("terminal marker count = %d, want 1", terminalCount)
	}
	if !bytes.Contains(payload, []byte("event: response.completed")) || !bytes.Contains(payload, []byte(`"type":"response.completed"`)) {
		t.Fatalf("successful legacy frame was not normalized: %s", payload)
	}
}

func TestCodexExecutorExecuteStream_TranslatedPathRequiresCompletionData(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.done\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"Say ok"}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatCodex,
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.StreamTerminalMarkerMetadataKey: true,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}

	var streamErr error
	for chunk := range result.Chunks {
		if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
			t.Fatal("event-only frame emitted successful terminal marker")
		}
		if bytes.Contains(chunk.Payload, []byte("event: response.completed")) {
			t.Fatalf("event-only frame was normalized as successful: %s", chunk.Payload)
		}
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	if streamErr == nil || !strings.Contains(streamErr.Error(), "successful terminal event") {
		t.Fatalf("stream error = %v, want incomplete terminal error", streamErr)
	}
}

func TestCodexExecutorExecuteStream_ImagePassthroughDoesNotReuseCompletionEventAcrossFrames(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.done\n\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.done","response":{"id":"resp_image","status":"completed","output":[]}}` + "\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"draw","tools":[{"type":"image_generation"}],"stream":true}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.ImageGenerationStreamPassthroughMetadataKey: true,
			cliproxyexecutor.StreamTerminalMarkerMetadataKey:             true,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}

	var payload []byte
	terminalCount := 0
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
			terminalCount++
			continue
		}
		payload = append(payload, chunk.Payload...)
	}
	if terminalCount != 1 {
		t.Fatalf("terminal marker count = %d, want 1", terminalCount)
	}
	if bytes.Contains(payload, []byte("event: response.completed")) {
		t.Fatalf("completion event crossed SSE frame boundary: %s", payload)
	}
	if !bytes.Contains(payload, []byte(`"type":"response.completed"`)) {
		t.Fatalf("completion data was not normalized: %s", payload)
	}
}

func TestCodexExecutorExecuteStream_ImagePassthroughMarksDoneTerminal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: image_generation.partial_image\n"))
		_, _ = w.Write([]byte(`data: {"type":"image_generation.partial_image","b64_json":"cGFydA=="}` + "\n\n"))
		_, _ = w.Write([]byte("event: response.done\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.done","response":{"id":"resp_image","output":[]}}` + "\n\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.created","response":{"id":"late"}}` + "\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"draw","tools":[{"type":"image_generation"}],"stream":true}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.ImageGenerationStreamPassthroughMetadataKey: true,
			cliproxyexecutor.StreamTerminalMarkerMetadataKey:             true,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}

	terminalCount := 0
	completedEvent := false
	completedPayload := false
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		if bytes.Contains(chunk.Payload, []byte(`"id":"late"`)) {
			t.Fatalf("received event after completion: %s", chunk.Payload)
		}
		if bytes.Contains(chunk.Payload, []byte("event: response.done")) {
			t.Fatalf("legacy response.done event was not normalized: %s", chunk.Payload)
		}
		if bytes.Contains(chunk.Payload, []byte("event: response.completed")) {
			completedEvent = true
		}
		if bytes.Contains(chunk.Payload, []byte(`"type":"response.completed"`)) {
			completedPayload = true
		}
		if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
			terminalCount++
			continue
		}
	}
	if terminalCount != 1 {
		t.Fatalf("terminal chunk count = %d, want 1", terminalCount)
	}
	if !completedEvent || !completedPayload {
		t.Fatal("missing normalized response.completed event and payload")
	}
}

func TestCodexExecutorExecuteStream_ImagePassthroughDoesNotEmitCompletedEventForFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.done\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.done","response":{"status":"failed","error":{"message":"image failed"}}}` + "\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"draw","tools":[{"type":"image_generation"}],"stream":true}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.ImageGenerationStreamPassthroughMetadataKey: true,
			cliproxyexecutor.StreamTerminalMarkerMetadataKey:             true,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}
	var streamErr error
	for chunk := range result.Chunks {
		if bytes.Contains(chunk.Payload, []byte("event: response.completed")) {
			t.Fatalf("failed response emitted completion event: %s", chunk.Payload)
		}
		if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
			t.Fatal("failed response emitted successful terminal marker")
		}
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	if streamErr == nil {
		t.Fatal("failed response did not emit an error")
	}
}
