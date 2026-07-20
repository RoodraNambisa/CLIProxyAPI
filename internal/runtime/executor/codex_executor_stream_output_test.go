package executor

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/translator"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	cliproxyusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
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

func TestCodexExecutorExecuteIncrementallyParsesSSEAndPreservesUsage(t *testing.T) {
	const authID = "codex-incremental-nonstream-usage"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, `data: {"type":"response.output_text.delta","delta":"ignored"}`+"\r")
		flusher.Flush()
		_, _ = io.WriteString(w, "\n"+`data: {"type":"response.output_item.done","output_index":0,"item":{"type":"image_generation_call","id":"img-1","result":"aW1hZ2U="}}`+"\r")
		flusher.Flush()
		_, _ = io.WriteString(w, `data: {"type":"response.completed","response":{"id":"resp-1","status":"completed","output":[],"usage":{"input_tokens":5,"output_tokens":7,"total_tokens":12},"tool_usage":{"image_gen":{"input_tokens":1,"output_tokens":7024,"total_tokens":7025}}}}`+"\r\r")
	}))
	defer server.Close()

	usageRecords := make(chan cliproxyusage.Record, 4)
	cliproxyusage.RegisterPlugin(&streamTerminalUsagePlugin{authID: authID, records: usageRecords})
	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: authID, Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	resp, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"draw","tools":[{"type":"image_generation","model":"gpt-image-2"}]}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := gjson.GetBytes(resp.Payload, "output.0.type").String(); got != "image_generation_call" {
		t.Fatalf("output type = %q; payload=%s", got, resp.Payload)
	}

	records := make(map[string]cliproxyusage.Record, 2)
	deadline := time.After(5 * time.Second)
	for len(records) < 2 {
		select {
		case record := <-usageRecords:
			records[record.Model] = record
		case <-deadline:
			t.Fatalf("timed out waiting for usage records: %#v", records)
		}
	}
	if record := records["gpt-5.4-mini"]; record.Detail.InputTokens != 5 || record.Detail.OutputTokens != 7 || record.Detail.TotalTokens != 12 {
		t.Fatalf("primary usage = %#v", record)
	}
	if record := records["gpt-image-2"]; record.Detail.InputTokens != 1 || record.Detail.OutputTokens != 7024 || record.Detail.TotalTokens != 7025 {
		t.Fatalf("image tool usage = %#v", record)
	}
}

func TestCodexExecutorExecuteDrainsSSEAfterCompletion(t *testing.T) {
	body := &codexDrainTrackingBody{chunks: [][]byte{
		[]byte(`data: {"type":"response.completed","response":{"status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}` + "\n\n"),
		[]byte(": trailing comment\n\n"),
	}}
	ctx := streamTerminalContext(streamTerminalRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
	}))
	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": "https://example.test", "api_key": "test"}}
	if _, err := executor.Execute(ctx, auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"draw"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !body.eofObserved {
		t.Fatal("SSE response was not drained to EOF after response.completed")
	}
}

func TestCodexExecutorExecuteReturns408WithoutTerminalSSEEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"type":"response.output_text.delta","delta":"partial"}`+"\n")
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	_, err := executor.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"draw"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if err == nil {
		t.Fatal("Execute() error = nil, want missing terminal error")
	}
	status, ok := err.(interface{ StatusCode() int })
	if !ok || status.StatusCode() != http.StatusRequestTimeout {
		t.Fatalf("Execute() error = %v, want status 408", err)
	}
}

func TestCodexExecutorExecutePrefersReadErrorReturnedWithTerminalData(t *testing.T) {
	upstreamErr := errors.New("upstream read failed")
	completed := `data: {"type":"response.completed","response":{"status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}` + "\n\n"
	ctx := streamTerminalContext(streamTerminalRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body: &streamTerminalErrorBody{
				reader:      strings.NewReader(completed),
				err:         upstreamErr,
				errWithData: true,
			},
		}, nil
	}))
	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": "https://example.test", "api_key": "test"}}
	_, err := executor.Execute(ctx, auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"draw"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if !errors.Is(err, upstreamErr) {
		t.Fatalf("Execute() error = %v, want upstream read error", err)
	}
}

type codexDrainTrackingBody struct {
	chunks      [][]byte
	index       int
	eofObserved bool
}

func (body *codexDrainTrackingBody) Read(p []byte) (int, error) {
	if body.index >= len(body.chunks) {
		body.eofObserved = true
		return 0, io.EOF
	}
	chunk := body.chunks[body.index]
	body.index++
	return copy(p, chunk), nil
}

func (*codexDrainTrackingBody) Close() error { return nil }

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
		t.Fatalf("chunks = %#v, want successful terminal marker", chunks)
	}
}

func TestCodexExecutorExecuteStream_TrustUpstreamSSEBypassesOutputRepair(t *testing.T) {
	requestBody := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, errRead := io.ReadAll(r.Body)
		if errRead != nil {
			t.Fatalf("read request body: %v", errRead)
		}
		requestBody <- payload
		upstreamPromptCacheKey := gjson.GetBytes(payload, "prompt_cache_key").String()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_item.done\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"ok\"}]},\"output_index\":0}\n\n"))
		_, _ = w.Write([]byte("event: response.completed\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"created_at\":1775555723,\"status\":\"completed\",\"model\":\"gpt-5.4-mini-2026-03-17\",\"prompt_cache_key\":\"" + upstreamPromptCacheKey + "\",\"output\":[],\"usage\":{\"input_tokens\":8,\"output_tokens\":28,\"total_tokens\":36}}}\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{
		Codex:   config.CodexConfig{IdentityConfuse: true},
		Routing: config.RoutingConfig{SessionAffinity: true},
	})
	auth := &cliproxyauth.Auth{ID: "trusted-sse-auth", Attributes: map[string]string{
		"base_url": server.URL,
		"api_key":  "test",
	}}

	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"Say ok","prompt_cache_key":"cache-1"}`),
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
	var forwarded bytes.Buffer
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
			terminalCount++
			continue
		}
		forwarded.Write(chunk.Payload)
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
	if got := gjson.GetBytes(completed, "response.prompt_cache_key").String(); got != "cache-1" {
		t.Fatalf("response.prompt_cache_key = %q, want cache-1", got)
	}
	if frames := bytes.Count(forwarded.Bytes(), []byte("\n\n")); frames != 2 {
		t.Fatalf("trusted SSE frame separators = %d, want 2; payload=%q", frames, forwarded.String())
	}
	select {
	case payload := <-requestBody:
		expected := codexIdentityConfuseUUID(auth.ID, "prompt-cache", "cache-1")
		if got := gjson.GetBytes(payload, "prompt_cache_key").String(); got != expected {
			t.Fatalf("upstream prompt_cache_key = %q, want %q", got, expected)
		}
	default:
		t.Fatal("upstream request body was not captured")
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

func TestCodexExecutorExecuteStream_DoesNotCommitBootstrapForUpstreamHeartbeat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, ": upstream accepted\n\n")
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
			cliproxyexecutor.TrustUpstreamSSEMetadataKey: true,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}

	var chunks []cliproxyexecutor.StreamChunk
	for chunk := range result.Chunks {
		chunks = append(chunks, chunk)
	}
	if len(chunks) != 2 {
		t.Fatalf("chunks = %#v, want heartbeat and terminal error", chunks)
	}
	if cliproxyexecutor.IsBootstrapCommitStreamChunk(chunks[0]) {
		t.Fatalf("chunks = %#v, heartbeat committed bootstrap before semantic data", chunks)
	}
	if got := string(chunks[0].Payload); got != ": upstream accepted\n\n" {
		t.Fatalf("heartbeat = %q", got)
	}
	if chunks[len(chunks)-1].Err == nil {
		t.Fatalf("final chunk = %#v, want incomplete stream error", chunks[len(chunks)-1])
	}
}

func TestCodexExecutorExecuteStream_TrustUpstreamSSEPreservesSuccessfulDoneFrame(t *testing.T) {
	upstream := "event: response.done\r\n" +
		"data: {\"type\":\"response.done\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"output\":[]}}\r\n\r\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(upstream))
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
	if string(payload) != upstream {
		t.Fatalf("trusted SSE bytes changed:\n got %q\nwant %q", payload, upstream)
	}
}

func TestCodexExecutorExecuteStream_TrustUpstreamSSERecognizesMultilineCompletion(t *testing.T) {
	upstream := "event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\n" +
		"data: \"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":2,\"output_tokens\":3,\"total_tokens\":5}}}\n\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(upstream))
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
	if string(payload) != upstream {
		t.Fatalf("trusted SSE bytes changed:\n got %q\nwant %q", payload, upstream)
	}
	data, ok := codexSSEFrameDataPayload(payload)
	if !ok || gjson.GetBytes(data, "response.usage.total_tokens").Int() != 5 {
		t.Fatalf("multiline data payload = %q", data)
	}
}

func TestCodexTrustedSSEFrameInspectionSkipsNonterminalImagePayload(t *testing.T) {
	nonterminal := []byte(`event: response.image_generation_call.partial_image
data: {"type":"response.image_generation_call.partial_image","partial_image_b64":"` + strings.Repeat("A", 1024) + `"}

`)
	if codexTrustedSSEFrameNeedsInspection(nonterminal) {
		t.Fatal("nonterminal image payload requested terminal parsing")
	}
	for _, event := range []string{"response.completed", "response.done", "response.failed", "response.incomplete", "error"} {
		frame := []byte(`data: {"type":"` + event + `"}` + "\n\n")
		if !codexTrustedSSEFrameNeedsInspection(frame) {
			t.Fatalf("terminal event %q was not inspected", event)
		}
	}
}

func TestAppendBoundedCodexTrustedSSEFrameRejectsCumulativeOverflow(t *testing.T) {
	frame, err := appendBoundedCodexTrustedSSEFrame([]byte("1234"), []byte("5678"), 8)
	if err != nil || string(frame) != "12345678" {
		t.Fatalf("bounded append = %q, %v", frame, err)
	}
	frame, err = appendBoundedCodexTrustedSSEFrame(frame, []byte("9"), 8)
	if err == nil || !strings.Contains(err.Error(), "exceeds 8 bytes") {
		t.Fatalf("overflow append error = %v", err)
	}
	if string(frame) != "12345678" {
		t.Fatalf("overflow append mutated frame: %q", frame)
	}
}

func TestSplitCodexSSELinesPreservesAllLineEndings(t *testing.T) {
	tests := []struct {
		name  string
		input string
		atEOF bool
		want  string
	}{
		{name: "LF", input: "data: one\nrest", want: "data: one\n"},
		{name: "CRLF", input: "data: one\r\nrest", want: "data: one\r\n"},
		{name: "CR", input: "data: one\rrest", want: "data: one\r"},
		{name: "terminal CR", input: "data: one\r", atEOF: true, want: "data: one\r"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			advance, token, err := splitCodexSSELinesPreserveEndings([]byte(test.input), test.atEOF)
			if err != nil {
				t.Fatalf("split error = %v", err)
			}
			if advance != len(test.want) || string(token) != test.want {
				t.Fatalf("split = (%d, %q), want (%d, %q)", advance, token, len(test.want), test.want)
			}
		})
	}
}

func TestCodexSSEFrameDataPayloadSupportsCROnlyFrames(t *testing.T) {
	payload, ok := codexSSEFrameDataPayload([]byte("event: response.completed\rdata: {\"type\":\"response.completed\"}\r\r"))
	if !ok || string(payload) != `{"type":"response.completed"}` {
		t.Fatalf("payload = %q, %t", payload, ok)
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

func TestCodexExecutorExecuteStream_ImagePassthroughRestoresCompletedOutput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.output_item.done\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.output_item.done","output_index":0,"item":{"type":"image_generation_call","id":"img-1","result":"ZmluYWw="}}` + "\n\n"))
		_, _ = w.Write([]byte("event: response.done\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.done","response":{"id":"resp_image","status":"completed","output":[]}}` + "\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"draw","tools":[{"type":"function","name":"image_gen.imagegen"}],"stream":true}`),
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

	var completed []byte
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
		line := bytes.TrimSpace(chunk.Payload)
		if !bytes.HasPrefix(line, dataTag) {
			continue
		}
		data := bytes.TrimSpace(line[len(dataTag):])
		if gjson.GetBytes(data, "type").String() == "response.completed" {
			completed = bytes.Clone(data)
		}
	}
	if len(completed) == 0 {
		t.Fatal("missing response.completed payload")
	}
	if got := gjson.GetBytes(completed, "response.output.0.type").String(); got != "image_generation_call" {
		t.Fatalf("completed output type = %q, want image_generation_call; payload=%s", got, completed)
	}
	if got := gjson.GetBytes(completed, "response.output.0.result").String(); got != "ZmluYWw=" {
		t.Fatalf("completed output result = %q, want restored image; payload=%s", got, completed)
	}
}

func TestCodexExecutorExecuteStreamReportsEffectivePassthroughAfterToolRemoval(t *testing.T) {
	upstreamBodies := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamBody, _ := io.ReadAll(r.Body)
		upstreamBodies <- upstreamBody
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"id":"resp_text","status":"completed","output":[]}}` + "\n\n"))
	}))
	defer server.Close()

	executor := NewCodexExecutor(&config.Config{AuthModelExclusions: []config.AuthModelExclusionRule{{
		DisableImageGeneration: true,
		Priorities:             []int{-1},
	}}})
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Attributes: map[string]string{
			"base_url": server.URL,
			"api_key":  "test",
			"priority": "-1",
		},
	}
	state := &cliproxyexecutor.ImageGenerationStreamPassthroughState{}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"draw","tools":[{"type":"function","name":"image_gen.imagegen"}],"stream":true}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.ImageGenerationStreamPassthroughMetadataKey:      true,
			cliproxyexecutor.ImageGenerationStreamPassthroughStateMetadataKey: state,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}
	if state.Enabled() {
		t.Fatal("effective image passthrough = true after policy removed the image tool")
	}
	upstreamBody := <-upstreamBodies
	if cliproxyauth.PayloadHasImageGenerationTool(upstreamBody) {
		t.Fatalf("upstream body still contains image tool: %s", upstreamBody)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error: %v", chunk.Err)
		}
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
	state := &cliproxyexecutor.ImageGenerationStreamPassthroughState{}
	result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"draw","tools":[{"type":"image_generation"}],"stream":true}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.ImageGenerationStreamPassthroughMetadataKey:      true,
			cliproxyexecutor.ImageGenerationStreamPassthroughStateMetadataKey: state,
			cliproxyexecutor.StreamTerminalMarkerMetadataKey:                  true,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream error: %v", err)
	}
	if !state.Enabled() {
		t.Fatal("effective image passthrough = false for an unchanged image request")
	}

	terminalCount := 0
	completedEvent := false
	completedPayload := false
	var forwarded bytes.Buffer
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
		forwarded.Write(chunk.Payload)
	}
	if terminalCount != 1 {
		t.Fatalf("terminal chunk count = %d, want 1", terminalCount)
	}
	if !completedEvent || !completedPayload {
		t.Fatal("missing normalized response.completed event and payload")
	}
	if frames := bytes.Count(forwarded.Bytes(), []byte("\n\n")); frames != 2 {
		t.Fatalf("image passthrough frame separators = %d, want 2; payload=%q", frames, forwarded.String())
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
