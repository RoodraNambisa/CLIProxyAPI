package executor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/wsrelay"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	cliproxyusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

type streamTerminalRoundTripFunc func(*http.Request) (*http.Response, error)

type streamTerminalUsagePlugin struct {
	authID  string
	records chan cliproxyusage.Record
}

func (p *streamTerminalUsagePlugin) HandleUsage(_ context.Context, record cliproxyusage.Record) {
	if p == nil || record.AuthID != p.authID {
		return
	}
	select {
	case p.records <- record:
	default:
	}
}

func (f streamTerminalRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type streamTerminalErrorBody struct {
	reader      *strings.Reader
	err         error
	errWithData bool
}

func (b *streamTerminalErrorBody) Read(p []byte) (int, error) {
	if b.reader.Len() > 0 {
		n, _ := b.reader.Read(p)
		if b.errWithData && b.reader.Len() == 0 {
			return n, b.err
		}
		return n, nil
	}
	return 0, b.err
}

func (*streamTerminalErrorBody) Close() error { return nil }

type streamTerminalRetirement struct {
	retired atomic.Bool
}

func (r *streamTerminalRetirement) Retired() bool { return r.retired.Load() }

func streamTerminalContext(rt http.RoundTripper) context.Context {
	return context.WithValue(context.Background(), "cliproxy.roundtripper", rt)
}

func streamTerminalOptions(format sdktranslator.Format, marker bool) cliproxyexecutor.Options {
	opts := cliproxyexecutor.Options{SourceFormat: format, Stream: true}
	if marker {
		opts.Metadata = map[string]any{cliproxyexecutor.StreamTerminalMarkerMetadataKey: true}
	}
	return opts
}

func collectStreamTerminalChunks(t *testing.T, result *cliproxyexecutor.StreamResult) (payloads [][]byte, markers int, streamErrs []error) {
	t.Helper()
	if result == nil || result.Chunks == nil {
		t.Fatal("stream result or chunks is nil")
	}
	for chunk := range result.Chunks {
		switch {
		case cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk):
			markers++
		case chunk.Err != nil:
			streamErrs = append(streamErrs, chunk.Err)
		default:
			payloads = append(payloads, bytes.Clone(chunk.Payload))
		}
	}
	return payloads, markers, streamErrs
}

func requireStreamErrorContains(t *testing.T, streamErrs []error, text string) {
	t.Helper()
	for _, streamErr := range streamErrs {
		if streamErr != nil && strings.Contains(streamErr.Error(), text) {
			return
		}
	}
	t.Fatalf("stream errors = %v, want text %q", streamErrs, text)
}

func TestGeminiAPIKeyExecuteStreamTerminalMarker(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		marker      bool
		readErr     error
		wantMarkers int
		wantErrors  int
		wantError   string
	}{
		{
			name:        "successful finish reason",
			body:        "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"ok\"}]},\"finishReason\":\"STOP\"}]}\n\n",
			marker:      true,
			wantMarkers: 1,
		},
		{
			name:        "stop followed by usage",
			body:        "data: {\"traceId\":\"trace-1\",\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\ndata: {\"traceId\":\"trace-1\",\"usageMetadata\":{\"promptTokenCount\":1,\"candidatesTokenCount\":2,\"totalTokenCount\":3}}\n\n",
			marker:      true,
			wantMarkers: 1,
		},
		{
			name:        "stop followed by usage without trace id",
			body:        "data: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\ndata: {\"usageMetadata\":{\"promptTokenCount\":1,\"candidatesTokenCount\":2,\"totalTokenCount\":3}}\n\n",
			marker:      true,
			wantMarkers: 1,
		},
		{
			name:        "blocked prompt feedback",
			body:        "data: {\"promptFeedback\":{\"blockReason\":\"SAFETY\"}}\n\n",
			marker:      true,
			wantMarkers: 1,
		},
		{
			name:   "direct call omits marker",
			body:   "data: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\n",
			marker: false,
		},
		{
			name:        "scanner error before completion",
			body:        "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"partial\"}]}}]}\n",
			marker:      true,
			readErr:     io.ErrUnexpectedEOF,
			wantErrors:  1,
			wantMarkers: 0,
		},
		{
			name:        "terminal token with scanner error",
			body:        "data: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n",
			marker:      true,
			readErr:     io.ErrUnexpectedEOF,
			wantErrors:  1,
			wantMarkers: 0,
		},
		{
			name:       "protocol error before completion",
			body:       "data: {\"error\":{\"message\":\"failed\"}}\ndata: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n",
			marker:     true,
			wantErrors: 1,
			wantError:  "failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := streamTerminalRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				if got := req.Header.Get("x-goog-api-key"); got != "gemini-key" {
					t.Fatalf("x-goog-api-key = %q, want gemini-key", got)
				}
				var body io.ReadCloser = io.NopCloser(strings.NewReader(tt.body))
				if tt.readErr != nil {
					body = &streamTerminalErrorBody{reader: strings.NewReader(tt.body), err: tt.readErr, errWithData: strings.Contains(tt.name, "terminal token")}
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
			})
			exec := NewGeminiExecutor(&config.Config{})
			auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "gemini-key", "base_url": "https://gemini.test"}}
			result, err := exec.ExecuteStream(streamTerminalContext(rt), auth, cliproxyexecutor.Request{
				Model:   "gemini-2.5-flash",
				Payload: []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`),
			}, streamTerminalOptions(sdktranslator.FormatGemini, tt.marker))
			if err != nil {
				t.Fatalf("ExecuteStream() error = %v", err)
			}
			_, markers, streamErrs := collectStreamTerminalChunks(t, result)
			if markers != tt.wantMarkers || len(streamErrs) != tt.wantErrors {
				t.Fatalf("markers = %d, errors = %v; want markers=%d errors=%d", markers, streamErrs, tt.wantMarkers, tt.wantErrors)
			}
			if tt.wantError != "" {
				requireStreamErrorContains(t, streamErrs, tt.wantError)
			}
		})
	}
}

func TestGeminiAPIKeyExecuteStreamPublishesUsageTailWithoutTraceID(t *testing.T) {
	const authID = "gemini-usage-tail-without-trace-id"
	body := "data: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\n" +
		"data: {\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":7,\"totalTokenCount\":12}}\n\n"
	rt := streamTerminalRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	})
	usageRecords := make(chan cliproxyusage.Record, 1)
	cliproxyusage.RegisterPlugin(&streamTerminalUsagePlugin{authID: authID, records: usageRecords})
	exec := NewGeminiExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		ID:         authID,
		Attributes: map[string]string{"api_key": "gemini-key", "base_url": "https://gemini.test"},
	}
	result, err := exec.ExecuteStream(streamTerminalContext(rt), auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`),
	}, streamTerminalOptions(sdktranslator.FormatGemini, true))
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	_, markers, streamErrs := collectStreamTerminalChunks(t, result)
	if markers != 1 || len(streamErrs) != 0 {
		t.Fatalf("markers = %d, errors = %v; want one marker and no errors", markers, streamErrs)
	}
	select {
	case record := <-usageRecords:
		if record.Failed || record.Detail.InputTokens != 5 || record.Detail.OutputTokens != 7 || record.Detail.TotalTokens != 12 {
			t.Fatalf("usage record = %#v, want final cumulative 5/7/12 success", record)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Gemini usage record")
	}
}

func TestGeminiVertexAPIKeyExecuteStreamTerminalMarker(t *testing.T) {
	rt := streamTerminalRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		if got := req.Header.Get("x-goog-api-key"); got != "vertex-key" {
			t.Fatalf("x-goog-api-key = %q, want vertex-key", got)
		}
		body := `data: {"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}]}` + "\n\n"
		body += `data: {"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":2,"totalTokenCount":3}}` + "\n\n"
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	exec := NewGeminiVertexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "vertex-key", "base_url": "https://vertex.test"}}
	result, err := exec.ExecuteStream(streamTerminalContext(rt), auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`),
	}, streamTerminalOptions(sdktranslator.FormatGemini, true))
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	payloads, markers, streamErrs := collectStreamTerminalChunks(t, result)
	if markers != 1 || len(streamErrs) != 0 {
		t.Fatalf("markers = %d, errors = %v; want one marker and no errors", markers, streamErrs)
	}
	if !bytes.Contains(bytes.Join(payloads, nil), []byte("usageMetadata")) {
		t.Fatalf("payloads = %q, want usage tail after STOP", payloads)
	}
}

func TestGeminiVertexAPIKeyCompletesBeforePostTerminalReadError(t *testing.T) {
	body := "data: {\"candidates\":[{\"finishReason\":\"STOP\"}]}\n\n" +
		"data: {\"usageMetadata\":{\"promptTokenCount\":1,\"candidatesTokenCount\":2,\"totalTokenCount\":3}}\n\n"
	rt := streamTerminalRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       &streamTerminalErrorBody{reader: strings.NewReader(body), err: io.ErrUnexpectedEOF, errWithData: true},
		}, nil
	})
	exec := NewGeminiVertexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "vertex-key", "base_url": "https://vertex.test"}}
	result, err := exec.ExecuteStream(streamTerminalContext(rt), auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`),
	}, streamTerminalOptions(sdktranslator.FormatGemini, true))
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	_, markers, streamErrs := collectStreamTerminalChunks(t, result)
	if markers != 1 || len(streamErrs) != 0 {
		t.Fatalf("markers = %d, errors = %v, want terminal success before trailing read error", markers, streamErrs)
	}
}

func TestGeminiVertexBlockedPromptFeedbackTerminalMarker(t *testing.T) {
	rt := streamTerminalRoundTripFunc(func(*http.Request) (*http.Response, error) {
		body := "data: {\"promptFeedback\":{\"blockReason\":\"SAFETY\"}}\n\n"
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	exec := NewGeminiVertexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "vertex-key", "base_url": "https://vertex.test"}}
	result, err := exec.ExecuteStream(streamTerminalContext(rt), auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`),
	}, streamTerminalOptions(sdktranslator.FormatGemini, true))
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	_, markers, streamErrs := collectStreamTerminalChunks(t, result)
	if markers != 1 || len(streamErrs) != 0 {
		t.Fatalf("markers = %d, errors = %v; want one marker and no errors", markers, streamErrs)
	}
}

func TestClaudeExecuteStreamTerminalMarker(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		marker      bool
		wantMarkers int
		wantBlank   bool
		wantErrors  int
		wantError   string
	}{
		{name: "message stop", body: "data: {\"type\":\"message_stop\"}\n\n", marker: true, wantMarkers: 1},
		{name: "direct call", body: "data: {\"type\":\"message_stop\"}\n\n", wantBlank: true},
		{name: "error before stop", body: "data: {\"type\":\"error\",\"error\":{\"message\":\"failed\"}}\ndata: {\"type\":\"message_stop\"}\n", marker: true, wantErrors: 1, wantError: "failed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, tt.body)
			}))
			defer server.Close()

			exec := NewClaudeExecutor(&config.Config{})
			auth := &cliproxyauth.Auth{ProxyURL: "direct", Attributes: map[string]string{"api_key": "claude-key", "base_url": server.URL}}
			result, err := exec.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
				Model:   "claude-3-5-sonnet-20241022",
				Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
			}, streamTerminalOptions(sdktranslator.FormatClaude, tt.marker))
			if err != nil {
				t.Fatalf("ExecuteStream() error = %v", err)
			}
			payloads, markers, streamErrs := collectStreamTerminalChunks(t, result)
			if markers != tt.wantMarkers || len(streamErrs) != tt.wantErrors {
				t.Fatalf("markers = %d, errors = %v; want markers=%d errors=%d", markers, streamErrs, tt.wantMarkers, tt.wantErrors)
			}
			if tt.wantBlank && !bytes.HasSuffix(bytes.Join(payloads, nil), []byte("\n\n")) {
				t.Fatalf("direct Claude payloads = %q, want SSE blank-line terminator", payloads)
			}
			if tt.wantError != "" {
				requireStreamErrorContains(t, streamErrs, tt.wantError)
			}
		})
	}
}

func TestOpenAICompatExecuteStreamTerminalMarker(t *testing.T) {
	tests := []struct {
		name        string
		body        string
		marker      bool
		readErr     error
		wantMarkers int
		wantErrors  int
		wantError   string
	}{
		{name: "done", body: "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\ndata: [DONE]\n", marker: true, wantMarkers: 1},
		{name: "direct call", body: "data: [DONE]\n"},
		{name: "clean eof without done", body: "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n", marker: true, wantErrors: 1},
		{name: "protocol error before done", body: "data: {\"error\":{\"message\":\"failed\"}}\ndata: [DONE]\n", marker: true, wantErrors: 1, wantError: "failed"},
		{name: "scanner error", body: "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n", marker: true, readErr: io.ErrUnexpectedEOF, wantErrors: 1},
		{name: "done with scanner error", body: "data: [DONE]\n", marker: true, readErr: io.ErrUnexpectedEOF, wantErrors: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := streamTerminalRoundTripFunc(func(*http.Request) (*http.Response, error) {
				var body io.ReadCloser = io.NopCloser(strings.NewReader(tt.body))
				if tt.readErr != nil {
					body = &streamTerminalErrorBody{reader: strings.NewReader(tt.body), err: tt.readErr, errWithData: strings.Contains(tt.name, "done with")}
				}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
			})
			exec := NewOpenAICompatExecutor("openai-compatibility", &config.Config{})
			auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "openai-key", "base_url": "https://openai.test/v1"}}
			result, err := exec.ExecuteStream(streamTerminalContext(rt), auth, cliproxyexecutor.Request{
				Model:   "custom-openai",
				Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
			}, streamTerminalOptions(sdktranslator.FormatOpenAI, tt.marker))
			if err != nil {
				t.Fatalf("ExecuteStream() error = %v", err)
			}
			_, markers, streamErrs := collectStreamTerminalChunks(t, result)
			if markers != tt.wantMarkers || len(streamErrs) != tt.wantErrors {
				t.Fatalf("markers = %d, errors = %v; want markers=%d errors=%d", markers, streamErrs, tt.wantMarkers, tt.wantErrors)
			}
			if tt.wantError != "" {
				requireStreamErrorContains(t, streamErrs, tt.wantError)
			}
		})
	}
}

func TestKimiExecuteStreamTerminalMarker(t *testing.T) {
	tests := []struct {
		name        string
		body        io.ReadCloser
		marker      bool
		wantMarkers int
		wantErrors  int
	}{
		{
			name:        "done",
			body:        io.NopCloser(strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\ndata: [DONE]\n")),
			marker:      true,
			wantMarkers: 1,
		},
		{
			name:       "read error",
			body:       &streamTerminalErrorBody{reader: strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n"), err: io.ErrUnexpectedEOF},
			marker:     true,
			wantErrors: 1,
		},
		{
			name:       "done with read error",
			body:       &streamTerminalErrorBody{reader: strings.NewReader("data: [DONE]\n"), err: io.ErrUnexpectedEOF, errWithData: true},
			marker:     true,
			wantErrors: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := streamTerminalRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: tt.body}, nil
			})
			exec := NewKimiExecutor(&config.Config{})
			auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "kimi-key"}}
			result, err := exec.ExecuteStream(streamTerminalContext(rt), auth, cliproxyexecutor.Request{
				Model:   "kimi-k2.5",
				Payload: []byte(`{"messages":[{"role":"user","content":"hi"}]}`),
			}, streamTerminalOptions(sdktranslator.FormatOpenAI, tt.marker))
			if err != nil {
				t.Fatalf("ExecuteStream() error = %v", err)
			}
			_, markers, streamErrs := collectStreamTerminalChunks(t, result)
			if markers != tt.wantMarkers || len(streamErrs) != tt.wantErrors {
				t.Fatalf("markers = %d, errors = %v; want markers=%d errors=%d", markers, streamErrs, tt.wantMarkers, tt.wantErrors)
			}
		})
	}
}

func TestAntigravityExecuteStreamCompletionWinsRetirementRace(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"ok\"}]},\"finishReason\":\"STOP\"}]}}\n\ndata: {\"response\":{\"usageMetadata\":{\"promptTokenCount\":1,\"candidatesTokenCount\":2,\"totalTokenCount\":3}}}\n\n")
	}))
	defer server.Close()

	retirement := &streamTerminalRetirement{}
	exec := NewAntigravityExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		ID:       "antigravity-terminal",
		ProxyURL: "direct",
		Attributes: map[string]string{
			"base_url": server.URL,
		},
		Metadata: map[string]any{
			"access_token": "token",
			"project_id":   "project-1",
			"expired":      time.Now().Add(2 * time.Hour).Format(time.RFC3339),
		},
	}
	opts := streamTerminalOptions(sdktranslator.FormatAntigravity, true)
	opts.Metadata[cliproxyexecutor.SelectedAuthInstanceRetirementMetadataKey] = retirement
	result, err := exec.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`),
	}, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}

	first, ok := <-result.Chunks
	if !ok || first.Err != nil || cliproxyexecutor.IsSuccessfulStreamTerminalChunk(first) {
		t.Fatalf("first chunk = (%#v, %t), want terminal response payload", first, ok)
	}
	retirement.retired.Store(true)

	var remaining []cliproxyexecutor.StreamChunk
	for chunk := range result.Chunks {
		remaining = append(remaining, chunk)
	}
	if len(remaining) == 0 || !cliproxyexecutor.IsSuccessfulStreamTerminalChunk(remaining[len(remaining)-1]) {
		t.Fatalf("remaining chunks = %#v, want successful marker after retirement", remaining)
	}
}

func TestAntigravityExecuteStreamPublishesUsageTailWithoutTraceID(t *testing.T) {
	const authID = "antigravity-usage-tail-without-trace-id"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"response\":{\"candidates\":[{\"finishReason\":\"STOP\"}]}}\n\n"+
			"data: {\"response\":{\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":7,\"totalTokenCount\":12}}}\n\n")
	}))
	defer server.Close()

	usageRecords := make(chan cliproxyusage.Record, 1)
	cliproxyusage.RegisterPlugin(&streamTerminalUsagePlugin{authID: authID, records: usageRecords})
	exec := NewAntigravityExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		ID:       authID,
		ProxyURL: "direct",
		Attributes: map[string]string{
			"base_url": server.URL,
		},
		Metadata: map[string]any{
			"access_token": "token",
			"project_id":   "project-1",
			"expired":      time.Now().Add(2 * time.Hour).Format(time.RFC3339),
		},
	}
	result, err := exec.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`),
	}, streamTerminalOptions(sdktranslator.FormatAntigravity, true))
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	_, markers, streamErrs := collectStreamTerminalChunks(t, result)
	if markers != 1 || len(streamErrs) != 0 {
		t.Fatalf("markers = %d, errors = %v; want one marker and no errors", markers, streamErrs)
	}
	select {
	case record := <-usageRecords:
		if record.Failed || record.Detail.InputTokens != 5 || record.Detail.OutputTokens != 7 || record.Detail.TotalTokens != 12 {
			t.Fatalf("usage record = %#v, want final cumulative 5/7/12 success", record)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Antigravity usage record")
	}
}

func TestAntigravityBlockedPromptFeedbackTerminalMarker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"response\":{\"promptFeedback\":{\"blockReason\":\"SAFETY\"}}}\n\n")
	}))
	defer server.Close()

	exec := NewAntigravityExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		ID: "antigravity-blocked", ProxyURL: "direct",
		Attributes: map[string]string{"base_url": server.URL},
		Metadata: map[string]any{
			"access_token": "token",
			"project_id":   "project-1",
			"expired":      time.Now().Add(2 * time.Hour).Format(time.RFC3339),
		},
	}
	result, err := exec.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`),
	}, streamTerminalOptions(sdktranslator.FormatAntigravity, true))
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	_, markers, streamErrs := collectStreamTerminalChunks(t, result)
	if markers != 1 || len(streamErrs) != 0 {
		t.Fatalf("markers = %d, errors = %v; want one marker and no errors", markers, streamErrs)
	}
}

func TestCodexAndXAIExecuteStreamFailuresOmitTerminalMarker(t *testing.T) {
	t.Run("Codex", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"failed\"}}}\n\n")
		}))
		defer server.Close()

		exec := NewCodexExecutor(&config.Config{})
		auth := &cliproxyauth.Auth{ProxyURL: "direct", Attributes: map[string]string{"api_key": "codex-key", "base_url": server.URL}}
		result, err := exec.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
			Model:   "gpt-5.4-mini",
			Payload: []byte(`{"model":"gpt-5.4-mini","input":"hi"}`),
		}, streamTerminalOptions(sdktranslator.FormatCodex, true))
		if err != nil {
			t.Fatalf("ExecuteStream() error = %v", err)
		}
		_, markers, streamErrs := collectStreamTerminalChunks(t, result)
		if markers != 0 || len(streamErrs) != 1 {
			t.Fatalf("markers = %d, errors = %v; want no marker and one error", markers, streamErrs)
		}
	})

	t.Run("xAI", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"failed\"}}}\n\n")
		}))
		defer server.Close()

		exec := NewXAIExecutor(&config.Config{})
		auth := &cliproxyauth.Auth{ProxyURL: "direct", Attributes: map[string]string{"api_key": "xai-key", "base_url": server.URL}}
		result, err := exec.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
			Model:   "grok-4.3",
			Payload: []byte(`{"model":"grok-4.3","input":"hi"}`),
		}, streamTerminalOptions(sdktranslator.FormatOpenAIResponse, true))
		if err != nil {
			t.Fatalf("ExecuteStream() error = %v", err)
		}
		_, markers, streamErrs := collectStreamTerminalChunks(t, result)
		if markers != 0 || len(streamErrs) != 1 {
			t.Fatalf("markers = %d, errors = %v; want no marker and one error", markers, streamErrs)
		}
	})
}

func TestCodexAndXAITerminalReadErrorsOmitMarker(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		execute func(context.Context, *cliproxyauth.Auth) (*cliproxyexecutor.StreamResult, error)
	}{
		{
			name: "Codex",
			body: "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"output\":[]}}\n",
			execute: func(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyexecutor.StreamResult, error) {
				return NewCodexExecutor(&config.Config{}).ExecuteStream(ctx, auth, cliproxyexecutor.Request{
					Model:   "gpt-5.4-mini",
					Payload: []byte(`{"model":"gpt-5.4-mini","input":"hi"}`),
				}, streamTerminalOptions(sdktranslator.FormatCodex, true))
			},
		},
		{
			name: "xAI",
			body: "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"output\":[]}}\n",
			execute: func(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyexecutor.StreamResult, error) {
				return NewXAIExecutor(&config.Config{}).ExecuteStream(ctx, auth, cliproxyexecutor.Request{
					Model:   "grok-4.3",
					Payload: []byte(`{"model":"grok-4.3","input":"hi"}`),
				}, streamTerminalOptions(sdktranslator.FormatOpenAIResponse, true))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := streamTerminalRoundTripFunc(func(*http.Request) (*http.Response, error) {
				body := &streamTerminalErrorBody{reader: strings.NewReader(tt.body), err: io.ErrUnexpectedEOF, errWithData: true}
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}, nil
			})
			auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "key", "base_url": "https://terminal.test"}}
			result, err := tt.execute(streamTerminalContext(rt), auth)
			if err != nil {
				t.Fatalf("ExecuteStream() error = %v", err)
			}
			_, markers, streamErrs := collectStreamTerminalChunks(t, result)
			if markers != 0 || len(streamErrs) != 1 {
				t.Fatalf("markers = %d, errors = %v; want no marker and one error", markers, streamErrs)
			}
		})
	}
}

func TestAIStudioExecuteStreamTerminalMarker(t *testing.T) {
	for _, tt := range []struct {
		name        string
		respond     func(*websocket.Conn, wsrelay.Message) error
		marker      bool
		wantMarkers int
		wantErrors  int
		usageFailed bool
	}{
		{
			name: "stream end",
			respond: func(conn *websocket.Conn, req wsrelay.Message) error {
				if err := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamStart, Payload: map[string]any{"status": http.StatusOK}}); err != nil {
					return err
				}
				if err := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamChunk, Payload: map[string]any{"data": `data: {"candidates":[{"finishReason":"STOP"}]}` + "\n\n"}}); err != nil {
					return err
				}
				return conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamEnd})
			},
			marker:      true,
			wantMarkers: 1,
		},
		{
			name: "empty stream end",
			respond: func(conn *websocket.Conn, req wsrelay.Message) error {
				if err := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamStart, Payload: map[string]any{"status": http.StatusOK}}); err != nil {
					return err
				}
				return conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamEnd})
			},
			marker:      true,
			wantErrors:  1,
			usageFailed: true,
		},
		{
			name: "protocol error before stream end",
			respond: func(conn *websocket.Conn, req wsrelay.Message) error {
				if err := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamStart, Payload: map[string]any{"status": http.StatusOK}}); err != nil {
					return err
				}
				if err := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamChunk, Payload: map[string]any{"data": `data: {"error":{"message":"failed"}}` + "\n\n"}}); err != nil {
					return err
				}
				return conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamEnd})
			},
			marker:      true,
			wantErrors:  1,
			usageFailed: true,
		},
		{
			name: "malformed payload before stream end",
			respond: func(conn *websocket.Conn, req wsrelay.Message) error {
				if err := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamStart, Payload: map[string]any{"status": http.StatusOK}}); err != nil {
					return err
				}
				if err := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamChunk, Payload: map[string]any{"data": "data: {malformed}\n"}}); err != nil {
					return err
				}
				if err := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamChunk, Payload: map[string]any{"data": `data: {"candidates":[{"finishReason":"STOP"}]}` + "\n\n"}}); err != nil {
					return err
				}
				return conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamEnd})
			},
			marker:      true,
			wantErrors:  1,
			usageFailed: true,
		},
		{
			name: "terminal split across chunks",
			respond: func(conn *websocket.Conn, req wsrelay.Message) error {
				if err := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamStart, Payload: map[string]any{"status": http.StatusOK}}); err != nil {
					return err
				}
				if err := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamChunk, Payload: map[string]any{"data": `data: {"candidates":[{"finish`}}); err != nil {
					return err
				}
				if err := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamChunk, Payload: map[string]any{"data": `Reason":"STOP"}]}` + "\n\n"}}); err != nil {
					return err
				}
				return conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamEnd})
			},
			marker:      true,
			wantMarkers: 1,
		},
		{
			name: "partial payload without finish reason",
			respond: func(conn *websocket.Conn, req wsrelay.Message) error {
				if err := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamStart, Payload: map[string]any{"status": http.StatusOK}}); err != nil {
					return err
				}
				if err := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamChunk, Payload: map[string]any{"data": `data: {"candidates":[{"content":{"parts":[{"text":"partial"}]}}]}` + "\n\n"}}); err != nil {
					return err
				}
				return conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamEnd})
			},
			marker:      true,
			wantErrors:  1,
			usageFailed: true,
		},
		{
			name: "blocked prompt feedback",
			respond: func(conn *websocket.Conn, req wsrelay.Message) error {
				if err := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamStart, Payload: map[string]any{"status": http.StatusOK}}); err != nil {
					return err
				}
				if err := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamChunk, Payload: map[string]any{"data": `data: {"promptFeedback":{"blockReason":"SAFETY"}}` + "\n\n"}}); err != nil {
					return err
				}
				return conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamEnd})
			},
			marker:      true,
			wantMarkers: 1,
		},
		{
			name: "complete prompt feedback response",
			respond: func(conn *websocket.Conn, req wsrelay.Message) error {
				return conn.WriteJSON(wsrelay.Message{
					ID:   req.ID,
					Type: wsrelay.MessageTypeHTTPResp,
					Payload: map[string]any{
						"status": http.StatusOK,
						"body":   `{"promptFeedback":{"blockReason":"SAFETY"}}`,
					},
				})
			},
			marker:      true,
			wantMarkers: 1,
		},
		{
			name: "invalid complete response",
			respond: func(conn *websocket.Conn, req wsrelay.Message) error {
				return conn.WriteJSON(wsrelay.Message{
					ID:   req.ID,
					Type: wsrelay.MessageTypeHTTPResp,
					Payload: map[string]any{
						"status": http.StatusOK,
						"body":   "not-json",
					},
				})
			},
			marker:      true,
			wantErrors:  1,
			usageFailed: true,
		},
		{
			name: "relay error",
			respond: func(conn *websocket.Conn, req wsrelay.Message) error {
				return conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeError, Payload: map[string]any{"error": "relay failed", "status": http.StatusBadGateway}})
			},
			marker:      true,
			wantErrors:  1,
			usageFailed: true,
		},
		{
			name: "direct call",
			respond: func(conn *websocket.Conn, req wsrelay.Message) error {
				if err := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamStart, Payload: map[string]any{"status": http.StatusOK}}); err != nil {
					return err
				}
				return conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamEnd})
			},
			wantErrors:  1,
			usageFailed: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			authID := "aistudio-terminal-" + strings.ReplaceAll(tt.name, " ", "-")
			connected := make(chan struct{})
			manager := wsrelay.NewManager(wsrelay.Options{
				ProviderFactory: func(*http.Request) (string, error) { return authID, nil },
				OnConnected:     func(string) { close(connected) },
			})
			server := httptest.NewServer(manager.Handler())
			defer server.Close()
			defer manager.Stop(context.Background())

			clientErr := make(chan error, 1)
			go func() {
				wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + manager.Path()
				conn, _, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
				if errDial != nil {
					clientErr <- errDial
					return
				}
				defer conn.Close()
				var req wsrelay.Message
				if errRead := conn.ReadJSON(&req); errRead != nil {
					clientErr <- errRead
					return
				}
				clientErr <- tt.respond(conn, req)
			}()

			select {
			case <-connected:
			case errClient := <-clientErr:
				t.Fatalf("relay client setup error = %v", errClient)
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for relay client")
			}

			exec := NewAIStudioExecutor(&config.Config{}, authID, manager)
			usageRecords := make(chan cliproxyusage.Record, 1)
			cliproxyusage.RegisterPlugin(&streamTerminalUsagePlugin{authID: authID, records: usageRecords})
			result, err := exec.ExecuteStream(context.Background(), &cliproxyauth.Auth{ID: authID}, cliproxyexecutor.Request{
				Model:   "gemini-2.5-flash",
				Payload: []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`),
			}, streamTerminalOptions(sdktranslator.FormatGemini, tt.marker))
			if err != nil {
				t.Fatalf("ExecuteStream() error = %v", err)
			}
			_, markers, streamErrs := collectStreamTerminalChunks(t, result)
			if markers != tt.wantMarkers || len(streamErrs) != tt.wantErrors {
				t.Fatalf("markers = %d, errors = %v; want markers=%d errors=%d", markers, streamErrs, tt.wantMarkers, tt.wantErrors)
			}
			select {
			case record := <-usageRecords:
				if record.Failed != tt.usageFailed {
					t.Fatalf("usage failed = %t, want %t", record.Failed, tt.usageFailed)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for usage record")
			}
			if errClient := <-clientErr; errClient != nil && !errors.Is(errClient, io.EOF) {
				t.Fatalf("relay client error = %v", errClient)
			}
		})
	}
}

func TestAIStudioExecuteStreamPublishesFinalCumulativeUsage(t *testing.T) {
	const authID = "aistudio-final-cumulative-usage"
	connected := make(chan struct{})
	manager := wsrelay.NewManager(wsrelay.Options{
		ProviderFactory: func(*http.Request) (string, error) { return authID, nil },
		OnConnected:     func(string) { close(connected) },
	})
	server := httptest.NewServer(manager.Handler())
	defer server.Close()
	defer manager.Stop(context.Background())

	clientErr := make(chan error, 1)
	releaseStreamEnd := make(chan struct{})
	go func() {
		wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + manager.Path()
		conn, _, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
		if errDial != nil {
			clientErr <- errDial
			return
		}
		defer conn.Close()
		var req wsrelay.Message
		if errRead := conn.ReadJSON(&req); errRead != nil {
			clientErr <- errRead
			return
		}
		if errWrite := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamStart, Payload: map[string]any{"status": http.StatusOK}}); errWrite != nil {
			clientErr <- errWrite
			return
		}
		for _, payload := range []string{
			`data: {"candidates":[{"content":{"parts":[{"text":"partial"}]}}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}` + "\n\n",
			`data: {"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":7,"totalTokenCount":12}}` + "\n\n",
		} {
			if errWrite := conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamChunk, Payload: map[string]any{"data": payload}}); errWrite != nil {
				clientErr <- errWrite
				return
			}
		}
		<-releaseStreamEnd
		clientErr <- conn.WriteJSON(wsrelay.Message{ID: req.ID, Type: wsrelay.MessageTypeStreamEnd})
	}()

	select {
	case <-connected:
	case errClient := <-clientErr:
		t.Fatalf("relay client setup error = %v", errClient)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for relay client")
	}

	usageRecords := make(chan cliproxyusage.Record, 1)
	cliproxyusage.RegisterPlugin(&streamTerminalUsagePlugin{authID: authID, records: usageRecords})
	exec := NewAIStudioExecutor(&config.Config{}, authID, manager)
	result, err := exec.ExecuteStream(t.Context(), &cliproxyauth.Auth{ID: authID}, cliproxyexecutor.Request{
		Model:   "gemini-2.5-flash",
		Payload: []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`),
	}, streamTerminalOptions(sdktranslator.FormatGemini, true))
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	_, markers, streamErrs := collectStreamTerminalChunks(t, result)
	if markers != 1 || len(streamErrs) != 0 {
		t.Fatalf("markers = %d, errors = %v, want one early success marker", markers, streamErrs)
	}

	select {
	case record := <-usageRecords:
		if record.Failed || record.Detail.InputTokens != 5 || record.Detail.OutputTokens != 7 || record.Detail.TotalTokens != 12 {
			t.Fatalf("usage record = %#v, want final cumulative 5/7/12 success", record)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for usage record")
	}
	close(releaseStreamEnd)
	if errClient := <-clientErr; errClient != nil && !errors.Is(errClient, io.EOF) {
		t.Fatalf("relay client error = %v", errClient)
	}
}

func TestCodexTrustedResponseDonePublishesNormalizedUsage(t *testing.T) {
	const authID = "codex-trusted-response-done-usage"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: response.done\n")
		_, _ = io.WriteString(w, `data: {"type":"response.done","response":{"status":"completed","usage":{"input_tokens":5,"output_tokens":7,"total_tokens":12}}}`+"\n\n")
	}))
	defer server.Close()

	usageRecords := make(chan cliproxyusage.Record, 1)
	cliproxyusage.RegisterPlugin(&streamTerminalUsagePlugin{authID: authID, records: usageRecords})
	exec := NewCodexExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: authID, Attributes: map[string]string{"base_url": server.URL, "api_key": "test"}}
	result, err := exec.ExecuteStream(t.Context(), auth, cliproxyexecutor.Request{
		Model:   "gpt-5.4-mini",
		Payload: []byte(`{"model":"gpt-5.4-mini","input":"hi","stream":true}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.TrustUpstreamSSEMetadataKey:     true,
			cliproxyexecutor.StreamTerminalMarkerMetadataKey: true,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	_, markers, streamErrs := collectStreamTerminalChunks(t, result)
	if markers != 1 || len(streamErrs) != 0 {
		t.Fatalf("markers = %d, errors = %v", markers, streamErrs)
	}
	select {
	case record := <-usageRecords:
		if record.Failed || record.Detail.InputTokens != 5 || record.Detail.OutputTokens != 7 || record.Detail.TotalTokens != 12 {
			t.Fatalf("usage record = %#v", record)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Codex usage record")
	}
}
