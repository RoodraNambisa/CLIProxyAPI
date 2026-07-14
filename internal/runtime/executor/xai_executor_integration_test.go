package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	xaiauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/xai"
	internalcache "github.com/router-for-me/CLIProxyAPI/v6/internal/cache"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/translator"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	cliproxyusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func makeValidXAIEncryptedContent(seed byte) string {
	buf := make([]byte, 0, 256)
	for i := 0; len(buf) < 256; i++ {
		sum := sha256.Sum256([]byte{seed, byte(i), byte(i >> 8)})
		buf = append(buf, sum[:]...)
	}
	return base64.RawStdEncoding.EncodeToString(buf[:256])
}

func TestXAIRuntimeExecutionBodyPreservesEOFWhenRetired(t *testing.T) {
	body := &xaiRuntimeExecutionBody{
		ReadCloser: io.NopCloser(strings.NewReader("ok")),
		release:    func() bool { return true },
	}
	data, errRead := io.ReadAll(body)
	if errRead != nil {
		t.Fatalf("ReadAll() error = %v", errRead)
	}
	if string(data) != "ok" {
		t.Fatalf("ReadAll() data = %q, want ok", data)
	}
}

func TestXAIExecuteAcceptsResponseDoneAndRestoresOutput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\"}]}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.done\",\"response\":{\"id\":\"resp_done\",\"status\":\"completed\",\"output\":[]}}\n\n"))
	}))
	defer server.Close()
	exec := NewXAIExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "token"}}
	response, errExecute := exec.Execute(t.Context(), auth, xaiStreamRequest(), cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if !bytes.Contains(response.Payload, []byte("hello")) {
		t.Fatalf("Execute() payload = %s, want restored output", response.Payload)
	}
}

func TestXAIUsingAPIRoutingAndChatProxyHeaders(t *testing.T) {
	oauth := &cliproxyauth.Auth{Attributes: map[string]string{
		"auth_kind": "oauth",
		"base_url":  xaiauth.DefaultAPIBaseURL,
	}}
	if got := xaiChatBaseURL(oauth); got != xaiauth.CLIChatProxyBaseURL {
		t.Fatalf("OAuth base URL = %q, want %q", got, xaiauth.CLIChatProxyBaseURL)
	}
	req := httptest.NewRequest(http.MethodPost, xaiauth.CLIChatProxyBaseURL+"/responses", nil)
	applyXAIChatHeaders(req, oauth, "token", true, "conversation")
	if got := req.Header.Get(xaiTokenAuthHeader); got != xaiTokenAuthValue {
		t.Fatalf("%s = %q, want %q", xaiTokenAuthHeader, got, xaiTokenAuthValue)
	}
	if got := req.Header.Get(xaiClientVersionHeader); got != xaiClientVersionValue {
		t.Fatalf("%s = %q, want %q", xaiClientVersionHeader, got, xaiClientVersionValue)
	}

	official := &cliproxyauth.Auth{Attributes: map[string]string{
		"auth_kind":     "oauth",
		"base_url":      xaiauth.DefaultAPIBaseURL,
		xaiUsingAPIAttr: "true",
	}}
	if got := xaiChatBaseURL(official); got != xaiauth.DefaultAPIBaseURL {
		t.Fatalf("using_api base URL = %q, want %q", got, xaiauth.DefaultAPIBaseURL)
	}
	req = httptest.NewRequest(http.MethodPost, xaiauth.DefaultAPIBaseURL+"/responses", nil)
	applyXAIChatHeaders(req, official, "token", true, "")
	if req.Header.Get(xaiTokenAuthHeader) != "" || req.Header.Get(xaiClientVersionHeader) != "" {
		t.Fatalf("official API unexpectedly received CLI headers: %v", req.Header)
	}
}

func TestXAIExecutorCompactUsesCompactEndpoint(t *testing.T) {
	var path string
	var body []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		body, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_1","object":"response.compaction","output":[{"type":"compaction","encrypted_content":"opaque"}]}`))
	}))
	defer server.Close()

	exec := NewXAIExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "token"}}
	resp, err := exec.Execute(context.Background(), auth, cliproxyexecutor.Request{
		Model:   "grok-4.3",
		Payload: []byte(`{"model":"grok-4.3","stream":true,"input":[{"role":"user","content":"hello"}],"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}],"tool_choice":"auto","parallel_tool_calls":true}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Alt: "responses/compact"})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if path != "/responses/compact" || gjson.GetBytes(body, "stream").Exists() {
		t.Fatalf("compact path=%q body=%s", path, body)
	}
	if gjson.GetBytes(body, "tools").Exists() || gjson.GetBytes(body, "tool_choice").Exists() || gjson.GetBytes(body, "parallel_tool_calls").Exists() {
		t.Fatalf("compact body retained tool controls: %s", body)
	}
	if got := gjson.GetBytes(resp.Payload, "object").String(); got != "response.compaction" {
		t.Fatalf("response object = %q", got)
	}
}

func TestXAIExecutorReturnsTerminalFailureFromSuccessfulHTTP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\",\"message\":\"generation failed\"}}}\n\n"))
	}))
	defer server.Close()

	exec := NewXAIExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: "xai-terminal", Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "token"}}
	_, err := exec.Execute(context.Background(), auth, xaiStreamRequest(), cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if err == nil || !strings.Contains(err.Error(), "generation failed") {
		t.Fatalf("Execute() error = %v, want terminal failure", err)
	}
}

func TestXAIExecutorPreservesRetryAfterHeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"code":"rate_limit","error":"too many requests"}`))
	}))
	defer server.Close()

	exec := NewXAIExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "token"}}
	_, err := exec.Execute(context.Background(), auth, xaiStreamRequest(), cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if err == nil {
		t.Fatal("Execute() error = nil, want 429")
	}
	retryable, ok := err.(interface{ RetryAfter() *time.Duration })
	if !ok {
		t.Fatalf("error type = %T, want RetryAfter support", err)
	}
	if retryable.RetryAfter() == nil || *retryable.RetryAfter() != 2*time.Minute {
		t.Fatalf("RetryAfter = %v, want 2m", retryable.RetryAfter())
	}
}

func TestXAIExecuteStreamEmitsTerminalFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n"))
		_, _ = w.Write([]byte("data: {\"type\":\"response.incomplete\",\"response\":{\"incomplete_details\":{\"reason\":\"content_filter\"}}}\n\n"))
	}))
	defer server.Close()

	exec := NewXAIExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: "xai-stream-terminal", Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "token"}}
	result, err := exec.ExecuteStream(context.Background(), auth, xaiStreamRequest(), cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	var terminalErr error
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			terminalErr = chunk.Err
		}
	}
	if terminalErr == nil || !strings.Contains(terminalErr.Error(), "content_filter") {
		t.Fatalf("stream terminal error = %v", terminalErr)
	}
	skipper, ok := terminalErr.(interface{ SkipAuthResult() bool })
	if !ok || !skipper.SkipAuthResult() {
		t.Fatalf("SkipAuthResult = %v, want true", ok)
	}
}

func TestXAIExecuteStreamRejectsEOFWithoutTerminal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n"))
	}))
	defer server.Close()

	const authID = "xai-stream-incomplete"
	usageRecords := make(chan cliproxyusage.Record, 1)
	cliproxyusage.RegisterPlugin(&streamTerminalUsagePlugin{authID: authID, records: usageRecords})
	exec := NewXAIExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: authID, Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "token"}}
	result, err := exec.ExecuteStream(t.Context(), auth, xaiStreamRequest(), cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.StreamTerminalMarkerMetadataKey: true,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	_, markers, streamErrs := collectStreamTerminalChunks(t, result)
	if markers != 0 || len(streamErrs) != 1 || !strings.Contains(streamErrs[0].Error(), "successful terminal event") {
		t.Fatalf("markers = %d, errors = %v; want one incomplete stream error", markers, streamErrs)
	}
	select {
	case record := <-usageRecords:
		if !record.Failed {
			t.Fatalf("usage record = %#v, want failure", record)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for usage record")
	}
}

func TestXAIExecuteStreamEmitsSuccessfulTerminalAfterTranslatedCompletion(t *testing.T) {
	responseFormat := registerXAITerminalOrderTestTranslator()
	for _, completionType := range []string{"response.completed", "response.done"} {
		t.Run(completionType, func(t *testing.T) {
			releaseHandler := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte("event: " + completionType + "\n"))
				_, _ = w.Write([]byte(`data: {"type":"` + completionType + `","response":{"id":"resp_1","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}` + "\n\n"))
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				select {
				case <-r.Context().Done():
				case <-releaseHandler:
				}
			}))
			defer server.Close()
			defer close(releaseHandler)

			manager := cliproxyauth.NewManager(nil, nil, nil)
			authID := "xai-http-terminal-" + strings.ReplaceAll(completionType, ".", "-")
			selected, errRegister := manager.Register(t.Context(), &cliproxyauth.Auth{
				ID:       authID,
				Provider: "xai",
				Attributes: map[string]string{
					"base_url": server.URL,
					"api_key":  "token",
				},
				Metadata: map[string]any{"type": "xai"},
			})
			if errRegister != nil {
				t.Fatalf("Register() error = %v", errRegister)
			}

			exec := NewXAIExecutor(&config.Config{})
			result, err := exec.ExecuteStream(t.Context(), selected, xaiStreamRequest(), cliproxyexecutor.Options{
				SourceFormat:   sdktranslator.FormatOpenAIResponse,
				ResponseFormat: responseFormat,
				Stream:         true,
				Metadata: map[string]any{
					cliproxyexecutor.StreamTerminalMarkerMetadataKey: true,
				},
			})
			if err != nil {
				t.Fatalf("ExecuteStream() error = %v", err)
			}

			var first cliproxyexecutor.StreamChunk
			select {
			case first = <-result.Chunks:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for first translated completion chunk")
			}
			if first.Err != nil || string(first.Payload) != "completion-1" {
				t.Fatalf("first chunk = %#v, want completion-1", first)
			}

			if _, errUpdate := manager.Update(t.Context(), &cliproxyauth.Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errUpdate != nil {
				t.Fatalf("Update() error = %v", errUpdate)
			}

			remaining := make([]cliproxyexecutor.StreamChunk, 0, 2)
			closed := false
			for !closed {
				select {
				case chunk, ok := <-result.Chunks:
					if !ok {
						closed = true
						continue
					}
					remaining = append(remaining, chunk)
				case <-time.After(5 * time.Second):
					t.Fatal("timed out waiting for successful terminal marker")
				}
			}
			if len(remaining) != 2 {
				t.Fatalf("remaining chunks = %#v, want completion-2 then terminal marker", remaining)
			}
			if remaining[0].Err != nil || string(remaining[0].Payload) != "completion-2" {
				t.Fatalf("second chunk = %#v, want completion-2", remaining[0])
			}
			if !cliproxyexecutor.IsSuccessfulStreamTerminalChunk(remaining[1]) {
				t.Fatalf("last chunk = %#v, want successful terminal marker", remaining[1])
			}
		})
	}
}

func TestXAIExecuteStreamCancellationDoesNotBlockTerminalMarker(t *testing.T) {
	responseFormat := registerXAITerminalOrderTestTranslator()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: response.completed\n"))
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"id":"resp_1","status":"completed","output":[]}}` + "\n\n"))
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(t.Context())
	exec := NewXAIExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: "xai-cancel-terminal", Provider: "xai", Attributes: map[string]string{
		"base_url": server.URL,
		"api_key":  "token",
	}}
	result, errExecute := exec.ExecuteStream(ctx, auth, xaiStreamRequest(), cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FormatOpenAIResponse,
		ResponseFormat: responseFormat,
		Stream:         true,
		Metadata: map[string]any{
			cliproxyexecutor.StreamTerminalMarkerMetadataKey: true,
		},
	})
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	for range 2 {
		select {
		case chunk := <-result.Chunks:
			if chunk.Err != nil || len(chunk.Payload) == 0 {
				t.Fatalf("translated completion chunk = %#v", chunk)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for translated completion chunk")
		}
	}
	cancel()
	closed := false
	for !closed {
		select {
		case _, ok := <-result.Chunks:
			closed = !ok
		case <-time.After(5 * time.Second):
			t.Fatal("stream goroutine remained blocked on terminal marker")
		}
	}
}

func TestXAIExecuteStreamDirectCallOmitsSuccessfulTerminalMarker(t *testing.T) {
	responseFormat := registerXAITerminalOrderTestTranslator()
	for _, completionType := range []string{"response.completed", "response.done"} {
		t.Run(completionType, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte(`data: {"type":"` + completionType + `","response":{"id":"resp_1","status":"completed","output":[]}}` + "\n\n"))
				_, _ = w.Write([]byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"late\"}}\n\n"))
			}))
			defer server.Close()

			exec := NewXAIExecutor(&config.Config{})
			auth := &cliproxyauth.Auth{Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "token"}}
			result, err := exec.ExecuteStream(t.Context(), auth, xaiStreamRequest(), cliproxyexecutor.Options{
				SourceFormat:   sdktranslator.FormatOpenAIResponse,
				ResponseFormat: responseFormat,
				Stream:         true,
			})
			if err != nil {
				t.Fatalf("ExecuteStream() error = %v", err)
			}

			var chunks []cliproxyexecutor.StreamChunk
			for chunk := range result.Chunks {
				chunks = append(chunks, chunk)
			}
			if len(chunks) != 2 || string(chunks[0].Payload) != "completion-1" || string(chunks[1].Payload) != "completion-2" {
				t.Fatalf("chunks = %#v, want only translated completion chunks", chunks)
			}
			for _, chunk := range chunks {
				if chunk.Err != nil || cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
					t.Fatalf("direct executor exposed internal terminal state: %#v", chunk)
				}
			}
		})
	}
}

func TestXAICompactionTriggerStreamTerminalMarker(t *testing.T) {
	for _, markerEnabled := range []bool{false, true} {
		name := "direct"
		if markerEnabled {
			name = "manager"
		}
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"id":"resp_compact","object":"response.compaction","output":[{"type":"compaction","encrypted_content":"opaque"}],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}`))
			}))
			defer server.Close()

			metadata := map[string]any(nil)
			if markerEnabled {
				metadata = map[string]any{cliproxyexecutor.StreamTerminalMarkerMetadataKey: true}
			}
			exec := NewXAIExecutor(&config.Config{})
			auth := &cliproxyauth.Auth{Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "token"}}
			result, err := exec.ExecuteStream(t.Context(), auth, cliproxyexecutor.Request{
				Model:   "grok-4.3",
				Payload: []byte(`{"model":"grok-4.3","input":[{"role":"user","content":"hello"},{"type":"compaction_trigger"}]}`),
			}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Stream: true, Metadata: metadata})
			if err != nil {
				t.Fatalf("ExecuteStream() error = %v", err)
			}

			var chunks []cliproxyexecutor.StreamChunk
			for chunk := range result.Chunks {
				if chunk.Err != nil {
					t.Fatalf("stream chunk error = %v", chunk.Err)
				}
				chunks = append(chunks, chunk)
			}
			wantChunks := 6
			if markerEnabled {
				wantChunks++
			}
			if len(chunks) != wantChunks {
				t.Fatalf("chunk count = %d, want %d", len(chunks), wantChunks)
			}
			if !bytes.Contains(chunks[5].Payload, []byte(`"type":"response.completed"`)) {
				t.Fatalf("last synthetic payload is not response.completed: %s", chunks[5].Payload)
			}
			for i, chunk := range chunks {
				isMarker := cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk)
				if isMarker != (markerEnabled && i == len(chunks)-1) {
					t.Fatalf("chunk %d marker = %t, markerEnabled = %t", i, isMarker, markerEnabled)
				}
			}
		})
	}
}

func registerXAITerminalOrderTestTranslator() sdktranslator.Format {
	format := sdktranslator.FromString("xai-terminal-order-test")
	sdktranslator.Register(format, sdktranslator.FormatCodex, nil, sdktranslator.ResponseTransform{
		Stream: func(_ context.Context, _ string, _, _, raw []byte, _ *any) [][]byte {
			line := bytes.TrimSpace(raw)
			if !bytes.HasPrefix(line, xaiDataTag) {
				return nil
			}
			switch gjson.GetBytes(bytes.TrimSpace(line[len(xaiDataTag):]), "type").String() {
			case "response.completed":
				return [][]byte{[]byte("completion-1"), []byte("completion-2")}
			case "response.created":
				return [][]byte{[]byte("late")}
			default:
				return nil
			}
		},
	})
	return format
}

func TestNormalizeXAIToolsAndForeignEncryptedContent(t *testing.T) {
	body := []byte(`{"tools":[{"type":"namespace","name":"codex_app","tools":[{"type":"function","name":"automation_update","strict":true,"parameters":{"oneOf":[{"type":"object"}]}}]},{"type":"tool_search"},{"type":"image_generation"}],"tool_choice":"auto","parallel_tool_calls":true}`)
	out := normalizeXAIToolChoiceForTools(normalizeXAITools(body))
	if got := gjson.GetBytes(out, "tools.#").Int(); got != 1 {
		t.Fatalf("normalized tools = %s", out)
	}
	if got := gjson.GetBytes(out, "tools.0.name").String(); got != "automation_update" {
		t.Fatalf("tool name = %q", got)
	}
	if gjson.GetBytes(out, "tools.0.parameters.oneOf").Exists() || gjson.GetBytes(out, "tools.0.strict").Bool() {
		t.Fatalf("automation schema was not simplified: %s", out)
	}

	valid := makeValidXAIEncryptedContent(7)
	input := []byte(`{"input":[{"type":"reasoning","encrypted_content":"gAAAA-invalid"},{"type":"reasoning","encrypted_content":""}]}`)
	input, _ = sjson.SetBytes(input, "input.1.encrypted_content", valid)
	cleaned := sanitizeXAIInputEncryptedContent(input)
	if gjson.GetBytes(cleaned, "input.0.encrypted_content").Exists() {
		t.Fatalf("foreign encrypted_content remained: %s", cleaned)
	}
	if got := gjson.GetBytes(cleaned, "input.1.encrypted_content").String(); got != valid {
		t.Fatalf("valid Grok encrypted_content = %q", got)
	}
}

func TestNormalizeXAIToolsDropsChoiceForRemovedTool(t *testing.T) {
	body := []byte(`{"tools":[{"type":"tool_search"},{"type":"function","name":"lookup","parameters":{"type":"object"}}],"tool_choice":{"type":"tool_search"},"parallel_tool_calls":true}`)
	out := normalizeXAIToolChoiceForTools(normalizeXAITools(body))
	if got := gjson.GetBytes(out, "tools.#").Int(); got != 1 {
		t.Fatalf("normalized tools = %s", out)
	}
	if gjson.GetBytes(out, "tool_choice").Exists() {
		t.Fatalf("removed tool choice remained: %s", out)
	}
	if !gjson.GetBytes(out, "parallel_tool_calls").Bool() {
		t.Fatalf("parallel_tool_calls should remain with usable tools: %s", out)
	}
}

func TestXAIReasoningReplayCachesAndInjectsClaudeTurn(t *testing.T) {
	internalcache.ClearXAIReasoningReplayCache()
	t.Cleanup(internalcache.ClearXAIReasoningReplayCache)
	valid := makeValidXAIEncryptedContent(9)
	scope := xaiReasoningReplayScope{namespace: "tenant-a", modelName: "grok-4.3", sessionKey: "execution:session-1"}
	completed := []byte(`{"response":{"output":[{"type":"reasoning","encrypted_content":""}]}}`)
	completed, _ = sjson.SetBytes(completed, "response.output.0.encrypted_content", valid)
	cacheXAIReasoningReplayFromCompleted(context.Background(), scope, completed)

	req := cliproxyexecutor.Request{Model: "grok-4.3", Payload: []byte(`{"input":[{"role":"user","content":"next"}]}`)}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatClaude,
		Metadata:     map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "session-1"},
	}
	body, _, err := applyXAIReasoningReplayCacheRequired(context.Background(), "tenant-a", opts.SourceFormat, req, opts, req.Payload)
	if err != nil {
		t.Fatalf("applyXAIReasoningReplayCacheRequired() error = %v", err)
	}
	if got := gjson.GetBytes(body, "input.0.encrypted_content").String(); got != valid {
		t.Fatalf("replayed encrypted_content = %q; body=%s", got, body)
	}
	if !strings.EqualFold(gjson.GetBytes(body, "input.1.role").String(), "user") {
		t.Fatalf("user turn shifted incorrectly: %s", body)
	}
	body, _, err = applyXAIReasoningReplayCacheRequired(context.Background(), "tenant-b", opts.SourceFormat, req, opts, req.Payload)
	if err != nil {
		t.Fatalf("isolated replay error = %v", err)
	}
	if gjson.GetBytes(body, "input.0.encrypted_content").Exists() {
		t.Fatalf("tenant-b received tenant-a replay: %s", body)
	}
}

func TestClearXAIReasoningReplayOnInvalidSignature(t *testing.T) {
	internalcache.ClearXAIReasoningReplayCache()
	t.Cleanup(internalcache.ClearXAIReasoningReplayCache)

	scope := xaiReasoningReplayScope{namespace: "tenant-a", modelName: "grok-4.3", sessionKey: "execution:session-1"}
	completed := []byte(`{"response":{"output":[{"type":"reasoning","encrypted_content":""}]}}`)
	completed, _ = sjson.SetBytes(completed, "response.output.0.encrypted_content", makeValidXAIEncryptedContent(7))
	cacheXAIReasoningReplayFromCompleted(context.Background(), scope, completed)
	if !clearXAIReasoningReplayOnInvalidSignature(context.Background(), scope, http.StatusBadRequest, []byte(`{"type":"error","error":{"code":"invalid_signature","message":"verification failed"}}`)) {
		t.Fatal("expected invalid_signature to clear xAI replay")
	}
	if _, ok := internalcache.GetXAIReasoningReplayItems(scope.modelName, scope.cacheSessionKey()); ok {
		t.Fatal("xAI replay entry still exists")
	}
}

func TestXAIPrepareResponsesAppliesRegisteredThinkingProvider(t *testing.T) {
	exec := NewXAIExecutor(&config.Config{})
	req := cliproxyexecutor.Request{
		Model:   "grok-4.3",
		Payload: []byte(`{"model":"grok-4.3","input":"hello","reasoning":{"effort":"high"}}`),
	}
	prepared, err := exec.prepareResponsesRequest(context.Background(), nil, req, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}, false)
	if err != nil {
		t.Fatalf("prepareResponsesRequest() error = %v", err)
	}
	if got := gjson.GetBytes(prepared.body, "reasoning.effort").String(); got != "high" {
		t.Fatalf("reasoning.effort = %q, want high; body=%s", got, prepared.body)
	}
}

func TestXAICountTokensReturnsPositiveInputCount(t *testing.T) {
	exec := NewXAIExecutor(&config.Config{})
	resp, err := exec.CountTokens(context.Background(), nil, cliproxyexecutor.Request{
		Model:   "custom-grok-model",
		Payload: []byte(`{"model":"custom-grok-model","input":"count these tokens"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse})
	if err != nil {
		t.Fatalf("CountTokens() error = %v", err)
	}
	if got := gjson.GetBytes(resp.Payload, "response.usage.input_tokens").Int(); got <= 0 {
		t.Fatalf("input_tokens = %d; payload=%s", got, resp.Payload)
	}
}

func TestXAIMediaEndpointRouting(t *testing.T) {
	tests := []struct {
		name        string
		format      sdktranslator.Format
		requestPath string
		wantImage   string
		wantVideo   string
	}{
		{name: "image generation", format: sdktranslator.FromString(xaiImageHandlerType), requestPath: "/v1/images/generations", wantImage: xaiImagesGenerationsPath},
		{name: "image edit", format: sdktranslator.FromString(xaiImageHandlerType), requestPath: "/v1/images/edits", wantImage: xaiImagesEditsPath},
		{name: "video generation", format: sdktranslator.FromString(xaiVideoHandlerType), requestPath: "/v1/videos/generations", wantVideo: xaiVideosGenerationsPath},
		{name: "video edit", format: sdktranslator.FromString(xaiVideoHandlerType), requestPath: "/v1/videos/edits", wantVideo: xaiVideosEditsPath},
		{name: "video extension", format: sdktranslator.FromString(xaiVideoHandlerType), requestPath: "/v1/videos/extensions", wantVideo: xaiVideosExtensionsPath},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := cliproxyexecutor.Options{
				SourceFormat: tt.format,
				Metadata:     map[string]any{cliproxyexecutor.RequestPathMetadataKey: tt.requestPath},
			}
			if got := xaiImageEndpointPath(opts); got != tt.wantImage {
				t.Fatalf("xaiImageEndpointPath() = %q, want %q", got, tt.wantImage)
			}
			if got := xaiVideoEndpointPath(opts); got != tt.wantVideo {
				t.Fatalf("xaiVideoEndpointPath() = %q, want %q", got, tt.wantVideo)
			}
		})
	}
}

func TestXAIVideoRetrieveLogsGETWithoutBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	requestMethod := make(chan string, 1)
	requestBody := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		requestMethod <- r.Method
		requestBody <- data
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"video_1","status":"completed"}`))
	}))
	defer server.Close()

	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	exec := NewXAIExecutor(&config.Config{SDKConfig: config.SDKConfig{RequestLog: true}})
	auth := &cliproxyauth.Auth{Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "token"}}
	_, err := exec.Execute(ctx, auth, cliproxyexecutor.Request{
		Model:   "grok-imagine-video",
		Payload: []byte(`{"request_id":"video_1"}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString(xaiVideoHandlerType),
		Metadata:     map[string]any{cliproxyexecutor.RequestPathMetadataKey: "/v1/videos/:request_id"},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := <-requestMethod; got != http.MethodGet {
		t.Fatalf("upstream method = %s, want GET", got)
	}
	if got := <-requestBody; len(got) != 0 {
		t.Fatalf("upstream body = %q, want empty", got)
	}

	raw, ok := ginCtx.Get("API_REQUEST")
	if !ok {
		t.Fatal("API request log missing")
	}
	logBody, ok := raw.([]byte)
	if !ok {
		t.Fatalf("API request log type = %T, want []byte", raw)
	}
	text := string(logBody)
	if !strings.Contains(text, "HTTP Method: GET") || !strings.Contains(text, "Body:\n<empty>") {
		t.Fatalf("unexpected API request log: %s", text)
	}
	if strings.Contains(text, "request_id") {
		t.Fatalf("GET request log retained unsent body: %s", text)
	}
}
