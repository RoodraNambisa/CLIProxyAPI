package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexMalformedCompletionIsNotCacheEligible(t *testing.T) {
	for _, kind := range []string{"response.completed", "response.done"} {
		for _, tail := range []string{"", ",}", "}} trailing"} {
			payload := []byte(fmt.Sprintf(`{"type":%q,"response":{"status":"completed","output":[]%s`, kind, tail))
			if isCodexSuccessfulCompletion(payload) {
				t.Fatal("malformed terminal was accepted as a successful cache-eligible response")
			}
			if string(normalizeCodexCompletion(payload)) != string(payload) {
				t.Fatal("normalization repaired malformed terminal JSON into an apparent success")
			}
		}
	}
}

func TestCodexMalformedCompletionAcrossTransports(t *testing.T) {
	for _, mode := range []string{"http", "sse", "trusted", "image", "ws", "ws-stream"} {
		for _, kind := range []string{"response.completed", "response.done"} {
			t.Run(mode+"/"+kind, func(t *testing.T) {
				payload := []byte(fmt.Sprintf(`{"type":%q,"response":{"status":"completed","output":[]`, kind))
				websocketMode := strings.HasPrefix(mode, "ws")
				stream := mode != "http" && mode != "ws"
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if websocketMode {
						upgrader := websocket.Upgrader{}
						conn, err := upgrader.Upgrade(w, r, nil)
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = conn.Close() }()
						if _, _, err = conn.ReadMessage(); err == nil {
							_ = conn.WriteMessage(websocket.TextMessage, payload)
						}
						return
					}
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = fmt.Fprintf(w, "data: %s\n\n", payload)
				}))
				defer server.Close()
				var executor coreauth.ProviderExecutor = NewCodexExecutor(&config.Config{})
				if websocketMode {
					executor = NewCodexWebsocketsExecutor(&config.Config{})
				}
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
				auth := &coreauth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
				req := core.Request{Model: "gpt-5.4-mini", Payload: []byte(`{"model":"gpt-5.4-mini","input":"fixture","tools":[{"type":"image_generation"}]}`)}
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, Stream: stream, Metadata: map[string]any{core.StreamTerminalMarkerMetadataKey: true, core.TrustUpstreamSSEMetadataKey: mode == "trusted", core.ImageGenerationStreamPassthroughMetadataKey: mode == "image"}}
				var terminalErr error
				if stream {
					response, err := executor.ExecuteStream(ctx, auth, req, opts)
					terminalErr = err
					if err == nil {
						for chunk := range response.Chunks {
							if core.IsSuccessfulStreamTerminalChunk(chunk) {
								t.Error("malformed response emitted a successful terminal marker")
							}
							if chunk.Err != nil {
								terminalErr = chunk.Err
							}
						}
					}
				} else {
					_, terminalErr = executor.Execute(ctx, auth, req, opts)
				}
				if terminalErr == nil || !strings.Contains(terminalErr.Error(), "invalid upstream Codex terminal JSON") {
					t.Fatalf("malformed completion did not report its protocol error: %v", terminalErr)
				}
			})
		}
	}
}
