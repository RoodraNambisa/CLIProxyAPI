package executor

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexInputItemIDsAllResponsesTransports(t *testing.T) {
	for _, operation := range []string{"http", "stream", "compact", "websocket", "websocket-stream"} {
		t.Run(operation, func(t *testing.T) {
			captured := make(chan []byte, 1)
			terminal := []byte(`{"type":"response.completed","response":{"id":"resp_test","output":[]}}`)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if websocket.IsWebSocketUpgrade(r) {
					upgrader := websocket.Upgrader{}
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer func() { _ = conn.Close() }()
					_, body, errRead := conn.ReadMessage()
					if errRead != nil {
						t.Error(errRead)
						return
					}
					captured <- body
					if errWrite := conn.WriteMessage(websocket.TextMessage, terminal); errWrite != nil {
						t.Error(errWrite)
					}
					return
				}
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				captured <- body
				if operation == "compact" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"id":"resp_test","object":"response.compaction","output":[]}`))
				} else {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = w.Write(append(append([]byte("data: "), terminal...), []byte("\n\n")...))
				}
			}))
			defer server.Close()
			cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}}
			auth := &cliproxyauth.Auth{ID: "id-test", Provider: "codex", Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL}}
			req := cliproxyexecutor.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":[{"type":"message","role":"user","id":"source","content":[{"type":"input_text","text":"hello"}]}]}`)}
			opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response")}
			var provider cliproxyauth.ProviderExecutor = NewCodexExecutor(cfg)
			if operation == "websocket" || operation == "websocket-stream" {
				provider = NewCodexWebsocketsExecutor(cfg)
				opts.SourceFormat = sdktranslator.FromString("codex")
			}
			if operation == "compact" {
				opts.Alt = "responses/compact"
			}
			if operation == "stream" || operation == "websocket-stream" {
				result, err := provider.ExecuteStream(t.Context(), auth, req, opts)
				if err != nil {
					t.Fatal(err)
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
				}
			} else if _, err := provider.Execute(t.Context(), auth, req, opts); err != nil {
				t.Fatal(err)
			}
			select {
			case body := <-captured:
				if gjson.GetBytes(body, "input.0.id").Str != "msg_source" {
					t.Fatal("input ID sanitizer was not applied to the outbound request")
				}
			default:
				t.Fatal("no upstream request captured")
			}
		})
	}
}
