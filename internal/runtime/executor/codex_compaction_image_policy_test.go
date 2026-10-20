package executor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexCompactionRemovesDisabledImageDeclarations(t *testing.T) {
	for _, transport := range []string{"http", "stream", "websocket", "websocket-stream", "websocket-http-fallback", "compact"} {
		for _, declaration := range []string{
			`"tools":[{"type":"image_generation"},{"type":"function","name":"lookup"}]`,
			`"tools":[{"type":"namespace","name":"image_gen","tools":[{"type":"function","name":"imagegen"}]}]`,
			`"tools":[]`,
		} {
			t.Run(transport+"/"+declaration, func(t *testing.T) {
				var calls atomic.Int64
				inspect := func(body []byte) {
					if cliproxyauth.PayloadHasImageGenerationTool(body) {
						t.Error("disabled image declaration reached upstream")
					}
					if gjson.GetBytes(body, "input.0.content").String() != "keep history" ||
						gjson.GetBytes(body, "input.1.result").String() != "keep image result" {
						t.Error("history changed")
					}
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if websocket.IsWebSocketUpgrade(r) {
						if transport == "websocket-http-fallback" {
							w.WriteHeader(http.StatusUpgradeRequired)
							return
						}
						upgrader := websocket.Upgrader{}
						conn, err := upgrader.Upgrade(w, r, nil)
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = conn.Close() }()
						_, body, err := conn.ReadMessage()
						if err != nil {
							t.Error(err)
							return
						}
						calls.Add(1)
						inspect(body)
						_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"fixture","output":[]}}`))
						return
					}
					calls.Add(1)
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					inspect(body)
					if transport == "compact" {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"id":"compact","output":[]}`)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"fixture\",\"output\":[]}}\n\n")
				}))
				defer server.Close()
				cfg := &config.Config{
					SDKConfig:                         sdkconfig.SDKConfig{ProxyURL: "direct"},
					DisabledImageGenerationToolAction: config.DisabledImageGenerationToolActionError,
					AuthModelExclusions:               []config.AuthModelExclusionRule{{DisableImageGeneration: true, Priorities: []int{3}}},
				}
				auth := &cliproxyauth.Auth{ID: "compact", Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL, "priority": "3"}}
				trigger := `,{"type":"compaction_trigger"}`
				if transport == "compact" {
					trigger = ""
				}
				payload := []byte(`{"model":"gpt-6-astra","input":[{"role":"user","content":"keep history"},{"type":"image_generation_call","id":"img_previous","result":"keep image result"},{"type":"additional_tools","tools":[{"type":"image_generation"}]}` + trigger + `],` + declaration + `,"tool_choice":{"type":"image_generation"}}`)
				req := core.Request{Model: "gpt-6-astra", Payload: payload}
				opts := core.Options{SourceFormat: translator.FormatCodex}
				if transport == "compact" {
					opts.Alt = "responses/compact"
				}
				ctx := context.Background()
				var err error
				switch transport {
				case "stream", "websocket-stream":
					var result *core.StreamResult
					if transport == "websocket-stream" {
						result, err = NewCodexWebsocketsExecutor(cfg).ExecuteStream(ctx, auth, req, opts)
					} else {
						result, err = NewCodexExecutor(cfg).ExecuteStream(ctx, auth, req, opts)
					}
					if err == nil {
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								err = chunk.Err
							}
						}
					}
				case "websocket", "websocket-http-fallback":
					_, err = NewCodexWebsocketsExecutor(cfg).Execute(ctx, auth, req, opts)
				default:
					_, err = NewCodexExecutor(cfg).Execute(ctx, auth, req, opts)
				}
				if err != nil {
					t.Fatal(err)
				}
				if calls.Load() == 0 {
					t.Fatal("compaction did not reach upstream")
				}
			})
		}
	}
}
