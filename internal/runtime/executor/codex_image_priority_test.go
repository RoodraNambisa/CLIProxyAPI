package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexImagePriorityScopeTransports(t *testing.T) {
	for _, transport := range []string{"http", "sse", "websocket", "websocket-stream", "compact", "compaction-trigger", "images/generations", "images/edits"} {
		for _, priority := range []string{"3", "5"} {
			for _, action := range []string{"remove", "error"} {
				t.Run(fmt.Sprintf("%s/%s/%s", transport, priority, action), func(t *testing.T) {
					image := strings.HasPrefix(transport, "images/")
					compact := transport == "compact" || transport == "compaction-trigger"
					refuse := priority == "5" && (image || (action == "error" && !compact))
					var calls atomic.Int64
					inspect := func(body []byte) {
						if !image && cliproxyauth.PayloadHasImageGenerationTool(body) != (priority == "3") {
							t.Error("image declaration did not follow its priority scope")
						}
					}
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						if websocket.IsWebSocketUpgrade(r) {
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
							inspect(body)
							_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"fixture","output":[]}}`))
							return
						}
						body, err := io.ReadAll(r.Body)
						if err != nil {
							t.Error(err)
							return
						}
						inspect(body)
						if image {
							w.Header().Set("Content-Type", "application/json")
							_, _ = io.WriteString(w, `{"created":1700000000,"data":[{"b64_json":"aW1n"}]}`)
						} else if transport == "compact" {
							w.Header().Set("Content-Type", "application/json")
							_, _ = io.WriteString(w, `{"id":"compact","output":[]}`)
						} else {
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"fixture\",\"output\":[]}}\n\n")
						}
					}))
					defer server.Close()
					cfg := &config.Config{SDKConfig: config.SDKConfig{ProxyURL: "direct"},
						DisabledImageGenerationToolAction: action,
						AuthModelExclusions: []config.AuthModelExclusionRule{{
							Priorities: []int{3, 5}, DisableImageGeneration: true, DisableImageGenerationPriorities: []int{5},
						}},
					}
					a := &cliproxyauth.Auth{ID: "scope-fixture", Provider: "codex", Attributes: map[string]string{
						"api_key": "fixture", "base_url": server.URL, "priority": priority,
					}}
					req := core.Request{Model: "gpt-6-astra", Payload: []byte(`{"model":"gpt-6-astra","input":[],"tools":[{"type":"image_generation"}]}`)}
					opts := core.Options{SourceFormat: translator.FormatCodex}
					if transport == "compact" {
						opts.Alt = "responses/compact"
					}
					if transport == "compaction-trigger" {
						req.Payload = []byte(`{"model":"gpt-6-astra","input":[{"type":"compaction_trigger"}],"tools":[{"type":"image_generation"}]}`)
					}
					if image {
						req = core.Request{Model: "gpt-image-2", Payload: []byte(`{"model":"gpt-image-2","prompt":"draw"}`)}
						opts.SourceFormat = translator.FromString(codexOpenAIImageSourceFormat)
						opts.Alt = transport
					}
					var err error
					switch transport {
					case "websocket":
						_, err = NewCodexWebsocketsExecutor(cfg).Execute(t.Context(), a, req, opts)
					case "sse", "websocket-stream":
						var stream *core.StreamResult
						if transport == "sse" {
							stream, err = NewCodexExecutor(cfg).ExecuteStream(t.Context(), a, req, opts)
						} else {
							stream, err = NewCodexWebsocketsExecutor(cfg).ExecuteStream(t.Context(), a, req, opts)
						}
						if err == nil {
							for chunk := range stream.Chunks {
								if chunk.Err != nil {
									err = chunk.Err
								}
							}
						}
					default:
						_, err = NewCodexExecutor(cfg).Execute(t.Context(), a, req, opts)
					}
					if refuse {
						status, ok := err.(interface{ StatusCode() int })
						if !ok || status.StatusCode() != 400 || calls.Load() != 0 {
							t.Fatalf("refusal bypassed local policy: calls=%d err=%v", calls.Load(), err)
						}
					} else if err != nil || calls.Load() != 1 {
						t.Fatalf("allowed request failed: calls=%d err=%v", calls.Load(), err)
					}
				})
			}
		}
	}
}
