package executor

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

const codexSearchHistoryFixture = `{"type":"web_search_call","id":"ws_fixture","status":"completed","action":{"type":"search","query":"fixture"}}`

func TestCodexWebSearchHistoryOutbound(t *testing.T) {
	for _, transport := range []string{"http", "websocket", "websocket-http-fallback"} {
		for _, streaming := range []bool{false, true} {
			for _, tc := range []struct {
				name, fields, choice, toolType string
				lite, apiKey, compact          bool
				payloadChoice                  bool
			}{
				{name: "empty-tools", fields: `,"tools":[]`, choice: `"none"`, toolType: "web_search"},
				{name: "lite", fields: `,"tools":[],"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":"true"}`, lite: true, choice: `"none"`, toolType: "web_search"},
				{name: "forced-choice", fields: `,"tool_choice":{"type":"function","name":"lookup"}`, choice: `{"type":"function","name":"lookup"}`, toolType: "web_search"},
				{name: "existing-tools", fields: `,"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}],"tool_choice":"required"`, choice: `"required"`, toolType: "web_search"},
				{name: "payload-choice", fields: `,"tools":[],"tool_choice":"auto"`, payloadChoice: true, choice: `"required"`, toolType: "web_search"},
				{name: "existing-search", fields: `,"tools":[{"type":"web_search","external_web_access":true}],"tool_choice":"auto"`, choice: `"auto"`, toolType: "web_search"},
				{name: "api-key", fields: `,"tools":[]`, apiKey: true},
				{name: "compact", fields: `,"tools":[]`, compact: true},
			} {
				if tc.compact && (streaming || transport == "websocket-http-fallback") {
					continue
				}
				t.Run(fmt.Sprintf("%s/stream=%t/%s", transport, streaming, tc.name), func(t *testing.T) {
					var attempts atomic.Int64
					wire := make(chan []byte, 1)
					terminal := []byte(`{"type":"response.completed","response":{"id":"resp_search","output":[]}}`)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						attempts.Add(1)
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
							_, payload, errRead := conn.ReadMessage()
							if errRead != nil {
								t.Error(errRead)
								return
							}
							wire <- payload
							_ = conn.WriteMessage(websocket.TextMessage, terminal)
							return
						}
						payload, errRead := io.ReadAll(r.Body)
						if errRead != nil {
							t.Error(errRead)
							return
						}
						wire <- payload
						if tc.compact {
							if r.URL.Path != "/responses/compact" {
								t.Errorf("unexpected compact endpoint: %s", r.URL.Path)
							}
							w.Header().Set("Content-Type", "application/json")
							_, _ = w.Write([]byte(`{"id":"resp_compact","object":"response.compaction","output":[]}`))
							return
						}
						if (r.Header.Get(helps.CodexResponsesLiteHeader) == "true") != tc.lite {
							t.Error("HTTP mode disagrees with the request snapshot")
						}
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "data: %s\n\n", terminal)
					}))
					defer server.Close()
					cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}}
					if tc.payloadChoice {
						cfg.Payload.Override = []config.PayloadRule{{Models: []config.PayloadModelRule{{Name: "*", Protocol: "codex"}}, Params: map[string]any{"tool_choice": "required"}}}
					}
					auth := &cliproxyauth.Auth{ID: "search-history", Provider: "codex", Attributes: map[string]string{"base_url": server.URL}, Metadata: map[string]any{"access_token": "fixture"}}
					if tc.apiKey {
						auth.Attributes["api_key"] = "fixture"
					}
					var executor cliproxyauth.ProviderExecutor = NewCodexExecutor(cfg)
					if strings.HasPrefix(transport, "websocket") {
						executor = NewCodexWebsocketsExecutor(cfg)
					}
					opts := core.Options{SourceFormat: translator.FormatCodex}
					if tc.compact {
						opts.Alt = "responses/compact"
					}
					body := []byte(`{"model":"gpt-6.1-sol","input":[` + codexSearchHistoryFixture + `,{"type":"compaction_trigger"}]` + tc.fields + `}`)
					original := bytes.Clone(body)
					req := core.Request{Model: "gpt-6.1-sol", Payload: body}
					if streaming {
						result, err := executor.ExecuteStream(t.Context(), auth, req, opts)
						if err != nil {
							t.Fatal(err)
						}
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								t.Fatal(chunk.Err)
							}
						}
					} else if _, err := executor.Execute(t.Context(), auth, req, opts); err != nil {
						t.Fatal(err)
					}
					got := <-wire
					wantAttempts := int64(1)
					if transport == "websocket-http-fallback" {
						wantAttempts = 2
					}
					if attempts.Load() != wantAttempts {
						t.Fatal("history compatibility added an upstream attempt")
					}
					if !bytes.Equal(body, original) || gjson.GetBytes(got, "input.0").Raw != codexSearchHistoryFixture {
						t.Fatalf("caller buffer or search history changed: %s", got)
					}
					path := "tools"
					if tc.lite {
						path = "input.1.tools"
						if gjson.GetBytes(got, "tools.#").Int() != 0 || gjson.GetBytes(got, "input.2.type").String() != "compaction_trigger" {
							t.Fatalf("Lite tool location or final trigger changed: %s", got)
						}
					}
					toolCount := gjson.GetBytes(got, path+".#").Int()
					if tc.toolType == "" {
						if toolCount != 0 {
							t.Fatalf("excluded request gained tools: %s", got)
						}
						return
					}
					wantCount := int64(1)
					if tc.name == "existing-tools" {
						wantCount = 2
					}
					tool := gjson.GetBytes(got, fmt.Sprintf("%s.%d", path, wantCount-1))
					if toolCount != wantCount || tool.Get("type").String() != tc.toolType || tool.Get("external_web_access").Type != gjson.False && tc.name != "existing-search" {
						t.Fatalf("missing cached-only search declaration: %s", got)
					}
					if tc.name == "existing-search" && !tool.Get("external_web_access").Bool() {
						t.Fatal("existing search configuration changed")
					}
					if choice := gjson.GetBytes(got, "tool_choice").Raw; choice != tc.choice {
						t.Fatalf("tool choice = %s, want %s", choice, tc.choice)
					}
				})
			}
		}
	}
}

func TestCodexWebSearchHistoryWebsocketReuse(t *testing.T) {
	var handshakes atomic.Int64
	wire := make(chan []byte, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handshakes.Add(1)
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close() }()
		for i := range 3 {
			_, payload, errRead := conn.ReadMessage()
			if errRead != nil {
				t.Error(errRead)
				return
			}
			wire <- payload
			terminal := fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_%d","output":[]}}`, i)
			if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(terminal)); errWrite != nil {
				t.Error(errWrite)
				return
			}
		}
	}))
	defer server.Close()
	executor := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}})
	credential := &cliproxyauth.Auth{ID: "search-history-reuse", Provider: "codex", Attributes: map[string]string{"base_url": server.URL}, Metadata: map[string]any{"access_token": "fixture"}}
	session := uuid.NewString()
	defer executor.CloseExecutionSession(session)
	ctx := core.WithDownstreamWebsocket(t.Context())
	opts := core.Options{SourceFormat: translator.FormatCodex, Metadata: map[string]any{core.ExecutionSessionMetadataKey: session}}
	for i, payload := range []string{
		`{"input":[` + codexSearchHistoryFixture + `],"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":"true"}}`,
		`{"input":[` + codexSearchHistoryFixture + `]}`,
		`{"input":[{"role":"user","content":"hello"}]}`,
	} {
		result, err := executor.ExecuteStream(ctx, credential, core.Request{Model: "gpt-6.1-sol", Payload: []byte(payload)}, opts)
		if err != nil {
			t.Fatal(err)
		}
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				t.Fatal(chunk.Err)
			}
		}
		got := <-wire
		switch i {
		case 0:
			if gjson.GetBytes(got, "input.1.tools.0.type").String() != "web_search" || gjson.GetBytes(got, "tools.#").Int() != 0 {
				t.Fatalf("Lite declaration missing: %s", got)
			}
		case 1:
			if gjson.GetBytes(got, "tools.0.type").String() != "web_search" || gjson.GetBytes(got, "input.#").Int() != 1 {
				t.Fatalf("previous turn's Lite mode leaked: %s", got)
			}
		case 2:
			if helps.HasCodexToolDeclarations(got) || gjson.GetBytes(got, "tool_choice").Exists() {
				t.Fatalf("history repair leaked into a request without history: %s", got)
			}
		}
	}
	if handshakes.Load() != 1 {
		t.Fatal("test did not exercise an actual reused websocket")
	}
}
