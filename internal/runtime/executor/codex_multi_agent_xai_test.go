package executor

import (
	"context"
	"errors"
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
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestXAIMultiAgentHistoryAcrossOperations(t *testing.T) {
	for _, operation := range []string{"execute", "stream", "compact", "count"} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", operation, enabled), func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					wantType := "agent_message"
					if enabled {
						wantType = "message"
					}
					if gjson.GetBytes(body, "input.1.type").String() != wantType || gjson.GetBytes(body, "input.1.content.0.text").String() != "worker result" {
						t.Error("xAI did not preserve or normalize the collaboration envelope")
					}
					if operation == "compact" {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"id":"compact","object":"response.compaction","output":[]}`)
					} else {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"result\",\"status\":\"completed\",\"output\":[]}}\n\n")
					}
				}))
				defer server.Close()
				executor := NewXAIExecutor(&config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: enabled}})
				auth := &cliproxyauth.Auth{ID: "multi-agent-xai", Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
				req := core.Request{Model: "grok-4", Payload: []byte(codexPlainAgentHistory)}
				opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: req.Payload, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}}
				if operation == "compact" {
					opts.Alt = "responses/compact"
				}
				if operation == "count" {
					prepared, err := executor.prepareResponsesRequest(t.Context(), auth, req, opts, false)
					if err != nil {
						t.Fatal(err)
					}
					if (gjson.GetBytes(prepared.body, "input.1.type").String() == "message") != enabled {
						t.Fatal("token counting preparation lost the policy")
					}
				}
				if _, err := executeMultiAgentTranslation(t.Context(), executor, operation, auth, req, opts); err != nil {
					t.Fatal(err)
				}
				expectedCalls := int32(1)
				if operation == "count" {
					expectedCalls = 0
				}
				if calls.Load() != expectedCalls {
					t.Fatal("xAI changed upstream call count")
				}
			})
		}
	}
}

func TestXAIMultiAgentCiphertextCannotUseResponsesWireFormatBypass(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(http.StatusBadRequest) }))
	defer server.Close()
	executor := NewXAIExecutor(&config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}})
	auth := &cliproxyauth.Auth{Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
	for _, operation := range []string{"execute", "stream", "compact", "count"} {
		for _, originalOnly := range []bool{false, true} {
			req := core.Request{Model: "grok-4", Payload: []byte(codexEncryptedAgentHistory)}
			opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}}
			if operation == "compact" {
				opts.Alt = "responses/compact"
			}
			if originalOnly {
				opts.OriginalRequest = req.Payload
				req.Payload = []byte(codexPlainAgentHistory)
			}
			_, err := executeMultiAgentTranslation(t.Context(), executor, operation, auth, req, opts)
			var local interface {
				SkipAuthResult() bool
				RetryOtherAuth() bool
			}
			if err == nil || !errors.As(err, &local) || !local.SkipAuthResult() || local.RetryOtherAuth() ||
				strings.Contains(err.Error(), "fixture-ciphertext") {
				t.Fatalf("%s bypassed the foreign ciphertext check", operation)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := executeMultiAgentTranslation(ctx, executor, operation, auth, req, opts); !errors.Is(err, context.Canceled) {
				t.Fatal("normalization replaced cancellation")
			}
		}
	}
	if calls.Load() != 0 {
		t.Fatal("unsupported ciphertext reached xAI")
	}
}

func TestXAIMultiAgentWebsocketPreparationAndEarlyRejection(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			var connections atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				connections.Add(1)
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
				if (gjson.GetBytes(body, "input.1.type").String() == "message") != enabled {
					t.Error("WebSocket lost its collaboration policy")
				}
				_ = conn.WriteMessage(websocket.TextMessage, []byte(xaiCompletedEvent))
			}))
			defer server.Close()
			executor := NewXAIWebsocketsExecutor(&config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: enabled}})
			auth := &cliproxyauth.Auth{ID: "multi-agent-xai-ws", Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
			opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}}
			ctx := core.WithDownstreamWebsocket(t.Context())
			if _, err := executeMultiAgentTranslation(ctx, executor, "stream", auth, core.Request{Model: "grok-4", Payload: []byte(codexPlainAgentHistory)}, opts); err != nil {
				t.Fatal(err)
			}
			if enabled {
				_, err := executor.ExecuteStream(ctx, auth, core.Request{Model: "grok-4", Payload: []byte(codexEncryptedAgentHistory)}, opts)
				if err == nil || !strings.Contains(err.Error(), "codex_encrypted_agent_message_unsupported") {
					t.Fatal("WebSocket ciphertext was not rejected before dialing")
				}
			}
			if connections.Load() != 1 {
				t.Fatal("unexpected upstream WebSocket connection")
			}
		})
	}
}
