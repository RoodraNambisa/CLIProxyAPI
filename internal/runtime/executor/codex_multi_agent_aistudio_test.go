package executor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/wsrelay"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestAIStudioMultiAgentHistoryAcrossRelayOperations(t *testing.T) {
	for _, operation := range []string{"execute", "stream", "count"} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", operation, enabled), func(t *testing.T) {
				const authID = "aistudio-multi-agent"
				connected := make(chan struct{})
				relay := wsrelay.NewManager(wsrelay.Options{ProviderFactory: func(*http.Request) (string, error) { return authID, nil }, OnConnected: func(string) { close(connected) }})
				server := httptest.NewServer(relay.Handler())
				defer server.Close()
				defer relay.Stop(context.Background())
				conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+relay.Path(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = conn.Close() }()
				select {
				case <-connected:
				case <-t.Context().Done():
					t.Fatal("relay setup canceled")
				}
				clientErr := make(chan error, 1)
				go func() {
					var request wsrelay.Message
					if err := conn.ReadJSON(&request); err != nil {
						clientErr <- err
						return
					}
					body, _ := request.Payload["body"].(string)
					contents := gjson.Get(body, "contents").Raw
					before, worker, after := strings.Index(contents, `"before"`), strings.Index(contents, `"worker result"`), strings.Index(contents, `"after"`)
					if before < 0 || after <= before || (worker >= 0) != enabled || enabled && (worker <= before || worker >= after) {
						clientErr <- fmt.Errorf("AI Studio lost collaboration order or default behavior")
						_ = conn.Close()
						return
					}
					response := `{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":1,"totalTokenCount":2}}`
					if operation == "count" {
						response = `{"totalTokens":12}`
					}
					messages := []wsrelay.Message{{ID: request.ID, Type: wsrelay.MessageTypeHTTPResp, Payload: map[string]any{"status": http.StatusOK, "body": response}}}
					if operation == "stream" {
						messages = []wsrelay.Message{
							{ID: request.ID, Type: wsrelay.MessageTypeStreamStart, Payload: map[string]any{"status": http.StatusOK}},
							{ID: request.ID, Type: wsrelay.MessageTypeStreamChunk, Payload: map[string]any{"data": "data: " + response + "\n\n"}},
							{ID: request.ID, Type: wsrelay.MessageTypeStreamEnd},
						}
					}
					for _, message := range messages {
						if err := conn.WriteJSON(message); err != nil {
							clientErr <- err
							_ = conn.Close()
							return
						}
					}
					clientErr <- nil
				}()
				executor := NewAIStudioExecutor(&config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: enabled}}, authID, relay)
				req := core.Request{Model: "gemini-2.5-flash", Payload: []byte(codexPlainAgentHistory)}
				opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: req.Payload, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}}
				if _, err := executeMultiAgentTranslation(t.Context(), executor, operation, &cliproxyauth.Auth{ID: authID}, req, opts); err != nil {
					t.Fatal(err)
				}
				if err := <-clientErr; err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestAIStudioMultiAgentRejectsCiphertextBeforeRelay(t *testing.T) {
	executor := NewAIStudioExecutor(&config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}}, "fixture", nil)
	for _, operation := range []string{"execute", "stream", "count"} {
		req := core.Request{Model: "gemini-2.5-flash", Payload: []byte(codexEncryptedAgentHistory)}
		opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}}
		_, err := executeMultiAgentTranslation(t.Context(), executor, operation, &cliproxyauth.Auth{ID: "fixture"}, req, opts)
		var local interface {
			SkipAuthResult() bool
			RetryOtherAuth() bool
		}
		if err == nil || !errors.As(err, &local) || !local.SkipAuthResult() || local.RetryOtherAuth() {
			t.Fatalf("%s did not reject before accessing the relay", operation)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := executeMultiAgentTranslation(ctx, executor, operation, &cliproxyauth.Auth{ID: "fixture"}, req, opts); !errors.Is(err, context.Canceled) {
			t.Fatal("normalization replaced cancellation")
		}
	}
}
