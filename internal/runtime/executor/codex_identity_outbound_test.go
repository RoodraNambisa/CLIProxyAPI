package executor

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexIdentityOutboundProjection(t *testing.T) {
	for _, transport := range []string{"http", "ws"} {
		for _, mode := range []string{"off", "device", "session", "full"} {
			for _, confuse := range []bool{false, true} {
				name := transport + "/" + mode
				if confuse {
					name += "/confuse"
				}
				t.Run(name, func(t *testing.T) {
					thread, turn, window := uuid.Must(uuid.NewV7()).String(), uuid.Must(uuid.NewV7()).String(), uuid.Must(uuid.NewV7()).String()
					nested, _ := json.Marshal(map[string]any{"session_id": thread, "thread_id": thread, "turn_id": turn, "root_turn_id": turn, "root_thread_id": thread, "parent_thread_id": thread, "window_id": thread + ":7", "window_number": 7, "context_window_id": window})
					payload, _ := json.Marshal(map[string]any{"model": "gpt-5.4", "input": "hello", "prompt_cache_key": thread, "client_metadata": map[string]any{"session_id": thread, "thread_id": thread, "turn_id": turn, "root_turn_id": turn, "context_window_id": window, "x-codex-turn-metadata": string(nested)}})
					type capture struct {
						body    []byte
						headers http.Header
					}
					captured := make(chan capture, 1)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						completed := []byte(`{"type":"response.completed","response":{"id":"resp_identity","status":"completed","output":[]}}`)
						if transport == "ws" {
							upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
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
							captured <- capture{body, r.Header.Clone()}
							if err := conn.WriteMessage(websocket.TextMessage, completed); err != nil {
								t.Error(err)
							}
						} else {
							body, err := io.ReadAll(r.Body)
							if err != nil {
								t.Error(err)
								return
							}
							captured <- capture{body, r.Header.Clone()}
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = w.Write(append(append([]byte("data: "), completed...), '\n', '\n'))
						}
					}))
					defer server.Close()
					cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Routing: config.RoutingConfig{Strategy: "fill-first"}, Codex: config.CodexConfig{IdentityConfuse: confuse}, CodexFingerprint: config.CodexFingerprintConfig{SessionIdentityPoolSize: 4}}
					exec := NewCodexExecutor(cfg)
					auth := prepareCodexFingerprintAuthForTest(t, exec, &coreauth.Auth{ID: "identity-test", Provider: "codex", Metadata: map[string]any{"access_token": "test-token", "account_id": "test-account", codexauth.FingerprintModeMetadataKey: mode}, Attributes: map[string]string{"base_url": server.URL}})
					gc, _ := gin.CreateTestContext(httptest.NewRecorder())
					gc.Request = httptest.NewRequest("POST", "/v1/responses", nil)
					gc.Request.Header.Set("X-Codex-Turn-Metadata", string(nested))
					gc.Request.Header.Set("X-Codex-Parent-Thread-Id", thread)
					ctx := context.WithValue(t.Context(), "gin", gc)
					req := core.Request{Model: "gpt-5.4", Payload: payload}
					opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, OriginalRequest: payload, Headers: gc.Request.Header.Clone()}
					var err error
					if transport == "ws" {
						_, err = NewCodexWebsocketsExecutor(cfg).Execute(ctx, auth, req, opts)
					} else {
						_, err = exec.Execute(ctx, auth, req, opts)
					}
					if err != nil {
						t.Fatal(err)
					}
					got := <-captured
					metadata := gjson.GetBytes(got.body, "client_metadata.x-codex-turn-metadata").String()
					converged := mode == "session" || mode == "full"
					if converged || confuse {
						want := helps.MapCodexContextWindow("test-account", window)
						if gjson.Get(metadata, "context_window_id").Str != want || gjson.GetBytes(got.body, "client_metadata.context_window_id").Str != want {
							t.Fatal("context ID not mapped exactly once")
						}
					}
					if gjson.Get(metadata, "root_turn_id").Str != gjson.Get(metadata, "turn_id").Str || gjson.GetBytes(got.body, "client_metadata.root_turn_id").Str != gjson.GetBytes(got.body, "client_metadata.turn_id").Str {
						t.Fatalf("broken root turn relation: %s", got.body)
					}
					if converged && (gjson.Get(metadata, "window_number").Int() != 0 || gjson.Get(metadata, "window_id").Str != gjson.Get(metadata, "thread_id").Str+":0") {
						t.Fatal("window ID and number disagree")
					}
					if !converged && gjson.Get(metadata, "window_number").Int() != 7 {
						t.Fatal("non-converged window reset")
					}
					if headerValueCaseInsensitive(got.headers, "X-Codex-Turn-Metadata") != metadata {
						t.Fatal("body/header metadata diverged")
					}
					if transport == "ws" && headerValueCaseInsensitive(got.headers, "Conversation_id") != gjson.GetBytes(got.body, "prompt_cache_key").Str {
						t.Fatal("websocket compatibility header retained an old cache key")
					}
					if converged && got.headers.Get("X-Codex-Parent-Thread-Id") != gjson.Get(metadata, "parent_thread_id").Str {
						t.Fatal("parent header lost projection")
					}
				})
			}
		}
	}
}

func TestCodexMemoryProjectionDoesNotInventTurnIdentity(t *testing.T) {
	exec := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{SpoofSessionIdentity: true}})
	auth := prepareCodexFingerprintAuthForTest(t, exec, &coreauth.Auth{ID: "memory-test", Provider: "codex", Metadata: map[string]any{"account_id": "account", "codex_fingerprint_mode": "session"}})
	body := []byte(`{"client_metadata":{"x-codex-turn-metadata":"{\"request_kind\":\"memory\"}"}}`)
	req := core.Request{Model: "gpt-5.4", Payload: body}
	opaque, err := exec.PrepareProviderRequest(t.Context(), req, core.Options{}, core.RequestOperationExecute)
	if err != nil {
		t.Fatal(err)
	}
	opts := core.WithProviderPreparedRequest(core.Options{}, exec.Identifier(), opaque)
	out, state, err := exec.projectCodexSessionIdentity(t.Context(), auth, req, opts, body, &codexIdentityConfuseState{})
	if err != nil {
		t.Fatal(err)
	}
	if state.projectSession {
		t.Fatal("memory became a user turn")
	}
	for _, field := range []string{"session_id", "thread_id", "turn_id", "window_id", "window_number"} {
		if gjson.Get(state.turnMetadata, field).Exists() || gjson.GetBytes(out, "client_metadata."+field).Exists() {
			t.Fatalf("invented memory %s", field)
		}
	}
}
