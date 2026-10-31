package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexIdentityPriorityWireAndReturnedAlias(t *testing.T) {
	for _, transport := range []string{"http", "sse", "ws", "ws-stream", "compact"} {
		for _, mode := range []string{"off", "session", "full"} {
			for _, restore := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/restore=%t", transport, mode, restore), func(t *testing.T) {
					observed := make(chan string, 2)
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						var body []byte
						var conn *websocket.Conn
						if strings.HasPrefix(transport, "ws") {
							var err error
							conn, err = (&websocket.Upgrader{}).Upgrade(w, r, nil)
							if err != nil {
								t.Error(err)
								return
							}
							defer func() { _ = conn.Close() }()
							_, body, _ = conn.ReadMessage()
						} else {
							body, _ = io.ReadAll(r.Body)
						}
						session := gjson.GetBytes(body, "client_metadata.session_id").String()
						observed <- session
						response, _ := json.Marshal(map[string]any{"id": "resp_test", "status": "completed", "session_id": session, "output": []any{}})
						if transport == "compact" {
							w.Header().Set("Content-Type", "application/json")
							_, _ = w.Write(response)
							return
						}
						event := append(append([]byte(`{"type":"response.completed","response":`), response...), '}')
						if conn != nil {
							_ = conn.WriteMessage(websocket.TextMessage, event)
						} else {
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = w.Write(append(append([]byte("data: "), event...), '\n', '\n'))
						}
					}))
					defer server.Close()
					on, off, poolSize := true, false, 8
					cfg := &config.Config{Routing: config.RoutingConfig{PriorityOverrides: []config.RoutingPriorityOverride{{Priority: 5, Codex: &config.CodexIdentityOverride{IdentityConfuse: &on, RequireSessionAffinity: &off, RestoreResponse: &restore, FingerprintMode: &mode, SessionIdentityPoolSize: &poolSize}}}}}
					auth := &coreauth.Auth{ID: "wire-" + transport + mode + fmt.Sprint(restore), Provider: "codex", Attributes: map[string]string{"priority": "5", "base_url": server.URL}, Metadata: map[string]any{"access_token": "test"}}
					httpExec := NewCodexExecutor(cfg)
					auth = prepareCodexFingerprintAuthForTest(t, httpExec, auth)
					wsExec := NewCodexWebsocketsExecutor(cfg)
					run := func(id string) string {
						body, _ := json.Marshal(map[string]any{"model": "gpt-5.4", "input": "hello", "client_metadata": map[string]string{"session_id": id, "thread_id": id, "turn_id": "turn"}})
						req := core.Request{Model: "gpt-5.4", Payload: body}
						opts := core.Options{SourceFormat: translator.FormatOpenAIResponse}
						if transport == "compact" {
							opts.Alt = "responses/compact"
						}
						var out []byte
						var err error
						if transport == "sse" || transport == "ws-stream" {
							opts.Stream = true
							var result *core.StreamResult
							if transport == "sse" {
								result, err = httpExec.ExecuteStream(t.Context(), auth, req, opts)
							} else {
								result, err = wsExec.ExecuteStream(t.Context(), auth, req, opts)
							}
							if err == nil {
								for chunk := range result.Chunks {
									if chunk.Err != nil {
										err = chunk.Err
									}
									out = append(out, chunk.Payload...)
								}
							}
							if err == nil {
								at := bytes.Index(out, []byte("data: "))
								if at >= 0 {
									out = bytes.TrimSpace(out[at+6:])
									if end := bytes.Index(out, []byte("\n\n")); end >= 0 {
										out = out[:end]
									}
								}
							}
						} else {
							var response core.Response
							if transport == "ws" {
								response, err = wsExec.Execute(t.Context(), auth, req, opts)
							} else {
								response, err = httpExec.Execute(t.Context(), auth, req, opts)
							}
							out = response.Payload
						}
						if err != nil {
							t.Fatal(err)
						}
						value := gjson.GetBytes(out, "session_id").String()
						if value == "" {
							value = gjson.GetBytes(out, "response.session_id").String()
						}
						if value == "" {
							t.Fatalf("missing response session: %s", out)
						}
						return value
					}
					returned := run("client-session")
					first := <-observed
					if first == "client-session" {
						t.Fatal("priority override not applied")
					}
					if restore && returned != "client-session" || !restore && returned != first {
						t.Fatalf("restoration mismatch %s %s", returned, first)
					}
					_ = run(returned)
					if next := <-observed; next != first {
						t.Fatalf("cross-turn identity changed: %s %s", first, next)
					}
				})
			}
		}
	}
}

func TestCodexIdentityChatTranslationPreservesProtocolSources(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			var upstream []byte
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstream, _ = io.ReadAll(r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_chat\",\"status\":\"completed\",\"output\":[]}}\n\n")
			}))
			defer server.Close()
			off := false
			cfg := &config.Config{Codex: config.CodexConfig{IdentityConfuse: true, IdentityConfuseRequireSessionAffinity: &off}}
			auth := &coreauth.Auth{ID: "chat-source", Provider: "codex", Attributes: map[string]string{"base_url": server.URL}, Metadata: map[string]any{"access_token": "fixture", "codex_fingerprint_mode": "off"}}
			body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"prompt_cache_key":"client-session","client_metadata":{"session_id":"client-session","thread_id":"client-session","turn_id":"client-turn","x-codex-turn-metadata":{"root_thread_id":"client-session","parent_thread_id":"parent"},"business_data":"do not retain"}}`)
			req := core.Request{Model: "gpt-5.4", Payload: body}
			opts := core.Options{SourceFormat: translator.FormatOpenAI, Stream: stream}
			exec := NewCodexExecutor(cfg)
			if stream {
				result, err := exec.ExecuteStream(t.Context(), auth, req, opts)
				if err != nil {
					t.Fatal(err)
				}
				for c := range result.Chunks {
					if c.Err != nil {
						t.Fatal(c.Err)
					}
				}
			} else {
				if _, err := exec.Execute(t.Context(), auth, req, opts); err != nil {
					t.Fatal(err)
				}
			}
			session := gjson.GetBytes(upstream, "client_metadata.session_id").String()
			if session == "" || session == "client-session" || gjson.GetBytes(upstream, "prompt_cache_key").String() != session {
				t.Fatalf("lost identity after Chat translation: %s", upstream)
			}
			if gjson.GetBytes(upstream, "client_metadata.business_data").Exists() {
				t.Fatal("retained arbitrary metadata")
			}
		})
	}
}

func TestCodexIndependentConfusionPoolExcludesClientKeysAndModels(t *testing.T) {
	off := false
	cfg := &config.Config{Codex: config.CodexConfig{IdentityConfuse: true, IdentityConfuseRequireSessionAffinity: &off}, CodexFingerprint: config.CodexFingerprintConfig{SessionIdentityPoolSize: 64}}
	exec := NewCodexExecutor(cfg)
	auth := prepareCodexFingerprintAuthForTest(t, exec, &coreauth.Auth{ID: "same-credential", Provider: "codex", Metadata: map[string]any{"codex_fingerprint_mode": "full"}})
	var first string
	for i := 0; i < 4; i++ {
		ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
		ginCtx.Set("apiKey", fmt.Sprint("caller-", i))
		ctx := context.WithValue(t.Context(), "gin", ginCtx)
		req := core.Request{Model: fmt.Sprint("model-", i), Payload: []byte(`{"client_metadata":{"session_id":"same-original","thread_id":"same-original"}}`)}
		opts := core.Options{Metadata: map[string]any{core.ExecutionSessionMetadataKey: fmt.Sprint("connection-", i)}}
		raw, err := exec.PrepareProviderRequest(ctx, req, opts, core.RequestOperationExecute)
		if err != nil {
			t.Fatal(err)
		}
		prepared := raw.(codexPreparedSessionIdentity).forAuth(auth)
		fingerprint, err := resolveCodexConvergedFingerprint(auth, prepared, prepared.ClientThreadID)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = fingerprint.sessionID
		} else if first != fingerprint.sessionID {
			t.Fatal("client key/model/connection changed the same original identity")
		}
	}
}
