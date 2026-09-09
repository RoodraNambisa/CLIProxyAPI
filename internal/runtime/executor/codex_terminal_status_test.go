package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexTerminalExplicitStatusPrecedesReasonAndPolicyCode(t *testing.T) {
	for _, event := range []string{"response.failed", "response.incomplete", "response.completed", "response.done", "error"} {
		for _, status := range []int{http.StatusUnauthorized, http.StatusPaymentRequired, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
			for _, field := range []string{"status", "status_code"} {
				t.Run(fmt.Sprintf("%s/%s/%d", event, field, status), func(t *testing.T) {
					payload := fmt.Sprintf(`{"type":%q,%q:%d,"response":{"status":"incomplete","error":{"code":"misalignment_policy_violation","type":"server_error","message":"denied"},"incomplete_details":{"reason":"max_tokens"}},"error":{"code":"misalignment_policy_violation","message":"denied"}}`, event, field, status)
					err, ok := codexTerminalStreamError([]byte(payload))
					if !ok || err.StatusCode() != status || err.SkipAuthResult() {
						t.Fatal("explicit upstream failure became a generic or request-only incomplete error")
					}
					if (status == http.StatusPaymentRequired || status == http.StatusTooManyRequests) && coreauth.IsPolicyRefusalError(err) {
						t.Fatal("policy text hid payment or real quota status")
					}
				})
			}
		}
	}
}

func TestCodexTerminalNestedStatusAndErrorTypes(t *testing.T) {
	err, ok := codexTerminalStreamError([]byte(`{"type":"response.failed","status":401,"response":{"error":{"type":"authentication_error","code":"misalignment_policy_violation","message":"expired"}}}`))
	if !ok || err.StatusCode() != 401 || coreauth.IsPolicyRefusalError(err) {
		t.Fatal("explicit authentication evidence was treated as a policy refusal")
	}
	for _, payload := range []string{
		`{"type":"response.failed","response":{"error":{"status_code":429,"code":"server_error","message":"failed"}}}`,
		`{"type":"response.incomplete","response":{"error":{"status":429,"code":"server_error","message":"failed"},"incomplete_details":{"reason":"max_tokens"}}}`,
		`{"type":"response.failed","response":{"status_code":429,"error":{"code":"server_error","message":"failed"}}}`,
		`{"type":"response.failed","response":{"error":{"type":"rate_limit_error","message":"failed"}}}`,
		`{"type":"error","error":{"status":429,"message":"failed"}}`,
	} {
		err, ok := codexTerminalStreamError([]byte(payload))
		if !ok || err.StatusCode() != 429 || err.SkipAuthResult() {
			t.Fatal("nested quota status or type did not remain a credential failure")
		}
	}
}

func TestCodexTerminalTopLevelErrorIsNeverCompleted(t *testing.T) {
	for _, event := range []string{"response.failed", "response.incomplete", "response.completed", "response.done"} {
		payload := []byte(fmt.Sprintf(`{"type":%q,"response":{"status":"completed","error":null},"error":{"code":"misalignment_policy_violation","message":"denied"}}`, event))
		err, ok := codexTerminalStreamError(payload)
		if !ok || !coreauth.IsPolicyRefusalError(err) || isCodexSuccessfulCompletion(payload) {
			t.Fatal("top-level policy error was lost or treated as successful completion")
		}
	}
}

func TestCodexTerminal429SurvivesEveryTransport(t *testing.T) {
	for _, websocketTransport := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			for _, event := range []string{"response.failed", "response.incomplete", "response.completed", "response.done", "error"} {
				t.Run(fmt.Sprintf("ws=%t/stream=%t/%s", websocketTransport, stream, event), func(t *testing.T) {
					responseStatus := "incomplete"
					if event == "response.completed" || event == "response.done" {
						responseStatus = "cancelled"
					}
					payload := []byte(fmt.Sprintf(`{"type":%q,"status":429,"response":{"status":%q,"error":null,"incomplete_details":{"reason":"max_tokens"}},"error":{"code":"misalignment_policy_violation","message":"failed"}}`, event, responseStatus))
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if websocketTransport {
							upgrader := websocket.Upgrader{}
							conn, err := upgrader.Upgrade(w, r, nil)
							if err != nil {
								t.Error(err)
								return
							}
							defer func() { _ = conn.Close() }()
							if _, _, err := conn.ReadMessage(); err == nil {
								_ = conn.WriteMessage(websocket.TextMessage, payload)
							}
							return
						}
						_, _ = io.Copy(io.Discard, r.Body)
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = w.Write(append(append([]byte("data: "), payload...), '\n', '\n'))
					}))
					defer server.Close()
					var executor coreauth.ProviderExecutor = NewCodexExecutor(&config.Config{})
					if websocketTransport {
						executor = NewCodexWebsocketsExecutor(&config.Config{})
					}
					auth := &coreauth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
					req := core.Request{Model: "gpt-5.4-mini", Payload: []byte(`{"model":"gpt-5.4-mini","input":"question"}`)}
					opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, Stream: stream}
					var err error
					if stream {
						var result *core.StreamResult
						result, err = executor.ExecuteStream(t.Context(), auth, req, opts)
						if result != nil {
							for chunk := range result.Chunks {
								if chunk.Err != nil {
									err = chunk.Err
								}
							}
						}
					} else {
						_, err = executor.Execute(t.Context(), auth, req, opts)
					}
					status, ok := err.(interface{ StatusCode() int })
					if !ok || status.StatusCode() != 429 || coreauth.IsPolicyRefusalError(err) || !strings.Contains(err.Error(), "misalignment_policy_violation") {
						t.Fatal("actual terminal failure did not preserve its upstream quota status")
					}
				})
			}
		}
	}
}

func TestCodexWebsocketErrorRejectsInvalidExplicitStatus(t *testing.T) {
	for _, status := range []string{"200", "600", `"429"`, "429.5", "9223372036854775808"} {
		if _, ok := parseCodexWebsocketError([]byte(`{"type":"error","status":` + status + `,"error":{"code":"server_error","message":"failed"}}`)); ok {
			t.Fatal("invalid websocket error status was accepted as an HTTP status")
		}
	}
}
