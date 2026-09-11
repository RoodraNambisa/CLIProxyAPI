package executor

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexUsageLimitNormalization(t *testing.T) {
	quota := `{"type":" USAGE_LIMIT_REACHED ","message":"quota fixture","resets_in_seconds":90}`
	for _, status := range []int{400, 401, 404, 429, 500} {
		for _, path := range []string{"http", "http_root", "handshake", "websocket", "websocket_body", "terminal", "terminal_root"} {
			t.Run(fmt.Sprintf("%s/%d", path, status), func(t *testing.T) {
				body := []byte(`{"error":` + quota + `}`)
				var err error
				switch path {
				case "http":
					err = newCodexStatusErr(status, body)
				case "http_root":
					body = []byte(quota)
					err = newCodexStatusErr(status, body)
				case "handshake":
					err = newCodexWebsocketHandshakeStatusErr(status, body, http.Header{"Retry-After": {"1"}})
				case "websocket", "websocket_body":
					body = []byte(fmt.Sprintf(`{"type":"error","status":%d,"error":%s}`, status, quota))
					if path == "websocket_body" {
						body = []byte(fmt.Sprintf(`{"type":"error","status":%d,"body":{"error":%s}}`, status, quota))
					}
					var ok bool
					err, ok = parseCodexWebsocketError(body)
					if !ok {
						t.Fatal("unrecognized websocket error")
					}
				case "terminal", "terminal_root":
					body = []byte(fmt.Sprintf(`{"type":"response.failed","status":%d,"response":{"error":%s}}`, status, quota))
					if path == "terminal_root" {
						body = []byte(fmt.Sprintf(`{"type":"error","status":%d,"code":"usage_limit_reached","resets_in_seconds":90}`, status))
					}
					var terminal bool
					err, terminal = codexTerminalStreamError(body)
					if !terminal {
						t.Fatal("unrecognized terminal error")
					}
				}
				var result interface {
					StatusCode() int
					RetryAfter() *time.Duration
					SkipAuthResult() bool
					ResponseBody() []byte
				}
				if !errors.As(err, &result) || result.StatusCode() != 429 || result.SkipAuthResult() {
					t.Fatalf("quota error not normalized to retryable 429: %v", err)
				}
				if delay := result.RetryAfter(); delay == nil || *delay != 90*time.Second {
					t.Fatal("quota recovery delay was lost")
				}
				if string(result.ResponseBody()) != string(body) {
					t.Fatal("original error body was lost")
				}
				if coreauth.IsPolicyRefusalError(err) || coreauth.IsModelNotFoundError(err) {
					t.Fatal("quota failure was classified as policy refusal or missing model")
				}
			})
		}
	}
}

func TestCodexUsageLimitRequiresExplicitErrorEvidence(t *testing.T) {
	for _, body := range []string{
		`{"error":{"type":"invalid_request_error","code":"model_not_found","message":"usage_limit_reached"}}`,
		`{"error":{"type":"invalid_request_error","code":"misalignment_policy_violation"},"metadata":{"type":"usage_limit_reached","resets_in_seconds":90}}`,
		`{"error":{"type":"server_error"},"type":"usage_limit_reached","resets_in_seconds":90}`,
		`{"error":{"type":"rate_limit_error","resets_in_seconds":90}}`,
		`{"error":{"type":"usage_limit_reached","resets_in_seconds":90}`,
	} {
		err := newCodexStatusErr(404, []byte(body))
		if err.StatusCode() != 404 || err.RetryAfter() != nil {
			t.Fatal("non-quota or malformed error changed into quota exhaustion")
		}
	}
}

func TestCodexUsageLimitWithoutResetOrWebsocketStatus(t *testing.T) {
	for _, body := range []string{
		`{"error":{"type":"usage_limit_reached"}}`,
		`{"error":{"type":"invalid_request_error","code":"usage_limit_reached","message":"maximum context mentioned by upstream"}}`,
	} {
		err := newCodexStatusErr(404, []byte(body))
		if err.StatusCode() != 429 || err.SkipAuthResult() || err.RetryAfter() != nil {
			t.Fatal("explicit quota without recovery hint did not retain existing backoff")
		}
	}
	for _, body := range []string{
		`{"type":"error","error":{"type":"usage_limit_reached","resets_in_seconds":9}}`,
		`{"type":"error","code":"usage_limit_reached","resets_in_seconds":9}`,
	} {
		err, ok := parseCodexWebsocketError([]byte(body))
		var quota interface {
			StatusCode() int
			RetryAfter() *time.Duration
		}
		if !ok || !errors.As(err, &quota) || quota.StatusCode() != 429 || quota.RetryAfter() == nil || *quota.RetryAfter() != 9*time.Second {
			t.Fatal("quota without explicit websocket status was not recognized")
		}
	}
}

func TestCodexUsageLimitFlexibleReset(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	for _, envelope := range []string{`%s`, `{"error":%s}`, `{"response":{"error":%s}}`, `{"body":{"error":%s}}`} {
		for _, tc := range []struct {
			name, fields string
			want         time.Duration
		}{
			{"absolute", `"resets_at":1700000300,"resets_in_seconds":1`, 5 * time.Minute},
			{"past_absolute", `"resets_at":1699999999,"resets_in_seconds":77`, 77 * time.Second},
			{"string_seconds", `"resets_in_seconds":"90"`, 90 * time.Second},
			{"string_absolute", `"resets_at":"1700000090"`, 90 * time.Second},
			{"missing", `"message":"quota fixture"`, 0},
			{"negative", `"resets_in_seconds":-1`, 0},
			{"zero", `"resets_in_seconds":0`, 0},
			{"fractional", `"resets_in_seconds":1.5`, 0},
			{"boolean", `"resets_in_seconds":true`, 0},
			{"overflow", `"resets_in_seconds":9223372037`, 0},
			{"overflow_integer", `"resets_in_seconds":9223372036854775808`, 0},
			{"overflow_absolute", `"resets_at":9223372036854775807`, 0},
			{"invalid_absolute_fallback", `"resets_at":"invalid","resets_in_seconds":7`, 7 * time.Second},
		} {
			t.Run(envelope+"/"+tc.name, func(t *testing.T) {
				body := []byte(fmt.Sprintf(envelope, `{"code":"usage_limit_reached",`+tc.fields+`}`))
				got := parseCodexRetryAfter(429, body, now)
				if tc.want == 0 {
					if got != nil {
						t.Fatal("invalid reset produced a retry delay")
					}
				} else if got == nil || *got != tc.want {
					t.Fatalf("retry delay = %v, want %s", got, tc.want)
				}
			})
		}
	}
}

func TestCodexUsageLimitAcrossActualEndpoints(t *testing.T) {
	for _, path := range []string{"http", "sse", "websocket", "websocket_body", "handshake404", "handshake426", "compact", "images/generations", "images/edits"} {
		for _, stream := range []bool{false, true} {
			if stream && path == "compact" {
				continue
			}
			t.Run(fmt.Sprintf("%s/stream=%t", path, stream), func(t *testing.T) {
				quota := `{"type":"usage_limit_reached","message":"quota fixture","resets_in_seconds":90}`
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if strings.HasPrefix(path, "websocket") {
						upgrader := websocket.Upgrader{}
						conn, err := upgrader.Upgrade(w, r, nil)
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = conn.Close() }()
						if _, _, err := conn.ReadMessage(); err == nil {
							payload := `{"type":"response.failed","status":404,"response":{"error":` + quota + `}}`
							if path == "websocket_body" {
								payload = `{"type":"error","status":404,"body":{"error":` + quota + `}}`
							}
							_ = conn.WriteMessage(websocket.TextMessage, []byte(payload))
						}
						return
					}
					_, _ = io.Copy(io.Discard, r.Body)
					if path == "sse" {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.failed\",\"status\":404,\"response\":{\"error\":%s}}\n\n", quota)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					status := 404
					if path == "handshake426" {
						status = 426
					}
					w.WriteHeader(status)
					_, _ = io.WriteString(w, `{"error":`+quota+`}`)
				}))
				defer server.Close()
				var executor coreauth.ProviderExecutor = NewCodexExecutor(&config.Config{})
				if strings.HasPrefix(path, "websocket") || strings.HasPrefix(path, "handshake") {
					executor = NewCodexWebsocketsExecutor(&config.Config{})
				}
				auth := &coreauth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
				req := core.Request{Model: "gpt-5.5", Payload: []byte(`{"model":"gpt-5.5","input":"fixture"}`)}
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, Stream: stream}
				if path == "compact" {
					opts.Alt = "responses/compact"
				}
				if strings.HasPrefix(path, "images/") {
					opts.SourceFormat = translator.FromString(codexOpenAIImageSourceFormat)
					opts.Alt = path
					req.Model = "gpt-image-1"
					req.Payload = []byte(`{"model":"gpt-image-1","prompt":"fixture"}`)
				}
				var err error
				if stream {
					var response *core.StreamResult
					response, err = executor.ExecuteStream(t.Context(), auth, req, opts)
					if response != nil {
						for chunk := range response.Chunks {
							if chunk.Err != nil {
								err = chunk.Err
							}
						}
					}
				} else {
					_, err = executor.Execute(t.Context(), auth, req, opts)
				}
				var failure interface {
					StatusCode() int
					RetryAfter() *time.Duration
				}
				if !errors.As(err, &failure) || failure.StatusCode() != 429 || failure.RetryAfter() == nil || *failure.RetryAfter() != 90*time.Second {
					t.Fatalf("actual endpoint lost quota status or reset: %v", err)
				}
				if calls.Load() != 1 {
					t.Fatalf("quota normalization retried within executor: %d calls", calls.Load())
				}
			})
		}
	}
}

func TestCodexUsageLimit404RotatesWithModelScopedReset(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body, _ := io.ReadAll(r.Body)
				if r.Header.Get("Authorization") == "Bearer quota-fixture-a" && gjson.GetBytes(body, "model").String() == "gpt-5.5" {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(404)
					_, _ = io.WriteString(w, `{"error":{"type":"usage_limit_reached","resets_in_seconds":90}}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"quota-fixture\",\"status\":\"completed\",\"output\":[]}}\n\n")
			}))
			defer server.Close()
			manager := coreauth.NewManager(nil, &coreauth.FillFirstSelector{}, nil)
			manager.SetRetryConfig(0, 0, 2)
			manager.RegisterExecutor(NewCodexExecutor(&config.Config{}))
			for _, id := range []string{"quota-fixture-a", "quota-fixture-b"} {
				if _, err := manager.Register(t.Context(), &coreauth.Auth{ID: id, Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": id, "base_url": server.URL}}); err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "gpt-5.5"}, {ID: "gpt-5.4"}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
			}
			req := core.Request{Model: "gpt-5.5", Payload: []byte(`{"model":"gpt-5.5","input":"fixture"}`)}
			opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, Stream: stream}
			if stream {
				result, err := manager.ExecuteStream(t.Context(), []string{"codex"}, req, opts)
				if err != nil {
					t.Fatal(err)
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
				}
			} else if _, err := manager.Execute(t.Context(), []string{"codex"}, req, opts); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 2 {
				t.Fatalf("credential rotation used %d calls, want 2", calls.Load())
			}
			current, _ := manager.GetByID("quota-fixture-a")
			state := current.ModelStates["gpt-5.5"]
			if state == nil || !state.Quota.Exceeded || state.LastError == nil || state.LastError.HTTPStatus != 429 {
				t.Fatal("explicit quota was stored as model-not-found instead of 429")
			}
			if remaining := time.Until(state.NextRetryAfter); remaining < 85*time.Second || remaining > 90*time.Second {
				t.Fatalf("quota reset became the 404 cooldown: %v", remaining)
			}
			opts.Metadata = map[string]any{core.PinnedAuthMetadataKey: current.ID}
			opts.Stream = false
			req.Model = "gpt-5.4"
			req.Payload = []byte(`{"model":"gpt-5.4","input":"fixture"}`)
			if _, err := manager.Execute(t.Context(), []string{"codex"}, req, opts); err != nil {
				t.Fatalf("quota cooldown blocked another model on the same credential: %v", err)
			}
			if calls.Load() != 3 {
				t.Fatal("sibling model did not execute exactly once")
			}
		})
	}
}
