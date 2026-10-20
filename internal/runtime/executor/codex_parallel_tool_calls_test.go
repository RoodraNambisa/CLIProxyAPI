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
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestCodexParallelToolCallsAtActualTransport(t *testing.T) {
	for _, source := range []translator.Format{translator.FormatOpenAIResponse, translator.FormatOpenAI, translator.FormatCodex} {
		for _, transport := range []string{"http", "stream", "websocket", "websocket-stream", "websocket-http-fallback", "compact"} {
			if source == translator.FormatOpenAI && transport == "compact" {
				// The compact endpoint accepts Responses input rather than Chat messages.
				continue
			}
			for _, value := range []string{"", "true", "false"} {
				for _, lite := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/value=%s/lite=%t", source, transport, value, lite), func(t *testing.T) {
						want := value
						if want == "" {
							want = "true"
						}
						if lite {
							want = "false"
						}
						testCodexParallelToolCallsWire(t, source, transport, value, lite, nil, want)
					})
				}
			}
		}
	}
}

func TestCodexParallelToolCallsPayloadPolicyOrder(t *testing.T) {
	for _, source := range []translator.Format{translator.FormatOpenAIResponse, translator.FormatOpenAI, translator.FormatCodex} {
		for _, transport := range []string{"http", "websocket", "websocket-stream"} {
			for _, policy := range []string{"override", "filter", "lite-override"} {
				t.Run(fmt.Sprintf("%s/%s/%s", source, transport, policy), func(t *testing.T) {
					cfg := &config.Config{}
					models := []config.PayloadModelRule{{Name: "*", Protocol: "codex"}}
					want := ""
					lite := policy == "lite-override"
					if policy != "filter" {
						cfg.Payload.Override = []config.PayloadRule{{Models: models, Params: map[string]any{"parallel_tool_calls": lite}}}
						want = "false"
					} else {
						cfg.Payload.Filter = []config.PayloadFilterRule{{Models: models, Params: []string{"parallel_tool_calls"}}}
					}
					testCodexParallelToolCallsWire(t, source, transport, "", lite, cfg, want)
				})
			}
		}
	}
}

func TestCodexNativeParallelToolCallsConfiguredDefault(t *testing.T) {
	for _, transport := range []string{"http", "websocket", "websocket-stream"} {
		for _, value := range []string{"", "true", "false"} {
			t.Run(transport+"/"+value, func(t *testing.T) {
				cfg := &config.Config{}
				cfg.Payload.Default = []config.PayloadRule{{
					Models: []config.PayloadModelRule{{Name: "*", Protocol: "codex"}},
					Params: map[string]any{"parallel_tool_calls": false},
				}}
				want := value
				if want == "" {
					want = "false"
				}
				testCodexParallelToolCallsWire(t, translator.FormatCodex, transport, value, false, cfg, want)
			})
		}
	}
}

func testCodexParallelToolCallsWire(t *testing.T, source translator.Format, transport, value string, lite bool, cfg *config.Config, want string) {
	t.Helper()
	payload := []byte(`{"model":"gpt-6-astra","input":[{"role":"user","content":"hello"}],"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}`)
	if source == translator.FormatOpenAI {
		payload = []byte(`{"model":"gpt-6-astra","messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}]}`)
	}
	if value != "" {
		payload, _ = sjson.SetRawBytes(payload, "parallel_tool_calls", []byte(value))
	}
	body := testCodexRequestControlsWire(t, source, transport, payload, lite, cfg)
	if got := gjson.GetBytes(body, "parallel_tool_calls").Raw; got != want {
		t.Fatalf("wire parallel_tool_calls=%q, want %q", got, want)
	}
	if len(gjson.GetBytes(body, "tools").Array()) != 1 || !gjson.GetBytes(body, "input").IsArray() {
		t.Fatal("tool declarations or input history were lost")
	}
}

func testCodexRequestControlsWire(t *testing.T, source translator.Format, transport string, payload []byte, lite bool, cfg *config.Config) []byte {
	t.Helper()
	if cfg == nil {
		cfg = &config.Config{}
	}
	payloads := make(chan []byte, 1)
	terminal := `{"type":"response.completed","response":{"id":"parallel-fixture","status":"completed","output":[]}}`
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
			_, body, errRead := conn.ReadMessage()
			if errRead != nil {
				t.Error(errRead)
				return
			}
			payloads <- body
			if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(terminal)); errWrite != nil {
				t.Error(errWrite)
			}
			return
		}
		body, errRead := io.ReadAll(r.Body)
		if errRead != nil {
			t.Error(errRead)
			return
		}
		payloads <- body
		if transport == "compact" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"id":"compact-fixture","object":"response.compaction","output":[]}`)
		} else {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprintf(w, "data: %s\n\n", terminal)
		}
	}))
	defer server.Close()
	opts := core.Options{SourceFormat: source, Headers: make(http.Header)}
	ctx := t.Context()
	if lite {
		opts.Headers.Set(helps.CodexResponsesLiteHeader, "true")
	}
	if strings.HasPrefix(transport, "websocket") && source != translator.FormatOpenAI {
		ctx = core.WithDownstreamWebsocket(ctx)
		if lite {
			payload, _ = sjson.SetBytes(payload, helps.CodexResponsesLiteMetadata, "true")
		}
	}
	opts.OriginalRequest = payload
	request := core.Request{Model: "gpt-6-astra", Payload: payload}
	credential := &coreauth.Auth{ID: t.Name(), Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
	var executor coreauth.ProviderExecutor = NewCodexExecutor(cfg)
	if strings.HasPrefix(transport, "websocket") {
		executor = NewCodexWebsocketsExecutor(cfg)
	}
	if transport == "compact" {
		opts.Alt = "responses/compact"
	}
	if transport == "stream" || transport == "websocket-stream" {
		opts.Stream = true
		if transport == "websocket-stream" && source != translator.FormatCodex && source != translator.FormatOpenAIResponse {
			// The WebSocket stream entry consumes an already translated Responses frame.
			request.Payload = translator.TranslateRequest(source, translator.FormatCodex, request.Model, payload, true)
		}
		result, err := executor.ExecuteStream(ctx, credential, request, opts)
		if err != nil {
			t.Fatal(err)
		}
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				t.Fatal(chunk.Err)
			}
		}
	} else if _, err := executor.Execute(ctx, credential, request, opts); err != nil {
		t.Fatal(err)
	}
	select {
	case body := <-payloads:
		return body
	default:
		t.Fatal("request did not reach the test upstream")
	}
	return nil
}
