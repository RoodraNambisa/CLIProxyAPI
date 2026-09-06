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
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func TestCodexCustomToolStreamsKeepNamesAndInputAcrossBodyRelease(t *testing.T) {
	for _, transport := range []string{"http", "websocket"} {
		for _, native := range []bool{false, true} {
			for _, release := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/native=%t/release=%t", transport, native, release), func(t *testing.T) {
					originalName := "custom_" + strings.Repeat("long_name_", 9)
					payload := []byte(`{"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"custom"}]}`)
					namePath := "tools.0.name"
					if native {
						namePath = "tools.0.custom.name"
					}
					payload, _ = sjson.SetBytes(payload, namePath, originalName)
					ctrl := core.NewRequestBodyReleaseController(int64(len(payload)), []byte("<released>"))
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						var body []byte
						var conn *websocket.Conn
						var err error
						if transport == "websocket" {
							upgrader := websocket.Upgrader{}
							conn, err = upgrader.Upgrade(w, r, nil)
							if err != nil {
								t.Error(err)
								return
							}
							defer func() { _ = conn.Close() }()
							_, body, err = conn.ReadMessage()
						} else {
							body, err = io.ReadAll(r.Body)
							w.Header().Set("Content-Type", "text/event-stream")
						}
						if err != nil {
							t.Error(err)
							return
						}
						name := gjson.GetBytes(body, "tools.0.name").String()
						if name == "" || len(name) > 64 || name == originalName {
							t.Error("upstream custom name was not normalized")
							return
						}
						if release {
							ctrl.Release()
						}
						for _, event := range []string{
							fmt.Sprintf(`{"type":"response.output_item.added","output_index":0,"item":{"type":"custom_tool_call","id":"ctc_a","call_id":"call_a","name":%q}}`, name),
							`{"type":"response.custom_tool_call_input.delta","item_id":"ctc_a","delta":"free "}`,
							`{"type":"response.custom_tool_call_input.delta","item_id":"ctc_a","delta":"form"}`,
							`{"type":"response.custom_tool_call_input.done","item_id":"ctc_a","input":"free form"}`,
							fmt.Sprintf(`{"type":"response.output_item.done","output_index":0,"item":{"type":"custom_tool_call","id":"ctc_a","call_id":"call_a","name":%q,"input":"free form"}}`, name),
							`{"type":"response.completed","response":{"id":"resp_a","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
						} {
							if conn != nil {
								_ = conn.WriteMessage(websocket.TextMessage, []byte(event))
							} else {
								_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
							}
						}
					}))
					defer server.Close()
					cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}}
					var exec auth.ProviderExecutor = NewCodexExecutor(cfg)
					if transport == "websocket" {
						exec = NewCodexWebsocketsExecutor(cfg)
					}
					credential := &auth.Auth{Provider: "codex", Attributes: map[string]string{"api_key": "test", "base_url": server.URL}}
					opts := core.Options{SourceFormat: translator.FormatOpenAI, OriginalRequest: payload}
					requestPayload := payload
					if transport == "websocket" {
						// The WebSocket stream entry consumes the already translated Responses frame.
						requestPayload = translator.TranslateRequest(translator.FormatOpenAI, translator.FormatCodex, "gpt-5.4", payload, true)
					}
					if release {
						opts.Metadata = map[string]any{core.BodyReleaseControllerMetadataKey: ctrl}
					}
					result, err := exec.ExecuteStream(t.Context(), credential, core.Request{Model: "gpt-5.4", Payload: requestPayload}, opts)
					if err != nil {
						t.Fatal(err)
					}
					var input strings.Builder
					announcements := 0
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
						json := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(string(chunk.Payload)), "data:"))
						call := gjson.Get(json, "choices.0.delta.tool_calls.0")
						inputPath, fieldName, kind := "function.arguments", "function.name", "function"
						if native {
							inputPath, fieldName, kind = "custom.input", "custom.name", "custom"
						}
						input.WriteString(call.Get(inputPath).String())
						if call.Get("id").Exists() {
							announcements++
							if call.Get("type").String() != kind || call.Get(fieldName).String() != originalName {
								t.Error("custom call lost its original name or declaration shape")
							}
						}
					}
					if ctrl.Released() != release || announcements != 1 || input.String() != "free form" {
						t.Fatalf("custom stream lost data or release behavior: released=%t announcements=%d", ctrl.Released(), announcements)
					}
				})
			}
		}
	}
}
