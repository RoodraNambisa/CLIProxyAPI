package executor

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexWebsocketExecuteAppliesExistingImageToolPolicy(t *testing.T) {
	for _, additional := range []bool{false, true} {
		for _, action := range []string{"unmatched", "remove", "error"} {
			t.Run(fmt.Sprintf("additional=%t/%s", additional, action), func(t *testing.T) {
				var calls atomic.Int64
				wire := make(chan []byte, 1)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					upgrader := websocket.Upgrader{}
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer func() { _ = conn.Close() }()
					_, payload, err := conn.ReadMessage()
					if err != nil {
						t.Error(err)
						return
					}
					wire <- payload
					_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"done","output":[]}}`))
				}))
				defer server.Close()
				cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, DisabledImageGenerationToolAction: action, DisabledImageGenerationToolError: config.DisabledImageGenerationToolErrorConfig{StatusCode: 451, Code: "image_disabled", Message: "image disabled", Type: "policy_error"}, AuthModelExclusions: []config.AuthModelExclusionRule{{DisableImageGeneration: true, Priorities: []int{-1}}}}
				priority := "-1"
				if action == "unmatched" {
					priority = "0"
					cfg.DisabledImageGenerationToolAction = "remove"
				}
				credential := &cliproxyauth.Auth{ID: "ws-image-policy", Provider: "codex", Attributes: map[string]string{"api_key": "test", "base_url": server.URL, "priority": priority}}
				tools := `[{"type":"image_generation"},{"type":"function","name":"lookup"}]`
				body := `{"model":"gpt-5.4","input":[],"tools":` + tools + `,"tool_choice":{"type":"image_generation"}}`
				path := "tools"
				if additional {
					body = `{"model":"gpt-5.4","input":[{"type":"additional_tools","tools":` + tools + `}],"tool_choice":{"type":"image_generation"}}`
					path = "input.0.tools"
				}
				_, err := NewCodexWebsocketsExecutor(cfg).Execute(t.Context(), credential, core.Request{Model: "gpt-5.4", Payload: []byte(body)}, core.Options{SourceFormat: translator.FromString("codex")})
				if action == "error" {
					status, ok := err.(interface{ StatusCode() int })
					if !ok || status.StatusCode() != 451 || calls.Load() != 0 {
						t.Fatal("configured image refusal reached upstream or lost its status")
					}
					return
				}
				if err != nil || calls.Load() != 1 {
					t.Fatal("allowed request did not keep its original single attempt")
				}
				payload := <-wire
				if cliproxyauth.PayloadHasImageGenerationTool(payload) != (action == "unmatched") {
					t.Fatal("outgoing tools bypassed credential image policy")
				}
				if action == "remove" && (gjson.GetBytes(payload, path+".0.name").String() != "lookup" || gjson.GetBytes(payload, "tool_choice").Exists()) {
					t.Fatal("image removal changed another tool or left a dangling selection")
				}
			})
		}
	}
}
