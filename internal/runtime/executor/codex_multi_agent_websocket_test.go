package executor

import (
	"fmt"
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

func TestCodexMultiAgentWebsocketExecuteKeepsIncrementalNamespaceProvenance(t *testing.T) {
	captured := make(chan []byte, 13)
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
		for turn := 1; ; turn++ {
			_, body, err := conn.ReadMessage()
			if err != nil {
				return
			}
			captured <- body
			if gjson.GetBytes(body, "metadata.reject_fixture").Bool() {
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.failed","response":{"status":"failed","error":{"code":"misalignment_policy_violation","message":"fixture rejection"}}}`))
				continue
			}
			response := fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_%d","status":"completed","output":[{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent","call_id":"pair","arguments":"{}"}]}}`, turn)
			if err := conn.WriteMessage(websocket.TextMessage, []byte(response)); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	cfg := &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}}
	executor := NewCodexWebsocketsExecutor(cfg)
	const sessionID = "multi-agent-execute-incremental"
	defer executor.CloseExecutionSession(sessionID)
	auth := &cliproxyauth.Auth{ID: "multi-agent-fixture", Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
	opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}, Metadata: map[string]any{core.ExecutionSessionMetadataKey: sessionID}}
	full := `{"model":"gpt-5.4","tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","parameters":{"properties":{"message":{"type":"string","encrypted":true}}}}]}],"input":"hello"}`
	conflict := `{"model":"gpt-5.4","tools":[{"type":"namespace","name":"collaboration-optimize","tools":[{"type":"function","name":"spawn_agent"}]}],"input":"custom tool"}`
	for index, tc := range []struct {
		body    string
		restore bool
	}{
		{full, true},
		{`{"model":"gpt-5.4","previous_response_id":"resp_1","input":[]}`, true},
		{conflict, false},
		{`{"model":"gpt-5.4","previous_response_id":"resp_3","input":[]}`, false},
		{full, true},
		{`{"model":"gpt-5.4","previous_response_id":"resp_5","input":[]}`, true},
		{full, false},
		{`{"model":"gpt-5.4","previous_response_id":"resp_7","input":[]}`, false},
		{full, true},
		{`{"model":"gpt-5.4","tools":[{"type":"namespace","name":"collaboration-optimize","tools":[{"type":"function","name":"spawn_agent"}]}],"metadata":{"reject_fixture":true},"input":"rejected change"}`, false},
		{`{"model":"gpt-5.4","previous_response_id":"resp_9","input":[]}`, true},
		{`{"model":"gpt-5.4","input":"fresh credential"}`, false},
	} {
		if index == 6 {
			cfg.Codex.OptimizeMultiAgentV2 = false
		}
		if index == 8 {
			cfg.Codex.OptimizeMultiAgentV2 = true
		}
		if index == 11 {
			auth = &cliproxyauth.Auth{ID: "replacement-auth", Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
		}
		response, err := executor.Execute(t.Context(), auth, core.Request{Model: "gpt-5.4", Payload: []byte(tc.body)}, opts)
		if index == 9 {
			if err == nil || !strings.Contains(err.Error(), "misalignment_policy_violation") {
				t.Fatal("fixture refusal lost its original error")
			}
			<-captured
			continue
		}
		if err != nil {
			t.Fatalf("turn %d: %v", index+1, err)
		}
		body := <-captured
		want := "collaboration-optimize"
		if tc.restore {
			want = "collaboration"
		}
		if gjson.GetBytes(response.Payload, "output.0.namespace").String() != want {
			t.Fatalf("turn %d: wrong client namespace", index+1)
		}
		if gjson.GetBytes(response.Payload, "output.0.encrypted_function_args").Exists() != tc.restore {
			t.Fatalf("turn %d: wrong plaintext provenance", index+1)
		}
		if index == 0 || index == 4 {
			if gjson.GetBytes(body, "tools.0.name").String() != "collaboration-optimize" {
				t.Fatal("outbound namespace was not prepared")
			}
		}
		if gjson.GetBytes([]byte(tc.body), "previous_response_id").Exists() && gjson.GetBytes(body, "tools").Exists() {
			t.Fatal("incremental fixture gained implicit tool declarations")
		}
	}
	if connections.Load() != 2 {
		t.Fatal("only the credential replacement should open another connection")
	}
}

func TestCodexMultiAgentQueuedCompletionSurvivesImmediateDisconnect(t *testing.T) {
	for iteration := range 12 {
		t.Run(fmt.Sprint(iteration), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer func() { _ = conn.Close() }()
				for turn := 1; turn <= 2; turn++ {
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
					event := fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_%d","status":"completed","output":[{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent","call_id":"pair","arguments":"{}"}]}}`, turn)
					if err := conn.WriteMessage(websocket.TextMessage, []byte(event)); err != nil {
						return
					}
				}
			}))
			defer server.Close()
			executor := NewCodexWebsocketsExecutor(&config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true}})
			sessionID := "multi-agent-close-" + t.Name()
			defer executor.CloseExecutionSession(sessionID)
			auth := &cliproxyauth.Auth{ID: "close-fixture", Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
			opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}}, Metadata: map[string]any{core.ExecutionSessionMetadataKey: sessionID}}
			for _, body := range []string{
				`{"model":"gpt-5.4","input":"hello","tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent"}]}]}`,
				`{"model":"gpt-5.4","previous_response_id":"resp_1","input":[]}`,
			} {
				response, err := executor.Execute(t.Context(), auth, core.Request{Model: "gpt-5.4", Payload: []byte(body)}, opts)
				if err != nil {
					t.Fatal(err)
				}
				if gjson.GetBytes(response.Payload, "output.0.namespace").String() != "collaboration" ||
					gjson.GetBytes(response.Payload, "output.0.encrypted_function_args").Raw != "[]" {
					t.Fatal("disconnect erased the queued response's namespace policy")
				}
			}
		})
	}
}
