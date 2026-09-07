package executor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexMultiAgentWebsocketExecuteKeepsIncrementalNamespaceProvenance(t *testing.T) {
	runCodexMultiAgentWebsocketTurns(t, false)
}

func TestCodexMultiAgentWebsocketStreamKeepsIncrementalNamespaceProvenance(t *testing.T) {
	runCodexMultiAgentWebsocketTurns(t, true)
}

func runCodexMultiAgentWebsocketTurns(t *testing.T, stream bool) {
	t.Helper()
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
	sessionID := fmt.Sprintf("multi-agent-incremental-%t", stream)
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
		response, err := runCodexMultiAgentWebsocketRequest(t.Context(), executor, auth, core.Request{Model: "gpt-5.4", Payload: []byte(tc.body)}, opts, stream)
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
	runCodexMultiAgentImmediateDisconnect(t, false)
}

func TestCodexMultiAgentStreamQueuedCompletionSurvivesImmediateDisconnect(t *testing.T) {
	runCodexMultiAgentImmediateDisconnect(t, true)
}

func runCodexMultiAgentImmediateDisconnect(t *testing.T, stream bool) {
	t.Helper()
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
				response, err := runCodexMultiAgentWebsocketRequest(t.Context(), executor, auth, core.Request{Model: "gpt-5.4", Payload: []byte(body)}, opts, stream)
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

func runCodexMultiAgentWebsocketRequest(ctx context.Context, executor *CodexWebsocketsExecutor, auth *cliproxyauth.Auth, req core.Request, opts core.Options, stream bool) (core.Response, error) {
	if !stream {
		return executor.Execute(ctx, auth, req, opts)
	}
	result, err := executor.ExecuteStream(ctx, auth, req, opts)
	if err != nil {
		return core.Response{}, err
	}
	var response core.Response
	for chunk := range result.Chunks {
		if chunk.Err != nil && err == nil {
			err = chunk.Err
		}
		event := gjson.ParseBytes(helps.JSONPayload(chunk.Payload))
		if event.Get("type").String() == "response.completed" {
			response.Payload = []byte(event.Get("response").Raw)
		}
	}
	return response, err
}

func TestCodexMultiAgentWebsocketStreamEventsSurviveBodyReleaseAndBootstrap(t *testing.T) {
	raw := []byte(`{"model":"gpt-5.4","input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","parameters":{"properties":{"message":{"type":"string","encrypted":true}}}}]}]}],"prompt_cache_key":"stable-stream-cache"}`)
	ctrl := core.NewRequestBodyReleaseController(int64(len(raw)), []byte("<released>"))
	captured := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		captured <- body
		ctrl.Release()
		item := `{"type":"function_call","namespace":"collaboration-optimize","name":"spawn_agent","id":"fc_worker","call_id":"pair","arguments":"{\"message\":\"business\"}"}`
		for _, event := range []string{
			`{"type":"response.created","response":{"id":"resp_1","output":[]}}`,
			fmt.Sprintf(`{"type":"response.output_item.added","output_index":0,"item":%s}`, item),
			`{"type":"response.function_call_arguments.delta","item_id":"fc_worker","delta":"business"}`,
			fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_1","status":"completed","output":[%s]}}`, item),
		} {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(event)); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	cfg := &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: true, StreamBootstrapBuffering: true, PassthroughPromptCacheKey: true}}
	executor := NewCodexWebsocketsExecutor(cfg)
	ctx := core.WithRequestBodyReleaseController(t.Context(), ctrl)
	auth := &cliproxyauth.Auth{ID: "stream-release", Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
	result, err := executor.ExecuteStream(ctx, auth, core.Request{Model: "gpt-5.4", Payload: raw}, core.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: raw, Headers: http.Header{"User-Agent": {"codex_cli_rs/0.153.4"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatal(chunk.Err)
		}
		event := gjson.ParseBytes(helps.JSONPayload(chunk.Payload))
		var item gjson.Result
		switch event.Get("type").String() {
		case "response.output_item.added":
			item = event.Get("item")
		case "response.completed":
			item = event.Get("response.output.0")
		default:
			continue
		}
		checked++
		if item.Get("namespace").String() != "collaboration" || item.Get("encrypted_function_args").Raw != "[]" || item.Get("call_id").String() != "pair" {
			t.Fatal("released request lost an intermediate or terminal tool identity")
		}
	}
	body := <-captured
	if checked != 2 || gjson.GetBytes(body, "input.0.tools.0.name").String() != "collaboration-optimize" ||
		gjson.GetBytes(body, "prompt_cache_key").String() != "stable-stream-cache" {
		t.Fatal("additional tools, cache key or event coverage changed")
	}
}
