package executor

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexTransportsUseSelectedModelCapabilities(t *testing.T) {
	for _, tc := range []struct {
		name, transport string
		stream          bool
		source          translator.Format
		effort          string
	}{
		{"http", "http", false, translator.FormatCodex, "xhigh"},
		{"sse", "http", true, translator.FormatCodex, "xhigh"},
		{"compact", "compact", false, translator.FormatCodex, "xhigh"},
		{"websocket", "websocket", false, translator.FormatCodex, "xhigh"},
		{"websocket-stream", "websocket", true, translator.FormatCodex, "xhigh"},
		{"responses", "http", false, translator.FormatOpenAIResponse, "max"},
		{"chat", "http", false, translator.FormatOpenAI, "xhigh"},
		{"original-source", "http", false, translator.FormatCodex, "xhigh"},
		{"missing-original", "http", false, translator.FormatCodex, "xhigh"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type requestFields struct{ model, effort, path string }
			observed := make(chan requestFields, 1)
			completed := []byte(`{"type":"response.completed","response":{"id":"resp_fixture","object":"response","status":"completed","model":"capability-upstream","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`)
			upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body []byte
				if tc.transport == "websocket" {
					conn, errUpgrade := upgrader.Upgrade(w, r, nil)
					if errUpgrade != nil {
						t.Error(errUpgrade)
						return
					}
					defer func() { _ = conn.Close() }()
					_, body, errUpgrade = conn.ReadMessage()
					if errUpgrade != nil {
						t.Error(errUpgrade)
						return
					}
					observed <- requestFields{gjson.GetBytes(body, "model").String(), gjson.GetBytes(body, "reasoning.effort").String(), r.URL.Path}
					if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
						t.Error(errWrite)
					}
					return
				}
				var errRead error
				body, errRead = io.ReadAll(r.Body)
				if errRead != nil {
					t.Error(errRead)
					return
				}
				observed <- requestFields{gjson.GetBytes(body, "model").String(), gjson.GetBytes(body, "reasoning.effort").String(), r.URL.Path}
				if tc.transport == "compact" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"id":"resp_fixture","object":"response.compaction","output":[]}`))
				} else {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = fmt.Fprintf(w, "data: %s\n\n", completed)
				}
			}))
			t.Cleanup(upstream.Close)
			cfg := &config.Config{
				SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"},
				CodexKey:  []config.CodexKey{{APIKey: "fixture", BaseURL: upstream.URL, Models: []config.CodexModel{{Name: "capability-upstream", Alias: "bound-model", Thinking: &registry.ThinkingSupport{Levels: []string{"xhigh"}}}}}},
			}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.SetConfig(cfg)
			if tc.transport == "websocket" {
				executor := NewCodexWebsocketsExecutor(cfg)
				manager.RegisterExecutor(executor)
				t.Cleanup(func() { executor.CloseExecutionSession(t.Name()) })
			} else {
				manager.RegisterExecutor(NewCodexExecutor(cfg))
			}
			authID := t.Name()
			if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: authID, Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": upstream.URL, "websockets": "true"}}); errRegister != nil {
				t.Fatal(errRegister)
			}
			reg := registry.GetGlobalRegistry()
			reg.RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: "bound-model"}, {ID: "capability-upstream", Type: "codex", Thinking: &registry.ThinkingSupport{Levels: []string{"low"}}}})
			t.Cleanup(func() { reg.UnregisterClient(authID) })
			payload := []byte(fmt.Sprintf(`{"model":"bound-model","input":"fixture","reasoning":{"effort":%q}}`, tc.effort))
			if tc.source == translator.FormatOpenAI {
				payload = []byte(fmt.Sprintf(`{"model":"bound-model","messages":[{"role":"user","content":"fixture"}],"reasoning_effort":%q}`, tc.effort))
			}
			req := core.Request{Model: "bound-model", Payload: payload}
			opts := core.Options{SourceFormat: tc.source, OriginalRequest: payload, Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name()}}
			if tc.name == "original-source" {
				req.Payload = []byte(`{"model":"bound-model","input":"fixture","reasoning":{"effort":"low"}}`)
			}
			if tc.name == "missing-original" {
				opts.OriginalRequest = nil
			}
			if tc.transport == "compact" {
				opts.Alt = "responses/compact"
			}
			if tc.stream {
				stream, errStream := manager.ExecuteStream(t.Context(), []string{"codex"}, req, opts)
				if errStream != nil {
					t.Fatal(errStream)
				}
				for chunk := range stream.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
				}
			} else if _, errExecute := manager.Execute(t.Context(), []string{"codex"}, req, opts); errExecute != nil {
				t.Fatal(errExecute)
			}
			select {
			case got := <-observed:
				wantPath := "/responses"
				if tc.transport == "compact" {
					wantPath += "/compact"
				}
				if got.model != "capability-upstream" || got.effort != "xhigh" || got.path != wantPath {
					t.Fatalf("selected outbound model fields: %+v", got)
				}
			default:
				t.Fatal("no upstream request")
			}
		})
	}
}

func TestCodexCountValidatesSelectedThinkingBeforeAnyUpstreamRequest(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer upstream.Close()
	cfg := &config.Config{CodexKey: []config.CodexKey{{APIKey: "fixture", BaseURL: upstream.URL, Models: []config.CodexModel{{Name: "private-model", Alias: "bound-model", Thinking: &registry.ThinkingSupport{Levels: []string{"low"}}}}}}}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.RegisterExecutor(NewCodexExecutor(cfg))
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: t.Name(), Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": upstream.URL}}); errRegister != nil {
		t.Fatal(errRegister)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(t.Name(), "codex", []*registry.ModelInfo{{ID: "bound-model"}})
	defer reg.UnregisterClient(t.Name())
	_, err := manager.ExecuteCount(t.Context(), []string{"codex"}, core.Request{Model: "bound-model", Payload: []byte(`{"model":"bound-model","input":"fixture","reasoning":{"effort":"xhigh"}}`)}, core.Options{SourceFormat: translator.FormatCodex})
	var issue *thinking.ThinkingError
	if !errors.As(err, &issue) || issue.Code != thinking.ErrLevelNotSupported || calls.Load() != 0 {
		t.Fatal("count skipped native capability validation or contacted upstream")
	}
}
