package executor

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestXAIResponsesOmitUnrequestedCodexDefaults(t *testing.T) {
	for _, tc := range []struct {
		format sdktranslator.Format
		body   string
	}{
		{sdktranslator.FormatOpenAI, `{"messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"a","parameters":{"type":"object"}}}]}`},
		{sdktranslator.FormatOpenAIResponse, `{"input":"hi","tools":[{"type":"function","name":"a","parameters":{"type":"object"}}]}`},
		{sdktranslator.FormatClaude, `{"messages":[{"role":"user","content":"hi"}],"tools":[{"name":"a","input_schema":{"type":"object"}}]}`},
		{sdktranslator.FormatGemini, `{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"tools":[{"functionDeclarations":[{"name":"a","parameters":{"type":"object"}}]}]}`},
	} {
		t.Run(tc.format.String(), func(t *testing.T) {
			req := core.Request{Model: "grok-4.3", Payload: []byte(tc.body)}
			opts := core.Options{SourceFormat: tc.format}
			prepared, err := NewXAIExecutor(&config.Config{}).prepareResponsesRequest(t.Context(), nil, req, opts, true)
			if err != nil {
				t.Fatal(err)
			}
			if gjson.GetBytes(prepared.body, "parallel_tool_calls").Exists() || gjson.GetBytes(prepared.body, "reasoning.effort").Exists() {
				t.Fatalf("unsolicited parameters: %s", prepared.body)
			}
			cfg := &config.Config{XAI: config.XAIConfig{RequestDefaults: map[string]any{"parallel_tool_calls": false, "reasoning": map[string]any{"effort": "high"}}}}
			prepared, err = NewXAIExecutor(cfg).prepareResponsesRequest(t.Context(), nil, req, opts, true)
			if err != nil {
				t.Fatal(err)
			}
			if !gjson.GetBytes(prepared.body, "parallel_tool_calls").Exists() || gjson.GetBytes(prepared.body, "parallel_tool_calls").Bool() || gjson.GetBytes(prepared.body, "reasoning.effort").String() != "high" {
				t.Fatalf("explicit defaults lost: %s", prepared.body)
			}
		})
	}
}

func TestXAIParameterIntentSurvivesDefaultsSuffixAndPayload(t *testing.T) {
	cfg := &config.Config{XAI: config.XAIConfig{RequestDefaults: map[string]any{"parallel_tool_calls": true, "reasoning": map[string]any{"effort": "high"}}}}
	for _, tc := range []struct {
		format sdktranslator.Format
		body   string
	}{
		{sdktranslator.FormatOpenAI, `{"messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"a"}}],"reasoning_effort":"low","parallel_tool_calls":false}`},
		{sdktranslator.FormatOpenAIResponse, `{"input":"hi","tools":[{"type":"function","name":"a"}],"reasoning":{"effort":"low"},"parallel_tool_calls":false}`},
		{sdktranslator.FormatClaude, `{"messages":[{"role":"user","content":"hi"}],"tools":[{"name":"a","input_schema":{"type":"object"}}],"thinking":{"type":"adaptive"},"output_config":{"effort":"low"},"tool_choice":{"type":"auto","disable_parallel_tool_use":true}}`},
	} {
		for _, mode := range []string{"responses", "direct"} {
			if mode == "direct" && tc.format != sdktranslator.FormatOpenAI {
				continue
			}
			cfg.XAI.ChatCompletionsMode = mode
			exec := NewXAIExecutor(cfg)
			prepare := exec.prepareResponsesRequest
			effortPath := "reasoning.effort"
			if mode == "direct" {
				prepare = exec.prepareChatRequest
				effortPath = "reasoning_effort"
			}
			req := core.Request{Model: "grok-4.3", Payload: []byte(tc.body)}
			opts := core.Options{SourceFormat: tc.format}
			p, err := prepare(t.Context(), nil, req, opts, true)
			if err != nil {
				t.Fatal(err)
			}
			if gjson.GetBytes(p.body, effortPath).String() != "low" || gjson.GetBytes(p.body, "parallel_tool_calls").Bool() {
				t.Fatalf("lost client intent: %s", p.body)
			}
			req.Model = "grok-4.3(high)"
			p, err = prepare(t.Context(), nil, req, opts, true)
			if err != nil {
				t.Fatal(err)
			}
			if gjson.GetBytes(p.body, effortPath).String() != "high" {
				t.Fatalf("suffix lost: %s", p.body)
			}
			cfg.Payload.Override = []config.PayloadRule{{Models: []config.PayloadModelRule{{Name: "grok-4.3"}}, Params: map[string]any{effortPath: "medium", "parallel_tool_calls": false}}}
			p, err = prepare(t.Context(), nil, req, opts, true)
			if err != nil {
				t.Fatal(err)
			}
			if gjson.GetBytes(p.body, effortPath).String() != "medium" || gjson.GetBytes(p.body, "parallel_tool_calls").Bool() {
				t.Fatalf("payload override lost: %s", p.body)
			}
			cfg.Payload.Override = nil
		}
	}
}

func TestXAIAllowedToolsSurviveConversionAndSearchInjection(t *testing.T) {
	for _, tc := range []struct {
		format sdktranslator.Format
		body   string
	}{
		{sdktranslator.FormatOpenAI, `{"messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"a"}},{"type":"function","function":{"name":"b"}}],"tool_choice":{"type":"allowed_tools","allowed_tools":{"mode":"required","tools":[{"type":"function","function":{"name":"a"}}]}}}`},
		{sdktranslator.FormatOpenAIResponse, `{"input":"hi","tools":[{"type":"function","name":"a"},{"type":"function","name":"b"}],"tool_choice":{"type":"allowed_tools","mode":"required","tools":[{"type":"function","name":"a"}]}}`},
	} {
		p, err := NewXAIExecutor(&config.Config{XAI: config.XAIConfig{InjectXSearch: true}}).prepareResponsesRequest(t.Context(), nil, core.Request{Model: "grok-4.3", Payload: []byte(tc.body)}, core.Options{SourceFormat: tc.format}, true)
		if err != nil {
			t.Fatal(err)
		}
		if gjson.GetBytes(p.body, "tool_choice.type").String() != "allowed_tools" || gjson.GetBytes(p.body, "tool_choice.mode").String() != "required" || gjson.GetBytes(p.body, "tool_choice.tools.#").Int() != 1 || gjson.GetBytes(p.body, "tool_choice.tools.0.name").String() != "a" {
			t.Fatalf("restriction lost or broadened: %s", p.body)
		}
	}
}

const xaiChatFixture = `{"id":"chat-test","object":"chat.completion","model":"grok-4.3","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call-a","type":"function","function":{"name":"a","arguments":"{}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":12,"completion_tokens":3,"total_tokens":15,"prompt_tokens_details":{"cached_tokens":8}}}`

func TestXAIDirectChatRoutesHeadersIdentityAndFrozenRetries(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	var headers []http.Header
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		paths = append(paths, r.URL.Path)
		headers = append(headers, r.Header.Clone())
		bodies = append(bodies, body)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, xaiChatFixture)
	}))
	defer server.Close()
	cfg := &config.Config{XAI: config.XAIConfig{ChatCompletionsMode: "direct", SessionIdentityConvergence: true, IdentityConfuse: true, SessionIdentityPoolSize: 4, Headers: map[string]string{"X-Test": "global"}, ModelRoutes: []config.XAIModelRoute{{Models: []string{"grok-4.3"}, Upstream: server.URL + "/selected"}}}}
	auth := &coreauth.Auth{ID: "chat-auth", Provider: "xai", Attributes: map[string]string{"api_key": "fixture", "base_url": "http://must-not-use.invalid", "header:X-Test": "credential"}, Metadata: map[string]any{helps.XAIIdentitySeedKey: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))}}
	req := core.Request{Model: "grok-4.3", Payload: []byte(`{"messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"a"}}],"tool_choice":{"type":"function","function":{"name":"a"}}}`)}
	opts := core.Options{SourceFormat: sdktranslator.FormatOpenAI, Headers: http.Header{"X-Grok-Session-Id": {"shared"}}}
	plan, err := helps.NewXAIRequestPlan(t.Context(), cfg, req, opts)
	if err != nil {
		t.Fatal(err)
	}
	opts = core.WithProviderPreparedRequest(opts, "xai", plan)
	exec := NewXAIExecutor(cfg)
	cfg.XAI.ChatCompletionsMode = "responses"
	cfg.XAI.ModelRoutes = nil
	cfg.XAI.Headers["X-Test"] = "changed"
	for range 2 {
		resp, err := exec.Execute(t.Context(), auth, req, opts)
		if err != nil {
			t.Fatal(err)
		}
		if string(resp.Payload) != xaiChatFixture {
			t.Fatalf("Chat response changed: %s", resp.Payload)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(paths) != 2 || paths[0] != "/selected/chat/completions" || paths[1] != paths[0] {
		t.Fatalf("paths=%v", paths)
	}
	for i, header := range headers {
		if header.Get("X-Test") != "credential" || header.Get("x-grok-model-override") != "grok-4.3" {
			t.Fatal("wrong headers")
		}
		if header.Get("x-grok-session-id") == "" || header.Get("x-grok-session-id") != headers[0].Get("x-grok-session-id") || header.Get("x-grok-conv-id") != gjson.GetBytes(bodies[i], "prompt_cache_key").String() || header.Get("x-grok-req-id") != headers[0].Get("x-grok-req-id") {
			t.Fatal("retry changed identity")
		}
		if gjson.GetBytes(bodies[i], "tools").Raw != gjson.GetBytes(req.Payload, "tools").Raw || gjson.GetBytes(bodies[i], "tool_choice").Raw != gjson.GetBytes(req.Payload, "tool_choice").Raw || gjson.GetBytes(bodies[i], "messages").Raw != gjson.GetBytes(req.Payload, "messages").Raw {
			t.Fatal("direct Chat translated client content")
		}
		if gjson.GetBytes(bodies[i], "reasoning_effort").Exists() || gjson.GetBytes(bodies[i], "parallel_tool_calls").Exists() {
			t.Fatal("injected defaults")
		}
	}
	if headers[0].Get("x-grok-transient-retry") != "" || headers[1].Get("x-grok-transient-retry") != "1" {
		t.Fatal("wrong retry counter")
	}
}

func TestXAIDirectChatStreamAndErrors(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		wantErr    bool
	}{
		{"complete", "data: {\"id\":\"chat-a\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-a\",\"type\":\"function\",\"function\":{\"name\":\"a\",\"arguments\":\"{}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":3,\"total_tokens\":15}}\n\ndata: [DONE]\n\n", 200, false},
		{"missing-terminal", "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"},\"finish_reason\":\"stop\"}]}\n\n", 200, true},
		{"stream-error", "event: error\ndata: {\"error\":{\"code\":\"server_error\",\"message\":\"overloaded\"}}\n\n", 200, true},
		{"http-error", `{"error":{"message":"rate limited","code":"rate_limit_exceeded"}}`, 429, true},
		{"json-instead-of-sse", xaiChatFixture, 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			exec := NewXAIExecutor(&config.Config{XAI: config.XAIConfig{ChatCompletionsMode: "direct"}})
			auth := &coreauth.Auth{ID: "chat-stream", Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
			stream, err := exec.ExecuteStream(t.Context(), auth, core.Request{Model: "grok-4.3", Payload: []byte(`{"messages":[{"role":"user","content":"hi"}],"stream_options":{"include_usage":true}}`)}, core.Options{SourceFormat: sdktranslator.FormatOpenAI, Metadata: map[string]any{core.StreamTerminalMarkerMetadataKey: true}})
			failed := err != nil
			terminal := false
			var output strings.Builder
			if err == nil {
				for chunk := range stream.Chunks {
					if chunk.Err != nil {
						failed = true
					}
					if core.IsSuccessfulStreamTerminalChunk(chunk) {
						terminal = true
					}
					output.Write(chunk.Payload)
				}
			}
			if failed != tc.wantErr {
				t.Fatalf("failed=%v err=%v", failed, err)
			}
			if terminal == tc.wantErr {
				t.Fatalf("terminal=%v", terminal)
			}
			if !tc.wantErr && (!strings.Contains(output.String(), "call-a") || !strings.Contains(output.String(), "prompt_tokens")) {
				t.Fatal("lost tool, usage or terminal event")
			}
		})
	}
}

func TestXAIDirectChatCancellationClosesUpstream(t *testing.T) {
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(closed)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream, err := NewXAIExecutor(&config.Config{XAI: config.XAIConfig{ChatCompletionsMode: "direct"}}).ExecuteStream(ctx, &coreauth.Auth{ID: "cancel", Attributes: map[string]string{"base_url": server.URL}}, core.Request{Model: "grok-4.3", Payload: []byte(`{"messages":[]}`)}, core.Options{SourceFormat: sdktranslator.FormatOpenAI})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream not cancelled")
	}
	for range stream.Chunks {
	}
}

func TestXAIChatModeScopeAndSharedSlot(t *testing.T) {
	auth := &coreauth.Auth{ID: "shared-slot", Metadata: map[string]any{helps.XAIIdentitySeedKey: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{8}, 32))}}
	cfg := &config.Config{XAI: config.XAIConfig{ChatCompletionsMode: "direct", SessionIdentityConvergence: true}}
	exec := NewXAIExecutor(cfg)
	var session string
	for _, format := range []sdktranslator.Format{sdktranslator.FormatOpenAI, sdktranslator.FormatOpenAIResponse} {
		for _, caller := range []string{"client-one", "client-two"} {
			req := core.Request{Model: "grok-4.3", Payload: []byte(`{"messages":[{"role":"user","content":"hi"}],"input":"hi"}`)}
			opts := core.Options{SourceFormat: format, Headers: http.Header{"X-Grok-Session-Id": {"same-session"}}, Metadata: map[string]any{core.CallerScopeMetadataKey: caller}}
			runner, scoped, err := exec.requestExecutor(t.Context(), req, opts)
			if err != nil {
				t.Fatal(err)
			}
			if runner.usesDirectChat(scoped) != (format == sdktranslator.FormatOpenAI) {
				t.Fatal("Chat mode affected other protocols")
			}
			prepare := runner.prepareResponsesRequest
			if runner.usesDirectChat(scoped) {
				prepare = runner.prepareChatRequest
			}
			p, err := prepare(t.Context(), auth, req, scoped, true)
			if err != nil {
				t.Fatal(err)
			}
			if session == "" {
				session = p.identity.Session
			}
			if session == "" || session != p.identity.Session || session != p.identity.CacheKey {
				t.Fatal("protocol or API key changed the convergence slot")
			}
		}
	}
	if NewXAIExecutor(&config.Config{}).usesDirectChat(core.Options{SourceFormat: sdktranslator.FormatOpenAI}) {
		t.Fatal("default transport changed")
	}
}

func TestXAIChatPassthroughProtectsDifferentConversationAndCache(t *testing.T) {
	cfg := &config.Config{XAI: config.XAIConfig{ChatCompletionsMode: "direct", PassthroughClientIdentity: true}}
	req := core.Request{Model: "grok-4.3", Payload: []byte(`{"messages":[],"prompt_cache_key":"parent-cache"}`)}
	opts := core.Options{SourceFormat: sdktranslator.FormatOpenAI, Headers: http.Header{"X-Grok-Session-Id": {"session"}, "X-Grok-Conv-Id": {"auxiliary"}}}
	runner, scoped, err := NewXAIExecutor(cfg).requestExecutor(t.Context(), req, opts)
	if err != nil {
		t.Fatal(err)
	}
	p, err := runner.prepareChatRequest(t.Context(), nil, req, scoped, false)
	if err != nil {
		t.Fatal(err)
	}
	if p.identity.Conversation != "auxiliary" || p.identity.CacheKey != "parent-cache" || p.identity.Session != "session" {
		t.Fatal("passthrough collapsed explicit domains")
	}
}

func TestXAIUnknownAndInvalidChoicesAreNotSilentlyDropped(t *testing.T) {
	for _, choice := range []string{`{"type":"function","name":"missing"}`, `{"type":"future-tool-policy","mode":"required"}`} {
		body := []byte(`{"tools":[{"type":"function","name":"a"}],"tool_choice":` + choice + `}`)
		out := normalizeXAIToolChoiceForTools(body)
		if gjson.GetBytes(out, "tool_choice").Raw != choice {
			t.Fatalf("silently removed constraint: %s", out)
		}
	}
}
