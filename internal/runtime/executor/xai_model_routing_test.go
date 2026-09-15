package executor

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestXAIModelRoutesDriveHTTPStreamCompactAndMedia(t *testing.T) {
	var mu sync.Mutex
	var paths, sessions []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Header.Get("X-Global") != "global" || r.Header.Get("X-Override") != "account" {
			t.Error("route lost header policy")
		}
		mu.Lock()
		paths = append(paths, r.Method+" "+r.URL.Path)
		sessions = append(sessions, r.Header.Get("x-grok-session-id"))
		mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/responses") {
			if gjson.GetBytes(body, "model").String() != r.Header.Get("x-grok-model-override") {
				t.Error("model header/body mismatch")
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: " + xaiCompletedEvent + "\n\n"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"fixture","output":[],"request_id":"video-id"}`))
	}))
	defer server.Close()
	cfg := &config.Config{XAI: config.XAIConfig{SessionIdentityConvergence: true, SessionIdentityPoolSize: 4, Headers: map[string]string{"X-Global": "global", "X-Override": "global"}, ModelRoutes: []config.XAIModelRoute{
		{Models: []string{"grok-4.3"}, Upstream: server.URL + "/text"}, {Models: []string{"grok-4.5"}, Upstream: server.URL + "/other"}, {Models: []string{"grok-imagine-*"}, Upstream: server.URL + "/media"},
	}}}
	auth := &coreauth.Auth{ID: "one-credential", Provider: "xai", Attributes: map[string]string{"base_url": "http://must-not-be-used.invalid/v1", "api_key": "fixture", "header:X-Override": "account"}}
	opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: http.Header{"X-Grok-Session-Id": []string{"same-session"}}}
	plan, err := helps.NewXAIRequestPlan(t.Context(), cfg, core.Request{Payload: []byte(`{}`)}, opts)
	if err != nil {
		t.Fatal(err)
	}
	auth, err = plan.PrepareRequestAuth(t.Context(), auth)
	if err != nil {
		t.Fatal(err)
	}
	exec := NewXAIExecutor(cfg)
	for _, model := range []string{"grok-4.3", "grok-4.5"} {
		req := core.Request{Model: model, Payload: []byte(`{"input":"hello"}`)}
		if _, err := exec.Execute(t.Context(), auth, req, opts); err != nil {
			t.Fatal(err)
		}
	}
	result, err := exec.ExecuteStream(t.Context(), auth, core.Request{Model: "grok-4.3", Payload: []byte(`{"input":"hello"}`)}, opts)
	if err != nil {
		t.Fatal(err)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatal(chunk.Err)
		}
	}
	compactOpts := opts
	compactOpts.Alt = "responses/compact"
	if _, _, _, err := exec.executeCompactRequest(t.Context(), auth, core.Request{Model: "grok-4.5", Payload: []byte(`{"input":"hello"}`)}, compactOpts); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ format, model, path, payload string }{
		{"openai-image", "grok-imagine-image", "/v1/images/generations", `{"prompt":"hello"}`},
		{"openai-video", "grok-imagine-video", "/v1/videos/generations", `{"prompt":"hello"}`},
		{"openai-video", "grok-imagine-video", "/v1/videos/video-id", `{"request_id":"video-id"}`},
	} {
		mediaOpts := core.Options{SourceFormat: sdktranslator.Format(tc.format), Metadata: map[string]any{core.RequestPathMetadataKey: tc.path}}
		if _, err := exec.Execute(t.Context(), auth, core.Request{Model: tc.model, Payload: []byte(tc.payload)}, mediaOpts); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"POST /text/responses", "POST /other/responses", "POST /text/responses", "POST /other/responses/compact", "POST /media/images/generations", "POST /media/videos/generations", "GET /media/videos/video-id"}
	if strings.Join(paths, ",") != strings.Join(want, ",") {
		t.Fatalf("destinations: %v", paths)
	}
	if sessions[0] == "" || sessions[0] != sessions[1] || sessions[1] != sessions[2] {
		t.Fatal("changing model route changed the same account/session identity")
	}
}

func TestXAIModelRoutesDriveWebSocketHandshake(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/regional/responses" || r.Header.Get("X-Global") != "fixture" {
			t.Errorf("wrong handshake %s", r.URL.Path)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(xaiCompletedEvent))
	}))
	defer server.Close()
	cfg := &config.Config{XAI: config.XAIConfig{Headers: map[string]string{"X-Global": "fixture"}, ModelRoutes: []config.XAIModelRoute{{Models: []string{"grok-4.3"}, Upstream: server.URL + "/regional"}}}}
	exec := NewXAIWebsocketsExecutor(cfg)
	auth := &coreauth.Auth{ID: "routed-ws", Provider: "xai", Attributes: map[string]string{"base_url": "http://must-not-be-used.invalid", "api_key": "fixture"}}
	stream, err := exec.ExecuteStream(t.Context(), auth, xaiStreamRequest(), core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Stream: true})
	if err != nil {
		t.Fatal(err)
	}
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			t.Fatal(chunk.Err)
		}
	}
}
