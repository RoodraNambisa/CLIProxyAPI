package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

type responseModelTestExecutor struct {
	coreauth.ProviderExecutor
	failID string
}

func (*responseModelTestExecutor) Identifier() string { return "codex" }
func (e *responseModelTestExecutor) Execute(_ context.Context, auth *coreauth.Auth, _ core.Request, _ core.Options) (core.Response, error) {
	if auth.ID == e.failID {
		return core.Response{}, &coreauth.Error{HTTPStatus: 503, Message: "fixture unavailable"}
	}
	return core.Response{Payload: []byte(`{"id":"fixture","model":"gpt-5.6-luna","object":"response","status":"completed","output":[],"usage":{"total_tokens":12}}`), Headers: http.Header{"X-Codex-Turn-State": {"untouched"}}}, nil
}
func (e *responseModelTestExecutor) ExecuteStream(_ context.Context, auth *coreauth.Auth, _ core.Request, _ core.Options) (*core.StreamResult, error) {
	if auth.ID == e.failID {
		return nil, &coreauth.Error{HTTPStatus: 503, Message: "fixture unavailable"}
	}
	chunks := make(chan core.StreamChunk, 3)
	chunks <- core.StreamChunk{Payload: []byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"fixture\",\"model\":\"gpt-5.6-luna\",\"output\":[]}}\n\n")}
	chunks <- core.StreamChunk{Payload: []byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"fixture\",\"model\":\"gpt-5.6-luna\",\"status\":\"completed\",\"output\":[],\"usage\":{\"total_tokens\":12}}}\n\n")}
	close(chunks)
	return &core.StreamResult{Chunks: chunks}, nil
}

func TestResponseModelRewriteUsesFinalCredentialAfterFailover(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, allowedPriority := range []int{0, 3} {
			s := newTestServerWithConfig(t, func(cfg *config.Config) {
				cfg.ResponseModelRewrite = config.ResponseModelRewriteConfig{Enabled: true, Rules: []config.ResponseModelRewriteRule{{AuthPriorities: []int{allowedPriority}}}}
			})
			m := s.handlers.AuthManager
			m.RegisterExecutor(&responseModelTestExecutor{failID: "failed-rewrite"})
			for _, auth := range []*coreauth.Auth{{ID: "failed-rewrite", Provider: "codex", Attributes: map[string]string{"priority": "3"}}, {ID: "final-rewrite", Provider: "codex"}} {
				if _, err := m.Register(t.Context(), auth); err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(auth.ID, "codex", []*registry.ModelInfo{{ID: "rewrite-switch"}})
			}
			body := `{"model":"rewrite-switch","input":"hi","stream":` + map[bool]string{true: "true", false: "false"}[stream] + `}`
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer test-key")
			w := httptest.NewRecorder()
			s.engine.ServeHTTP(w, req)
			want := "gpt-5.6-luna"
			if allowedPriority == 0 {
				want = "rewrite-switch"
			}
			for _, id := range []string{"failed-rewrite", "final-rewrite"} {
				registry.GetGlobalRegistry().UnregisterClient(id)
			}
			if w.Code != 200 || !strings.Contains(w.Body.String(), `"model":"`+want+`"`) {
				t.Fatalf("stream=%v priority=%d: %d %s", stream, allowedPriority, w.Code, w.Body.String())
			}
		}
	}
}

func TestResponseModelRewriteHTTPWebsocketAndDiagnosticBypass(t *testing.T) {
	s := newTestServerWithConfig(t, func(cfg *config.Config) {
		cfg.APIKeyGroups = []config.APIKeyGroup{{APIKey: "test-key", AllowCredentialTargeting: true}}
		cfg.ResponseModelRewrite = config.ResponseModelRewriteConfig{Enabled: true, Rules: []config.ResponseModelRewriteRule{{Providers: []string{"codex"}, AuthPriorities: []int{0}, RequestModels: []string{"rewrite-fixture"}}}}
	})
	m := s.handlers.AuthManager
	m.RegisterExecutor(&responseModelTestExecutor{})
	auth, err := m.Register(t.Context(), &coreauth.Auth{ID: "rewrite-fixture", FileName: "rewrite.json", Provider: "codex", Metadata: map[string]any{"routing_alias": "rewrite-test"}})
	if err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, "codex", []*registry.ModelInfo{{ID: "rewrite-fixture"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	for _, path := range []string{"/v1/responses", "/v1/chat/completions"} {
		for _, stream := range []bool{false, true} {
			for _, key := range []string{"test-key", "test-key-auth-rewrite-test"} {
				body := `{"model":"rewrite-fixture","input":"hi","messages":[{"role":"user","content":"hi"}],"stream":` + map[bool]string{true: "true", false: "false"}[stream] + `}`
				req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
				req.Header.Set("Authorization", "Bearer "+key)
				w := httptest.NewRecorder()
				s.engine.ServeHTTP(w, req)
				want := "rewrite-fixture"
				if key != "test-key" {
					want = "gpt-5.6-luna"
				}
				if w.Code != 200 || !strings.Contains(w.Body.String(), `"model":"`+want+`"`) {
					t.Fatalf("%s stream=%v key=%s: %d %s", path, stream, key, w.Code, w.Body.String())
				}
				if key == "test-key" && strings.Contains(w.Body.String(), "gpt-5.6-luna") {
					t.Fatal("unrewritten model leaked")
				}
			}
		}
	}
	server := httptest.NewServer(s.engine)
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", http.Header{"Authorization": {"Bearer test-key"}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if err = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"rewrite-fixture","input":[]}`)); err != nil {
		t.Fatal(err)
	}
	for {
		_, data, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Fatal(errRead)
		}
		if strings.Contains(string(data), "gpt-5.6-luna") {
			t.Fatalf("websocket model leaked: %s", data)
		}
		if gjson.GetBytes(data, "type").String() == "response.completed" {
			break
		}
	}
	if stats := m.AuthResponseModelRewriteSummary(auth, true); stats.Total != 5 {
		t.Fatalf("expected one count per ordinary request, got %+v", stats)
	}
	updated, err := config.Clone(s.currentConfig())
	if err != nil {
		t.Fatal(err)
	}
	updated.ResponseModelRewrite.Enabled = false
	if err = s.UpdateClients(updated); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"rewrite-fixture","input":[]}`))
	req.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	s.engine.ServeHTTP(w, req)
	if !strings.Contains(w.Body.String(), "gpt-5.6-luna") {
		t.Fatal("disable did not restore upstream model")
	}
}
