package executor

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexWebsocketScopeReconnectsForThreadKindAndModel(t *testing.T) {
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		connections.Add(1)
		defer func() { _ = conn.Close() }()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"response","status":"completed","output":[]}}`)); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	exec := NewCodexWebsocketsExecutor(&config.Config{})
	exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	auth := &coreauth.Auth{ID: "scope-auth", Provider: "codex", Attributes: map[string]string{"api_key": "test-token", "base_url": server.URL}}
	sessionID := uuid.NewString()
	defer exec.CloseExecutionSession(sessionID)
	opts := core.Options{SourceFormat: translator.FormatCodex, Metadata: map[string]any{core.ExecutionSessionMetadataKey: sessionID}}
	for i, step := range []struct {
		model, body string
		count       int32
	}{
		{"gpt-5.4", `{"model":"gpt-5.4","input":"one","client_metadata":{"thread_id":"first"}}`, 1},
		{"gpt-5.4", `{"model":"gpt-5.4","input":"two","client_metadata":{"thread_id":"first"}}`, 1},
		{"gpt-5.4", `{"model":"gpt-5.4","input":"memory","client_metadata":{"thread_id":"first","request_kind":"memory"}}`, 2},
		{"gpt-5.4", `{"model":"gpt-5.4","input":"other","client_metadata":{"thread_id":"second"}}`, 3},
		{"gpt-5.5", `{"model":"gpt-5.5","input":"model","client_metadata":{"thread_id":"second"}}`, 4},
	} {
		if _, err := exec.Execute(t.Context(), auth, core.Request{Model: step.model, Payload: []byte(step.body)}, opts); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if connections.Load() != step.count {
			t.Fatalf("step %d connections=%d want=%d", i, connections.Load(), step.count)
		}
	}
}
