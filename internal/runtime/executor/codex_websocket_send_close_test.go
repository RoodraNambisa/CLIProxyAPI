package executor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexWebsocketSendAfterMessageTooBigDoesNotReconnect(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, required := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/required=%t", stream, required), func(t *testing.T) {
				var connections atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					connections.Add(1)
					upgrader := websocket.Upgrader{}
					peer, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer func() { _ = peer.Close() }()
					if _, _, errRead := peer.ReadMessage(); errRead == nil {
						_ = peer.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"resp_unexpected_retry","output":[]}}`))
					}
				}))
				defer server.Close()
				wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/responses"
				conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = conn.Close() }()
				auth := &cliproxyauth.Auth{ID: "close-auth", Provider: "codex", Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL}}
				exec := NewCodexWebsocketsExecutor(&config.Config{})
				exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
				session := exec.getOrCreateSession("close-session")
				defer exec.CloseExecutionSession("close-session")
				session.conn, session.readerConn = conn, conn
				session.authID, session.authInstanceID = auth.ID, auth.RuntimeInstanceID()
				session.proxyBindingID, session.proxyIdentity = auth.EffectiveProxyBindingID(), websocketProxyIdentity(exec.cfg, auth)
				session.wsURL = wsURL
				session.configureConn(conn)
				// Reproduce a close callback winning the race with the next request's write.
				if errClose := conn.CloseHandler()(websocket.CloseMessageTooBig, "private upstream detail"); errClose != nil {
					t.Fatal(errClose)
				}
				ctx := t.Context()
				if required {
					ctx = cliproxyexecutor.WithRequiredUpstreamWebsocket(ctx)
				}
				opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, Stream: stream, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "close-session"}}
				req := cliproxyexecutor.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":"hello"}`)}
				if stream {
					var result *cliproxyexecutor.StreamResult
					result, err = exec.ExecuteStream(ctx, auth, req, opts)
					if result != nil {
						for range result.Chunks {
						}
					}
				} else {
					_, err = exec.Execute(ctx, auth, req, opts)
				}
				var status interface{ StatusCode() int }
				var closed *websocket.CloseError
				if !errors.As(err, &status) || status.StatusCode() != http.StatusRequestEntityTooLarge || !errors.As(err, &closed) || closed.Code != websocket.CloseMessageTooBig {
					t.Fatalf("close cause/status not preserved: %v", err)
				}
				if strings.Contains(err.Error(), "private upstream detail") {
					t.Fatal("public error exposes close detail")
				}
				if connections.Load() != 1 {
					t.Fatalf("upstream connections = %d, want 1", connections.Load())
				}
				if !session.reqMu.TryLock() {
					t.Fatal("request lock retained")
				}
				session.reqMu.Unlock()
			})
		}
	}
}

func TestCodexWebsocketReadErrorRetainsCloseCause(t *testing.T) {
	cause := &websocket.CloseError{Code: websocket.CloseMessageTooBig, Text: "private detail"}
	wrapped := fmt.Errorf("read failed: %w", cause)
	mapped := mapCodexWebsocketReadError(wrapped)
	if !errors.Is(mapped, wrapped) || !errors.Is(mapped, cause) {
		t.Fatal("mapped read error lost its cause")
	}
	for _, other := range []error{nil, context.Canceled, &websocket.CloseError{Code: websocket.CloseNormalClosure}} {
		if mapCodexWebsocketReadError(other) != other {
			t.Fatal("unrelated error changed")
		}
	}
}
