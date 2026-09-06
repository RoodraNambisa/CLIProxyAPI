package executor

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

type codexWebsocketLogBuffer struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (b *codexWebsocketLogBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.Write(data)
}

func (b *codexWebsocketLogBuffer) text() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.String()
}

func TestCodexWebsocketSessionCacheAliasIsNotLoggedAfterReuseOrCredentialChange(t *testing.T) {
	level, originalOutput := log.GetLevel(), log.StandardLogger().Out
	log.SetLevel(log.InfoLevel)
	t.Cleanup(func() { log.SetLevel(level); log.SetOutput(originalOutput) })
	for _, switchAuth := range []bool{false, true} {
		t.Run(fmt.Sprintf("switch-auth=%t", switchAuth), func(t *testing.T) {
			var diagnostics codexWebsocketLogBuffer
			log.SetOutput(&diagnostics)
			var dials atomic.Int32
			seen := make(chan string, 2)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{}
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					t.Error("websocket upgrade failed")
					return
				}
				dials.Add(1)
				defer func() { _ = conn.Close() }()
				for {
					_, body, errRead := conn.ReadMessage()
					if errRead != nil {
						return
					}
					seen <- gjson.GetBytes(body, "prompt_cache_key").String()
					if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"response-test","status":"completed","output":[]}}`)); errWrite != nil {
						return
					}
				}
			}))
			defer server.Close()
			executor := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{ProxyURL: "direct"}})
			firstKey := fmt.Sprintf("first-client-cache-key-%t", switchAuth)
			keys := []string{firstKey, "next-client-cache-key"}
			defer executor.CloseExecutionSession(firstKey)
			for index, key := range keys {
				authID := "initial-auth"
				if switchAuth && index > 0 {
					authID = "next-auth"
				}
				auth := &cliproxyauth.Auth{ID: authID, Provider: "codex", Attributes: map[string]string{"api_key": "test-key", "base_url": server.URL}}
				req := cliproxyexecutor.Request{Model: "gpt-5.4", Payload: []byte(fmt.Sprintf(`{"model":"gpt-5.4","input":"hello","prompt_cache_key":%q}`, key))}
				opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: firstKey}}
				if _, err := executor.Execute(t.Context(), auth, req, opts); err != nil {
					t.Fatal("websocket request failed")
				}
				if got := <-seen; got != key {
					t.Fatal("diagnostic projection changed the outgoing cache key")
				}
			}
			executor.CloseExecutionSession(firstKey)
			server.Close()
			wantDials := int32(1)
			if switchAuth {
				wantDials = 2
			}
			if dials.Load() != wantDials {
				t.Fatal("diagnostic policy changed connection reuse")
			}
			for _, key := range keys {
				if strings.Contains(diagnostics.text(), key) {
					t.Fatal("a cache key used as session identity escaped through connection diagnostics")
				}
			}
			if !strings.Contains(diagnostics.text(), "session=sha256:") {
				t.Fatal("connection diagnostics lost the stable session digest")
			}
		})
	}
}
