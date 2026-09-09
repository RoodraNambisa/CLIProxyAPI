package executor

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexWebsocketSummaryUsesCurrentTurnAcrossOneSession(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			var handlers sync.WaitGroup
			var calls atomic.Int32
			observed := make(chan string, 3)
			upgrader := websocket.Upgrader{}
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				handlers.Add(1)
				defer handlers.Done()
				conn, err := upgrader.Upgrade(w, req, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer func() { _ = conn.Close() }()
				for {
					_, body, errRead := conn.ReadMessage()
					if errRead != nil {
						return
					}
					call := calls.Add(1)
					if call > 3 {
						t.Error("unexpected extra summary attempt")
						return
					}
					observed <- gjson.GetBytes(body, "reasoning.summary").Raw
					completed := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_%d","object":"response","status":"completed","model":"gpt-5.4","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`, call))
					if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
						t.Error(errWrite)
						return
					}
				}
			}))
			t.Cleanup(upstream.Close)
			executor := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}})
			session := uuid.NewString()
			t.Cleanup(func() { executor.CloseExecutionSession(session); handlers.Wait() })
			auth := &coreauth.Auth{ID: session, Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": upstream.URL, "websockets": "true"}}
			for turn, fields := range []string{`,"reasoning":{"effort":"high","summary":"detailed"}`, ``, `,"reasoning":{"effort":"high","summary":null}`} {
				payload := []byte(fmt.Sprintf(`{"model":"gpt-5.4","input":[{"role":"user","content":"turn %d"}]%s}`, turn, fields))
				opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, OriginalRequest: payload, Metadata: map[string]any{core.ExecutionSessionMetadataKey: session}}
				if turn == 1 {
					opts.OriginalRequest = nil
				}
				req := core.Request{Model: "gpt-5.4", Payload: payload}
				if stream {
					result, err := executor.ExecuteStream(t.Context(), auth, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
					}
				} else if _, err := executor.Execute(t.Context(), auth, req, opts); err != nil {
					t.Fatal(err)
				}
				want := ""
				if turn == 0 {
					want = `"detailed"`
				}
				select {
				case got := <-observed:
					if got != want {
						t.Fatalf("turn %d summary=%s want=%q", turn, got, want)
					}
				default:
					t.Fatal("missing upstream frame")
				}
			}
			if calls.Load() != 3 {
				t.Fatalf("calls=%d want=3", calls.Load())
			}
		})
	}
}
