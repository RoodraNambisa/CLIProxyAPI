package executor

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

type cacheSessionWireFrame struct {
	connection int32
	session    string
	payload    []byte
}

func newCacheSessionWebsocketUpstream(t *testing.T) (*httptest.Server, <-chan cacheSessionWireFrame) {
	t.Helper()
	frames := make(chan cacheSessionWireFrame, 16)
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close() }()
		id := connections.Add(1)
		for {
			_, payload, errRead := conn.ReadMessage()
			if errRead != nil {
				return
			}
			frames <- cacheSessionWireFrame{connection: id, session: r.Header.Get("Session_id"), payload: payload}
			if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"cache-session-response","status":"completed","output":[]}}`)); errWrite != nil {
				return
			}
		}
	}))
	return server, frames
}

func TestCodexCacheSessionWebsocketReuseFollowsOriginalAndFinalIdentity(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			server, frames := newCacheSessionWebsocketUpstream(t)
			session := t.Name()
			base := NewCodexWebsocketsExecutor(&config.Config{})
			t.Cleanup(func() { base.CloseExecutionSession(session); server.Close() })
			auth := &coreauth.Auth{ID: session, Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
			for _, turn := range []struct {
				enabled      bool
				key, session string
				connections  int32
			}{
				{false, "legacy-a", "legacy-a", 1},
				{false, "legacy-b", "legacy-b", 2},
				{true, "cache-a", "explicit-session", 3},
				{true, "cache-b", "explicit-session", 3},
				{true, "fallback-session", "", 4},
				{true, "fallback-session", "", 4},
				{false, "legacy-c", "legacy-c", 5},
				{false, "legacy-d", "legacy-d", 6},
			} {
				// A fresh executor models hot reload while the session store is retained.
				executor := NewCodexWebsocketsExecutor(&config.Config{Codex: config.CodexConfig{PassthroughPromptCacheKey: turn.enabled}})
				payload, _ := json.Marshal(map[string]any{"model": "gpt-5.4", "input": "fixture", "prompt_cache_key": turn.key, "client_metadata": map[string]string{"session_id": turn.session}})
				req := core.Request{Model: "gpt-5.4", Payload: payload}
				opts := core.Options{SourceFormat: translator.FormatCodex, Stream: stream, Metadata: map[string]any{core.ExecutionSessionMetadataKey: session}}
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
				frame := <-frames
				if frame.connection != turn.connections {
					t.Fatalf("connection = %d, want %d", frame.connection, turn.connections)
				}
				if turn.enabled {
					want := turn.session
					if want == "" {
						want = turn.key
					}
					if frame.session != want || gjson.GetBytes(frame.payload, "client_metadata.session_id").Str != want || gjson.GetBytes(frame.payload, "prompt_cache_key").Str != turn.key {
						t.Fatal("reused connection and frame did not carry the current routing identity")
					}
				}
			}
		})
	}
}

func TestCodexCacheSessionChangeRequiresCompleteWebsocketReplay(t *testing.T) {
	server, frames := newCacheSessionWebsocketUpstream(t)
	executor := NewCodexWebsocketsExecutor(&config.Config{Codex: config.CodexConfig{PassthroughPromptCacheKey: true}})
	session := t.Name()
	t.Cleanup(func() { executor.CloseExecutionSession(session); server.Close() })
	auth := &coreauth.Auth{ID: session, Provider: "codex", ProxyURL: "direct", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}
	opts := core.Options{SourceFormat: translator.FormatCodex, Metadata: map[string]any{core.ExecutionSessionMetadataKey: session}}
	run := func(key, previous string) error {
		body := map[string]any{"model": "gpt-5.4", "input": "fixture", "prompt_cache_key": key}
		if previous != "" {
			body["previous_response_id"] = previous
		}
		payload, _ := json.Marshal(body)
		_, err := executor.Execute(t.Context(), auth, core.Request{Model: "gpt-5.4", Payload: payload}, opts)
		return err
	}
	if err := run("first", ""); err != nil {
		t.Fatal(err)
	}
	<-frames
	err := run("second", "cache-session-response")
	var replay *core.UpstreamWebsocketReplayRequiredError
	if !errors.As(err, &replay) {
		t.Fatalf("changed session reused incomplete connection context: %v", err)
	}
	select {
	case <-frames:
		t.Fatal("incomplete request was sent to upstream")
	default:
	}
	if err := run("second", ""); err != nil {
		t.Fatal(err)
	}
	if frame := <-frames; frame.connection != 2 || frame.session != "second" {
		t.Fatal("complete replay did not open the correctly identified connection")
	}
}
