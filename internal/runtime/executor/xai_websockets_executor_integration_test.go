package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	_ "github.com/router-for-me/CLIProxyAPI/v6/internal/translator"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const xaiCompletedEvent = `{"type":"response.completed","response":{"id":"resp_1","object":"response","created_at":0,"status":"completed","model":"grok-4.3","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`

func drainXAIStream(t *testing.T, result *cliproxyexecutor.StreamResult) {
	t.Helper()
	if result == nil {
		t.Fatal("stream result is nil")
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error = %v", chunk.Err)
		}
	}
}

func xaiStreamRequest() cliproxyexecutor.Request {
	return cliproxyexecutor.Request{Model: "grok-4.3", Payload: []byte(`{"model":"grok-4.3","input":[{"role":"user","content":"hello"}]}`)}
}

func TestXAIAutoExecutorDefaultsToHTTPForDownstreamWebsocket(t *testing.T) {
	requestKind := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if websocket.IsWebSocketUpgrade(r) {
			requestKind <- "websocket"
			http.Error(w, "unexpected websocket", http.StatusBadRequest)
			return
		}
		requestKind <- "http"
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: " + xaiCompletedEvent + "\n\n"))
	}))
	defer server.Close()

	exec := NewXAIAutoExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "token"}}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	result, err := exec.ExecuteStream(ctx, auth, xaiStreamRequest(), cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	drainXAIStream(t, result)
	if got := <-requestKind; got != "http" {
		t.Fatalf("transport = %q, want http", got)
	}
}

func TestXAIAutoExecutorUsesWebsocketAndClosesPersistentSession(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	connected := make(chan struct{}, 1)
	closed := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		connected <- struct{}{}
		if _, _, err = conn.ReadMessage(); err != nil {
			return
		}
		if err = conn.WriteMessage(websocket.TextMessage, []byte(xaiCompletedEvent)); err != nil {
			return
		}
		if _, _, err = conn.ReadMessage(); err != nil {
			closed <- struct{}{}
		}
	}))
	defer server.Close()

	exec := NewXAIAutoExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: "xai-ws-auth", Provider: "xai", Attributes: map[string]string{
		"base_url":   server.URL,
		"api_key":    "token",
		"websockets": "true",
	}}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: "xai-session-1",
		},
	}
	result, err := exec.ExecuteStream(ctx, auth, xaiStreamRequest(), opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	drainXAIStream(t, result)
	select {
	case <-connected:
	case <-time.After(2 * time.Second):
		t.Fatal("websocket was not connected")
	}
	exec.CloseExecutionSession("xai-session-1")
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream websocket was not closed")
	}
}

func TestXAIWebsocketSessionReconnectsWhenAuthChanges(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	connected := make(chan string, 2)
	closedA := make(chan struct{}, 1)
	requestsB := make(chan []byte, 1)
	newServer := func(label string, closed chan<- struct{}, requests chan<- []byte) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, errUpgrade := upgrader.Upgrade(w, r, nil)
			if errUpgrade != nil {
				return
			}
			defer conn.Close()
			connected <- label
			_, request, errRead := conn.ReadMessage()
			if errRead != nil {
				return
			}
			if requests != nil {
				requests <- request
			}
			if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(xaiCompletedEvent)); errWrite != nil {
				return
			}
			if _, _, errRead := conn.ReadMessage(); errRead != nil && closed != nil {
				closed <- struct{}{}
			}
		}))
	}
	serverA := newServer("a", closedA, nil)
	defer serverA.Close()
	serverB := newServer("b", nil, requestsB)
	defer serverB.Close()

	exec := NewXAIAutoExecutor(&config.Config{})
	t.Cleanup(func() { exec.CloseExecutionSession("xai-auth-switch") })
	newAuth := func(id, baseURL string) *cliproxyauth.Auth {
		return &cliproxyauth.Auth{ID: id, Provider: "xai", Attributes: map[string]string{
			"base_url":   baseURL,
			"api_key":    "token-" + id,
			"websockets": "true",
		}}
	}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: "xai-auth-switch",
		},
	}

	resultA, err := exec.ExecuteStream(ctx, newAuth("auth-a", serverA.URL), xaiStreamRequest(), opts)
	if err != nil {
		t.Fatalf("first ExecuteStream() error = %v", err)
	}
	drainXAIStream(t, resultA)
	state := getXAIWebsocketIDState(globalXAIWebsocketIDStates, "xai-auth-switch")
	state.mapDownstreamToUpstream("downstream-prev", "upstream-from-auth-a")
	requestB := xaiStreamRequest()
	requestB.Payload, _ = sjson.SetBytes(requestB.Payload, "previous_response_id", "downstream-prev")
	resultB, err := exec.ExecuteStream(ctx, newAuth("auth-b", serverB.URL), requestB, opts)
	if err != nil {
		t.Fatalf("second ExecuteStream() error = %v", err)
	}
	drainXAIStream(t, resultB)

	if first, second := <-connected, <-connected; first != "a" || second != "b" {
		t.Fatalf("connections = %q, %q; want a, b", first, second)
	}
	select {
	case <-closedA:
	case <-time.After(2 * time.Second):
		t.Fatal("old auth websocket was not closed")
	}
	select {
	case request := <-requestsB:
		if got := gjson.GetBytes(request, "previous_response_id").String(); got != "downstream-prev" {
			t.Fatalf("auth-b previous_response_id = %q; request=%s", got, request)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("auth-b websocket request was not captured")
	}
}

func TestXAIWebsocketActiveChannelIsBoundToReaderConnection(t *testing.T) {
	sess := &codexWebsocketSession{}
	connA := &websocket.Conn{}
	connB := &websocket.Conn{}
	chA := make(chan codexWebsocketRead, 1)
	chB := make(chan codexWebsocketRead, 1)

	sess.setActiveForConn(chA, connA)
	sess.setActiveForConn(chB, connB)
	if sess.clearActiveForConn(chA, connA) {
		t.Fatal("old reader cleared the new active channel")
	}
	if ch, _ := sess.activeForConn(connA); ch != nil {
		t.Fatal("old reader can still access the new active channel")
	}
	if ch, _ := sess.activeForConn(connB); ch != chB {
		t.Fatal("current reader lost its active channel")
	}
	if !sess.clearActiveForConn(chB, connB) {
		t.Fatal("current reader could not clear its active channel")
	}
}

func TestXAIWebsocketCompactionTriggerUsesHTTPCompact(t *testing.T) {
	path := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path <- r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"resp_compact","object":"response.compaction","output":[{"type":"compaction","encrypted_content":"opaque"}]}`))
	}))
	defer server.Close()

	exec := NewXAIAutoExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Provider: "xai", Attributes: map[string]string{
		"base_url":   server.URL,
		"api_key":    "token",
		"websockets": "true",
	}}
	req := cliproxyexecutor.Request{Model: "grok-4.3", Payload: []byte(`{"model":"grok-4.3","input":[{"role":"user","content":"hello"},{"type":"compaction_trigger"}]}`)}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	state := getXAIWebsocketIDState(globalXAIWebsocketIDStates, "compact-session")
	state.replaceTranscriptWithItems([]byte(`{"role":"user","content":"hello"}`))
	t.Cleanup(func() { deleteXAIWebsocketIDState(globalXAIWebsocketIDStates, "compact-session") })
	result, err := exec.ExecuteStream(ctx, auth, req, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: "compact-session",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	drainXAIStream(t, result)
	if got := <-path; got != "/responses/compact" {
		t.Fatalf("path = %q, want /responses/compact", got)
	}
}

func TestXAIWebsocketTerminalFailureEmitsError(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			return
		}
		defer conn.Close()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.failed","response":{"error":{"code":"server_error","message":"websocket failed"}}}`))
	}))
	defer server.Close()

	exec := NewXAIAutoExecutor(&config.Config{})
	t.Cleanup(func() { exec.CloseExecutionSession("xai-terminal-session") })
	auth := &cliproxyauth.Auth{ID: "xai-ws-terminal", Provider: "xai", Attributes: map[string]string{
		"base_url":   server.URL,
		"api_key":    "token",
		"websockets": "true",
	}}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	result, err := exec.ExecuteStream(ctx, auth, xaiStreamRequest(), cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: "xai-terminal-session",
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	var terminalErr error
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			terminalErr = chunk.Err
		}
	}
	if terminalErr == nil || !strings.Contains(terminalErr.Error(), "websocket failed") {
		t.Fatalf("websocket terminal error = %v", terminalErr)
	}
}

func TestXAIWebsocketResponseIDMappingAvoidsRepeatedDownstreamID(t *testing.T) {
	store := &xaiWebsocketIDStateStore{sessions: make(map[string]*xaiWebsocketIDState)}
	first := newXAIWebsocketRequestIDMapper(store, "session", []byte(`{"input":[]}`))
	firstPayload := first.downstreamResponsePayload([]byte(`{"type":"response.completed","response":{"id":"resp_same"}}`))
	if got := gjson.GetBytes(firstPayload, "response.id").String(); got != "resp_same" {
		t.Fatalf("first response id = %q", got)
	}

	second := newXAIWebsocketRequestIDMapper(store, "session", []byte(`{"previous_response_id":"resp_same","input":[]}`))
	secondPayload := second.downstreamResponsePayload([]byte(`{"type":"response.completed","response":{"id":"resp_same","previous_response_id":"resp_same"},"item_id":"item_resp_same"}`))
	secondID := gjson.GetBytes(secondPayload, "response.id").String()
	if secondID == "resp_same" || !strings.HasPrefix(secondID, "resp_same-xai-") {
		t.Fatalf("second response id = %q", secondID)
	}
	if got := gjson.GetBytes(secondPayload, "response.previous_response_id").String(); got != "resp_same" {
		t.Fatalf("downstream previous_response_id = %q", got)
	}
}
