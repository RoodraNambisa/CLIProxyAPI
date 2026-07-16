package executor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
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

func TestXAIWebsocketResponseDoneRestoresOutput(t *testing.T) {
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
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.output_item.done","output_index":0,"item":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.done","response":{"id":"resp_done","status":"completed","output":[]}}`))
	}))
	defer server.Close()

	exec := NewXAIWebsocketsExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: "xai-done", Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "token"}}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(t.Context())
	result, errExecute := exec.executeStream(ctx, auth, xaiStreamRequest(), cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Stream: true})
	if errExecute != nil {
		t.Fatalf("executeStream() error = %v", errExecute)
	}
	var completed []byte
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream error = %v", chunk.Err)
		}
		if gjson.GetBytes(chunk.Payload, "type").String() == "response.completed" {
			completed = chunk.Payload
		}
	}
	if got := gjson.GetBytes(completed, "response.output.0.content.0.text").String(); got != "hello" {
		t.Fatalf("completed output text = %q; payload=%s", got, completed)
	}
}

type retiredAuthInstanceState struct{}

func (retiredAuthInstanceState) Retired() bool { return true }

type xaiObservedDoneContext struct {
	context.Context
	once     sync.Once
	observed chan struct{}
}

func (c *xaiObservedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func TestXAIWebsocketConversationIdentityIgnoresProxyRebind(t *testing.T) {
	sess := &codexWebsocketSession{
		authID:         "auth-1",
		authInstanceID: "instance-1",
		proxyBindingID: "binding-1",
		proxyIdentity:  "proxy-hash-1",
		wsURL:          "wss://example.test/responses",
	}

	if xaiWebsocketConversationIdentityChanged(sess, "auth-1", "instance-1", "wss://example.test/responses") {
		t.Fatal("unchanged conversation identity was treated as new")
	}
	sess.proxyBindingID = "binding-2"
	sess.proxyIdentity = "proxy-hash-2"
	if xaiWebsocketConversationIdentityChanged(sess, "auth-1", "instance-1", "wss://example.test/responses") {
		t.Fatal("proxy rebind changed logical conversation identity")
	}
	if !xaiWebsocketConversationIdentityChanged(sess, "auth-2", "instance-1", "wss://example.test/responses") {
		t.Fatal("auth change did not invalidate conversation identity")
	}
}

func TestXAIWebsocketProxyRebindReconnectsWithoutDroppingConversationState(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	connections := make(chan struct{}, 2)
	firstConnectionClosed := make(chan struct{})
	var connectionCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			return
		}
		connectionNumber := connectionCount.Add(1)
		defer func() {
			_ = conn.Close()
			if connectionNumber == 1 {
				close(firstConnectionClosed)
			}
		}()
		connections <- struct{}{}
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
		}
	}))
	defer server.Close()

	store := &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	idStore := &xaiWebsocketIDStateStore{sessions: make(map[string]*xaiWebsocketIDState)}
	exec := NewXAIWebsocketsExecutor(&config.Config{})
	exec.store = store
	exec.idStore = idStore

	const sessionID = "proxy-rebind-session"
	sess := exec.getOrCreateSession(sessionID)
	auth := &cliproxyauth.Auth{
		ID:                    "auth-1",
		Provider:              "xai",
		RuntimeProxyURL:       "direct",
		RuntimeProxyBindingID: "binding-1",
	}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	connFirst, _, errDial := exec.ensureUpstreamConn(t.Context(), auth, sess, auth.ID, wsURL, http.Header{})
	if errDial != nil {
		t.Fatalf("first websocket dial: %v", errDial)
	}
	if connFirst == nil {
		t.Fatal("first websocket connection is nil")
	}
	select {
	case <-connections:
	case <-time.After(5 * time.Second):
		t.Fatal("first websocket connection was not observed")
	}

	state := getXAIWebsocketIDState(idStore, sessionID)
	state.mapDownstreamToUpstream("downstream-prev", "upstream-prev")
	state.replaceTranscriptWithItems([]byte(`{"type":"compaction","encrypted_content":"opaque"}`))

	auth.RuntimeProxyBindingID = "binding-2"
	if xaiWebsocketConversationIdentityChanged(sess, auth.ID, auth.RuntimeInstanceID(), wsURL) {
		deleteXAIWebsocketIDStateForSession(store, idStore, sessionID, sessionID, sess)
	}
	connSecond, _, errDial := exec.ensureUpstreamConn(t.Context(), auth, sess, auth.ID, wsURL, http.Header{})
	if errDial != nil {
		t.Fatalf("second websocket dial after proxy rebind: %v", errDial)
	}
	if connSecond == nil || connSecond == connFirst {
		t.Fatalf("second websocket connection = %p, first = %p; want replacement", connSecond, connFirst)
	}
	select {
	case <-connections:
	case <-time.After(5 * time.Second):
		t.Fatal("replacement websocket connection was not observed")
	}
	select {
	case <-firstConnectionClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("first websocket connection remained open after proxy rebind")
	}

	mapper := newXAIWebsocketRequestIDMapper(idStore, sessionID, []byte(`{"previous_response_id":"downstream-prev"}`))
	if mapper == nil {
		t.Fatal("request ID mapper was removed by proxy rebind")
	}
	if mapper.upstreamPreviousID != "upstream-prev" {
		t.Fatalf("upstream previous response ID = %q, want upstream-prev", mapper.upstreamPreviousID)
	}
	if got := string(state.snapshotTranscriptInput()); got != `[{"type":"compaction","encrypted_content":"opaque"}]` {
		t.Fatalf("transcript after proxy rebind = %s", got)
	}

	exec.CloseExecutionSession(sessionID)
}

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

func TestEnqueueXAIWebsocketTerminalChunkPreservesQueuedPayloads(t *testing.T) {
	out := make(chan cliproxyexecutor.StreamChunk, 2)
	out <- cliproxyexecutor.StreamChunk{Payload: []byte("first")}
	out <- cliproxyexecutor.StreamChunk{Payload: []byte("second")}
	sent := make(chan bool, 1)
	go func() {
		sent <- enqueueXAIWebsocketTerminalChunk(t.Context(), out, cliproxyexecutor.StreamChunk{Err: errXAIWebsocketSessionTerminated})
	}()

	select {
	case <-sent:
		t.Fatal("terminal chunk bypassed a full output queue")
	case <-time.After(20 * time.Millisecond):
	}
	if got := string((<-out).Payload); got != "first" {
		t.Fatalf("first payload = %q", got)
	}
	if got := string((<-out).Payload); got != "second" {
		t.Fatalf("second payload = %q", got)
	}
	terminal := <-out
	if !errors.Is(terminal.Err, errXAIWebsocketSessionTerminated) {
		t.Fatalf("terminal error = %v", terminal.Err)
	}
	if ok := <-sent; !ok {
		t.Fatal("terminal chunk was not sent")
	}
}

func TestEnqueueXAIWebsocketTerminalChunkCanBeCanceled(t *testing.T) {
	out := make(chan cliproxyexecutor.StreamChunk, 1)
	out <- cliproxyexecutor.StreamChunk{Payload: []byte("queued")}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if enqueueXAIWebsocketTerminalChunk(ctx, out, cliproxyexecutor.StreamChunk{Err: errXAIWebsocketSessionTerminated}) {
		t.Fatal("terminal chunk was sent after request cancellation")
	}
	if got := string((<-out).Payload); got != "queued" {
		t.Fatalf("queued payload = %q", got)
	}
	select {
	case chunk := <-out:
		t.Fatalf("unexpected extra chunk: %#v", chunk)
	default:
	}
}

func TestRelayXAIWebsocketStreamPrefersQueuedMarkerOnRuntimeRetirement(t *testing.T) {
	requestCtx := &xaiObservedDoneContext{Context: t.Context(), observed: make(chan struct{})}
	runtimeCtx, retire := context.WithCancel(t.Context())
	in := make(chan cliproxyexecutor.StreamChunk, 2)
	in <- cliproxyexecutor.StreamChunk{Payload: []byte("completed")}
	in <- cliproxyexecutor.SuccessfulStreamTerminalChunk()
	close(in)

	out := make(chan cliproxyexecutor.StreamChunk, 1)
	out <- cliproxyexecutor.StreamChunk{Payload: []byte("block-relay")}
	done := make(chan struct{})
	go func() {
		relayXAIWebsocketStream(requestCtx, runtimeCtx, nil, in, out, func() bool { return runtimeCtx.Err() != nil })
		close(done)
	}()

	select {
	case <-requestCtx.observed:
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not block while forwarding the completion payload")
	}
	retire()
	if got := string((<-out).Payload); got != "block-relay" {
		t.Fatalf("blocking payload = %q", got)
	}
	if got := string((<-out).Payload); got != "completed" {
		t.Fatalf("completion payload = %q", got)
	}
	marker, ok := <-out
	if !ok || !cliproxyexecutor.IsSuccessfulStreamTerminalChunk(marker) {
		t.Fatalf("terminal marker = (%#v, %t)", marker, ok)
	}
	if chunk, okExtra := <-out; okExtra {
		t.Fatalf("unexpected chunk after terminal marker: %#v", chunk)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not finish after forwarding the queued marker")
	}
}

func TestXAIWebsocketCanceledMarkerReleasesSessionForNextRequest(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	requests := make(chan int, 2)
	serverErrors := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			serverErrors <- errUpgrade
			return
		}
		defer func() { _ = conn.Close() }()
		for requestNumber := 1; requestNumber <= 2; requestNumber++ {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				serverErrors <- errRead
				return
			}
			requests <- requestNumber
			if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(xaiCompletedEvent)); errWrite != nil {
				serverErrors <- errWrite
				return
			}
		}
	}))
	defer server.Close()

	const sessionID = "xai-canceled-marker-session"
	exec := NewXAIWebsocketsExecutor(&config.Config{})
	t.Cleanup(func() { exec.CloseExecutionSession(sessionID) })
	auth := &cliproxyauth.Auth{ID: "xai-canceled-marker-auth", Provider: "xai", Attributes: map[string]string{
		"base_url": server.URL,
		"api_key":  "token",
	}}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Stream:       true,
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey:     sessionID,
			cliproxyexecutor.StreamTerminalMarkerMetadataKey: true,
		},
	}

	firstBaseCtx, cancelFirst := context.WithCancel(t.Context())
	firstCtx := cliproxyexecutor.WithDownstreamWebsocket(firstBaseCtx)
	first, errFirst := exec.executeStream(firstCtx, auth, xaiStreamRequest(), opts)
	if errFirst != nil {
		t.Fatalf("first executeStream() error = %v", errFirst)
	}
	select {
	case chunk := <-first.Chunks:
		if chunk.Err != nil || cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) || !strings.Contains(string(chunk.Payload), `"type":"response.completed"`) {
			t.Fatalf("first completion chunk = %#v", chunk)
		}
	case errServer := <-serverErrors:
		t.Fatalf("websocket server error: %v", errServer)
	case <-time.After(5 * time.Second):
		t.Fatal("first completion payload was not received")
	}
	cancelFirst()

	type streamCallResult struct {
		result *cliproxyexecutor.StreamResult
		err    error
	}
	secondCall := make(chan streamCallResult, 1)
	secondCtx := cliproxyexecutor.WithDownstreamWebsocket(t.Context())
	go func() {
		result, errStream := exec.executeStream(secondCtx, auth, xaiStreamRequest(), opts)
		secondCall <- streamCallResult{result: result, err: errStream}
	}()

	var second *cliproxyexecutor.StreamResult
	select {
	case call := <-secondCall:
		if call.err != nil {
			t.Fatalf("second executeStream() error = %v", call.err)
		}
		second = call.result
	case errServer := <-serverErrors:
		t.Fatalf("websocket server error: %v", errServer)
	case <-time.After(5 * time.Second):
		t.Fatal("second same-session request remained blocked after marker cancellation")
	}

	terminalCount := 0
	drainDeadline := time.After(5 * time.Second)
	for {
		select {
		case chunk, ok := <-second.Chunks:
			if !ok {
				if terminalCount != 1 {
					t.Fatalf("second terminal marker count = %d, want 1", terminalCount)
				}
				goto drained
			}
			if chunk.Err != nil {
				t.Fatalf("second stream chunk error = %v", chunk.Err)
			}
			if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
				terminalCount++
			}
		case errServer := <-serverErrors:
			t.Fatalf("websocket server error: %v", errServer)
		case <-drainDeadline:
			t.Fatal("second same-session stream did not finish")
		}
	}

drained:
	for want := 1; want <= 2; want++ {
		select {
		case got := <-requests:
			if got != want {
				t.Fatalf("request order = %d, want %d", got, want)
			}
		default:
			t.Fatalf("websocket request %d was not observed", want)
		}
	}
}

func TestDetachXAIWebsocketSessionForAuthRechecksCurrentAuth(t *testing.T) {
	store := &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	const sessionID = "switched-auth-session"
	sess := &codexWebsocketSession{sessionID: sessionID, authID: "auth-b"}
	store.sessions[sessionID] = sess
	if detached, ok := detachXAIWebsocketSessionForAuth(store, nil, sessionID, sess, "auth-a", ""); ok {
		t.Fatalf("detached switched session: %#v", detached)
	}
	store.mu.Lock()
	current := store.sessions[sessionID]
	store.mu.Unlock()
	if current != sess {
		t.Fatal("session switched to another auth was removed")
	}
}

func TestCloseXAIWebsocketSessionsScopesRuntimeInstance(t *testing.T) {
	store := &codexWebsocketSessionStore{sessions: map[string]*codexWebsocketSession{}}
	idStore := &xaiWebsocketIDStateStore{sessions: map[string]*xaiWebsocketIDState{}}
	oldActive := &codexWebsocketSession{sessionID: "xai-old-active", authID: "shared-auth", authInstanceID: "old"}
	oldPending := &codexWebsocketSession{sessionID: "xai-old-pending", pendingAuthID: "shared-auth", pendingAuthInstanceID: "old", dialGeneration: 1}
	newActive := &codexWebsocketSession{sessionID: "xai-new-active", authID: "shared-auth", authInstanceID: "new"}
	store.sessions[oldActive.sessionID] = oldActive
	store.sessions[oldPending.sessionID] = oldPending
	store.sessions[newActive.sessionID] = newActive

	closeXAIWebsocketSessionsForAuth(store, idStore, "shared-auth", "old", "auth_replaced")

	store.mu.Lock()
	_, hasOldActive := store.sessions[oldActive.sessionID]
	_, hasOldPending := store.sessions[oldPending.sessionID]
	gotNew := store.sessions[newActive.sessionID]
	store.mu.Unlock()
	if hasOldActive || hasOldPending || gotNew != newActive {
		t.Fatalf("scoped store state = old-active:%t old-pending:%t new:%p", hasOldActive, hasOldPending, gotNew)
	}
	newActive.connMu.Lock()
	newTerminated := newActive.terminated
	newActive.connMu.Unlock()
	if newTerminated {
		t.Fatal("new runtime instance session was terminated")
	}
}

func TestDetachXAIWebsocketSessionAndIDStateIsAtomic(t *testing.T) {
	const sessionID = "replacement-session"
	store := &codexWebsocketSessionStore{sessions: map[string]*codexWebsocketSession{}}
	idStore := &xaiWebsocketIDStateStore{sessions: map[string]*xaiWebsocketIDState{}}
	oldSession := &codexWebsocketSession{sessionID: sessionID, authID: "auth-a", authInstanceID: "old"}
	oldState := &xaiWebsocketIDState{downstreamToUpstream: map[string]string{"old": "old-upstream"}}
	store.sessions[sessionID] = oldSession
	idStore.sessions[sessionID] = oldState

	idStore.mu.Lock()
	idStoreLocked := true
	t.Cleanup(func() {
		if idStoreLocked {
			idStore.mu.Unlock()
		}
	})
	type detachResult struct {
		detached detachedXAIWebsocketSession
		ok       bool
	}
	detached := make(chan detachResult, 1)
	go func() {
		result, ok := detachXAIWebsocketSessionForAuth(store, idStore, sessionID, oldSession, "auth-a", "old")
		detached <- detachResult{detached: result, ok: ok}
	}()

	deadline := time.Now().Add(2 * time.Second)
	for store.mu.TryLock() {
		store.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("detach did not acquire the session store lock")
		}
		time.Sleep(time.Millisecond)
	}

	type replacementResult struct {
		session *codexWebsocketSession
		state   *xaiWebsocketIDState
	}
	replacement := make(chan replacementResult, 1)
	exec := &XAIWebsocketsExecutor{store: store, idStore: idStore}
	go func() {
		session := exec.getOrCreateSession(sessionID)
		state := getXAIWebsocketIDState(idStore, sessionID)
		state.mapDownstreamToUpstream("new", "new-upstream")
		state.replaceTranscriptWithItems([]byte(`{"type":"compaction","encrypted_content":"new"}`))
		replacement <- replacementResult{session: session, state: state}
	}()

	select {
	case <-replacement:
		t.Fatal("replacement session was installed before old state deletion completed")
	default:
	}
	idStore.mu.Unlock()
	idStoreLocked = false

	result := <-detached
	if !result.ok || result.detached.sessionID != sessionID {
		t.Fatalf("detach result = %#v", result)
	}
	newValue := <-replacement
	if newValue.session == oldSession || newValue.state == oldState {
		t.Fatal("replacement reused old session or ID state")
	}
	idStore.mu.Lock()
	currentState := idStore.sessions[sessionID]
	idStore.mu.Unlock()
	if currentState != newValue.state {
		t.Fatal("old cleanup removed the replacement ID state")
	}
	if got := newValue.state.upstreamIDForDownstream("new"); got != "new-upstream" {
		t.Fatalf("replacement ID mapping = %q", got)
	}
	if got := string(newValue.state.snapshotTranscriptInput()); !strings.Contains(got, `"encrypted_content":"new"`) {
		t.Fatalf("replacement transcript = %s", got)
	}
}

func TestStaleXAIWebsocketSessionCannotDeleteReplacementIDState(t *testing.T) {
	const sessionID = "stale-session"
	oldSession := &codexWebsocketSession{sessionID: sessionID}
	newSession := &codexWebsocketSession{sessionID: sessionID}
	newState := &xaiWebsocketIDState{downstreamToUpstream: map[string]string{"new": "new-upstream"}}
	store := &codexWebsocketSessionStore{sessions: map[string]*codexWebsocketSession{sessionID: newSession}}
	idStore := &xaiWebsocketIDStateStore{sessions: map[string]*xaiWebsocketIDState{sessionID: newState}}

	deleteXAIWebsocketIDStateForSession(store, idStore, sessionID, sessionID, oldSession)

	idStore.mu.Lock()
	current := idStore.sessions[sessionID]
	idStore.mu.Unlock()
	if current != newState {
		t.Fatal("stale session deleted replacement ID state")
	}
}

func TestXAIWebsocketAuthSessionClosersSupportLegacyAndScopedMethods(t *testing.T) {
	store := &codexWebsocketSessionStore{sessions: map[string]*codexWebsocketSession{}}
	idStore := &xaiWebsocketIDStateStore{sessions: map[string]*xaiWebsocketIDState{}}
	oldSession := &codexWebsocketSession{sessionID: "old", authID: "shared", authInstanceID: "old"}
	newSession := &codexWebsocketSession{sessionID: "new", authID: "shared", authInstanceID: "new"}
	store.sessions[oldSession.sessionID] = oldSession
	store.sessions[newSession.sessionID] = newSession
	idStore.sessions[oldSession.sessionID] = &xaiWebsocketIDState{}
	idStore.sessions[newSession.sessionID] = &xaiWebsocketIDState{}
	exec := &XAIWebsocketsExecutor{store: store, idStore: idStore}

	exec.CloseAuthInstanceExecutionSessions("shared", "old", "auth_replaced")
	store.mu.Lock()
	_, oldExists := store.sessions[oldSession.sessionID]
	currentNew := store.sessions[newSession.sessionID]
	store.mu.Unlock()
	if oldExists || currentNew != newSession {
		t.Fatalf("scoped close state = old:%t new:%p", oldExists, currentNew)
	}

	exec.CloseAuthExecutionSessions("shared", "auth_removed")
	store.mu.Lock()
	remaining := len(store.sessions)
	store.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("legacy close left %d sessions", remaining)
	}
}

func TestXAIWebsocketDialCannotReinstallTerminatedSession(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	dialStarted := make(chan struct{})
	releaseDial := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(dialStarted)
		<-releaseDial
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()

	exec := NewXAIWebsocketsExecutor(&config.Config{})
	store := &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	const sessionID = "terminated-during-dial"
	sess := &codexWebsocketSession{sessionID: sessionID}
	store.sessions[sessionID] = sess
	type dialResult struct {
		conn *websocket.Conn
		err  error
	}
	dialDone := make(chan dialResult, 1)
	go func() {
		conn, _, errDial := exec.ensureUpstreamConn(t.Context(), nil, sess, "auth-a", "ws"+strings.TrimPrefix(server.URL, "http"), http.Header{})
		dialDone <- dialResult{conn: conn, err: errDial}
	}()
	select {
	case <-dialStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("websocket dial did not start")
	}
	if _, ok := detachXAIWebsocketSessionForAuth(store, nil, sessionID, sess, "auth-a", ""); !ok {
		t.Fatal("failed to terminate session during dial")
	}
	close(releaseDial)
	select {
	case result := <-dialDone:
		if result.conn != nil || !errors.Is(result.err, errXAIWebsocketSessionTerminated) {
			t.Fatalf("dial result = (%v, %v), want terminated error", result.conn, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("websocket dial did not finish")
	}
}

func TestXAIWebsocketRetiredAuthInstanceCannotDial(t *testing.T) {
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer server.Close()

	const (
		authID     = "retired-instance-auth"
		instanceID = "retired-instance"
	)
	exec := NewXAIWebsocketsExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{ID: authID, Provider: "xai", Attributes: map[string]string{
		"base_url": server.URL,
		"api_key":  "token",
	}}
	for _, req := range []cliproxyexecutor.Request{
		xaiStreamRequest(),
		{Model: "grok-4.3", Payload: []byte(`{"input":[{"type":"compaction_trigger"}]}`)},
	} {
		_, errStream := exec.ExecuteStream(t.Context(), auth, req, cliproxyexecutor.Options{
			SourceFormat: sdktranslator.FormatOpenAIResponse,
			Stream:       true,
			Metadata: map[string]any{
				cliproxyexecutor.SelectedAuthInstanceMetadataKey:           instanceID,
				cliproxyexecutor.SelectedAuthInstanceRetirementMetadataKey: retiredAuthInstanceState{},
			},
		})
		if !errors.Is(errStream, errXAIWebsocketSessionTerminated) {
			t.Fatalf("ExecuteStream() error = %v, want terminated", errStream)
		}
	}
	if got := requestCount.Load(); got != 0 {
		t.Fatalf("retired auth instance sent %d upstream requests", got)
	}
}

func TestXAIHTTPRetiredAuthInstanceCannotRequest(t *testing.T) {
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer server.Close()

	auth := &cliproxyauth.Auth{ID: "retired-http-auth", Provider: "xai", Attributes: map[string]string{
		"base_url": server.URL,
		"api_key":  "token",
	}}
	opts := cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatOpenAIResponse,
		Metadata: map[string]any{
			cliproxyexecutor.SelectedAuthInstanceRetirementMetadataKey: retiredAuthInstanceState{},
		},
	}
	exec := NewXAIExecutor(&config.Config{})
	for _, test := range []struct {
		name string
		run  func() error
	}{
		{name: "execute", run: func() error {
			_, err := exec.Execute(t.Context(), auth, xaiStreamRequest(), opts)
			return err
		}},
		{name: "compact", run: func() error {
			compactOpts := opts
			compactOpts.Alt = "responses/compact"
			_, err := exec.Execute(t.Context(), auth, xaiStreamRequest(), compactOpts)
			return err
		}},
		{name: "stream", run: func() error {
			_, err := exec.ExecuteStream(t.Context(), auth, xaiStreamRequest(), opts)
			return err
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); !errors.Is(err, errXAIWebsocketSessionTerminated) {
				t.Fatalf("error = %v, want terminated", err)
			}
		})
	}
	httpReq, errRequest := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL, nil)
	if errRequest != nil {
		t.Fatalf("create request: %v", errRequest)
	}
	if _, errRequest = executeXAIHTTPRequest(server.Client(), httpReq, auth, opts); !errors.Is(errRequest, errXAIWebsocketSessionTerminated) {
		t.Fatalf("executeXAIHTTPRequest() error = %v, want terminated", errRequest)
	}
	if got := requestCount.Load(); got != 0 {
		t.Fatalf("retired auth instance sent %d HTTP requests", got)
	}
}

func TestXAIHTTPRetiredManagerCloneCannotRequest(t *testing.T) {
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer server.Close()

	const authID = "retired-direct-http-auth"
	manager := cliproxyauth.NewManager(nil, nil, nil)
	oldAuth, errRegister := manager.Register(t.Context(), &cliproxyauth.Auth{
		ID:       authID,
		Provider: "xai",
		Attributes: map[string]string{
			"base_url": server.URL,
			"api_key":  "old-token",
		},
		Metadata: map[string]any{"type": "xai"},
	})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if oldAuth == nil {
		t.Fatal("old auth is missing")
	}
	if _, errUpdate := manager.Update(t.Context(), &cliproxyauth.Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	httpReq, errRequest := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL, nil)
	if errRequest != nil {
		t.Fatalf("create request: %v", errRequest)
	}
	exec := NewXAIExecutor(&config.Config{})
	if _, errRequest = exec.HttpRequest(t.Context(), oldAuth, httpReq); !errors.Is(errRequest, errXAIWebsocketSessionTerminated) {
		t.Fatalf("HttpRequest() error = %v, want terminated", errRequest)
	}
	if got := requestCount.Load(); got != 0 {
		t.Fatalf("retired auth clone sent %d direct HTTP requests", got)
	}
}

func TestXAIWebsocketRetiredManagerCloneCannotDial(t *testing.T) {
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer server.Close()

	const authID = "retired-websocket-clone"
	manager := cliproxyauth.NewManager(nil, nil, nil)
	oldAuth, errRegister := manager.Register(t.Context(), &cliproxyauth.Auth{ID: authID, Provider: "xai", Metadata: map[string]any{"type": "xai"}})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	if _, errUpdate := manager.Update(t.Context(), &cliproxyauth.Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	exec := NewXAIWebsocketsExecutor(&config.Config{})
	sess := &codexWebsocketSession{sessionID: "retired-websocket-clone-session"}
	conn, resp, errDial := exec.ensureUpstreamConn(t.Context(), oldAuth, sess, authID, "ws"+strings.TrimPrefix(server.URL, "http"), http.Header{})
	if conn != nil || resp != nil || !errors.Is(errDial, errXAIWebsocketSessionTerminated) {
		t.Fatalf("retired clone dial = (%v, %v, %v), want terminated", conn, resp, errDial)
	}
	if got := requestCount.Load(); got != 0 {
		t.Fatalf("retired auth clone made %d websocket requests", got)
	}
}

func TestXAIWebsocketAuthRetirementIsManagerScoped(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()

	const authID = "shared-auth-id"
	execA := NewXAIWebsocketsExecutor(&config.Config{})
	managerA := cliproxyauth.NewManager(nil, nil, nil)
	managerA.RegisterExecutor(execA)
	if _, errRegister := managerA.Register(t.Context(), &cliproxyauth.Auth{ID: authID, Provider: "xai", Metadata: map[string]any{"type": "xai"}}); errRegister != nil {
		t.Fatalf("register manager A auth: %v", errRegister)
	}
	managerB := cliproxyauth.NewManager(nil, nil, nil)
	authB, errRegister := managerB.Register(t.Context(), &cliproxyauth.Auth{ID: authID, Provider: "xai", Metadata: map[string]any{"type": "xai"}})
	if errRegister != nil {
		t.Fatalf("register manager B auth: %v", errRegister)
	}
	if _, errUpdate := managerA.Update(t.Context(), &cliproxyauth.Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errUpdate != nil {
		t.Fatalf("replace manager A auth: %v", errUpdate)
	}
	if authB.RuntimeInstanceRetired() {
		t.Fatal("manager A retirement affected manager B auth instance")
	}
	execB := NewXAIWebsocketsExecutor(&config.Config{})
	conn, _, errDial := execB.ensureUpstreamConn(t.Context(), authB, nil, authID, "ws"+strings.TrimPrefix(server.URL, "http"), http.Header{})
	if errDial != nil {
		t.Fatalf("manager B websocket dial failed after manager A retirement: %v", errDial)
	}
	if conn == nil {
		t.Fatal("manager B websocket connection is nil")
	}
	if errClose := conn.Close(); errClose != nil {
		t.Fatalf("close websocket: %v", errClose)
	}
}

func TestXAIWebsocketRetirementClosesConnectionWithoutSessionID(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	requestReceived := make(chan struct{})
	connectionClosed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			return
		}
		defer conn.Close()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			return
		}
		close(requestReceived)
		_, _, _ = conn.ReadMessage()
		close(connectionClosed)
	}))
	defer server.Close()

	const authID = "retired-sessionless-websocket"
	manager := cliproxyauth.NewManager(nil, nil, nil)
	selected, errRegister := manager.Register(t.Context(), &cliproxyauth.Auth{
		ID:       authID,
		Provider: "xai",
		Attributes: map[string]string{
			"base_url": server.URL,
			"api_key":  "token",
		},
		Metadata: map[string]any{"type": "xai"},
	})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	exec := NewXAIWebsocketsExecutor(&config.Config{})
	result, errStream := exec.ExecuteStream(t.Context(), selected, xaiStreamRequest(), cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Stream: true})
	if errStream != nil {
		t.Fatalf("ExecuteStream() error = %v", errStream)
	}
	select {
	case <-requestReceived:
	case <-time.After(5 * time.Second):
		t.Fatal("websocket request was not received")
	}
	if _, errUpdate := manager.Update(t.Context(), &cliproxyauth.Auth{ID: authID, Provider: "codex", Metadata: map[string]any{"type": "codex"}}); errUpdate != nil {
		t.Fatalf("Update() error = %v", errUpdate)
	}
	select {
	case chunk, ok := <-result.Chunks:
		if !ok || !errors.Is(chunk.Err, errXAIWebsocketSessionTerminated) {
			t.Fatalf("retired stream chunk = (%#v, %t), want terminated error", chunk, ok)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("retired websocket stream did not terminate")
	}
	select {
	case <-connectionClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("retirement did not close sessionless websocket connection")
	}
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
			cliproxyexecutor.ExecutionSessionMetadataKey:     "compact-session",
			cliproxyexecutor.StreamTerminalMarkerMetadataKey: true,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	terminalCount := 0
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error = %v", chunk.Err)
		}
		if cliproxyexecutor.IsSuccessfulStreamTerminalChunk(chunk) {
			terminalCount++
		}
	}
	if terminalCount != 1 {
		t.Fatalf("terminal chunk count = %d, want 1", terminalCount)
	}
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

func TestXAIInvalidateUpstreamConnectionPreservesInstalledIdentity(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			return
		}
		defer conn.Close()
		_, _, _ = conn.ReadMessage()
	}))
	defer server.Close()
	conn, _, errDial := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if errDial != nil {
		t.Fatalf("dial websocket: %v", errDial)
	}

	exec := NewXAIWebsocketsExecutor(&config.Config{})
	sess := &codexWebsocketSession{
		sessionID:      "xai-invalidate",
		conn:           conn,
		readerConn:     conn,
		authID:         "auth-1",
		authInstanceID: "instance-1",
		wsURL:          "ws://example.test/ws",
	}
	exec.invalidateUpstreamConnWithoutDisconnectNotify(sess, conn, "test", nil)

	sess.connMu.Lock()
	defer sess.connMu.Unlock()
	if sess.conn != nil || sess.readerConn != nil {
		t.Fatal("invalidated session retained websocket connection")
	}
	if sess.authID != "auth-1" || sess.authInstanceID != "instance-1" || sess.wsURL != "ws://example.test/ws" {
		t.Fatalf("installed identity = %q/%q/%q, want preserved", sess.authID, sess.authInstanceID, sess.wsURL)
	}
}
