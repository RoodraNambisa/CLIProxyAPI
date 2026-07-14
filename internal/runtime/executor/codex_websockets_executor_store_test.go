package executor

import (
	"testing"

	"github.com/gorilla/websocket"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type legacyCodexAuthSessionCloser interface {
	CloseAuthExecutionSessions(authID string, reason string)
}

type codexAuthInstanceSessionCloser interface {
	CloseAuthInstanceExecutionSessions(authID string, authInstanceID string, reason string)
}

var (
	_ legacyCodexAuthSessionCloser   = (*CodexWebsocketsExecutor)(nil)
	_ legacyCodexAuthSessionCloser   = (*CodexAutoExecutor)(nil)
	_ codexAuthInstanceSessionCloser = (*CodexWebsocketsExecutor)(nil)
	_ codexAuthInstanceSessionCloser = (*CodexAutoExecutor)(nil)
)

func TestCodexWebsocketOldConnectionCannotClearNewActiveChannel(t *testing.T) {
	sess := &codexWebsocketSession{}
	oldConn := &websocket.Conn{}
	newConn := &websocket.Conn{}
	oldCh := make(chan codexWebsocketRead)
	newCh := make(chan codexWebsocketRead)
	sess.setActiveForConn(oldCh, oldConn)
	sess.setActiveForConn(newCh, newConn)

	if sess.clearActiveForConn(oldCh, oldConn) {
		t.Fatal("old connection cleared the new active channel")
	}
	active, _ := sess.activeForConn(newConn)
	if active != newCh {
		t.Fatalf("active channel = %p, want %p", active, newCh)
	}
}

func TestCodexWebsocketsExecutor_SessionStoreSurvivesExecutorReplacement(t *testing.T) {
	sessionID := "test-session-store-survives-replace"

	globalCodexWebsocketSessionStore.mu.Lock()
	delete(globalCodexWebsocketSessionStore.sessions, sessionID)
	globalCodexWebsocketSessionStore.mu.Unlock()

	exec1 := NewCodexWebsocketsExecutor(nil)
	sess1 := exec1.getOrCreateSession(sessionID)
	if sess1 == nil {
		t.Fatalf("expected session to be created")
	}

	exec2 := NewCodexWebsocketsExecutor(nil)
	sess2 := exec2.getOrCreateSession(sessionID)
	if sess2 == nil {
		t.Fatalf("expected session to be available across executors")
	}
	if sess1 != sess2 {
		t.Fatalf("expected the same session instance across executors")
	}

	exec1.CloseExecutionSession(cliproxyauth.CloseAllExecutionSessionsID)

	globalCodexWebsocketSessionStore.mu.Lock()
	_, stillPresent := globalCodexWebsocketSessionStore.sessions[sessionID]
	globalCodexWebsocketSessionStore.mu.Unlock()
	if !stillPresent {
		t.Fatalf("expected session to remain after executor replacement close marker")
	}

	exec2.CloseExecutionSession(sessionID)

	globalCodexWebsocketSessionStore.mu.Lock()
	_, presentAfterClose := globalCodexWebsocketSessionStore.sessions[sessionID]
	globalCodexWebsocketSessionStore.mu.Unlock()
	if presentAfterClose {
		t.Fatalf("expected session to be removed after explicit close")
	}
}

func TestCloseCodexWebsocketSessionsMatchesPendingDial(t *testing.T) {
	const (
		sessionID = "test-codex-pending-dial-close"
		authID    = "pending-auth"
	)
	sess := &codexWebsocketSession{
		sessionID:            sessionID,
		pendingAuthID:        authID,
		dialGeneration:       1,
		upstreamDisconnectCh: make(chan error, 1),
	}
	globalCodexWebsocketSessionStore.mu.Lock()
	globalCodexWebsocketSessionStore.sessions[sessionID] = sess
	globalCodexWebsocketSessionStore.mu.Unlock()
	t.Cleanup(func() {
		globalCodexWebsocketSessionStore.mu.Lock()
		delete(globalCodexWebsocketSessionStore.sessions, sessionID)
		globalCodexWebsocketSessionStore.mu.Unlock()
	})

	CloseCodexWebsocketSessionsForAuthID(authID, "auth_replaced")

	globalCodexWebsocketSessionStore.mu.Lock()
	_, present := globalCodexWebsocketSessionStore.sessions[sessionID]
	globalCodexWebsocketSessionStore.mu.Unlock()
	if present {
		t.Fatal("pending auth session remained in global store")
	}
	sess.connMu.Lock()
	terminated := sess.terminated
	generation := sess.dialGeneration
	pending := sess.pendingAuthID
	sess.connMu.Unlock()
	if !terminated || generation != 2 || pending != "" {
		t.Fatalf("closed pending session state = terminated:%t generation:%d pending:%q", terminated, generation, pending)
	}
}

func TestCloseCodexWebsocketSessionsScopesRuntimeInstance(t *testing.T) {
	store := &codexWebsocketSessionStore{sessions: map[string]*codexWebsocketSession{}}
	oldActive := &codexWebsocketSession{sessionID: "old-active", authID: "shared-auth", authInstanceID: "old"}
	oldPending := &codexWebsocketSession{sessionID: "old-pending", pendingAuthID: "shared-auth", pendingAuthInstanceID: "old", dialGeneration: 1}
	newActive := &codexWebsocketSession{sessionID: "new-active", authID: "shared-auth", authInstanceID: "new"}
	store.sessions[oldActive.sessionID] = oldActive
	store.sessions[oldPending.sessionID] = oldPending
	store.sessions[newActive.sessionID] = newActive

	closeCodexWebsocketSessionsForAuth(store, "shared-auth", "old", "auth_replaced")

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

func TestCodexWebsocketsExecutorAuthSessionCloseScopes(t *testing.T) {
	store := &codexWebsocketSessionStore{sessions: map[string]*codexWebsocketSession{}}
	oldInstance := &codexWebsocketSession{sessionID: "old", authID: "shared-auth", authInstanceID: "old"}
	newInstance := &codexWebsocketSession{sessionID: "new", authID: "shared-auth", authInstanceID: "new"}
	otherAuth := &codexWebsocketSession{sessionID: "other", authID: "other-auth", authInstanceID: "only"}
	store.sessions[oldInstance.sessionID] = oldInstance
	store.sessions[newInstance.sessionID] = newInstance
	store.sessions[otherAuth.sessionID] = otherAuth
	exec := NewCodexWebsocketsExecutor(nil)
	exec.store = store

	exec.CloseAuthInstanceExecutionSessions("shared-auth", "old", "auth_replaced")
	store.mu.Lock()
	_, hasOld := store.sessions[oldInstance.sessionID]
	gotNew := store.sessions[newInstance.sessionID]
	gotOther := store.sessions[otherAuth.sessionID]
	store.mu.Unlock()
	if hasOld || gotNew != newInstance || gotOther != otherAuth {
		t.Fatalf("scoped close state = old:%t new:%p other:%p", hasOld, gotNew, gotOther)
	}

	exec.CloseAuthExecutionSessions("shared-auth", "auth_removed")
	store.mu.Lock()
	_, hasNew := store.sessions[newInstance.sessionID]
	gotOther = store.sessions[otherAuth.sessionID]
	store.mu.Unlock()
	if hasNew || gotOther != otherAuth {
		t.Fatalf("legacy close state = new:%t other:%p", hasNew, gotOther)
	}
}
