package executor

import (
	"sync"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
)

func TestCodexMultiAgentConnectionPolicyRejectsStaleConnectionUpdates(t *testing.T) {
	first, second := &websocket.Conn{}, &websocket.Conn{}
	session := &codexWebsocketSession{conn: first}
	prepared := helps.CodexMultiAgentResponsePolicy{NamespaceOptimized: true, PlaintextCalls: true}
	session.commitMultiAgentResponseForConn(first, prepared)
	if session.multiAgentResponseForConn(first) != prepared {
		t.Fatal("current connection lost committed preparation")
	}
	if session.multiAgentResponseForConn(second) != (helps.CodexMultiAgentResponsePolicy{}) {
		t.Fatal("another connection inherited prepared tools")
	}
	session.connMu.Lock()
	session.conn = second
	session.multiAgentResponse = helps.CodexMultiAgentResponsePolicy{}
	session.connMu.Unlock()
	session.commitMultiAgentResponseForConn(first, prepared)
	if session.multiAgentResponseForConn(second) != (helps.CodexMultiAgentResponsePolicy{}) {
		t.Fatal("late result overwrote a replacement connection")
	}
	session.connMu.Lock()
	session.terminated = true
	session.connMu.Unlock()
	session.commitMultiAgentResponseForConn(second, prepared)
	if session.multiAgentResponseForConn(second) != (helps.CodexMultiAgentResponsePolicy{}) {
		t.Fatal("retired session accepted a late policy")
	}
	var absent *codexWebsocketSession
	absent.commitMultiAgentResponseForConn(first, prepared)
	if absent.multiAgentResponseForConn(first) != (helps.CodexMultiAgentResponsePolicy{}) || session.multiAgentResponseForConn(nil) != (helps.CodexMultiAgentResponsePolicy{}) {
		t.Fatal("missing connection acquired state")
	}
}

func TestCodexMultiAgentConnectionPolicyPublishesWholeValues(t *testing.T) {
	conn := &websocket.Conn{}
	session := &codexWebsocketSession{conn: conn}
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for range 500 {
				got := session.multiAgentResponseForConn(conn)
				if got.NamespaceOptimized != got.PlaintextCalls {
					t.Error("reader observed mixed policy revisions")
					return
				}
			}
		})
	}
	for index := range 500 {
		enabled := index%2 == 0
		session.commitMultiAgentResponseForConn(conn, helps.CodexMultiAgentResponsePolicy{NamespaceOptimized: enabled, PlaintextCalls: enabled})
	}
	readers.Wait()
}
