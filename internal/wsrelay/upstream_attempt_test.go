package wsrelay

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestRelayRecordsRequestSendAndExcludesClosedSession(t *testing.T) {
	received := make(chan Message, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer func() { _ = conn.Close() }()
		var msg Message
		if err = conn.ReadJSON(&msg); err != nil {
			t.Error(err)
			return
		}
		received <- msg
	}))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	active := &session{conn: conn, closed: make(chan struct{})}
	ctx := executor.WithUpstreamAttempt(t.Context())
	if err = active.send(ctx, Message{ID: "request", Type: MessageTypeHTTPReq}); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-received:
		if msg.ID != "request" {
			t.Fatal("relay changed the request")
		}
	case <-time.After(time.Second):
		t.Fatal("relay did not send the request")
	}
	if !executor.IsUpstreamAttemptError(executor.ErrorFromUpstreamAttempt(ctx, errors.New("observed request"))) {
		t.Fatal("relay request did not retain upstream evidence")
	}
	close(active.closed)
	next := executor.WithUpstreamAttempt(ctx)
	err = active.send(next, Message{ID: "closed", Type: MessageTypeHTTPReq})
	if !errors.Is(err, errClosed) || executor.IsUpstreamAttemptError(executor.ErrorFromUpstreamAttempt(next, err)) {
		t.Fatal("closed relay session was counted as an upstream attempt")
	}
}
