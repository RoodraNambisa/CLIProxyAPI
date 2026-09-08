package live

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func liveWebsocketPair(t *testing.T, blockers ...*liveRelayWriteBlock) (*websocket.Conn, *websocket.Conn) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Error(errUpgrade)
			return
		}
		accepted <- conn
	}))
	if len(blockers) > 0 {
		server.Listener = liveRelayListener{Listener: server.Listener, block: blockers[0]}
	}
	server.Start()
	t.Cleanup(server.Close)
	client, response, errDial := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if response != nil && response.Body != nil {
		if errClose := response.Body.Close(); errClose != nil {
			t.Error(errClose)
		}
	}
	if errDial != nil {
		t.Fatal(errDial)
	}
	peer := <-accepted
	t.Cleanup(func() { _ = client.Close(); _ = peer.Close() })
	return client, peer
}

type liveRelayWriteBlock struct {
	enabled   atomic.Bool
	entered   chan struct{}
	closed    chan struct{}
	enterOnce sync.Once
	closeOnce sync.Once
}

type liveRelayListener struct {
	net.Listener
	block *liveRelayWriteBlock
}

func (l liveRelayListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &liveRelayBlockedConn{Conn: conn, block: l.block}, nil
}

type liveRelayBlockedConn struct {
	net.Conn
	block *liveRelayWriteBlock
}

func (c *liveRelayBlockedConn) Write(data []byte) (int, error) {
	if c.block.enabled.Load() {
		c.block.enterOnce.Do(func() { close(c.block.entered) })
		<-c.block.closed
		return 0, net.ErrClosed
	}
	return c.Conn.Write(data)
}

func (c *liveRelayBlockedConn) Close() error {
	c.block.closeOnce.Do(func() { close(c.block.closed) })
	return c.Conn.Close()
}

func TestLiveRelayCancellationInterruptsBlockedDataAndControlWrites(t *testing.T) {
	for _, mode := range []string{"data", "ping"} {
		t.Run(mode, func(t *testing.T) {
			block := &liveRelayWriteBlock{entered: make(chan struct{}), closed: make(chan struct{})}
			var client, downstream, provider, upstream *websocket.Conn
			keepalive := time.Duration(0)
			if mode == "data" {
				client, downstream = liveWebsocketPair(t)
				provider, upstream = liveWebsocketPair(t, block)
			} else {
				client, downstream = liveWebsocketPair(t, block)
				provider, upstream = liveWebsocketPair(t)
				keepalive = time.Millisecond
			}
			_ = provider
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			block.enabled.Store(true)
			finished := make(chan error, 1)
			go func() { finished <- relayWebsockets(ctx, downstream, upstream, keepalive) }()
			if mode == "data" {
				if errWrite := client.WriteMessage(websocket.BinaryMessage, make([]byte, 16384)); errWrite != nil {
					t.Fatal(errWrite)
				}
			}
			select {
			case <-block.entered:
			case <-time.After(time.Second):
				t.Fatal("fixture did not block a relay write")
			}
			cancel()
			select {
			case errRelay := <-finished:
				if !errors.Is(errRelay, context.Canceled) {
					t.Fatal("blocked write obscured cancellation")
				}
			case <-time.After(time.Second):
				t.Fatal("blocked write retained a relay worker")
			}
		})
	}
}

func TestLiveRelayPreservesBidirectionalMessagesAndCancellation(t *testing.T) {
	client, downstream := liveWebsocketPair(t)
	provider, upstream := liveWebsocketPair(t)
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	finished := make(chan error, 1)
	go func() { finished <- relayWebsockets(ctx, downstream, upstream, 0) }()
	for _, direction := range []struct{ from, to *websocket.Conn }{{client, provider}, {provider, client}} {
		for _, kind := range []int{websocket.TextMessage, websocket.BinaryMessage} {
			payload := bytes.Repeat([]byte("native-audio-fixture "), 10000)
			if errWrite := direction.from.WriteMessage(kind, payload); errWrite != nil {
				t.Fatal(errWrite)
			}
			gotKind, got, errRead := direction.to.ReadMessage()
			if errRead != nil || gotKind != kind || !bytes.Equal(got, payload) {
				t.Fatal("realtime relay changed a message or message boundary")
			}
		}
	}
	cause := errors.New("fixture session ended")
	cancel(cause)
	select {
	case errRelay := <-finished:
		if !errors.Is(errRelay, cause) {
			t.Fatal("relay lost the session cancellation cause")
		}
	case <-time.After(time.Second):
		t.Fatal("relay did not join its workers after cancellation")
	}
}

func TestLiveRelayKeepaliveAndPongNeverEnterMessages(t *testing.T) {
	client, downstream := liveWebsocketPair(t)
	provider, upstream := liveWebsocketPair(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- relayWebsockets(ctx, downstream, upstream, 5*time.Millisecond) }()
	pings := make(chan struct{}, 1)
	client.SetPingHandler(func(string) error {
		select {
		case pings <- struct{}{}:
		default:
		}
		return nil
	})
	data := make(chan []byte, 1)
	go func() { _, payload, _ := client.ReadMessage(); data <- payload }()
	select {
	case <-pings:
	case <-time.After(time.Second):
		t.Fatal("idle relay did not emit Ping")
	}
	if errWrite := provider.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.done"}`)); errWrite != nil {
		t.Fatal(errWrite)
	}
	select {
	case got := <-data:
		if string(got) != `{"type":"response.done"}` {
			t.Fatal("control frame entered message data")
		}
	case <-time.After(time.Second):
		t.Fatal("relay did not resume after Ping")
	}
	pongs := make(chan string, 1)
	provider.SetPongHandler(func(data string) error { pongs <- data; return nil })
	received := make(chan []byte, 1)
	go func() { _, payload, _ := provider.ReadMessage(); received <- payload }()
	if errPing := provider.WriteControl(websocket.PingMessage, []byte("fixture-ping"), time.Time{}); errPing != nil {
		t.Fatal(errPing)
	}
	select {
	case pong := <-pongs:
		if pong != "fixture-ping" {
			t.Fatal("Pong payload changed")
		}
	case <-time.After(time.Second):
		t.Fatal("upstream Ping did not receive Pong")
	}
	if errWrite := client.WriteMessage(websocket.TextMessage, []byte("after-ping")); errWrite != nil {
		t.Fatal(errWrite)
	}
	select {
	case payload := <-received:
		if string(payload) != "after-ping" {
			t.Fatal("upstream Ping interrupted data forwarding")
		}
	case <-time.After(time.Second):
		t.Fatal("upstream did not receive forwarded data")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("keepalive timer or relay worker leaked")
	}
}

func TestLiveRelayEndsWhenEitherPeerCloses(t *testing.T) {
	for _, closeClient := range []bool{false, true} {
		client, downstream := liveWebsocketPair(t)
		provider, upstream := liveWebsocketPair(t)
		finished := make(chan error, 1)
		go func() { finished <- relayWebsockets(t.Context(), downstream, upstream, 0) }()
		peer := provider
		if closeClient {
			peer = client
		}
		if errClose := peer.Close(); errClose != nil {
			t.Fatal(errClose)
		}
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatal("peer termination leaked the other relay direction")
		}
	}
}
