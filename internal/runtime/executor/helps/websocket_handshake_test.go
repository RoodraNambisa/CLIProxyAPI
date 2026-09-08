package helps

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWebsocketHandshakeCancellationKeepsConfiguredDialPaths(t *testing.T) {
	for _, mode := range []string{"context", "legacy", "tls"} {
		t.Run(mode, func(t *testing.T) {
			entered := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done() }))
			defer server.Close()
			var calls atomic.Int32
			netDial := func(ctx context.Context, network, _ string) (net.Conn, error) {
				calls.Add(1)
				return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			}
			dialer := &websocket.Dialer{HandshakeTimeout: time.Minute}
			scheme := "ws"
			switch mode {
			case "context":
				dialer.NetDialContext = netDial
			case "legacy":
				dialer.NetDial = func(network, address string) (net.Conn, error) { return netDial(t.Context(), network, address) }
			case "tls":
				// A local plain connection stands in for an already-negotiated custom TLS transport.
				dialer.NetDialTLSContext = netDial
				scheme = "wss"
			}
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			cause := errors.New("fixture acquisition cancelled")
			done := make(chan error, 1)
			go func() {
				conn, response, errDial := DialWebsocketHandshake(ctx, dialer, scheme+"://fixture.invalid/realtime", nil)
				if conn != nil {
					_ = conn.Close()
				}
				if response != nil && response.Body != nil {
					_ = response.Body.Close()
				}
				done <- errDial
			}()
			<-entered
			cancel(cause)
			select {
			case errDial := <-done:
				if !errors.Is(errDial, cause) || calls.Load() != 1 {
					t.Fatal("dial path or cancellation cause changed")
				}
			case <-time.After(time.Second):
				server.CloseClientConnections()
				<-done
				t.Fatal("custom dial path ignored cancellation")
			}
			if dialer.HandshakeTimeout != time.Minute || (mode != "context" && dialer.NetDialContext != nil) {
				t.Fatal("shared dialer was mutated")
			}
		})
	}
}

func TestWebsocketHandshakeDetachesCancellationAfterSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Error(errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		kind, data, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Error(errRead)
			return
		}
		if errWrite := conn.WriteMessage(kind, data); errWrite != nil {
			t.Error(errWrite)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	conn, response, errDial := DialWebsocketHandshake(ctx, &websocket.Dialer{}, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if errDial != nil {
		t.Fatal(errDial)
	}
	defer func() { _ = conn.Close() }()
	cancel()
	if errWrite := conn.WriteMessage(websocket.TextMessage, []byte("fixture")); errWrite != nil {
		t.Fatal("completed connection remained attached to handshake cancellation")
	}
	_, data, errRead := conn.ReadMessage()
	if errRead != nil || string(data) != "fixture" {
		t.Fatal("completed connection did not remain caller-owned")
	}
}
