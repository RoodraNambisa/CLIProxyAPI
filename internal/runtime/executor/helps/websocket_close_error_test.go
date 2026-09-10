package helps

import (
	"errors"
	"sync"
	"testing"

	"github.com/gorilla/websocket"
)

func TestWebsocketCloseStateConnectionIsolation(t *testing.T) {
	var state WebsocketCloseState
	old, current := &websocket.Conn{}, &websocket.Conn{}
	old.SetCloseHandler(func(int, string) error { return nil })
	current.SetCloseHandler(func(int, string) error { return nil })
	state.Configure(old)
	_ = old.CloseHandler()(websocket.CloseMessageTooBig, "old close")
	if !IsWebsocketMessageTooBig(state.RecoverWrite(old, websocket.ErrCloseSent)) {
		t.Fatal("1009 cause lost")
	}
	state.Configure(current)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			_ = old.CloseHandler()(websocket.CloseMessageTooBig, "late close")
			if state.RecoverWrite(current, websocket.ErrCloseSent) != websocket.ErrCloseSent {
				t.Error("old close leaked to current connection")
			}
		})
	}
	wg.Wait()
	_ = current.CloseHandler()(websocket.CloseNormalClosure, "normal close")
	_ = current.CloseHandler()(websocket.CloseMessageTooBig, "second close")
	if state.RecoverWrite(current, websocket.ErrCloseSent) != websocket.ErrCloseSent {
		t.Fatal("first close was overwritten")
	}
	if state.RecoverWrite(old, websocket.ErrCloseSent) != websocket.ErrCloseSent {
		t.Fatal("old connection state retained")
	}
}

func TestWebsocketCloseStateWriteErrorBoundaries(t *testing.T) {
	var state WebsocketCloseState
	conn := &websocket.Conn{}
	conn.SetCloseHandler(func(int, string) error { return nil })
	state.Configure(conn)
	_ = conn.CloseHandler()(websocket.CloseMessageTooBig, "too large")
	for _, err := range []error{websocket.ErrCloseSent, errors.New("broken pipe")} {
		if !IsWebsocketMessageTooBig(state.RecoverWrite(conn, err)) {
			t.Fatal("close was not restored")
		}
	}
	if state.RecoverWrite(conn, nil) != nil || state.RecoverWrite(nil, websocket.ErrCloseSent) != websocket.ErrCloseSent {
		t.Fatal("nil boundary changed")
	}
	var closed *websocket.CloseError
	if IsWebsocketMessageTooBig(closed) {
		t.Fatal("nil close classified")
	}
}
