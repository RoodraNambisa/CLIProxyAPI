package openai

import (
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type blockedResponsesWebsocketOutput struct {
	started     chan struct{}
	closed      chan struct{}
	startOnce   sync.Once
	closeOnce   sync.Once
	closeFrames atomic.Int32
	controlErr  error
}

func (o *blockedResponsesWebsocketOutput) WriteMessage(int, []byte) error {
	o.startOnce.Do(func() { close(o.started) })
	<-o.closed
	return errors.New("socket closed")
}
func (o *blockedResponsesWebsocketOutput) WriteControl(kind int, _ []byte, _ time.Time) error {
	if kind == websocket.CloseMessage {
		o.closeFrames.Add(1)
		return o.controlErr
	}
	return o.WriteMessage(kind, nil)
}
func (o *blockedResponsesWebsocketOutput) Close() error {
	o.closeOnce.Do(func() { close(o.closed) })
	return nil
}

func TestResponsesWebsocketCloseDoesNotWaitForBlockedWriter(t *testing.T) {
	for _, ping := range []bool{false, true} {
		for _, tooBig := range []bool{false, true} {
			output := &blockedResponsesWebsocketOutput{started: make(chan struct{}), closed: make(chan struct{})}
			writer := newResponsesWebsocketWriter(output)
			done := make(chan struct{})
			go func() {
				defer close(done)
				if ping {
					_ = writer.WriteControl(websocket.PingMessage, nil, time.Time{})
				} else {
					_ = writer.WriteMessage(websocket.TextMessage, nil)
				}
			}()
			<-output.started
			closed := make(chan struct{})
			go func() {
				defer close(closed)
				if tooBig {
					writer.closeForUpstreamDisconnect(&websocket.CloseError{Code: websocket.CloseMessageTooBig})
				} else {
					_ = writer.Close()
				}
			}()
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				_ = output.Close()
				t.Fatal("close waited for a blocked writer")
			}
			<-done
			if output.closeFrames.Load() != 0 {
				t.Fatal("close frame competed with a blocked writer")
			}
			if err := writer.WriteMessage(websocket.TextMessage, nil); !errors.Is(err, websocket.ErrCloseSent) {
				t.Fatal("data written after close")
			}
			if err := writer.WriteControl(websocket.PingMessage, nil, time.Time{}); !errors.Is(err, websocket.ErrCloseSent) {
				t.Fatal("Ping written after close")
			}
		}
	}
}

func TestResponsesWebsocketCloseClassificationAndIdempotence(t *testing.T) {
	for _, err := range []error{nil, errors.New("message_too_big"), websocketPinnedFailoverStatusError{status: 413, msg: `{"error":{"code":"other"}}`}, websocketPinnedFailoverStatusError{status: 429, msg: `{"error":{"code":"message_too_big"}}`}, &websocket.CloseError{Code: websocket.CloseNormalClosure}} {
		if responsesWebsocketMessageTooBig(err) {
			t.Fatalf("unrelated error matched: %v", err)
		}
	}
	output := &blockedResponsesWebsocketOutput{closed: make(chan struct{})}
	writer := newResponsesWebsocketWriter(output)
	err := websocketPinnedFailoverStatusError{status: http.StatusRequestEntityTooLarge, msg: `{"error":{"code":"message_too_big"}}`}
	for range 3 {
		if matched, closeErr := writer.closeForUpstreamError(err); !matched || closeErr != nil {
			t.Fatal("message too big not closed")
		}
	}
	if output.closeFrames.Load() != 1 {
		t.Fatal("duplicate close frames")
	}
}

func TestResponsesWebsocketCloseFrameFailureStillClosesSocket(t *testing.T) {
	writeErr := errors.New("write failed")
	output := &blockedResponsesWebsocketOutput{closed: make(chan struct{}), controlErr: writeErr}
	writer := newResponsesWebsocketWriter(output)
	matched, err := writer.closeForUpstreamError(&websocket.CloseError{Code: websocket.CloseMessageTooBig})
	if !matched || !errors.Is(err, writeErr) {
		t.Fatal("write failure lost")
	}
	select {
	case <-output.closed:
	default:
		t.Fatal("socket retained after close-frame failure")
	}
}
