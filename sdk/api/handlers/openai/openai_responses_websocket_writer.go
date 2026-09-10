package openai

import (
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

type responsesWebsocketOutput interface {
	WriteMessage(int, []byte) error
	WriteControl(int, []byte, time.Time) error
	Close() error
}

type responsesWebsocketWriter struct {
	conn    responsesWebsocketOutput
	writeMu sync.Mutex
	closing atomic.Bool
}

func newResponsesWebsocketWriter(conn responsesWebsocketOutput) *responsesWebsocketWriter {
	if writer, ok := conn.(*responsesWebsocketWriter); ok {
		return writer
	}
	return &responsesWebsocketWriter{conn: conn}
}

func (w *responsesWebsocketWriter) WriteMessage(kind int, payload []byte) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	if w.closing.Load() {
		return websocket.ErrCloseSent
	}
	return w.conn.WriteMessage(kind, payload)
}

func (w *responsesWebsocketWriter) WriteControl(kind int, payload []byte, deadline time.Time) error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	if w.closing.Load() {
		return websocket.ErrCloseSent
	}
	return w.conn.WriteControl(kind, payload, deadline)
}

// Close never waits for a blocked data or Ping write.
func (w *responsesWebsocketWriter) Close() error {
	w.closing.Store(true)
	return w.conn.Close()
}

func responsesWebsocketMessageTooBig(err error) bool {
	if err == nil {
		return false
	}
	var closed *websocket.CloseError
	if errors.As(err, &closed) && closed != nil && closed.Code == websocket.CloseMessageTooBig {
		return true
	}
	var status interface{ StatusCode() int }
	return errors.As(err, &status) && status.StatusCode() == http.StatusRequestEntityTooLarge && gjson.Get(err.Error(), "error.code").String() == "message_too_big"
}

// closeForUpstreamError preserves the transport code when the writer is idle.
// If output is blocked, closing the socket takes precedence over a close frame.
func (w *responsesWebsocketWriter) closeForUpstreamError(err error) (bool, error) {
	if !responsesWebsocketMessageTooBig(err) {
		return false, nil
	}
	if !w.closing.CompareAndSwap(false, true) {
		return true, nil
	}
	if !w.writeMu.TryLock() {
		return true, w.conn.Close()
	}
	defer w.writeMu.Unlock()
	// Use the existing public reason instead of exposing arbitrary upstream text.
	payload := websocket.FormatCloseMessage(websocket.CloseMessageTooBig, "upstream websocket message too big")
	errWrite := w.conn.WriteControl(websocket.CloseMessage, payload, time.Time{})
	errClose := w.conn.Close()
	if errWrite != nil {
		return true, errWrite
	}
	return true, errClose
}

func (w *responsesWebsocketWriter) closeForUpstreamDisconnect(err error) {
	if matched, _ := w.closeForUpstreamError(err); !matched {
		_ = w.Close()
	}
}
