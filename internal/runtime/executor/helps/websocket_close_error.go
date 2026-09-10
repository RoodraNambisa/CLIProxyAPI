package helps

import (
	"errors"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// WebsocketCloseState remembers only the current connection's first close frame.
// Configure must run before that connection's reader starts.
type WebsocketCloseState struct {
	mu   sync.RWMutex
	conn *websocket.Conn
	err  error
}

func (s *WebsocketCloseState) Configure(conn *websocket.Conn) {
	if s == nil || conn == nil {
		return
	}
	s.mu.Lock()
	s.conn, s.err = conn, nil
	s.mu.Unlock()
	previous := conn.CloseHandler()
	conn.SetCloseHandler(func(code int, text string) error {
		s.mu.Lock()
		if s.conn == conn && s.err == nil {
			s.err = &websocket.CloseError{Code: code, Text: text}
		}
		s.mu.Unlock()
		return previous(code, text)
	})
}

// RecoverWrite preserves ordinary transport failures while restoring a 1009
// that arrived before a concurrent write observed the closed connection.
func (s *WebsocketCloseState) RecoverWrite(conn *websocket.Conn, err error) error {
	if s == nil || conn == nil || err == nil {
		return err
	}
	s.mu.RLock()
	var cause error
	if s.conn == conn {
		cause = s.err
	}
	s.mu.RUnlock()
	if IsWebsocketMessageTooBig(cause) {
		return MapWebsocketMessageTooBigError(cause)
	}
	return err
}

func IsWebsocketMessageTooBig(err error) bool {
	var closed *websocket.CloseError
	return errors.As(err, &closed) && closed != nil && closed.Code == websocket.CloseMessageTooBig
}

type websocketMessageTooBigError struct{ cause error }

func (websocketMessageTooBigError) Error() string {
	return `{"error":{"message":"upstream websocket message too big","type":"invalid_request_error","code":"message_too_big"}}`
}
func (websocketMessageTooBigError) StatusCode() int { return http.StatusRequestEntityTooLarge }
func (e websocketMessageTooBigError) Unwrap() error { return e.cause }

// MapWebsocketMessageTooBigError keeps the public message stable and retains the
// transport cause for close-code handling. Existing request-fault rules recognize it.
func MapWebsocketMessageTooBigError(err error) error {
	if IsWebsocketMessageTooBig(err) {
		return websocketMessageTooBigError{cause: err}
	}
	return err
}
