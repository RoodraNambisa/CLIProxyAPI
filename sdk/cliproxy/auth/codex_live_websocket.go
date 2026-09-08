package auth

import (
	"context"
	"errors"
	"net/http"
	"net/url"

	"github.com/gorilla/websocket"
)

type codexLiveWebsocketDialer interface {
	DialCodexLiveWebsocket(context.Context, *Auth, string, http.Header, []string) (*websocket.Conn, *http.Response, error)
}

// DialWebsocket uses the selected executor's proxy and fingerprint transport.
// The caller owns the resulting socket and must close it when Context ends.
func (l *CodexLiveLease) DialWebsocket(target string, headers http.Header, protocols []string) (*websocket.Conn, *http.Response, error) {
	dialer, ok := l.executor.(codexLiveWebsocketDialer)
	if !ok {
		return nil, nil, &Error{Code: "not_supported", Message: "executor does not support realtime WebSocket", HTTPStatus: http.StatusNotImplemented}
	}
	parsed, errURL := url.Parse(target)
	if errURL != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || (parsed.Scheme != "ws" && parsed.Scheme != "wss") {
		return nil, nil, &Error{Code: "invalid_request", Message: "invalid realtime WebSocket target", HTTPStatus: http.StatusBadRequest}
	}
	if parsed.Scheme == "wss" {
		parsed.Scheme = "https"
	} else {
		parsed.Scheme = "http"
	}
	req, errRequest := http.NewRequestWithContext(l.ctx, http.MethodGet, parsed.String(), nil)
	if errRequest != nil {
		return nil, nil, errRequest
	}
	req.Header = headers.Clone()
	if req.Header == nil {
		req.Header = make(http.Header)
	}
	if errPrepare := l.PrepareHttpRequest(req); errPrepare != nil {
		return nil, nil, errPrepare
	}
	l.mu.Lock()
	if l.websocketDialStarted {
		l.mu.Unlock()
		return nil, nil, &Error{Code: "realtime_connection_already_attempted", Message: "realtime lease already attempted a WebSocket connection", HTTPStatus: http.StatusConflict}
	}
	l.websocketDialStarted = true
	l.mu.Unlock()
	if errCommit := l.CommitUpstream(); errCommit != nil {
		return nil, nil, errCommit
	}
	conn, resp, errDial := dialer.DialCodexLiveWebsocket(l.ctx, l.auth, target, req.Header, protocols)
	if errDial != nil && conn != nil {
		errDial = errors.Join(errDial, conn.Close())
		conn = nil
	}
	if errCtx := context.Cause(l.ctx); errCtx != nil {
		if conn != nil {
			errCtx = errors.Join(errCtx, conn.Close())
			conn = nil
		}
		return nil, resp, errCtx
	}
	if conn == nil && errDial == nil {
		errDial = &Error{Code: "realtime_connection_missing", Message: "realtime executor returned no connection", HTTPStatus: http.StatusBadGateway}
	}
	return conn, resp, errDial
}
