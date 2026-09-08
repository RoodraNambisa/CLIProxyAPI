package live

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

func (h *Handler) HandleRealtimeWebsocket(c *gin.Context) {
	if _, present := c.Request.URL.Query()["call_id"]; present {
		h.HandleSideband(c)
		return
	}
	h.HandleDirectWebsocket(c)
}

// HandleSideband creates a new connection to an owned call. Ending the
// sideband releases its claim; the WebRTC call remains available for hangup.
func (h *Handler) HandleSideband(c *gin.Context) {
	r := h.begin(c)
	if r == nil {
		return
	}
	defer r.finish()
	if !websocket.IsWebSocketUpgrade(c.Request) {
		c.Header("Upgrade", "websocket")
		r.fail(c, nil, nil, http.StatusUpgradeRequired, "WebSocket upgrade required", "invalid_request_error", "websocket_upgrade_required", nil, nil)
		return
	}
	if !validRealtimeHandshake(c.Request) {
		r.fail(c, nil, nil, http.StatusBadRequest, "Invalid WebSocket handshake", "invalid_request_error", "invalid_websocket_handshake", nil, nil)
		return
	}
	id := strings.TrimSpace(c.Param("call_id"))
	if id == "" {
		id = strings.TrimSpace(c.Query("call_id"))
	}
	if !callIDPattern.MatchString(id) {
		r.fail(c, nil, nil, http.StatusBadRequest, "Invalid realtime call ID", "invalid_request_error", "invalid_call_id", nil, nil)
		return
	}
	owner, _ := requestCallOwner(c)
	call, busy := h.calls.claim(id, owner)
	if busy {
		r.fail(c, nil, nil, http.StatusConflict, "Realtime call already has an active sideband connection", "invalid_request_error", "realtime_call_busy", nil, nil)
		return
	}
	if call == nil {
		r.fail(c, nil, nil, http.StatusNotFound, "Realtime call not found", "invalid_request_error", "realtime_call_not_found", nil, nil)
		return
	}
	defer h.calls.release(call)
	relayCtx, cancelRelay := context.WithCancelCause(r.ctx)
	stopLease := context.AfterFunc(call.lease.Context(), func() { cancelRelay(context.Cause(call.lease.Context())) })
	defer func() { stopLease(); cancelRelay(nil) }()
	if errLease := context.Cause(call.lease.Context()); errLease != nil {
		cancelRelay(errLease)
	}
	upstream, response, errDial := call.lease.DialSessionWebsocket(relayCtx, h.sidebandURL(c, id), realtimeCallHeaders(c.Request.Header, call.lease), websocket.Subprotocols(c.Request))
	if errDial != nil {
		status := realtimeErrorStatus(errDial, http.StatusBadGateway)
		var body []byte
		var headers http.Header
		if response != nil {
			headers = response.Header
			if response.StatusCode >= 400 {
				status = response.StatusCode
			}
			if response.Body != nil {
				var errRead error
				body, errRead = io.ReadAll(io.LimitReader(response.Body, maxRealtimeErrorBody+1))
				errDial = errors.Join(errDial, errRead)
				if len(body) > maxRealtimeErrorBody {
					body = nil
				}
			}
		}
		closeRealtimeResponse(response)
		r.fail(c, errDial, call.lease, status, "Realtime sideband upstream is unavailable", "api_error", "realtime_sideband_unavailable", body, headers)
		return
	}
	closeRealtimeResponse(response)
	defer closeRealtimeSocket(upstream)
	if errCommit := r.commitWith(func() error { return context.Cause(call.lease.Context()) }); errCommit != nil {
		r.fail(c, errCommit, nil, http.StatusServiceUnavailable, "Realtime setup is unavailable", "server_error", "realtime_setup_unavailable", nil, nil)
		return
	}
	headers := make(http.Header)
	if upstream.Subprotocol() == "realtime" {
		headers.Set("Sec-WebSocket-Protocol", "realtime")
	}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	downstream, errUpgrade := upgrader.Upgrade(c.Writer, c.Request, headers)
	if errUpgrade != nil {
		return
	}
	_ = relayWebsockets(relayCtx, downstream, upstream, r.keepalive)
}

func (h *Handler) sidebandURL(c *gin.Context, id string) string {
	base := strings.TrimRight(h.websocketBaseURL, "/")
	if strings.HasPrefix(c.Request.URL.Path, "/v1/live/") {
		return base + "/live/" + id
	}
	if c.Param("call_id") != "" {
		return base + "/realtime/calls/" + id
	}
	return base + "/realtime?intent=quicksilver&call_id=" + url.QueryEscape(id)
}
