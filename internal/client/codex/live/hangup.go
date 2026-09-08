package live

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
)

// HandleHangup uses only the call's existing owner and credential instance.
// Disabling new admission must not prevent authenticated resource cleanup.
func (h *Handler) HandleHangup(c *gin.Context) {
	r := h.beginRequest(c, false)
	if r == nil {
		return
	}
	defer r.finish()
	id := strings.TrimSpace(c.Param("call_id"))
	if !callIDPattern.MatchString(id) {
		r.fail(c, nil, nil, http.StatusBadRequest, "Invalid realtime call ID", "invalid_request_error", "invalid_call_id", nil, nil)
		return
	}
	owner, _ := requestCallOwner(c)
	call := h.calls.find(id, owner)
	if call == nil {
		r.fail(c, nil, nil, http.StatusNotFound, "Realtime call not found", "invalid_request_error", "realtime_call_not_found", nil, nil)
		return
	}
	body, errRead := readCallBody(c.Request.Body)
	if errRead != nil {
		status := http.StatusBadRequest
		if errors.Is(errRead, errCallBodyTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		r.fail(c, errRead, nil, status, "Failed to read realtime hangup request", "invalid_request_error", "invalid_realtime_request", nil, nil)
		return
	}
	status, payload, headers, errHangup := h.hangupCall(r.ctx, call.lease, id, body, c.Request.Header)
	if errHangup != nil {
		r.fail(c, errHangup, call.lease, realtimeErrorStatus(errHangup, http.StatusBadGateway), "Failed to hang up realtime call", "api_error", "realtime_upstream_unavailable", nil, headers)
		return
	}
	if status < 200 || status >= 300 {
		r.fail(c, nil, call.lease, status, "Realtime hangup was rejected by the upstream", "api_error", "realtime_upstream_unavailable", payload, headers)
		return
	}
	h.calls.remove(call)
	if handlers.PassthroughHeadersEnabled(r.base.Cfg) {
		handlers.WriteUpstreamHeaders(c.Writer.Header(), handlers.FilterUpstreamHeaders(headers))
	}
	contentType := headers.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
	}
	c.Data(status, contentType, payload)
}
