package live

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
)

const maxRealtimeErrorBody = 16 << 20

// HandleDirectWebsocket establishes a new native Realtime connection.
func (h *Handler) HandleDirectWebsocket(c *gin.Context) {
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
	if strings.TrimSpace(c.Query("call_id")) != "" {
		r.fail(c, nil, nil, http.StatusBadRequest, "An existing call requires the sideband endpoint", "invalid_request_error", "realtime_sideband_required", nil, nil)
		return
	}
	model := strings.TrimSpace(c.Query("model"))
	grant, temporary := clientSecretAuthorization(c)
	if temporary {
		if model != "" && model != grant.model {
			r.fail(c, nil, nil, http.StatusForbidden, "Realtime credential is not valid for the requested model", "authentication_error", "realtime_client_secret_scope_mismatch", nil, nil)
			return
		}
		model = grant.model
	}
	if model == "" {
		model = registry.CodexRealtimeModelID
	}
	if errActive := r.active(); errActive != nil {
		r.fail(c, errActive, nil, http.StatusServiceUnavailable, "Realtime setup is unavailable", "server_error", "realtime_setup_unavailable", nil, nil)
		return
	}
	lease, errAcquire := h.manager.AcquireCodexLive(r.ctx, model, core.Options{Headers: c.Request.Header.Clone()})
	if errAcquire != nil {
		r.fail(c, errAcquire, nil, realtimeErrorStatus(errAcquire, http.StatusServiceUnavailable), "No available Codex realtime credential for this model", "server_error", "codex_auth_unavailable", nil, nil)
		return
	}
	defer lease.Close()
	selected := lease.CloneAuth()
	headers := realtimeProtocolHeaders(c.Request.Header)
	if headers.Get("Originator") == "" {
		headers.Set("Originator", "Codex Desktop")
	}
	if account, ok := selected.Metadata["account_id"].(string); ok && strings.TrimSpace(account) != "" {
		headers.Set("Chatgpt-Account-Id", strings.TrimSpace(account))
	}
	if errActive := r.active(); errActive != nil {
		r.fail(c, errActive, nil, http.StatusServiceUnavailable, "Realtime setup is unavailable", "server_error", "realtime_setup_unavailable", nil, nil)
		return
	}
	upstream, response, errDial := lease.DialWebsocket(h.directRealtimeURL(lease.Model()), headers, websocket.Subprotocols(c.Request))
	if errDial != nil {
		status := realtimeErrorStatus(errDial, http.StatusBadGateway)
		var body []byte
		var responseHeaders http.Header
		if response != nil {
			if response.StatusCode >= 400 {
				status = response.StatusCode
			}
			responseHeaders = response.Header
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
		message, kind, code := "Codex Realtime WebSocket upstream unavailable", "api_error", "realtime_upstream_unavailable"
		if status == http.StatusNotFound || status == http.StatusNotImplemented {
			status, message, kind, code, body = http.StatusNotImplemented, "Direct Realtime WebSocket is not supported by this upstream", "not_supported_error", "realtime_capability_not_supported", nil
		}
		r.fail(c, errDial, lease, status, message, kind, code, body, responseHeaders)
		return
	}
	closeRealtimeResponse(response)
	defer closeRealtimeSocket(upstream)
	stopUpstream := context.AfterFunc(lease.Context(), func() { closeRealtimeSocket(upstream) })
	defer stopUpstream()
	if errCtx := context.Cause(lease.Context()); errCtx != nil {
		r.fail(c, errCtx, lease, http.StatusServiceUnavailable, "Realtime credential is no longer available", "server_error", "codex_auth_unavailable", nil, nil)
		return
	}
	if temporary {
		if errActive := r.active(); errActive != nil {
			r.fail(c, errActive, nil, http.StatusServiceUnavailable, "Realtime setup is unavailable", "server_error", "realtime_setup_unavailable", nil, nil)
			return
		}
		update, errUpdate := clientSecretSessionUpdate(grant)
		if errUpdate != nil {
			r.fail(c, errUpdate, nil, http.StatusInternalServerError, "Failed to prepare realtime session", "server_error", "realtime_session_failed", nil, nil)
			return
		}
		if errWrite := upstream.WriteMessage(websocket.TextMessage, update); errWrite != nil {
			r.fail(c, errWrite, lease, http.StatusBadGateway, "Failed to initialize realtime session", "api_error", "realtime_upstream_unavailable", nil, nil)
			return
		}
	}
	if errCommit := r.commit(); errCommit != nil {
		r.fail(c, errCommit, nil, http.StatusServiceUnavailable, "Realtime setup is unavailable", "server_error", "realtime_setup_unavailable", nil, nil)
		return
	}
	upgradeHeaders := make(http.Header)
	if upstream.Subprotocol() == "realtime" {
		upgradeHeaders.Set("Sec-WebSocket-Protocol", "realtime")
	}
	// Authentication is explicit; no ambient browser cookies authorize this route.
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	downstream, errUpgrade := upgrader.Upgrade(c.Writer, c.Request, upgradeHeaders)
	if errUpgrade != nil {
		return
	}
	_ = relayWebsockets(lease.Context(), downstream, upstream, r.keepalive)
}

func (h *Handler) directRealtimeURL(model string) string {
	if model == registry.CodexLiveModelID {
		model = registry.CodexRealtimeModelID
	}
	query := url.Values{"model": {model}}
	return strings.TrimRight(h.websocketBaseURL, "/") + "/realtime?" + query.Encode()
}

func realtimeProtocolHeaders(source http.Header) http.Header {
	headers := make(http.Header)
	for _, name := range []string{"User-Agent", "Version", "X-Session-Id", "Session-Id", "Session_id", "Thread-Id", "Originator", "X-Client-Request-Id", "OpenAI-Safety-Identifier", "OpenAI-Organization", "OpenAI-Project", "X-Oai-Attestation"} {
		if values := source.Values(name); len(values) > 0 {
			for _, value := range values {
				headers.Add(name, value)
			}
		}
	}
	return headers
}

func validRealtimeHandshake(request *http.Request) bool {
	if request.Method != http.MethodGet {
		return false
	}
	versionOK := false
	for _, raw := range request.Header.Values("Sec-WebSocket-Version") {
		for _, token := range strings.Split(raw, ",") {
			if strings.TrimSpace(token) == "13" {
				versionOK = true
			}
		}
	}
	key, err := base64.StdEncoding.DecodeString(request.Header.Get("Sec-WebSocket-Key"))
	return versionOK && err == nil && len(key) == 16
}

func closeRealtimeResponse(response *http.Response) {
	if response != nil && response.Body != nil {
		if errClose := response.Body.Close(); errClose != nil {
			log.WithError(errClose).Debug("codex realtime: close handshake response")
		}
	}
}

func closeRealtimeSocket(conn *websocket.Conn) {
	if conn != nil {
		if errClose := conn.Close(); errClose != nil && !errors.Is(errClose, net.ErrClosed) {
			log.WithError(errClose).Debug("codex realtime: close connection")
		}
	}
}
