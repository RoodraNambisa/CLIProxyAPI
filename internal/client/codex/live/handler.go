package live

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

type handlerRuntime struct {
	base          *handlers.BaseAPIHandler
	keepalive     time.Duration
	media         *pionMediaRelay
	mediaError    error
	mediaProxyURL string
}

// Handler owns native realtime admission and process lifetime independently
// from the Responses executor's connection and conversation caches.
type Handler struct {
	manager          *auth.Manager
	gate             admissionGate
	runtime          atomic.Pointer[handlerRuntime]
	updateMu         sync.Mutex
	closed           bool
	root             context.Context
	shutdown         context.CancelFunc
	websocketBaseURL string
	calls            *callStore
	callURL          string
	clientSecrets    *clientSecretStore
	secretRandom     io.Reader
	mediaLimiter     mediaSessionLimiter
}

func NewHandler(cfg *config.Config, manager *auth.Manager) *Handler {
	ctx, cancel := context.WithCancel(context.Background())
	h := &Handler{manager: manager, root: ctx, shutdown: cancel, websocketBaseURL: "wss://api.openai.com/v1", calls: newCallStore(), callURL: "https://chatgpt.com/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas", clientSecrets: newClientSecretStore(), secretRandom: rand.Reader}
	h.UpdateConfig(cfg)
	return h
}

func (*Handler) HandlerType() string      { return translator.FormatCodexLive.String() }
func (*Handler) Models() []map[string]any { return nil }

// UpdateConfig publishes immutable request settings and changes new admission.
// Existing committed sessions retain their settings and independent context.
func (h *Handler) UpdateConfig(cfg *config.Config) {
	if cfg == nil {
		cfg = &config.Config{}
	}
	h.updateMu.Lock()
	defer h.updateMu.Unlock()
	if h.closed {
		return
	}
	runtime := &handlerRuntime{base: handlers.NewBaseAPIHandlers(&cfg.SDKConfig, h.manager), keepalive: handlers.StreamingKeepAliveInterval(&cfg.SDKConfig), mediaProxyURL: cfg.ProxyURL}
	if cfg.Codex.LiveMediaRelay.Enabled {
		// Factory construction opens no sockets. Capacity belongs to the handler,
		// so publishing a new configuration cannot reset active session usage.
		runtime.media, runtime.mediaError = newPionMediaRelay(cfg.Codex.LiveMediaRelay, &h.mediaLimiter)
	}
	h.runtime.Store(runtime)
	h.gate.update(cfg.Codex.LiveEnabled)
}

// Close rejects new work and cancels all process-owned realtime sessions.
func (h *Handler) Close() {
	if h == nil {
		return
	}
	h.updateMu.Lock()
	if h.closed {
		h.updateMu.Unlock()
		return
	}
	h.closed = true
	h.gate.update(false)
	h.shutdown()
	h.updateMu.Unlock()
	h.calls.close()
	h.clientSecrets.close()
	h.mediaLimiter.close()
}

type liveRequest struct {
	*handlerRuntime
	ctx       context.Context
	admission *liveAdmission
	stopSetup func() bool
	finish    func()
	validate  func() error
}

// begin requires an authenticated caller even when legacy API authentication
// permits anonymous requests. Ownership cannot be assigned to ambient cookies.
func (h *Handler) begin(c *gin.Context) *liveRequest {
	return h.beginRequest(c, true)
}

func (h *Handler) beginRequest(c *gin.Context, newSession bool) *liveRequest {
	if h == nil || h.runtime.Load() == nil {
		writeRealtimeError(c, http.StatusServiceUnavailable, "Codex realtime is unavailable", "server_error", "codex_live_unavailable")
		return nil
	}
	if strings.TrimSpace(c.GetString("apiKey")) == "" {
		writeRealtimeError(c, http.StatusUnauthorized, "Realtime requires an authenticated API caller", "authentication_error", "realtime_auth_required")
		return nil
	}
	runtime := h.runtime.Load()
	ctx, cancelRequest := runtime.base.GetContextWithCancel(h, c, c.Request.Context())
	var validateGrant func() error
	if value, present := c.Get(clientSecretContextKey); present {
		grant, validType := value.(ClientSecretAuthorization)
		if !newSession || !validType || !h.clientSecrets.valid(grant) {
			h.WriteClientSecretError(c, errInvalidClientSecret)
			cancelRequest()
			return nil
		}
		validateGrant = func() error {
			if !h.clientSecrets.valid(grant) {
				return errInvalidClientSecret
			}
			return nil
		}
	}
	if !newSession {
		// Authenticated cleanup checks stored ownership in the handler. It must
		// remain possible after new admission or provider access is disabled.
		cleanupCtx, cancelCleanup := context.WithCancelCause(ctx)
		stopRoot := context.AfterFunc(h.root, func() { cancelCleanup(context.Cause(h.root)) })
		if errRoot := context.Cause(h.root); errRoot != nil {
			cancelCleanup(errRoot)
		}
		return &liveRequest{handlerRuntime: runtime, ctx: cleanupCtx, finish: func() {
			stopRoot()
			cancelCleanup(nil)
			cancelRequest()
		}}
	}
	if denied := runtime.base.ValidateProviderAccess(ctx, "codex"); denied != nil {
		runtime.base.WriteErrorResponse(c, denied)
		cancelRequest()
		return nil
	}
	admission, errBegin := h.gate.begin(ctx)
	if errBegin != nil {
		if errors.Is(errBegin, errLiveDisabled) {
			writeRealtimeError(c, http.StatusServiceUnavailable, errLiveDisabled.Error(), "server_error", liveDisabledCode)
		} else {
			status := 499
			if errors.Is(errBegin, context.DeadlineExceeded) {
				status = http.StatusRequestTimeout
			}
			writeRealtimeError(c, status, "Realtime request was cancelled", "invalid_request_error", "realtime_request_cancelled")
		}
		cancelRequest()
		return nil
	}
	sessionCtx, cancelSession := context.WithCancelCause(ctx)
	stopSetup := context.AfterFunc(admission.ctx, func() { cancelSession(context.Cause(admission.ctx)) })
	stopRoot := context.AfterFunc(h.root, func() { cancelSession(context.Cause(h.root)) })
	request := &liveRequest{handlerRuntime: runtime, ctx: sessionCtx, admission: admission, stopSetup: stopSetup, validate: validateGrant}
	request.finish = func() {
		stopSetup()
		stopRoot()
		admission.close()
		cancelSession(nil)
		cancelRequest()
	}
	return request
}

func (r *liveRequest) active() error {
	if r.admission != nil {
		if errCtx := context.Cause(r.admission.ctx); errCtx != nil {
			return errCtx
		}
	}
	if errCtx := context.Cause(r.ctx); errCtx != nil {
		return errCtx
	}
	if r.validate != nil {
		return r.validate()
	}
	return nil
}

// commit detaches setup cancellation before releasing admission bookkeeping.
func (r *liveRequest) commit() error {
	return r.commitWith(nil)
}

func (r *liveRequest) commitWith(publish func() error) error {
	if errCtx := context.Cause(r.ctx); errCtx != nil {
		return errCtx
	}
	if errCommit := r.admission.commitWith(func() error {
		if r.validate != nil {
			if errValidate := r.validate(); errValidate != nil {
				return errValidate
			}
		}
		if publish != nil {
			return publish()
		}
		return nil
	}); errCommit != nil {
		return errCommit
	}
	r.stopSetup()
	r.admission.close()
	return nil
}

func writeRealtimeError(c *gin.Context, status int, message, kind, code string) {
	c.JSON(status, gin.H{"error": gin.H{"message": message, "type": kind, "code": code}})
}
