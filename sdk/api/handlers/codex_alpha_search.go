package handlers

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

type CodexAlphaSearchAPIHandler struct{ *BaseAPIHandler }

func NewCodexAlphaSearchAPIHandler(base *BaseAPIHandler) *CodexAlphaSearchAPIHandler {
	return &CodexAlphaSearchAPIHandler{BaseAPIHandler: base}
}

func (*CodexAlphaSearchAPIHandler) HandlerType() string {
	return translator.FormatCodexAlphaSearch.String()
}
func (*CodexAlphaSearchAPIHandler) Models() []map[string]any { return nil }

// Search uses the regular Manager lifecycle with a native search protocol.
// No credential or upstream connection is acquired until this route is called.
func (h *CodexAlphaSearchAPIHandler) Search(c *gin.Context) {
	if h == nil || h.BaseAPIHandler == nil || h.AuthManager == nil || h.Cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Codex search is unavailable"})
		return
	}
	ctx, cancel := h.GetContextWithCancel(h, c, c.Request.Context())
	defer cancel()
	providers, denied := restrictExecutionProviders(ctx, []string{"codex"})
	if denied != nil {
		h.WriteErrorResponse(c, denied)
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, helps.CodexAlphaSearchMaxRequestBytes))
	if err != nil {
		code := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			code = http.StatusRequestEntityTooLarge
		}
		h.WriteErrorResponse(c, &interfaces.ErrorMessage{StatusCode: code, Error: errors.New("failed to read Codex search request; maximum size is 16 MiB")})
		return
	}
	routing, err := helps.ParseCodexAlphaSearchRouting(body)
	if err != nil {
		h.WriteErrorResponse(c, &interfaces.ErrorMessage{StatusCode: http.StatusBadRequest, Error: err})
		return
	}
	model := strings.TrimSpace(routing.Model)
	metadata := requestExecutionMetadata(ctx)
	metadata[core.RequestedModelMetadataKey] = model
	ctx, _ = h.attachRequestBodyRelease(ctx, body, metadata, false)
	selectionHeaders := c.Request.Header.Clone()
	if selectionHeaders == nil {
		selectionHeaders = make(http.Header)
	}
	if id := strings.TrimSpace(routing.SessionID); id != "" {
		selectionHeaders.Set("X-Session-ID", id)
	}
	opts := core.Options{SourceFormat: translator.FormatCodexAlphaSearch, OriginalRequest: body, Headers: selectionHeaders, Metadata: metadata}
	resp, err := h.AuthManager.Execute(ctx, providers, core.Request{Model: model, Payload: body}, opts)
	if err != nil {
		h.WriteErrorResponse(c, h.RewriteExecutionErrorResponseForContext(ctx, executionErrorMessage(err, providers, model)))
		return
	}
	upstreamHeaders := FilterUpstreamHeaders(resp.Headers)
	contentType := upstreamHeaders.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
	}
	if PassthroughHeadersEnabled(h.Cfg) {
		WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
	}
	status := resp.StatusCode
	if status == 0 {
		status = http.StatusOK
	}
	c.Data(status, contentType, resp.Payload)
}
