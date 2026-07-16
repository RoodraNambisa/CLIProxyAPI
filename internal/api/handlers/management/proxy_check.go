package management

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
)

var (
	proxyTraceURL     = proxyutil.DefaultTraceEndpoint
	proxyTraceTimeout = proxyutil.DefaultTraceTimeout
)

type proxyCheckRequest struct {
	ProxyURL string `json:"proxy-url"`
}

type proxyCheckResponse struct {
	OK        bool   `json:"ok"`
	Mode      string `json:"mode"`
	ProxyURL  string `json:"proxy-url,omitempty"`
	IP        string `json:"ip,omitempty"`
	Location  string `json:"loc,omitempty"`
	HTTP      string `json:"http,omitempty"`
	TLS       string `json:"tls,omitempty"`
	Colo      string `json:"colo,omitempty"`
	ElapsedMS int64  `json:"elapsed_ms,omitempty"`
	Error     string `json:"error,omitempty"`
	Message   string `json:"message,omitempty"`
}

// GetProxyURLCheck checks the currently configured global proxy-url.
func (h *Handler) GetProxyURLCheck(c *gin.Context) {
	proxyURL := ""
	if h != nil {
		h.mu.Lock()
		if h.cfg != nil {
			proxyURL = h.cfg.ProxyURL
		}
		h.mu.Unlock()
	}
	h.writeProxyURLCheck(c, proxyURL)
}

// PostProxyURLCheck checks a provided proxy-url without persisting it.
func (h *Handler) PostProxyURLCheck(c *gin.Context) {
	var body proxyCheckRequest
	if errBind := c.ShouldBindJSON(&body); errBind != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	h.writeProxyURLCheck(c, body.ProxyURL)
}

func (h *Handler) writeProxyURLCheck(c *gin.Context, proxyURL string) {
	result := checkProxyURL(c.Request.Context(), proxyURL)
	c.JSON(http.StatusOK, result)
}

func checkProxyURL(ctx context.Context, proxyURL string) proxyCheckResponse {
	proxyURL = strings.TrimSpace(proxyURL)
	trace := proxyutil.CheckTrace(ctx, proxyURL, proxyutil.TraceOptions{
		Endpoint: proxyTraceURL,
		Timeout:  proxyTraceTimeout,
	})
	return proxyCheckResponse{
		OK:        trace.OK,
		Mode:      proxyCheckMode(proxyURL),
		ProxyURL:  proxyutil.MaskProxyURL(proxyURL),
		IP:        trace.IP,
		Location:  trace.Location,
		HTTP:      trace.HTTP,
		TLS:       trace.TLS,
		Colo:      trace.Colo,
		ElapsedMS: trace.Elapsed.Milliseconds(),
		Error:     trace.Error,
		Message:   trace.Message,
	}
}

func proxyCheckMode(proxyURL string) string {
	setting, errParse := proxyutil.Parse(proxyURL)
	if errParse != nil {
		return "invalid"
	}
	switch setting.Mode {
	case proxyutil.ModeInherit:
		return "inherit"
	case proxyutil.ModeDirect:
		return "direct"
	case proxyutil.ModeProxy:
		return "proxy"
	default:
		return "invalid"
	}
}
