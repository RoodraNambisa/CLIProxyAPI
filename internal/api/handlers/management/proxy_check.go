package management

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
)

const proxyTraceUserAgent = "Mozilla/5.0"

var (
	proxyTraceURL     = "https://cloudflare.com/cdn-cgi/trace"
	proxyTraceTimeout = 8 * time.Second
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
	started := time.Now()
	proxyURL = strings.TrimSpace(proxyURL)
	transport, mode, errTransport := proxyCheckTransport(proxyURL)
	result := proxyCheckResponse{
		Mode:     mode,
		ProxyURL: proxyutil.MaskProxyURL(proxyURL),
	}
	if errTransport != nil {
		result.Error = "invalid_proxy"
		result.Message = "invalid proxy configuration"
		return result
	}

	req, errRequest := http.NewRequestWithContext(ctx, http.MethodGet, proxyTraceURL, nil)
	if errRequest != nil {
		result.Error = "request_create_failed"
		result.Message = errRequest.Error()
		return result
	}
	req.Header.Set("User-Agent", proxyTraceUserAgent)

	client := &http.Client{
		Timeout:   proxyTraceTimeout,
		Transport: transport,
	}
	resp, errDo := client.Do(req)
	result.ElapsedMS = time.Since(started).Milliseconds()
	if errDo != nil {
		result.Error = "request_failed"
		result.Message = errDo.Error()
		if proxyURL != "" {
			result.Message = strings.ReplaceAll(result.Message, proxyURL, proxyutil.MaskProxyURL(proxyURL))
		}
		return result
	}
	defer func() { _ = resp.Body.Close() }()

	body, errRead := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if errRead != nil {
		result.Error = "read_failed"
		result.Message = errRead.Error()
		return result
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result.Error = "unexpected_status"
		result.Message = http.StatusText(resp.StatusCode)
		if result.Message == "" {
			result.Message = strings.TrimSpace(string(body))
		}
		return result
	}

	trace := parseCloudflareTrace(string(body))
	result.IP = trace["ip"]
	result.Location = trace["loc"]
	result.HTTP = trace["http"]
	result.TLS = trace["tls"]
	result.Colo = trace["colo"]
	result.OK = result.IP != "" || result.Location != ""
	if !result.OK {
		result.Error = "invalid_trace"
		result.Message = "trace response missing ip and loc"
	}
	return result
}

func proxyCheckTransport(proxyURL string) (http.RoundTripper, string, error) {
	setting, errParse := proxyutil.Parse(proxyURL)
	if errParse != nil {
		return nil, "invalid", errParse
	}
	switch setting.Mode {
	case proxyutil.ModeInherit:
		return nil, "inherit", nil
	case proxyutil.ModeDirect:
		return proxyutil.NewDirectTransport(), "direct", nil
	case proxyutil.ModeProxy:
		transport, _, errBuild := proxyutil.BuildHTTPTransport(proxyURL)
		return transport, "proxy", errBuild
	default:
		return nil, "invalid", nil
	}
}

func parseCloudflareTrace(body string) map[string]string {
	trace := make(map[string]string)
	for _, line := range strings.Split(body, "\n") {
		if !strings.Contains(line, "=") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if key == "" {
			continue
		}
		trace[key] = value
	}
	return trace
}
