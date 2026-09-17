package management

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type authFileProxyRoute struct {
	ID        string     `json:"id"`
	Source    string     `json:"source"`
	Mode      string     `json:"mode"`
	Address   string     `json:"address,omitempty"`
	Pending   bool       `json:"pending,omitempty"`
	Linked    bool       `json:"linked,omitempty"`
	Pool      string     `json:"pool,omitempty"`
	IP        string     `json:"ip,omitempty"`
	Location  string     `json:"loc,omitempty"`
	CheckedAt *time.Time `json:"checked_at,omitempty"`
	OK        *bool      `json:"ok,omitempty"`
	Error     string     `json:"error,omitempty"`
}

type authFileProxyChecks struct {
	mu      sync.Mutex
	salt    string
	results map[string]authFileProxyRoute
}

func (checks *authFileProxyChecks) routeID(identity string) string {
	checks.mu.Lock()
	defer checks.mu.Unlock()
	if checks.salt == "" {
		checks.salt = rand.Text()
	}
	mac := hmac.New(sha256.New, []byte(checks.salt))
	_, _ = mac.Write([]byte(identity))
	return hex.EncodeToString(mac.Sum(nil)[:16])
}

func (checks *authFileProxyChecks) load(id string) (authFileProxyRoute, bool) {
	checks.mu.Lock()
	defer checks.mu.Unlock()
	result, ok := checks.results[id]
	return result, ok
}

func (checks *authFileProxyChecks) store(result authFileProxyRoute) {
	checks.mu.Lock()
	defer checks.mu.Unlock()
	if checks.results == nil {
		checks.results = make(map[string]authFileProxyRoute)
	}
	if len(checks.results) >= 512 {
		oldestID := ""
		var oldest time.Time
		for id, entry := range checks.results {
			if oldestID == "" || entry.CheckedAt != nil && entry.CheckedAt.Before(oldest) {
				oldestID = id
				if entry.CheckedAt != nil {
					oldest = *entry.CheckedAt
				}
			}
		}
		delete(checks.results, oldestID)
	}
	checks.results[result.ID] = result
}

func (h *Handler) authFileProxyRoute(auth *coreauth.Auth, summary authFileRuntimeSummary) authFileProxyRoute {
	result, _ := h.authFileProxyRouteAndURL(auth, summary)
	return result
}

func (h *Handler) authFileProxyRouteAndURL(auth *coreauth.Auth, summary authFileRuntimeSummary) (authFileProxyRoute, string) {
	preview := coreauth.ProxyPreview{ResolvedProxy: coreauth.ResolvedProxy{URL: auth.ProxyURL, Source: "auth"}}
	manager := h.coreAuthRuntimeManager()
	var err error
	if manager != nil {
		preview, err = manager.PreviewProxyAuth(auth)
	}
	if preview.URL == "" && !preview.Pending && preview.Source != "relay" && err == nil {
		preview.Source = "inherit"
		if cfg := h.currentConfig(); cfg != nil && strings.TrimSpace(cfg.ProxyURL) != "" {
			preview.URL, preview.Source = strings.TrimSpace(cfg.ProxyURL), "global"
		}
	}
	result := authFileProxyRoute{Source: preview.Source, Pending: preview.Pending, Linked: preview.Linked, Mode: proxyCheckMode(preview.URL)}
	if err != nil || result.Mode == "invalid" {
		result.Mode, result.Error = "invalid", "proxy_unavailable"
	} else if parsed, errParse := url.Parse(preview.URL); errParse == nil && result.Mode == "proxy" {
		// Userinfo, paths and query strings can contain proxy credentials.
		result.Address = parsed.Scheme + "://" + parsed.Host
	}
	identity := auth.ID + "\x00" + coreauth.ChatGPTWebCredentialUID(auth) + "\x00" + preview.URL + "\x00" + preview.BindingID + "\x00" + preview.Source
	if result.Mode == "inherit" {
		for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy"} {
			identity += "\x00" + key + "=" + os.Getenv(key)
		}
	}
	result.ID = h.authProxyChecks.routeID(identity)
	if binding := summary.proxyBinding; binding != nil && preview.BindingID != "" && binding.BindingID == preview.BindingID {
		result.Pool = binding.Pool
		result.IP, result.Location = binding.IP, binding.Location
		result.CheckedAt, result.OK = binding.LastCheckAt, binding.Healthy
		if binding.Healthy != nil && !*binding.Healthy {
			result.Error = "check_failed"
		}
	}
	if cached, found := h.authProxyChecks.load(result.ID); found && result.Error != "proxy_unavailable" && (result.CheckedAt == nil || cached.CheckedAt.After(*result.CheckedAt)) {
		result.IP, result.Location, result.CheckedAt, result.OK, result.Error = cached.IP, cached.Location, cached.CheckedAt, cached.OK, cached.Error
	}
	return result, preview.URL
}

// CheckAuthFileProxy probes only the selected credential's effective route.
// No account token or inference request is sent to the trace endpoint.
func (h *Handler) CheckAuthFileProxy(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	var body struct {
		Name string `json:"name"`
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4096)
	if c.ShouldBindJSON(&body) != nil || strings.TrimSpace(body.Name) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "credential name is required"})
		return
	}
	manager := h.coreAuthRuntimeManager()
	if manager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "credential manager unavailable"})
		return
	}
	auth := h.findManagedAuthWithManager(body.Name, manager)
	if auth == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "credential not found"})
		return
	}
	if strings.EqualFold(auth.Provider, "aistudio") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "AI Studio uses the browser relay network; server egress checks do not apply"})
		return
	}
	resolved, err := manager.ResolveProxyAuth(c.Request.Context(), auth)
	if err != nil {
		route := h.authFileProxyRoute(auth, h.authFileRuntimeSummary(auth.ID))
		failed := false
		route.OK, route.Error, route.IP = &failed, "proxy_unavailable", ""
		c.JSON(http.StatusOK, gin.H{"proxy_route": route})
		return
	}
	route, raw := h.authFileProxyRouteAndURL(auth, h.authFileRuntimeSummary(auth.ID))
	if route.Mode == "invalid" {
		failed := false
		route.OK, route.Error = &failed, "invalid_proxy"
		c.JSON(http.StatusOK, gin.H{"proxy_route": route})
		return
	}
	// A concurrent route change must not label this measurement as the new route.
	if route.Pending || resolved.EffectiveProxyURL() != "" && resolved.EffectiveProxyURL() != raw {
		c.JSON(http.StatusConflict, gin.H{"error": "proxy route changed; refresh and retry"})
		return
	}
	trace := checkProxyURL(c.Request.Context(), raw)
	if c.Request.Context().Err() != nil {
		return
	}
	current, ok := manager.CurrentAuthInstallation(auth)
	if !ok || h.authFileProxyRoute(current, h.authFileRuntimeSummary(current.ID)).ID != route.ID {
		c.JSON(http.StatusConflict, gin.H{"error": "credential or proxy route changed; refresh and retry"})
		return
	}
	now := time.Now().UTC()
	route.IP, route.Location, route.CheckedAt, route.OK = trace.IP, trace.Location, &now, &trace.OK
	route.Error = trace.Error
	h.authProxyChecks.store(route)
	c.JSON(http.StatusOK, gin.H{"proxy_route": route})
}
