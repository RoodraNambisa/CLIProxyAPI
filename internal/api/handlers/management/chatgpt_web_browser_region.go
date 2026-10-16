package management

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type chatGPTWebBrowserRegionResponse struct {
	config.ResolvedChatGPTWebBrowserRegion
	SharedProxyIP         bool   `json:"shared_proxy_ip"`
	Timezone              string `json:"timezone"`
	TimezoneOffsetMinutes *int   `json:"timezone-offset-minutes"`
}
type chatGPTWebBrowserRegionController interface {
	BrowserRegionPreview(*coreauth.Auth, bool) (chatgptwebauth.BrowserRegionSnapshot, string)
}

func (h *Handler) GetChatGPTWebBrowserRegion(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if h == nil || h.currentConfig() == nil {
		c.JSON(503, gin.H{"error": "configuration unavailable"})
		return
	}
	cfg := h.currentConfig().ChatGPTWeb
	zone := cfg.Timezone
	if zone == "" {
		zone = config.DefaultChatGPTWebTimezone
	}
	common := h.currentConfig().EffectiveProxyIP().Resolved()
	view := cfg.BrowserRegion.Resolved()
	view.LookupURL = common.LookupURL
	view.LookupTimeoutSeconds = common.LookupTimeoutSeconds
	// Legacy route settings are compatibility-only; proxy health owns observations.

	view.IPCacheSize = common.IPCacheSize
	c.JSON(200, chatGPTWebBrowserRegionResponse{view, true, zone, cfg.TimezoneOffsetMinutes})
}
func (h *Handler) PatchChatGPTWebBrowserRegion(c *gin.Context) {
	var fields map[string]json.RawMessage
	if err := decodeBrowserRegionBody(c, &fields); err != nil || fields == nil {
		c.JSON(400, gin.H{"error": "invalid browser region configuration"})
		return
	}
	if h == nil {
		c.JSON(503, gin.H{"error": "configuration unavailable"})
		return
	}
	h.mu.Lock()
	if h.cfg == nil {
		h.mu.Unlock()
		c.JSON(503, gin.H{"error": "configuration unavailable"})
		return
	}
	for _, key := range []string{"lookup-url", "lookup-timeout-seconds", "cache-ttl-seconds", "route-cache-size", "ip-cache-size", "ip-cache-ttl-seconds"} {
		if _, ok := fields[key]; ok {
			h.mu.Unlock()
			c.JSON(409, gin.H{"error": "shared_proxy_ip_settings", "message": "Edit common settings in proxy/IP management."})
			return
		}
	}
	previous := h.cfg.ChatGPTWeb
	candidate := previous
	if err := applyBrowserRegionPatch(&candidate, fields); err != nil {
		h.mu.Unlock()
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if err := candidate.Validate(); err != nil {
		h.mu.Unlock()
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	h.cfg.ChatGPTWeb = candidate
	if !h.persistLocked(c) {
		h.cfg.ChatGPTWeb = previous
		h.mu.Unlock()
		return
	}
	applied := h.configSnapshot.Load()
	manager := h.authManager
	h.mu.Unlock()
	if manager != nil {
		if registered, ok := manager.Executor(chatgptwebauth.Provider); ok {
			if updater, ok := registered.(chatGPTWebSentinelConfigUpdater); ok {
				updater.UpdateConfig(applied)
			}
		}
	}
}
func applyBrowserRegionPatch(cfg *config.ChatGPTWebConfig, fields map[string]json.RawMessage) error {
	for key, raw := range fields {
		var err error
		switch key {
		case "auto-timezone", "auto-language":
			var value bool
			value, err = decodeSentinelBool(raw)
			if key == "auto-timezone" {
				cfg.BrowserRegion.AutoTimezone = value
			} else {
				cfg.BrowserRegion.AutoLanguage = value
			}
		case "language", "lookup-url", "timezone":
			var value string
			value, err = decodeLoginProxyString(raw)
			switch key {
			case "language":
				cfg.BrowserRegion.Language = value
			case "lookup-url":
				cfg.BrowserRegion.LookupURL = value
			case "timezone":
				cfg.Timezone = value
			}
		case "lookup-timeout-seconds", "cache-ttl-seconds", "route-cache-size", "ip-cache-size", "timezone-offset-minutes":
			if key == "timezone-offset-minutes" && bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				cfg.TimezoneOffsetMinutes = nil
				continue
			}
			var value int
			value, err = decodeSentinelInt(raw)
			switch key {
			case "lookup-timeout-seconds":
				cfg.BrowserRegion.LookupTimeoutSeconds = &value
			case "cache-ttl-seconds":
				cfg.BrowserRegion.CacheTTLSeconds = &value
			case "route-cache-size":
				cfg.BrowserRegion.RouteCacheSize = &value
			case "ip-cache-size":
				cfg.BrowserRegion.IPCacheSize = &value
			case "timezone-offset-minutes":
				cfg.TimezoneOffsetMinutes = &value
			}
		default:
			return fmt.Errorf("unknown browser region field")
		}
		if err != nil {
			return fmt.Errorf("invalid browser region field: %s", key)
		}
	}
	return nil
}
func decodeBrowserRegionBody(c *gin.Context, value any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("unexpected trailing JSON")
	}
	return nil
}
func (h *Handler) GetChatGPTWebBrowserRegionPreview(c *gin.Context) { h.browserRegionPreview(c, false) }
func (h *Handler) RefreshChatGPTWebBrowserRegion(c *gin.Context)    { h.browserRegionPreview(c, true) }
func (h *Handler) browserRegionPreview(c *gin.Context, refresh bool) {
	c.Header("Cache-Control", "no-store")
	index := strings.TrimSpace(c.Query("auth_index"))
	if refresh {
		var request struct {
			AuthIndex string `json:"auth_index"`
		}
		if decodeBrowserRegionBody(c, &request) != nil {
			c.JSON(400, gin.H{"error": "invalid refresh request"})
			return
		}
		index = strings.TrimSpace(request.AuthIndex)
	}
	if index == "" || len(index) > 256 {
		c.JSON(400, gin.H{"error": "auth_index is required"})
		return
	}
	if h == nil {
		c.JSON(503, gin.H{"error": "runtime unavailable"})
		return
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		c.JSON(503, gin.H{"error": "runtime unavailable"})
		return
	}
	auth, found := manager.GetByAuthIndex(index)
	if !found {
		c.JSON(404, gin.H{"error": "credential not found"})
		return
	}
	if auth.Provider != chatgptwebauth.Provider {
		c.JSON(400, gin.H{"error": "ChatGPT Web credential required"})
		return
	}
	registered, _ := manager.Executor(chatgptwebauth.Provider)
	controller, ok := registered.(chatGPTWebBrowserRegionController)
	if !ok {
		c.JSON(503, gin.H{"error": "browser region runtime unavailable"})
		return
	}
	snapshot, code := controller.BrowserRegionPreview(auth, refresh)
	status := http.StatusOK
	if refresh {
		status = http.StatusAccepted
	}
	switch code {
	case "":
	case "route_unresolved":
		if refresh {
			status = 409
		}
	case "queue_full", "refresh_cooldown":
		status = 429
		snapshot.Reason = code
		if code == "queue_full" {
			snapshot.RetryAfterSeconds = max(30, snapshot.RetryAfterSeconds)
		}
		c.Header("Retry-After", strconv.FormatInt(max(1, snapshot.RetryAfterSeconds), 10))
	case "credential_invalid", "invalid_provider":
		status = 400
		snapshot.Reason = code
	default:
		status = 503
		snapshot.Reason = "unavailable"
	}
	c.JSON(status, struct {
		chatgptwebauth.BrowserRegionSnapshot
		Application proxyRegionApplicationView `json:"application"`
	}{snapshot, h.proxyRegionApplication(c.Request.Context(), auth, "")})
}
