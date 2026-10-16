package management

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxyregion"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
)

func (h *Handler) GetProxyIP(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(503, gin.H{"error": "configuration unavailable"})
		return
	}
	commonBytes, _ := json.Marshal(cfg.EffectiveProxyIP().Resolved())
	common := map[string]any{}
	_ = json.Unmarshal(commonBytes, &common)
	health, _ := config.NormalizeProxyHealthCheckConfiguration(cfg.EffectiveProxyHealthCheck())
	common["temporary-route-check-interval-seconds"] = *health.TemporaryRouteCheckIntervalSeconds
	common["temporary-route-cache-size"] = *health.TemporaryRouteCacheSize
	c.JSON(200, gin.H{"config": common, "proxy_region": cfg.ProxyRegion, "legacy": cfg.ProxyIP == nil, "capabilities": gin.H{"proxy_ip": true, "scheduled_refresh": true, "cold_cache_wait": true, "codex_time_context": true, "credential_region": true, "region_lifecycle": true, "proxy_region": true}})
}
func (h *Handler) PatchProxyIP(c *gin.Context) {
	var fields map[string]json.RawMessage
	if decodeBrowserRegionBody(c, &fields) != nil || fields == nil {
		c.JSON(400, gin.H{"error": "invalid proxy IP settings"})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		c.JSON(503, gin.H{"error": "configuration unavailable"})
		return
	}
	candidate := h.cfg.EffectiveProxyIP()
	region := h.cfg.ProxyRegion
	if raw, ok := fields["proxy-region"]; ok {
		if err := strictRegionJSON(raw, &region); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		if err := region.Validate(); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}
		delete(fields, "proxy-region")
	}

	health := h.cfg.EffectiveProxyHealthCheck()
	for _, key := range []string{"temporary-route-check-interval-seconds", "temporary-route-cache-size"} {
		if raw, ok := fields[key]; ok {
			value, err := decodeSentinelInt(raw)
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}
			if key == "temporary-route-cache-size" {
				health.TemporaryRouteCacheSize = &value
			} else {
				health.TemporaryRouteCheckIntervalSeconds = &value
			}
			delete(fields, key)
		}
	}
	var errHealth error
	health, errHealth = config.NormalizeProxyHealthCheckConfiguration(health)
	if errHealth != nil {
		c.JSON(400, gin.H{"error": errHealth.Error()})
		return
	}
	// Decode into the existing configuration to preserve unspecified fields.
	b, _ := json.Marshal(fields)
	if err := strictRegionJSON(b, &candidate); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if err := candidate.Validate(); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	previousRegion := h.cfg.ProxyRegion
	h.cfg.ProxyRegion = region
	previous := h.cfg.ProxyIP
	previousHealth := h.cfg.ProxyHealthCheck
	h.cfg.ProxyHealthCheck = health
	h.cfg.ProxyIP = &candidate
	if !h.persistLocked(c) {
		h.cfg.ProxyIP = previous
		h.cfg.ProxyRegion = previousRegion
		h.cfg.ProxyHealthCheck = previousHealth
	}
}
func (h *Handler) GetProxyIPStatus(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	h.mu.Lock()
	manager := h.proxyPoolManager
	h.mu.Unlock()
	if manager == nil {
		c.JSON(503, gin.H{"error": "proxy IP service unavailable"})
		return
	}
	offset, _ := strconv.Atoi(c.Query("offset"))
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	entries, routes, ips := manager.IPRegions().Status(offset, limit)
	c.JSON(200, gin.H{"counts": manager.IPRegions().Counts(), "entries": entries, "route_count": routes, "ip_count": ips, "offset": max(0, offset), "limit": min(200, max(1, limit))})
}
func (h *Handler) PreviewProxyIP(c *gin.Context) { h.proxyIPLookup(c, false) }
func (h *Handler) RefreshProxyIP(c *gin.Context) { h.proxyIPLookup(c, true) }
func (h *Handler) proxyIPLookup(c *gin.Context, refresh bool) {
	var request struct {
		AuthIndex string `json:"auth_index"`
	}
	if decodeBrowserRegionBody(c, &request) != nil || request.AuthIndex == "" {
		c.JSON(400, gin.H{"error": "auth_index is required"})
		return
	}
	h.mu.Lock()
	manager, authManager := h.proxyPoolManager, h.authManager
	h.mu.Unlock()
	if manager == nil || authManager == nil {
		c.JSON(503, gin.H{"error": "runtime unavailable"})
		return
	}
	auth, ok := authManager.GetByAuthIndex(request.AuthIndex)
	if !ok {
		c.JSON(404, gin.H{"error": "credential not found"})
		return
	}
	// PreviewProxyAuth never allocates a binding or probes the network.
	resolved, err := authManager.PreviewProxyAuth(auth)
	if err != nil || resolved.Pending {
		c.JSON(409, gin.H{"error": "route_unresolved"})
		return
	}
	routed := auth.Clone()
	routed.ProxyURL = resolved.URL
	routed.RuntimeProxyURL = ""
	target := proxyregion.ChatGPTScope
	if auth.Provider == "codex" {
		target = helps.ResolveCodexUpstream(routed, h.currentConfig()).BaseURL
	}
	route := proxyregion.Route{ProxyURL: helps.ResolvedRequestProxy(h.currentConfig(), routed, target), BindingID: resolved.BindingID, OwnerID: auth.ID, Scope: proxyregion.ScopeForTarget(target)}
	service := manager.IPRegions()
	reason := ""
	if refresh {
		reason = service.Refresh(route)
	}
	snapshot := service.Preview(route)
	manual, source := service.Manual(route)
	if manual.Timezone != "" {
		snapshot.Timezone = manual.Timezone
		snapshot.TimezoneSource = source
	}
	if manual.Language != "" {
		snapshot.SuggestedLanguage = manual.Language
		snapshot.LanguageSource = source
	}

	if manual.Timezone != "" && (auth.Provider == "codex" || manual.Language != "") {
		snapshot.CacheState = "manual"
		snapshot.Reason = ""
	}
	status := 200
	if refresh {
		status = 202
	}
	if reason != "" {
		snapshot.Reason = reason
		status = 409
		if reason == "refresh_cooldown" || reason == "queue_full" {
			status = 429
			c.Header("Retry-After", strconv.FormatInt(max(1, snapshot.RetryAfterSeconds), 10))
		}
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(status, snapshot)
}
func (h *Handler) GetCodexTimeContext(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(503, gin.H{"error": "configuration unavailable"})
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(200, gin.H{"config": cfg.Codex.TimeContext.Resolved(), "capabilities": gin.H{"codex_time_context": true, "credential_region": true, "proxy_ip": true}})
}
func (h *Handler) PatchCodexTimeContext(c *gin.Context) {
	var fields map[string]json.RawMessage
	if decodeBrowserRegionBody(c, &fields) != nil || fields == nil {
		c.JSON(400, gin.H{"error": "invalid time context configuration"})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		c.JSON(503, gin.H{"error": "configuration unavailable"})
		return
	}
	candidate := h.cfg.Codex.TimeContext
	b, _ := json.Marshal(fields)
	if err := strictRegionJSON(b, &candidate); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if candidate.Mode == "inherit" || candidate.Mode == "off" {
		c.JSON(400, gin.H{"error": "global mode must be proxy or manual"})
		return
	}
	if err := candidate.Validate(); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	previous := h.cfg.Codex.TimeContext
	h.cfg.Codex.TimeContext = candidate
	if !h.persistLocked(c) {
		h.cfg.Codex.TimeContext = previous
	}
}
func strictRegionJSON(data []byte, target any) error {
	// Reject null patches: silently treating null as zero would erase explicit settings.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for key, value := range fields {
		if string(value) == "null" {
			return fmt.Errorf("%s cannot be null", key)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}
