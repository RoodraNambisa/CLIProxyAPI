package management

import (
	"cmp"
	"net/http"
	"slices"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type codexStateCredentialOption struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Alias    string `json:"alias,omitempty"`
	Priority int    `json:"priority"`
	Plan     string `json:"plan"`
	Disabled bool   `json:"disabled"`
}

// CheckCodexStateProxy expands one temporary proxy session using the acquisition
// rules. It sends no account headers, acquires no State, and persists nothing.
func (h *Handler) CheckCodexStateProxy(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 8192)
	var body proxyCheckRequest
	if c.ShouldBindJSON(&body) != nil {
		c.JSON(400, gin.H{"error": "invalid proxy test request"})
		return
	}
	policy := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{ProxyMode: "custom", ProxyURL: strings.TrimSpace(body.ProxyURL)}}}
	if err := policy.ValidateCodexStateOverride(); err != nil {
		c.JSON(400, gin.H{"error": "invalid State proxy URL or placeholder; use HTTP/HTTPS/SOCKS5 and {1} through {64}"})
		return
	}
	expanded, err := codexstate.ExpandProxy(policy.Codex.StateOverride.ProxyURL)
	if err != nil {
		c.JSON(500, gin.H{"error": "could not expand State proxy placeholder"})
		return
	}
	h.writeProxyURLCheck(c, expanded)
}

type codexStateModelOption struct {
	ID         string `json:"id"`
	UpstreamID string `json:"upstream_id"`
}

// GetCodexStateOptions reads only local routing metadata and the registered catalog.
// Discovery works before enabling State and never starts acquisition or upstream traffic.
func (h *Handler) GetCodexStateOptions(c *gin.Context) { h.getCodexStateOptions(c, false) }

func (h *Handler) GetCodexResponseGuardOptions(c *gin.Context) { h.getCodexStateOptions(c, true) }

func (h *Handler) getCodexStateOptions(c *gin.Context, guard bool) {
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		c.JSON(503, gin.H{"error": "credential manager unavailable"})
		return
	}
	credentials := []codexStateCredentialOption{}
	models := []codexStateModelOption{}
	priorities := []int{}
	plans := []string{}
	seenModels := map[codexStateModelOption]bool{}
	policy := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true}}}
	for _, a := range manager.List() {
		if a == nil || a.ExecutionProvider() != "codex" || a.RuntimeInstanceRetired() || !guard && a.Attributes["api_key"] != "" {
			continue
		}
		id := strings.TrimSpace(a.Index)
		if id == "" {
			id = a.ID
		}
		name := a.FileName
		if name == "" {
			name = a.ID
		}
		priority := helps.StateCredentialPriority(a)
		plan := helps.StateCredential(a, "").Plan
		credentials = append(credentials, codexStateCredentialOption{ID: id, Name: name, Alias: auth.CredentialRoutingAlias(a), Priority: priority, Plan: plan, Disabled: a.Disabled || a.Status == auth.StatusDisabled})
		if !slices.Contains(priorities, priority) {
			priorities = append(priorities, priority)
		}
		if !slices.Contains(plans, plan) {
			plans = append(plans, plan)
		}
		allowed := map[string]bool{}
		for _, model := range helps.ManagedStateModels(policy, a) {
			allowed[model.Model] = true
		}
		for _, info := range registry.GetGlobalRegistry().GetModelsForClient(a.ID) {
			if info == nil {
				continue
			}
			upstream := info.UpstreamID
			if upstream == "" {
				upstream = info.ID
			}
			upstream = thinking.ParseSuffix(upstream).ModelName
			if !allowed[upstream] && !guard {
				continue
			}
			if guard && len(info.SupportedOutputModalities) > 0 && !slices.ContainsFunc(info.SupportedOutputModalities, func(value string) bool { return strings.EqualFold(value, "text") }) {
				continue
			}
			option := codexStateModelOption{ID: thinking.ParseSuffix(info.ID).ModelName, UpstreamID: upstream}
			if !seenModels[option] {
				seenModels[option] = true
				models = append(models, option)
			}
		}
	}
	slices.SortFunc(credentials, func(a, b codexStateCredentialOption) int {
		return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.ID, b.ID))
	})
	slices.SortFunc(models, func(a, b codexStateModelOption) int {
		return cmp.Or(cmp.Compare(a.ID, b.ID), cmp.Compare(a.UpstreamID, b.UpstreamID))
	})
	slices.Sort(priorities)
	slices.Sort(plans)
	autoCookieEnabled := false
	if cfg := h.currentConfig(); cfg != nil {
		autoCookieEnabled = cfg.Codex.AutoCookie
	}
	c.JSON(200, gin.H{"credentials": credentials, "models": models, "priorities": priorities, "plans": plans, "auto_cookie_enabled": autoCookieEnabled, "features": gin.H{"rule_model_overrides": true, "state_retry_rounds": true, "cookie_model_rules": true, "cookie_backup_pool": true, "cookie_only": true, "auto_cookie": true, "state_seconds": true, "response_guard": true}})
}
