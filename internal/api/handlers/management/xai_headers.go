package management

import (
	"net/http"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	xaiauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/xai"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

func (h *Handler) GetXAIOptions(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		c.JSON(503, gin.H{"error": "credential manager unavailable"})
		return
	}
	credentials := []gin.H{}
	for _, auth := range manager.List() {
		if auth == nil || auth.ExecutionProvider() != "xai" || auth.RuntimeInstanceRetired() {
			continue
		}
		name := auth.FileName
		if name == "" {
			name = auth.ID
		}
		models := []string{}
		for _, model := range registry.GetGlobalRegistry().GetModelsForClient(auth.ID) {
			if model != nil {
				models = append(models, model.ID)
			}
		}
		if len(models) == 0 {
			for _, model := range helps.XAIMergedModelsForAuth(auth, h.currentConfig()) {
				models = append(models, model.ID)
			}
		}
		sort.Strings(models)
		credentials = append(credentials, gin.H{"name": name, "id": auth.EnsureIndex(), "models": models, "disabled": auth.Disabled})
	}
	sort.Slice(credentials, func(i, j int) bool { return credentials[i]["name"].(string) < credentials[j]["name"].(string) })
	version, source, updated := xaiauth.ClientVersionStatus()
	if cfg := h.currentConfig(); cfg != nil && cfg.XAI.HeaderDefaults.ClientVersion != "" {
		version, source = cfg.XAI.HeaderDefaults.ClientVersion, "global-default"
	}
	c.JSON(200, gin.H{"credentials": credentials, "client_version": version, "version_source": source, "version_updated_at": updated,
		"capabilities": gin.H{"headers_preview": true, "xai_diagnostics": true, "request_compression_diagnostics": true, "credential_capabilities": true, "image_capability_declarations": true, "official_build_headers": true}})
}

func (h *Handler) PreviewXAIHeaders(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 512*1024)
	var input helps.XAIHeaderPreviewInput
	if c.ShouldBindJSON(&input) != nil {
		c.JSON(400, gin.H{"error": "invalid Grok header preview"})
		return
	}
	input.Name, input.Model = strings.TrimSpace(input.Name), strings.TrimSpace(input.Model)
	if input.Name == "" || len(input.Name) > 512 || input.Model == "" || len(input.Model) > 256 || strings.ContainsAny(input.Model, "\r\n\x00") || len(input.RequestBody) > 128*1024 || len(input.RequestBody) > 0 && (!gjson.ValidBytes(input.RequestBody) || !gjson.ParseBytes(input.RequestBody).IsObject()) {
		c.JSON(400, gin.H{"error": "credential and model are required; preview body must be valid JSON"})
		return
	}
	if err := helps.ValidateXAIClientHeaders(input.ClientHeaders); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if input.Purpose == "" {
		input.Purpose = "inference"
	}
	switch input.Purpose {
	case "inference", "models", "quota", "device-login", "token-exchange":
	default:
		c.JSON(400, gin.H{"error": "unsupported preview purpose"})
		return
	}
	if input.Protocol == "" {
		input.Protocol = "responses"
	}
	switch input.Protocol {
	case "responses", "chat", "websocket":
	default:
		c.JSON(400, gin.H{"error": "unsupported preview protocol"})
		return
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		c.JSON(503, gin.H{"error": "credential manager unavailable"})
		return
	}
	auth := h.findManagedAuthWithManager(input.Name, manager)
	if auth == nil || auth.ExecutionProvider() != "xai" {
		c.JSON(404, gin.H{"error": "Grok credential not found"})
		return
	}
	cfg, err := config.Clone(h.currentConfig())
	if err != nil {
		c.JSON(500, gin.H{"error": "configuration unavailable"})
		return
	}
	if cfg == nil {
		cfg = &config.Config{}
	}
	if input.Config != nil {
		cfg.XAI = input.Config.Clone()
	}
	if cfg.XAI.SessionIdentityPoolSize == 0 {
		cfg.XAI.SessionIdentityPoolSize = config.DefaultXAISessionIdentityPoolSize
	}
	if err = cfg.XAI.Validate(); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	req, err := manager.PreviewCredentialRequest(auth, core.Request{Model: input.Model})
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	ctx := helps.WithXAIDiagnostics(c.Request.Context(), true, nil)
	result, err := runtimeexecutor.NewXAIExecutor(cfg).PreviewHeaders(ctx, auth, input, req)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, result)
}
