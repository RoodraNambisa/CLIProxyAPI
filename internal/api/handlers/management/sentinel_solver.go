package management

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelconfig"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelservice"
)

// NewSentinelOnlyHandler avoids token stores, journals and business task managers.
func NewSentinelOnlyHandler(cfg *config.Config, path string) (*Handler, error) {
	secret := strings.TrimSpace(os.Getenv("MANAGEMENT_PASSWORD"))
	h := &Handler{cfg: cfg, configFilePath: path, failedAttempts: map[string]*attemptInfo{}, envSecret: secret, allowRemoteOverride: secret != "", sentinelOnly: true}
	if err := h.publishConfigSnapshot(cfg); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cleanupCancel = cancel
	h.startAttemptCleanup(ctx)
	return h, nil
}

func (h *Handler) SetSentinelSolver(service *sentinelservice.Service) {
	h.mu.Lock()
	h.sentinelSolver = service
	h.mu.Unlock()
}

func (h *Handler) GetRuntimeCapabilities(c *gin.Context) {
	role := "proxy"
	if h.sentinelOnly {
		role = "sentinel-solver"
	}
	c.JSON(http.StatusOK, gin.H{"role": role, "sentinel_solver": true, "sentinel_protocol": chatgptweb.SentinelComputeProtocol})
}

func (h *Handler) GetSentinelSolver(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(503, gin.H{"error": "configuration unavailable"})
		return
	}
	h.mu.Lock()
	service := h.sentinelSolver
	h.mu.Unlock()
	var status any
	if service != nil {
		status = service.Snapshot()
	}
	c.JSON(200, gin.H{"config": cfg.SentinelSolver, "status": status})
}

func (h *Handler) PatchSentinelSolver(c *gin.Context) {
	raw, err := io.ReadAll(io.LimitReader(c.Request.Body, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		c.JSON(400, gin.H{"error": "invalid body"})
		return
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(raw, &fields); err != nil || fields == nil {
		c.JSON(400, gin.H{"error": "object required"})
		return
	}
	// Old clients may still submit the retired listener settings, including null.
	delete(fields, "listen")
	delete(fields, "tls")
	h.mu.Lock()
	if h.cfg == nil {
		h.mu.Unlock()
		c.JSON(503, gin.H{"error": "configuration unavailable"})
		return
	}
	before := h.cfg.SentinelSolver
	data, _ := json.Marshal(before)
	var merged map[string]json.RawMessage
	_ = json.Unmarshal(data, &merged)
	for key, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			h.mu.Unlock()
			c.JSON(400, gin.H{"error": "null settings are not allowed"})
			return
		}
		merged[key] = value
	}
	data, _ = json.Marshal(merged)
	var candidate sentinelconfig.Server
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&candidate); err != nil {
		h.mu.Unlock()
		c.JSON(400, gin.H{"error": "invalid solver settings"})
		return
	}
	validationConfig := *h.cfg
	validationConfig.SentinelSolver = candidate
	if err = validationConfig.ValidateSentinelSolver(); err != nil {
		h.mu.Unlock()
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	h.cfg.SentinelSolver = candidate
	if !h.persistLocked(c) {
		h.cfg.SentinelSolver = before
	}
	h.mu.Unlock()
}

func (h *Handler) TestSentinelNode(c *gin.Context) {
	var node sentinelconfig.Node
	decoder := json.NewDecoder(io.LimitReader(c.Request.Body, 16<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&node); err != nil {
		c.JSON(400, gin.H{"error": "invalid node"})
		return
	}
	status, err := chatgptweb.TestSentinelComputeNode(c.Request.Context(), node)
	if err != nil {
		c.JSON(502, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, status)
}
