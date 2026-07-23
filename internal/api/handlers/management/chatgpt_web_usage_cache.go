package management

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
)

type chatGPTWebUsageCacheSnapshotter interface {
	UsageCacheSnapshot() helps.ChatGPTWebUsageCacheSnapshot
}

type chatGPTWebUsageCacheRequest struct {
	EstimateTokenUsage json.RawMessage `json:"estimate-token-usage"`
	UsageCache         json.RawMessage `json:"usage-cache"`
	ImageUsage         json.RawMessage `json:"image-usage"`
}

type chatGPTWebUsageCacheSettingsRequest struct {
	Enabled         json.RawMessage `json:"enabled"`
	DiskThresholdMB json.RawMessage `json:"disk-threshold-mb"`
	MaxDiskSizeMB   json.RawMessage `json:"max-disk-size-mb"`
	Path            json.RawMessage `json:"path"`
}

type chatGPTWebImageUsageRequest struct {
	AutoOutputQuality json.RawMessage `json:"auto-output-quality"`
}

type chatGPTWebImageUsageResponse struct {
	AutoOutputQuality string `json:"auto-output-quality"`
}

type chatGPTWebUsageCacheResponse struct {
	EstimateTokenUsage bool                                      `json:"estimate-token-usage"`
	UsageCache         config.ResolvedChatGPTWebUsageCacheConfig `json:"usage-cache"`
	ImageUsage         chatGPTWebImageUsageResponse              `json:"image-usage"`
	Stats              helps.ChatGPTWebUsageCacheSnapshot        `json:"stats"`
}

// GetChatGPTWebUsageCache returns effective accounting settings and runtime storage statistics.
func (h *Handler) GetChatGPTWebUsageCache(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	h.mu.Lock()
	if h.cfg == nil {
		h.mu.Unlock()
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	response := chatGPTWebUsageCacheResponse{
		EstimateTokenUsage: h.cfg.ChatGPTWeb.TokenUsageEstimationEnabled(),
		UsageCache:         h.cfg.ChatGPTWeb.UsageCache.Resolved(),
		ImageUsage: chatGPTWebImageUsageResponse{
			AutoOutputQuality: h.cfg.ChatGPTWeb.ImageUsage.ResolvedAutoOutputQuality(),
		},
	}
	if h.authManager != nil {
		if registered, ok := h.authManager.Executor(chatgptwebauth.Provider); ok {
			if snapshotter, okSnapshotter := registered.(chatGPTWebUsageCacheSnapshotter); okSnapshotter {
				response.Stats = snapshotter.UsageCacheSnapshot()
			}
		}
	}
	h.mu.Unlock()
	c.JSON(http.StatusOK, response)
}

// PutChatGPTWebUsageCache replaces all Web accounting settings.
func (h *Handler) PutChatGPTWebUsageCache(c *gin.Context) {
	h.updateChatGPTWebUsageCache(c, true)
}

// PatchChatGPTWebUsageCache updates supplied Web accounting settings.
func (h *Handler) PatchChatGPTWebUsageCache(c *gin.Context) {
	h.updateChatGPTWebUsageCache(c, false)
}

func (h *Handler) updateChatGPTWebUsageCache(c *gin.Context, replace bool) {
	request, errDecode := decodeChatGPTWebUsageCacheRequest(c)
	if errDecode != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errDecode.Error()})
		return
	}
	if replace && !request.complete() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "all ChatGPT Web usage fields are required"})
		return
	}
	if h == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	previous := h.cfg.ChatGPTWeb
	candidate := previous
	if replace {
		candidate.EstimateTokenUsage = nil
		candidate.UsageCache = config.ChatGPTWebUsageCacheConfig{}
		candidate.ImageUsage = config.ChatGPTWebImageUsageConfig{}
	}
	if errApply := request.apply(&candidate, replace); errApply != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errApply.Error()})
		return
	}
	if errValidate := candidate.Validate(); errValidate != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errValidate.Error()})
		return
	}
	h.cfg.ChatGPTWeb = candidate
	if !h.persistLocked(c) {
		if h.cfg != nil {
			h.cfg.ChatGPTWeb = previous
		}
		return
	}
	if h.authManager != nil {
		if registered, ok := h.authManager.Executor(chatgptwebauth.Provider); ok {
			if updater, okUpdater := registered.(chatGPTWebSentinelConfigUpdater); okUpdater {
				updater.UpdateConfig(h.cfg)
			}
		}
	}
}

func decodeChatGPTWebUsageCacheRequest(c *gin.Context) (chatGPTWebUsageCacheRequest, error) {
	var raw json.RawMessage
	decoder := json.NewDecoder(c.Request.Body)
	if errDecode := decoder.Decode(&raw); errDecode != nil {
		return chatGPTWebUsageCacheRequest{}, fmt.Errorf("invalid body: %w", errDecode)
	}
	if errTrailing := decoder.Decode(&struct{}{}); errTrailing != io.EOF {
		return chatGPTWebUsageCacheRequest{}, fmt.Errorf("invalid body")
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return chatGPTWebUsageCacheRequest{}, fmt.Errorf("invalid body: object required")
	}
	var request chatGPTWebUsageCacheRequest
	strict := json.NewDecoder(bytes.NewReader(trimmed))
	strict.DisallowUnknownFields()
	if errDecode := strict.Decode(&request); errDecode != nil {
		return request, fmt.Errorf("invalid body: %w", errDecode)
	}
	return request, nil
}

func (request chatGPTWebUsageCacheRequest) complete() bool {
	if len(request.EstimateTokenUsage) == 0 || len(request.UsageCache) == 0 || len(request.ImageUsage) == 0 {
		return false
	}
	usageCache, errCache := decodeChatGPTWebUsageCacheSettings(request.UsageCache)
	imageUsage, errImage := decodeChatGPTWebImageUsage(request.ImageUsage)
	return errCache == nil && errImage == nil && usageCache.complete() && imageUsage.complete()
}

func (request chatGPTWebUsageCacheRequest) apply(candidate *config.ChatGPTWebConfig, replace bool) error {
	if candidate == nil {
		return fmt.Errorf("configuration unavailable")
	}
	if len(request.EstimateTokenUsage) > 0 {
		value, errValue := decodeSentinelBool(request.EstimateTokenUsage)
		if errValue != nil {
			return fmt.Errorf("invalid estimate-token-usage")
		}
		candidate.EstimateTokenUsage = &value
	}
	if len(request.UsageCache) > 0 {
		decoded, errCache := decodeChatGPTWebUsageCacheSettings(request.UsageCache)
		if errCache != nil {
			return errCache
		}
		if replace && !decoded.complete() {
			return fmt.Errorf("all usage-cache fields are required")
		}
		if errApply := decoded.apply(&candidate.UsageCache); errApply != nil {
			return errApply
		}
	}
	if len(request.ImageUsage) > 0 {
		decoded, errImage := decodeChatGPTWebImageUsage(request.ImageUsage)
		if errImage != nil {
			return errImage
		}
		if replace && !decoded.complete() {
			return fmt.Errorf("all image-usage fields are required")
		}
		if len(decoded.AutoOutputQuality) > 0 {
			var value string
			if errValue := json.Unmarshal(decoded.AutoOutputQuality, &value); errValue != nil {
				return fmt.Errorf("invalid auto-output-quality")
			}
			candidate.ImageUsage.AutoOutputQuality = value
		}
	}
	return nil
}

func decodeChatGPTWebUsageCacheSettings(raw json.RawMessage) (chatGPTWebUsageCacheSettingsRequest, error) {
	var request chatGPTWebUsageCacheSettingsRequest
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return request, fmt.Errorf("usage-cache must be an object")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if errDecode := decoder.Decode(&request); errDecode != nil {
		return request, fmt.Errorf("invalid usage-cache: %w", errDecode)
	}
	return request, nil
}

func (request chatGPTWebUsageCacheSettingsRequest) complete() bool {
	return len(request.Enabled) > 0 && len(request.DiskThresholdMB) > 0 && len(request.MaxDiskSizeMB) > 0 && len(request.Path) > 0
}

func (request chatGPTWebUsageCacheSettingsRequest) apply(candidate *config.ChatGPTWebUsageCacheConfig) error {
	if len(request.Enabled) > 0 {
		value, errValue := decodeSentinelBool(request.Enabled)
		if errValue != nil {
			return fmt.Errorf("invalid usage-cache.enabled")
		}
		candidate.Enabled = &value
	}
	decodeInt64 := func(name string, raw json.RawMessage, target **int64) error {
		if len(raw) == 0 {
			return nil
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("invalid %s", name)
		}
		var value int64
		if errValue := json.Unmarshal(raw, &value); errValue != nil {
			return fmt.Errorf("invalid %s", name)
		}
		*target = &value
		return nil
	}
	if errThreshold := decodeInt64("usage-cache.disk-threshold-mb", request.DiskThresholdMB, &candidate.DiskThresholdMB); errThreshold != nil {
		return errThreshold
	}
	if errMax := decodeInt64("usage-cache.max-disk-size-mb", request.MaxDiskSizeMB, &candidate.MaxDiskSizeMB); errMax != nil {
		return errMax
	}
	if len(request.Path) > 0 {
		if bytes.Equal(bytes.TrimSpace(request.Path), []byte("null")) {
			return fmt.Errorf("invalid usage-cache.path")
		}
		if errValue := json.Unmarshal(request.Path, &candidate.Path); errValue != nil {
			return fmt.Errorf("invalid usage-cache.path")
		}
	}
	return nil
}

func decodeChatGPTWebImageUsage(raw json.RawMessage) (chatGPTWebImageUsageRequest, error) {
	var request chatGPTWebImageUsageRequest
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return request, fmt.Errorf("image-usage must be an object")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if errDecode := decoder.Decode(&request); errDecode != nil {
		return request, fmt.Errorf("invalid image-usage: %w", errDecode)
	}
	return request, nil
}

func (request chatGPTWebImageUsageRequest) complete() bool {
	return len(request.AutoOutputQuality) > 0
}
