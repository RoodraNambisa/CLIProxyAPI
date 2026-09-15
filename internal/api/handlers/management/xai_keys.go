package management

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func normalizeXAIKey(entry *config.XAIKey) {
	normalizeCodexKey(entry)
	if entry.BaseURL == "" {
		entry.BaseURL, _ = config.XAIBaseURLForMode("api")
	}
	if canonical, errURL := config.NormalizeXAIBaseURL(entry.BaseURL); errURL == nil {
		entry.BaseURL = canonical
	}
	entry.AlphaSearch = false
}

// xai-api-key: []XAIKey
func (h *Handler) GetXAIKeys(c *gin.Context) {
	c.JSON(200, gin.H{"xai-api-key": h.xaiKeysWithAuthIndex()})
}
func (h *Handler) PutXAIKeys(c *gin.Context) {
	data, err := c.GetRawData()
	if err != nil {
		c.JSON(400, gin.H{"error": "failed to read body"})
		return
	}
	var arr []config.XAIKey
	if err = json.Unmarshal(data, &arr); err != nil {
		var obj struct {
			Items []config.XAIKey `json:"items"`
		}
		if err2 := json.Unmarshal(data, &obj); err2 != nil || len(obj.Items) == 0 {
			c.JSON(400, gin.H{"error": "invalid body"})
			return
		}
		arr = obj.Items
	}
	filtered := make([]config.XAIKey, 0, len(arr))
	for i := range arr {
		entry := arr[i]
		normalizeXAIKey(&entry)
		filtered = append(filtered, entry)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if errRestore := restoreMaskedProxyURLs(filtered, h.cfg.XAIKey,
		func(entry config.XAIKey) string { return entry.APIKey + "\x00" + entry.BaseURL },
		func(entry *config.XAIKey) *string { return &entry.ProxyURL }, replaceMaskedProxyRequested(c)); errRestore != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errRestore.Error()})
		return
	}
	previous := append([]config.XAIKey(nil), h.cfg.XAIKey...)
	h.cfg.XAIKey = filtered
	h.cfg.SanitizeXAIKeys()
	if !h.persistLocked(c) {
		h.cfg.XAIKey = previous
	}
}
func (h *Handler) PatchXAIKey(c *gin.Context) {
	type xaiKeyPatch struct {
		Weight              credentialWeightPatch       `json:"weight"`
		RequestRetry        credentialRequestRetryPatch `json:"request-retry"`
		RequestScopedErrors requestScopedErrorsPatch    `json:"request-scoped-errors"`
		APIKey              *string                     `json:"api-key"`
		Prefix              *string                     `json:"prefix"`
		BaseURL             *string                     `json:"base-url"`
		ProxyURL            *string                     `json:"proxy-url"`
		Models              *[]config.CodexModel        `json:"models"`
		Headers             *map[string]string          `json:"headers"`
		ExcludedModels      *[]string                   `json:"excluded-models"`
	}
	var body struct {
		Index *int         `json:"index"`
		Match *string      `json:"match"`
		Value *xaiKeyPatch `json:"value"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.Value == nil {
		c.JSON(400, gin.H{"error": "invalid body"})
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	targetIndex := -1
	if body.Index != nil && *body.Index >= 0 && *body.Index < len(h.cfg.XAIKey) {
		targetIndex = *body.Index
	}
	if targetIndex == -1 && body.Match != nil {
		match := strings.TrimSpace(*body.Match)
		matchCount := 0
		for i := range h.cfg.XAIKey {
			if h.cfg.XAIKey[i].APIKey == match {
				matchCount++
				targetIndex = i
			}
		}
		if matchCount > 1 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "multiple items match api-key; index is required"})
			return
		}
	}
	if targetIndex == -1 {
		c.JSON(404, gin.H{"error": "item not found"})
		return
	}

	previous := append([]config.XAIKey(nil), h.cfg.XAIKey...)
	entry := h.cfg.XAIKey[targetIndex]
	if body.Value.Weight.set {
		entry.Weight = body.Value.Weight.value
	}
	if body.Value.RequestRetry.set {
		entry.RequestRetry = body.Value.RequestRetry.value
	}
	if body.Value.RequestScopedErrors.set {
		entry.RequestScopedErrors = body.Value.RequestScopedErrors.value
	}
	if body.Value.APIKey != nil {
		entry.APIKey = strings.TrimSpace(*body.Value.APIKey)
	}
	if body.Value.Prefix != nil {
		entry.Prefix = strings.TrimSpace(*body.Value.Prefix)
	}
	if body.Value.BaseURL != nil {
		entry.BaseURL = strings.TrimSpace(*body.Value.BaseURL)
	}
	if body.Value.ProxyURL != nil {
		proxyURL, errProxy := applyProxyURLPatch(entry.ProxyURL, body.Value.ProxyURL, replaceMaskedProxyRequested(c))
		if errProxy != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errProxy.Error()})
			return
		}
		entry.ProxyURL = proxyURL
	}
	if body.Value.Models != nil {
		entry.Models = append([]config.CodexModel(nil), (*body.Value.Models)...)
	}
	if body.Value.Headers != nil {
		entry.Headers = config.NormalizeHeaders(*body.Value.Headers)
	}
	if body.Value.ExcludedModels != nil {
		entry.ExcludedModels = config.NormalizeExcludedModels(*body.Value.ExcludedModels)
	}
	normalizeXAIKey(&entry)
	h.cfg.XAIKey[targetIndex] = entry
	h.cfg.SanitizeXAIKeys()
	if !h.persistLocked(c) {
		h.cfg.XAIKey = previous
	}
}

func (h *Handler) DeleteXAIKey(c *gin.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()
	previous := append([]config.XAIKey(nil), h.cfg.XAIKey...)
	if val := strings.TrimSpace(c.Query("api-key")); val != "" {
		if baseRaw, okBase := c.GetQuery("base-url"); okBase {
			selector := config.XAIKey{BaseURL: baseRaw}
			normalizeXAIKey(&selector)
			base := selector.BaseURL
			out := make([]config.XAIKey, 0, len(h.cfg.XAIKey))
			for _, v := range h.cfg.XAIKey {
				if strings.TrimSpace(v.APIKey) == val && strings.TrimSpace(v.BaseURL) == base {
					continue
				}
				out = append(out, v)
			}
			h.cfg.XAIKey = out
			h.cfg.SanitizeXAIKeys()
			if !h.persistLocked(c) {
				h.cfg.XAIKey = previous
			}
			return
		}

		matchIndex := -1
		matchCount := 0
		for i := range h.cfg.XAIKey {
			if strings.TrimSpace(h.cfg.XAIKey[i].APIKey) == val {
				matchCount++
				if matchIndex == -1 {
					matchIndex = i
				}
			}
		}
		if matchCount > 1 {
			c.JSON(400, gin.H{"error": "multiple items match api-key; base-url is required"})
			return
		}
		if matchIndex != -1 {
			h.cfg.XAIKey = append(h.cfg.XAIKey[:matchIndex], h.cfg.XAIKey[matchIndex+1:]...)
		}
		h.cfg.SanitizeXAIKeys()
		if !h.persistLocked(c) {
			h.cfg.XAIKey = previous
		}
		return
	}
	if idxStr := c.Query("index"); idxStr != "" {
		var idx int
		_, err := fmt.Sscanf(idxStr, "%d", &idx)
		if err == nil && idx >= 0 && idx < len(h.cfg.XAIKey) {
			h.cfg.XAIKey = append(h.cfg.XAIKey[:idx], h.cfg.XAIKey[idx+1:]...)
			h.cfg.SanitizeXAIKeys()
			if !h.persistLocked(c) {
				h.cfg.XAIKey = previous
			}
			return
		}
	}
	c.JSON(400, gin.H{"error": "missing api-key or index"})
}
