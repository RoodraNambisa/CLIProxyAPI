package management

import (
	"encoding/json"
	"maps"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func (h *Handler) GetOAuthRequestScopedErrors(c *gin.Context) {
	rules := map[string][]config.RequestScopedErrorRule{}
	if cfg := h.currentConfig(); cfg != nil && cfg.OAuthRequestScopedErrors != nil {
		rules = cfg.OAuthRequestScopedErrors
	}
	c.JSON(http.StatusOK, gin.H{"oauth-request-scoped-errors": rules})
}

func (h *Handler) PutOAuthRequestScopedErrors(c *gin.Context) {
	data, err := c.GetRawData()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read body"})
		return
	}
	var entries map[string][]config.RequestScopedErrorRule
	var envelope map[string]json.RawMessage
	if json.Unmarshal(data, &envelope) == nil && len(envelope) == 1 {
		if item, exists := envelope["items"]; exists {
			var wrapped map[string][]config.RequestScopedErrorRule
			if json.Unmarshal(item, &wrapped) == nil {
				data = item
			}
		}
	}
	if err := json.Unmarshal(data, &entries); err != nil {
		var wrapper struct {
			Items map[string][]config.RequestScopedErrorRule `json:"items"`
		}
		if err := json.Unmarshal(data, &wrapper); err != nil || wrapper.Items == nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
			return
		}
		entries = wrapper.Items
	}
	if err := (&config.Config{OAuthRequestScopedErrors: entries}).ValidateRequestScopedErrorRules(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.persistOAuthRequestScopedErrorsLocked(c, entries)
}

func (h *Handler) PatchOAuthRequestScopedErrors(c *gin.Context) {
	var body struct {
		Provider *string                  `json:"provider"`
		Channel  *string                  `json:"channel"`
		Rules    requestScopedErrorsPatch `json:"rules"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || !body.Rules.set {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	channel := ""
	if body.Channel != nil {
		channel = *body.Channel
	} else if body.Provider != nil {
		channel = *body.Provider
	}
	channel = strings.ToLower(strings.TrimSpace(channel))
	if channel == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid channel"})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	entries := maps.Clone(h.cfg.OAuthRequestScopedErrors)
	key, exists := oauthRequestScopedErrorKey(entries, channel)
	if len(body.Rules.value) == 0 {
		if !exists {
			c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
			return
		}
		delete(entries, key)
	} else {
		if entries == nil {
			entries = make(map[string][]config.RequestScopedErrorRule)
		}
		entries[key] = body.Rules.value
	}
	h.persistOAuthRequestScopedErrorsLocked(c, entries)
}

func (h *Handler) DeleteOAuthRequestScopedErrors(c *gin.Context) {
	channel := strings.TrimSpace(c.Query("channel"))
	if channel == "" {
		channel = c.Query("provider")
	}
	channel = strings.ToLower(strings.TrimSpace(channel))
	if channel == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing channel"})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	entries := maps.Clone(h.cfg.OAuthRequestScopedErrors)
	key, exists := oauthRequestScopedErrorKey(entries, channel)
	if !exists {
		c.JSON(http.StatusNotFound, gin.H{"error": "channel not found"})
		return
	}
	delete(entries, key)
	h.persistOAuthRequestScopedErrorsLocked(c, entries)
}

func oauthRequestScopedErrorKey(entries map[string][]config.RequestScopedErrorRule, channel string) (string, bool) {
	for key := range entries {
		if strings.EqualFold(strings.TrimSpace(key), channel) {
			return key, true
		}
	}
	return channel, false
}

func (h *Handler) persistOAuthRequestScopedErrorsLocked(c *gin.Context, entries map[string][]config.RequestScopedErrorRule) {
	previous := h.cfg.OAuthRequestScopedErrors
	if len(entries) == 0 {
		entries = nil
	}
	h.cfg.OAuthRequestScopedErrors = entries
	if !h.persistLocked(c) {
		h.cfg.OAuthRequestScopedErrors = previous
	}
}
