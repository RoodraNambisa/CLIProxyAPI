package management

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

type chatGPTWebLoginProxyRequest struct {
	Enabled                   json.RawMessage `json:"enabled"`
	URLTemplate               json.RawMessage `json:"url-template"`
	PlaceholderCharset        json.RawMessage `json:"placeholder-charset"`
	RotateOnRetry             json.RawMessage `json:"rotate-on-retry"`
	RequestAttempts           json.RawMessage `json:"request-attempts"`
	FlowAttempts              json.RawMessage `json:"flow-attempts"`
	RetryDelayMilliseconds    json.RawMessage `json:"retry-delay-milliseconds"`
	AcquisitionTimeoutSeconds json.RawMessage `json:"acquisition-timeout-seconds"`
}

// GetChatGPTWebLoginProxy returns the complete login-only proxy template and effective settings.
func (h *Handler) GetChatGPTWebLoginProxy(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if h == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(http.StatusOK, cfg.ChatGPTWeb.LoginProxy.Resolved())
}

// PutChatGPTWebLoginProxy replaces all login-only proxy settings.
func (h *Handler) PutChatGPTWebLoginProxy(c *gin.Context) {
	h.updateChatGPTWebLoginProxy(c, true)
}

// PatchChatGPTWebLoginProxy updates supplied login-only proxy settings.
func (h *Handler) PatchChatGPTWebLoginProxy(c *gin.Context) {
	h.updateChatGPTWebLoginProxy(c, false)
}

func (h *Handler) updateChatGPTWebLoginProxy(c *gin.Context, replace bool) {
	request, errDecode := decodeChatGPTWebLoginProxyRequest(c)
	if errDecode != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errDecode.Error()})
		return
	}
	if replace && !request.complete() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "all ChatGPT Web login proxy fields are required"})
		return
	}
	if h == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}

	var updater chatGPTWebSentinelConfigUpdater
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager != nil {
		if registered, ok := manager.Executor(chatgptwebauth.Provider); ok {
			updater, _ = registered.(chatGPTWebSentinelConfigUpdater)
		}
	}

	h.mu.Lock()
	if h.cfg == nil {
		h.mu.Unlock()
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	previous := h.cfg.ChatGPTWeb.LoginProxy
	candidate := previous
	if replace {
		candidate = config.ChatGPTWebLoginProxyConfig{}
	}
	if errApply := request.apply(&candidate); errApply != nil {
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": errApply.Error()})
		return
	}
	if errValidate := candidate.Validate(); errValidate != nil {
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": errValidate.Error()})
		return
	}
	h.cfg.ChatGPTWeb.LoginProxy = candidate
	if !h.persistLocked(c) {
		if h.cfg != nil {
			h.cfg.ChatGPTWeb.LoginProxy = previous
		}
		h.mu.Unlock()
		return
	}
	appliedConfig := h.configSnapshot.Load()
	h.mu.Unlock()
	if updater != nil {
		updater.UpdateConfig(appliedConfig)
	}
}

func decodeChatGPTWebLoginProxyRequest(c *gin.Context) (chatGPTWebLoginProxyRequest, error) {
	var raw json.RawMessage
	decoder := json.NewDecoder(c.Request.Body)
	if errDecode := decoder.Decode(&raw); errDecode != nil {
		return chatGPTWebLoginProxyRequest{}, fmt.Errorf("invalid body: %w", errDecode)
	}
	if errTrailing := decoder.Decode(&struct{}{}); errTrailing != io.EOF {
		return chatGPTWebLoginProxyRequest{}, fmt.Errorf("invalid body")
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return chatGPTWebLoginProxyRequest{}, fmt.Errorf("invalid body: object required")
	}
	var request chatGPTWebLoginProxyRequest
	requestDecoder := json.NewDecoder(bytes.NewReader(trimmed))
	requestDecoder.DisallowUnknownFields()
	if errDecode := requestDecoder.Decode(&request); errDecode != nil {
		return request, fmt.Errorf("invalid body: %w", errDecode)
	}
	return request, nil
}

func (request chatGPTWebLoginProxyRequest) complete() bool {
	return len(request.Enabled) > 0 &&
		len(request.URLTemplate) > 0 &&
		len(request.PlaceholderCharset) > 0 &&
		len(request.RotateOnRetry) > 0 &&
		len(request.RequestAttempts) > 0 &&
		len(request.FlowAttempts) > 0 &&
		len(request.RetryDelayMilliseconds) > 0 &&
		len(request.AcquisitionTimeoutSeconds) > 0
}

func (request chatGPTWebLoginProxyRequest) apply(candidate *config.ChatGPTWebLoginProxyConfig) error {
	if candidate == nil {
		return fmt.Errorf("configuration unavailable")
	}
	if len(request.Enabled) > 0 {
		value, errValue := decodeSentinelBool(request.Enabled)
		if errValue != nil {
			return fmt.Errorf("invalid enabled")
		}
		candidate.Enabled = value
	}
	if len(request.URLTemplate) > 0 {
		value, errValue := decodeLoginProxyString(request.URLTemplate)
		if errValue != nil {
			return fmt.Errorf("invalid url-template")
		}
		candidate.URLTemplate = strings.TrimSpace(value)
	}
	if len(request.PlaceholderCharset) > 0 {
		value, errValue := decodeLoginProxyString(request.PlaceholderCharset)
		if errValue != nil {
			return fmt.Errorf("invalid placeholder-charset")
		}
		candidate.PlaceholderCharset = strings.TrimSpace(value)
	}
	if len(request.RotateOnRetry) > 0 {
		value, errValue := decodeSentinelBool(request.RotateOnRetry)
		if errValue != nil {
			return fmt.Errorf("invalid rotate-on-retry")
		}
		candidate.RotateOnRetry = &value
	}
	applyInt := func(name string, raw json.RawMessage, target **int) error {
		if len(raw) == 0 {
			return nil
		}
		value, errValue := decodeSentinelInt(raw)
		if errValue != nil {
			return fmt.Errorf("invalid %s", name)
		}
		*target = &value
		return nil
	}
	if errAttempts := applyInt("request-attempts", request.RequestAttempts, &candidate.RequestAttempts); errAttempts != nil {
		return errAttempts
	}
	if errFlows := applyInt("flow-attempts", request.FlowAttempts, &candidate.FlowAttempts); errFlows != nil {
		return errFlows
	}
	if errDelay := applyInt("retry-delay-milliseconds", request.RetryDelayMilliseconds, &candidate.RetryDelayMilliseconds); errDelay != nil {
		return errDelay
	}
	return applyInt("acquisition-timeout-seconds", request.AcquisitionTimeoutSeconds, &candidate.AcquisitionTimeoutSeconds)
}

func decodeLoginProxyString(raw json.RawMessage) (string, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return "", fmt.Errorf("value is required")
	}
	var value string
	if errDecode := json.Unmarshal(raw, &value); errDecode != nil {
		return "", errDecode
	}
	return value, nil
}
