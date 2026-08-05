package management

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

const chatGPTWebImportConfigMaxRequestBytes = 1 << 20

type chatGPTWebImportConfigRequest struct {
	Workers                       json.RawMessage `json:"workers"`
	ValidateModelsAfterUpload     json.RawMessage `json:"validate-models-after-upload"`
	RefreshAccountInfoAfterUpload json.RawMessage `json:"refresh-account-info-after-upload"`
}

type chatGPTWebImportConfigResponse struct {
	Config  config.ResolvedChatGPTWebImportConfig `json:"config"`
	Runtime chatGPTWebImportRuntimeSnapshot       `json:"runtime"`
}

// GetChatGPTWebImport returns fast-import configuration and task counters.
func (h *Handler) GetChatGPTWebImport(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
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
	resolved := h.cfg.ChatGPTWeb.Import.Resolved()
	tasks := h.chatGPTWebMutationTasks
	h.mu.Unlock()
	var runtime chatGPTWebImportRuntimeSnapshot
	if tasks != nil {
		runtime = tasks.importRuntimeSnapshot()
	}
	c.JSON(http.StatusOK, chatGPTWebImportConfigResponse{Config: resolved, Runtime: runtime})
}

// PutChatGPTWebImport replaces all fast-import settings.
func (h *Handler) PutChatGPTWebImport(c *gin.Context) {
	h.updateChatGPTWebImport(c, true)
}

// PatchChatGPTWebImport updates supplied fast-import settings.
func (h *Handler) PatchChatGPTWebImport(c *gin.Context) {
	h.updateChatGPTWebImport(c, false)
}

func (h *Handler) updateChatGPTWebImport(c *gin.Context, replace bool) {
	c.Header("Cache-Control", "no-store")
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, chatGPTWebImportConfigMaxRequestBytes)
	request, errDecode := decodeChatGPTWebImportConfigRequest(c)
	if errDecode != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errDecode.Error()})
		return
	}
	if replace && !request.complete() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "all ChatGPT Web import fields are required"})
		return
	}
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
	previous := h.cfg.ChatGPTWeb.Import
	candidate := previous
	if replace {
		candidate = config.ChatGPTWebImportConfig{}
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
	h.cfg.ChatGPTWeb.Import = candidate
	updatedConfig, errSnapshot := cloneChatGPTWebImportRuntimeConfig(h.cfg)
	if errSnapshot != nil {
		h.cfg.ChatGPTWeb.Import = previous
		h.mu.Unlock()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to snapshot runtime configuration"})
		return
	}
	if !h.persistLocked(c) {
		if h.cfg != nil {
			h.cfg.ChatGPTWeb.Import = previous
		}
		h.mu.Unlock()
		return
	}
	tasks := h.chatGPTWebMutationTasks
	h.mu.Unlock()
	if tasks != nil {
		tasks.updateWorkerLimit(candidate.Resolved().Workers)
	}
	if controller, ok := chatGPTWebAccountInfoControllerForManager(h.authManager); ok {
		controller.UpdateConfig(updatedConfig)
	}
}

func cloneChatGPTWebImportRuntimeConfig(input *config.Config) (*config.Config, error) {
	data, errMarshal := json.Marshal(input)
	if errMarshal != nil {
		return nil, errMarshal
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var snapshot config.Config
	if errDecode := decoder.Decode(&snapshot); errDecode != nil {
		return nil, errDecode
	}
	cloneChatGPTWebImportPayloadRules(snapshot.Payload.DefaultRaw, input.Payload.DefaultRaw)
	cloneChatGPTWebImportPayloadRules(snapshot.Payload.OverrideRaw, input.Payload.OverrideRaw)
	return &snapshot, nil
}

func cloneChatGPTWebImportPayloadRules(snapshot, source []config.PayloadRule) {
	for ruleIndex := range source {
		if ruleIndex >= len(snapshot) || snapshot[ruleIndex].Params == nil {
			continue
		}
		for key, value := range source[ruleIndex].Params {
			switch typed := value.(type) {
			case json.RawMessage:
				snapshot[ruleIndex].Params[key] = append(json.RawMessage(nil), typed...)
			case []byte:
				snapshot[ruleIndex].Params[key] = append([]byte(nil), typed...)
			}
		}
	}
}

func decodeChatGPTWebImportConfigRequest(c *gin.Context) (chatGPTWebImportConfigRequest, error) {
	var raw json.RawMessage
	decoder := json.NewDecoder(c.Request.Body)
	if errDecode := decoder.Decode(&raw); errDecode != nil {
		return chatGPTWebImportConfigRequest{}, fmt.Errorf("invalid body: %w", errDecode)
	}
	if errTrailing := decoder.Decode(&struct{}{}); errTrailing != io.EOF {
		return chatGPTWebImportConfigRequest{}, fmt.Errorf("invalid body")
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return chatGPTWebImportConfigRequest{}, fmt.Errorf("invalid body: object required")
	}
	var request chatGPTWebImportConfigRequest
	requestDecoder := json.NewDecoder(bytes.NewReader(trimmed))
	requestDecoder.DisallowUnknownFields()
	if errDecode := requestDecoder.Decode(&request); errDecode != nil {
		return request, fmt.Errorf("invalid body: %w", errDecode)
	}
	return request, nil
}

func (request chatGPTWebImportConfigRequest) complete() bool {
	return len(request.Workers) > 0 && len(request.ValidateModelsAfterUpload) > 0 && len(request.RefreshAccountInfoAfterUpload) > 0
}

func (request chatGPTWebImportConfigRequest) apply(candidate *config.ChatGPTWebImportConfig) error {
	if candidate == nil {
		return fmt.Errorf("configuration unavailable")
	}
	if len(request.Workers) > 0 {
		workers, errWorkers := decodeSentinelInt(request.Workers)
		if errWorkers != nil {
			return fmt.Errorf("invalid workers")
		}
		candidate.Workers = &workers
	}
	if len(request.ValidateModelsAfterUpload) > 0 {
		enabled, errEnabled := decodeSentinelBool(request.ValidateModelsAfterUpload)
		if errEnabled != nil {
			return fmt.Errorf("invalid validate-models-after-upload")
		}
		candidate.ValidateModelsAfterUpload = &enabled
	}
	if len(request.RefreshAccountInfoAfterUpload) > 0 {
		enabled, errEnabled := decodeSentinelBool(request.RefreshAccountInfoAfterUpload)
		if errEnabled != nil {
			return fmt.Errorf("invalid refresh-account-info-after-upload")
		}
		candidate.RefreshAccountInfoAfterUpload = &enabled
	}
	return nil
}
