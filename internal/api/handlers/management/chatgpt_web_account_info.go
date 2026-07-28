package management

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const (
	chatGPTWebAccountInfoMaxRequestBytes = 1 << 20
)

type chatGPTWebAccountInfoController interface {
	AccountInfoSnapshot() chatgptwebauth.AccountInfoRuntimeSnapshot
	AccountInfoAuthState(string) chatgptwebauth.AccountInfoAuthRuntimeState
	StartAccountInfoRefreshTask([]chatgptwebauth.AccountInfoRefreshTarget, bool) (*chatgptwebauth.AccountInfoRefreshTask, error)
	AccountInfoRefreshTask(string) (*chatgptwebauth.AccountInfoRefreshTask, bool)
	CancelAccountInfoRefreshTask(string) (*chatgptwebauth.AccountInfoRefreshTask, bool)
	UpdateConfig(*config.Config)
}

type chatGPTWebAccountInfoResponse struct {
	Config  config.ResolvedChatGPTWebAccountInfoConfig `json:"config"`
	Runtime chatgptwebauth.AccountInfoRuntimeSnapshot  `json:"runtime"`
}

type chatGPTWebAccountInfoConfigRequest struct {
	RefreshWorkers        json.RawMessage `json:"refresh-workers"`
	RefreshQueueSize      json.RawMessage `json:"refresh-queue-size"`
	RefreshTTLMinutes     json.RawMessage `json:"refresh-ttl-minutes"`
	RecoveryJitterSeconds json.RawMessage `json:"recovery-jitter-seconds"`
	MaxRetries            json.RawMessage `json:"max-retries"`
}

type chatGPTWebAccountInfoRefreshRequest struct {
	Names []string `json:"names"`
	Force bool     `json:"force"`
}

// GetChatGPTWebAccountInfo returns the effective refresh configuration and runtime state.
func (h *Handler) GetChatGPTWebAccountInfo(c *gin.Context) {
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
	resolved := h.cfg.ChatGPTWeb.AccountInfo.Resolved()
	manager := h.authManager
	h.mu.Unlock()
	var snapshot chatgptwebauth.AccountInfoRuntimeSnapshot
	if controller, ok := chatGPTWebAccountInfoControllerForManager(manager); ok {
		snapshot = controller.AccountInfoSnapshot()
	}
	c.JSON(http.StatusOK, chatGPTWebAccountInfoResponse{Config: resolved, Runtime: snapshot})
}

// PutChatGPTWebAccountInfo replaces all account profile refresh settings.
func (h *Handler) PutChatGPTWebAccountInfo(c *gin.Context) {
	h.updateChatGPTWebAccountInfo(c, true)
}

// PatchChatGPTWebAccountInfo updates supplied account profile refresh settings.
func (h *Handler) PatchChatGPTWebAccountInfo(c *gin.Context) {
	h.updateChatGPTWebAccountInfo(c, false)
}

func (h *Handler) updateChatGPTWebAccountInfo(c *gin.Context, replace bool) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, chatGPTWebAccountInfoMaxRequestBytes)
	request, errRequest := decodeChatGPTWebAccountInfoConfigRequest(c)
	if errRequest != nil {
		c.JSON(chatGPTWebAccountInfoRequestErrorStatus(errRequest), gin.H{"error": errRequest.Error()})
		return
	}
	if replace && !request.complete() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "all account-info fields are required"})
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
	previous := h.cfg.ChatGPTWeb.AccountInfo
	candidate := previous
	if replace {
		candidate = config.ChatGPTWebAccountInfoConfig{}
	}
	if errApply := request.apply(&candidate); errApply != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errApply.Error()})
		return
	}
	if errValidate := candidate.Validate(); errValidate != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errValidate.Error()})
		return
	}
	h.cfg.ChatGPTWeb.AccountInfo = candidate
	if !h.persistLocked(c) {
		if h.cfg != nil {
			h.cfg.ChatGPTWeb.AccountInfo = previous
		}
		return
	}
	if controller, ok := chatGPTWebAccountInfoControllerForManager(h.authManager); ok {
		controller.UpdateConfig(h.cfg)
	}
}

// StartChatGPTWebAccountInfoRefreshTask queues bounded profile and quota refreshes.
func (h *Handler) StartChatGPTWebAccountInfoRefreshTask(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "chatgpt web account info is unavailable"})
		return
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	controller, ok := chatGPTWebAccountInfoControllerForManager(manager)
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "chatgpt web account info is unavailable"})
		return
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, chatGPTWebAccountInfoMaxRequestBytes)
	request, errRequest := decodeChatGPTWebAccountInfoRefreshRequest(c)
	if errRequest != nil {
		c.JSON(chatGPTWebAccountInfoRequestErrorStatus(errRequest), gin.H{"error": errRequest.Error()})
		return
	}
	targets, errTargets := h.resolveChatGPTWebAccountInfoTargets(request.Names, manager)
	if errTargets != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errTargets.Error()})
		return
	}
	task, errStart := controller.StartAccountInfoRefreshTask(targets, request.Force)
	if errStart != nil {
		if errors.Is(errStart, chatgptwebauth.ErrAccountInfoTaskLimitReached) {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": errStart.Error()})
			return
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": errStart.Error()})
		return
	}
	c.JSON(http.StatusAccepted, task)
}

// GetChatGPTWebAccountInfoRefreshTask returns one refresh task snapshot.
func (h *Handler) GetChatGPTWebAccountInfoRefreshTask(c *gin.Context) {
	controller, ok := h.chatGPTWebAccountInfoController()
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "chatgpt web account info is unavailable"})
		return
	}
	task, found := controller.AccountInfoRefreshTask(c.Param("id"))
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "chatgpt web account info refresh task not found"})
		return
	}
	c.JSON(http.StatusOK, task)
}

// CancelChatGPTWebAccountInfoRefreshTask cancels one pending or running refresh task.
func (h *Handler) CancelChatGPTWebAccountInfoRefreshTask(c *gin.Context) {
	controller, ok := h.chatGPTWebAccountInfoController()
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "chatgpt web account info is unavailable"})
		return
	}
	task, found := controller.CancelAccountInfoRefreshTask(c.Param("id"))
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "chatgpt web account info refresh task not found"})
		return
	}
	c.JSON(http.StatusOK, task)
}

func (h *Handler) resolveChatGPTWebAccountInfoTargets(names []string, manager *coreauth.Manager) ([]chatgptwebauth.AccountInfoRefreshTarget, error) {
	seen := make(map[string]struct{}, len(names))
	uniqueNames := make([]string, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" || isUnsafeAuthFileName(name) {
			return nil, errors.New("names contains an invalid auth file name")
		}
		key := managedAuthNameKey(name)
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		if len(seen) > chatgptwebauth.AccountInfoMaxTargets {
			return nil, fmt.Errorf("at most %d auth file names are allowed", chatgptwebauth.AccountInfoMaxTargets)
		}
		uniqueNames = append(uniqueNames, name)
	}
	if len(uniqueNames) == 0 {
		return nil, errors.New("at least one auth file name is required")
	}
	targets := make([]chatgptwebauth.AccountInfoRefreshTarget, 0, len(uniqueNames))
	for _, name := range uniqueNames {
		target := chatgptwebauth.AccountInfoRefreshTarget{Name: name}
		auth := h.findManagedAuthWithManager(name, manager)
		if auth != nil && strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) {
			target.AuthID = auth.ID
			target.AuthInstanceID = auth.RuntimeInstanceID()
			target.AuthIndex = auth.EnsureIndex()
		}
		targets = append(targets, target)
	}
	return targets, nil
}

func (h *Handler) chatGPTWebAccountInfoController() (chatGPTWebAccountInfoController, bool) {
	if h == nil {
		return nil, false
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	return chatGPTWebAccountInfoControllerForManager(manager)
}

func chatGPTWebAccountInfoControllerForManager(manager *coreauth.Manager) (chatGPTWebAccountInfoController, bool) {
	if manager == nil {
		return nil, false
	}
	registered, ok := manager.Executor(chatgptwebauth.Provider)
	if !ok || registered == nil {
		return nil, false
	}
	controller, ok := registered.(chatGPTWebAccountInfoController)
	return controller, ok
}

func decodeChatGPTWebAccountInfoConfigRequest(c *gin.Context) (chatGPTWebAccountInfoConfigRequest, error) {
	var request chatGPTWebAccountInfoConfigRequest
	if errDecode := decodeStrictJSONBody(c, &request); errDecode != nil {
		return request, errDecode
	}
	return request, nil
}

func decodeChatGPTWebAccountInfoRefreshRequest(c *gin.Context) (chatGPTWebAccountInfoRefreshRequest, error) {
	var request chatGPTWebAccountInfoRefreshRequest
	if errDecode := decodeStrictJSONBody(c, &request); errDecode != nil {
		return request, errDecode
	}
	return request, nil
}

func decodeStrictJSONBody(c *gin.Context, target any) error {
	if c == nil || c.Request == nil || c.Request.Body == nil {
		return errors.New("invalid body")
	}
	var raw json.RawMessage
	decoder := json.NewDecoder(c.Request.Body)
	if errDecode := decoder.Decode(&raw); errDecode != nil {
		return fmt.Errorf("invalid body: %w", errDecode)
	}
	if errTrailing := decoder.Decode(&struct{}{}); errTrailing != io.EOF {
		if errTrailing != nil {
			return fmt.Errorf("invalid body: %w", errTrailing)
		}
		return errors.New("invalid body")
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return errors.New("invalid body: object required")
	}
	requestDecoder := json.NewDecoder(bytes.NewReader(trimmed))
	requestDecoder.DisallowUnknownFields()
	if errDecode := requestDecoder.Decode(target); errDecode != nil {
		return fmt.Errorf("invalid body: %w", errDecode)
	}
	return nil
}

func chatGPTWebAccountInfoRequestErrorStatus(err error) int {
	var maxBytesError *http.MaxBytesError
	if errors.As(err, &maxBytesError) {
		return http.StatusRequestEntityTooLarge
	}
	return http.StatusBadRequest
}

func (request chatGPTWebAccountInfoConfigRequest) complete() bool {
	return len(request.RefreshWorkers) > 0 &&
		len(request.RefreshQueueSize) > 0 &&
		len(request.RefreshTTLMinutes) > 0 &&
		len(request.RecoveryJitterSeconds) > 0 &&
		len(request.MaxRetries) > 0
}

func (request chatGPTWebAccountInfoConfigRequest) apply(candidate *config.ChatGPTWebAccountInfoConfig) error {
	if candidate == nil {
		return errors.New("configuration unavailable")
	}
	fields := []struct {
		name   string
		raw    json.RawMessage
		target **int
	}{
		{name: "refresh-workers", raw: request.RefreshWorkers, target: &candidate.RefreshWorkers},
		{name: "refresh-queue-size", raw: request.RefreshQueueSize, target: &candidate.RefreshQueueSize},
		{name: "refresh-ttl-minutes", raw: request.RefreshTTLMinutes, target: &candidate.RefreshTTLMinutes},
		{name: "recovery-jitter-seconds", raw: request.RecoveryJitterSeconds, target: &candidate.RecoveryJitterSeconds},
		{name: "max-retries", raw: request.MaxRetries, target: &candidate.MaxRetries},
	}
	for _, field := range fields {
		if len(field.raw) == 0 {
			continue
		}
		value, errValue := decodeSentinelInt(field.raw)
		if errValue != nil {
			return fmt.Errorf("invalid %s", field.name)
		}
		fieldValue := value
		*field.target = &fieldValue
	}
	return nil
}
