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
)

type chatGPTWebSentinelSnapshotter interface {
	SentinelSnapshot() chatgptwebauth.SentinelRuntimeSnapshot
}

type chatGPTWebSentinelConfigUpdater interface {
	UpdateConfig(*config.Config)
}

type chatGPTWebSentinelRequest struct {
	SDKRuntimeEnabled json.RawMessage `json:"sdk-runtime-enabled"`
	SDKWorkers        json.RawMessage `json:"sdk-workers"`
	SDKQueueSize      json.RawMessage `json:"sdk-queue-size"`
	SDKCacheVersions  json.RawMessage `json:"sdk-cache-versions"`
}

type chatGPTWebSentinelResponse struct {
	config.ResolvedChatGPTWebSentinelConfig
	Initialized          bool   `json:"initialized"`
	Available            bool   `json:"available"`
	WorkerLimit          int    `json:"worker_limit"`
	Busy                 int    `json:"busy"`
	Queued               int    `json:"queued"`
	SourcePending        int    `json:"source_pending"`
	SourceWaiters        int    `json:"source_waiters"`
	BytecodeWaiters      int    `json:"bytecode_waiters"`
	ObserverSessions     int    `json:"observer_sessions"`
	SDKVersion           string `json:"sdk_version"`
	SDKSHA256            string `json:"sdk_sha256"`
	SourceCacheEntries   int    `json:"source_cache_entries"`
	BytecodeCacheEntries int    `json:"bytecode_cache_entries"`
	FallbackCount        uint64 `json:"fallback_count"`
	LastError            string `json:"last_error"`
}

// GetChatGPTWebSentinel returns the effective SDK configuration and current runtime state.
func (h *Handler) GetChatGPTWebSentinel(c *gin.Context) {
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
	resolved := h.cfg.ChatGPTWeb.Sentinel.Resolved()
	snapshot := chatgptwebauth.SentinelRuntimeSnapshot{}
	if h.authManager != nil {
		if registered, ok := h.authManager.Executor(chatgptwebauth.Provider); ok {
			if snapshotter, okSnapshotter := registered.(chatGPTWebSentinelSnapshotter); okSnapshotter {
				snapshot = snapshotter.SentinelSnapshot()
				resolved = config.ResolvedChatGPTWebSentinelConfig{
					SDKRuntimeEnabled: snapshot.SDKRuntimeEnabled,
					SDKWorkers:        snapshot.SDKWorkers,
					SDKQueueSize:      snapshot.SDKQueueSize,
					SDKCacheVersions:  snapshot.SDKCacheVersions,
				}
			}
		}
	}
	h.mu.Unlock()
	c.JSON(http.StatusOK, chatGPTWebSentinelResponse{
		ResolvedChatGPTWebSentinelConfig: resolved,
		Initialized:                      snapshot.Initialized,
		Available:                        snapshot.Available,
		WorkerLimit:                      snapshot.WorkerLimit,
		Busy:                             snapshot.Busy,
		Queued:                           snapshot.Queued,
		SourcePending:                    snapshot.SourcePending,
		SourceWaiters:                    snapshot.SourceWaiters,
		BytecodeWaiters:                  snapshot.BytecodeWaiters,
		ObserverSessions:                 snapshot.ObserverSessions,
		SDKVersion:                       snapshot.SDKVersion,
		SDKSHA256:                        snapshot.SDKSHA256,
		SourceCacheEntries:               snapshot.SourceCacheEntries,
		BytecodeCacheEntries:             snapshot.BytecodeCacheEntries,
		FallbackCount:                    snapshot.FallbackCount,
		LastError:                        snapshot.LastError,
	})
}

// PutChatGPTWebSentinel replaces all Sentinel SDK settings.
func (h *Handler) PutChatGPTWebSentinel(c *gin.Context) {
	h.updateChatGPTWebSentinel(c, true)
}

// PatchChatGPTWebSentinel updates only supplied Sentinel SDK settings.
func (h *Handler) PatchChatGPTWebSentinel(c *gin.Context) {
	h.updateChatGPTWebSentinel(c, false)
}

func (h *Handler) updateChatGPTWebSentinel(c *gin.Context, replace bool) {
	request, errRequest := decodeChatGPTWebSentinelRequest(c)
	if errRequest != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errRequest.Error()})
		return
	}
	if replace && !request.complete() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "all Sentinel SDK fields are required"})
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
	previous := h.cfg.ChatGPTWeb.Sentinel
	candidate := previous
	if replace {
		candidate = config.ChatGPTWebSentinelConfig{}
	}
	if errApply := request.apply(&candidate); errApply != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errApply.Error()})
		return
	}
	if errValidate := candidate.Validate(); errValidate != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errValidate.Error()})
		return
	}
	h.cfg.ChatGPTWeb.Sentinel = candidate
	if !h.persistLocked(c) {
		if h.cfg != nil {
			h.cfg.ChatGPTWeb.Sentinel = previous
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

func decodeChatGPTWebSentinelRequest(c *gin.Context) (chatGPTWebSentinelRequest, error) {
	var raw json.RawMessage
	decoder := json.NewDecoder(c.Request.Body)
	if errDecode := decoder.Decode(&raw); errDecode != nil {
		return chatGPTWebSentinelRequest{}, fmt.Errorf("invalid body: %w", errDecode)
	}
	if errTrailing := decoder.Decode(&struct{}{}); errTrailing != io.EOF {
		return chatGPTWebSentinelRequest{}, fmt.Errorf("invalid body")
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return chatGPTWebSentinelRequest{}, fmt.Errorf("invalid body: object required")
	}
	var request chatGPTWebSentinelRequest
	requestDecoder := json.NewDecoder(bytes.NewReader(trimmed))
	requestDecoder.DisallowUnknownFields()
	if errDecode := requestDecoder.Decode(&request); errDecode != nil {
		return request, fmt.Errorf("invalid body: %w", errDecode)
	}
	return request, nil
}

func (request chatGPTWebSentinelRequest) complete() bool {
	return len(request.SDKRuntimeEnabled) > 0 && len(request.SDKWorkers) > 0 && len(request.SDKQueueSize) > 0 && len(request.SDKCacheVersions) > 0
}

func (request chatGPTWebSentinelRequest) apply(candidate *config.ChatGPTWebSentinelConfig) error {
	if candidate == nil {
		return fmt.Errorf("configuration unavailable")
	}
	if len(request.SDKRuntimeEnabled) > 0 {
		value, errValue := decodeSentinelBool(request.SDKRuntimeEnabled)
		if errValue != nil {
			return fmt.Errorf("invalid sdk-runtime-enabled")
		}
		candidate.SDKRuntimeEnabled = &value
	}
	applyInt := func(name string, raw json.RawMessage, target **int) error {
		if len(raw) == 0 {
			return nil
		}
		value, errValue := decodeSentinelInt(raw)
		if errValue != nil {
			return fmt.Errorf("invalid %s", name)
		}
		fieldValue := value
		*target = &fieldValue
		return nil
	}
	if errWorkers := applyInt("sdk-workers", request.SDKWorkers, &candidate.SDKWorkers); errWorkers != nil {
		return errWorkers
	}
	if errQueue := applyInt("sdk-queue-size", request.SDKQueueSize, &candidate.SDKQueueSize); errQueue != nil {
		return errQueue
	}
	if errCache := applyInt("sdk-cache-versions", request.SDKCacheVersions, &candidate.SDKCacheVersions); errCache != nil {
		return errCache
	}
	return nil
}

func decodeSentinelBool(raw json.RawMessage) (bool, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false, fmt.Errorf("value is required")
	}
	var value bool
	if errUnmarshal := json.Unmarshal(raw, &value); errUnmarshal != nil {
		return false, errUnmarshal
	}
	return value, nil
}

func decodeSentinelInt(raw json.RawMessage) (int, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, fmt.Errorf("value is required")
	}
	var value int
	if errUnmarshal := json.Unmarshal(raw, &value); errUnmarshal != nil {
		return 0, errUnmarshal
	}
	return value, nil
}
