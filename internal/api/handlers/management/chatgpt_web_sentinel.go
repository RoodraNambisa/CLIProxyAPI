package management

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/sentinelcompat"
)

type chatGPTWebSentinelSnapshotter interface {
	SentinelSnapshot() chatgptwebauth.SentinelRuntimeSnapshot
}

type chatGPTWebSentinelConfigUpdater interface {
	UpdateConfig(*config.Config)
}

type chatGPTWebSentinelRequest struct {
	Mode              json.RawMessage `json:"mode"`
	Remote            json.RawMessage `json:"remote"`
	GoVMCompatibility json.RawMessage `json:"go-vm-compatibility"`
	SDKRuntimeEnabled json.RawMessage `json:"sdk-runtime-enabled"`
	SDKWorkers        json.RawMessage `json:"sdk-workers"`
	SDKQueueSize      json.RawMessage `json:"sdk-queue-size"`
	SDKCacheVersions  json.RawMessage `json:"sdk-cache-versions"`
}

type chatGPTWebSentinelResponse struct {
	RemoteNodes            []chatgptwebauth.SentinelComputeNodeSnapshot `json:"remote_nodes,omitempty"`
	GoVMRulesHash          string                                       `json:"go_vm_rules_hash"`
	GoVMRuleCount          int                                          `json:"go_vm_rule_count"`
	GoVMRulesAppliedAt     time.Time                                    `json:"go_vm_rules_applied_at"`
	GoVMExtensionUses      uint64                                       `json:"go_vm_extension_uses"`
	GoVMExtensionFallbacks uint64                                       `json:"go_vm_extension_fallbacks"`
	config.ResolvedChatGPTWebSentinelConfig
	Initialized                    bool                                    `json:"initialized"`
	Available                      bool                                    `json:"available"`
	WorkerLimit                    int                                     `json:"worker_limit"`
	Busy                           int                                     `json:"busy"`
	Queued                         int                                     `json:"queued"`
	SourcePending                  int                                     `json:"source_pending"`
	SourceWaiters                  int                                     `json:"source_waiters"`
	BytecodeWaiters                int                                     `json:"bytecode_waiters"`
	ObserverSessions               int                                     `json:"observer_sessions"`
	SDKVersion                     string                                  `json:"sdk_version"`
	SDKSHA256                      string                                  `json:"sdk_sha256"`
	SourceCacheEntries             int                                     `json:"source_cache_entries"`
	BytecodeCacheEntries           int                                     `json:"bytecode_cache_entries"`
	CompatibilityFallbacks         uint64                                  `json:"compatibility_fallback_count"`
	TurnstileFallbacks             uint64                                  `json:"turnstile_compatibility_fallback_count"`
	ObserverFallbacks              uint64                                  `json:"observer_compatibility_fallback_count"`
	SDKPreferredHits               uint64                                  `json:"sdk_preferred_hit_count"`
	SessionObserverCount           uint64                                  `json:"session_observer_count"`
	FallbackCount                  uint64                                  `json:"fallback_count"`
	LastCompatibilityProgram       string                                  `json:"last_compatibility_program"`
	LastCompatibilityKind          string                                  `json:"last_compatibility_kind"`
	LastCompatibilityOperationHash string                                  `json:"last_compatibility_operation_hash"`
	LastError                      string                                  `json:"last_error"`
	PersonaOutcomes                []chatgptwebauth.PersonaOutcomeSnapshot `json:"persona_outcomes,omitempty"`
}

// GetChatGPTWebSentinel returns the effective SDK configuration and current runtime state.
func (h *Handler) GetChatGPTWebSentinel(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	resolved := cfg.ChatGPTWeb.Sentinel.Resolved()
	var remoteNodes []chatgptwebauth.SentinelComputeNodeSnapshot
	snapshot := chatgptwebauth.SentinelRuntimeSnapshot{}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager != nil {
		if registered, ok := manager.Executor(chatgptwebauth.Provider); ok {
			if reporter, ok := registered.(interface {
				SentinelComputeSnapshot() []chatgptwebauth.SentinelComputeNodeSnapshot
			}); ok {
				remoteNodes = reporter.SentinelComputeSnapshot()
			}
			if snapshotter, okSnapshotter := registered.(chatGPTWebSentinelSnapshotter); okSnapshotter {
				snapshot = snapshotter.SentinelSnapshot()
				resolved = config.ResolvedChatGPTWebSentinelConfig{
					Mode:              resolved.Mode,
					Remote:            resolved.Remote,
					GoVMCompatibility: resolved.GoVMCompatibility,
					SDKRuntimeEnabled: snapshot.SDKRuntimeEnabled,
					SDKWorkers:        snapshot.SDKWorkers,
					SDKQueueSize:      snapshot.SDKQueueSize,
					SDKCacheVersions:  snapshot.SDKCacheVersions,
				}
			}
		}
	}
	c.JSON(http.StatusOK, chatGPTWebSentinelResponse{
		RemoteNodes:                      remoteNodes,
		GoVMRulesHash:                    snapshot.GoVMRulesHash,
		GoVMRuleCount:                    snapshot.GoVMRuleCount,
		GoVMRulesAppliedAt:               snapshot.GoVMRulesAppliedAt,
		GoVMExtensionUses:                snapshot.GoVMExtensionUses,
		GoVMExtensionFallbacks:           snapshot.GoVMExtensionFallbacks,
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
		CompatibilityFallbacks:           snapshot.CompatibilityFallbacks,
		TurnstileFallbacks:               snapshot.TurnstileFallbacks,
		ObserverFallbacks:                snapshot.ObserverFallbacks,
		SDKPreferredHits:                 snapshot.SDKPreferredHits,
		SessionObserverCount:             snapshot.SessionObserverCount,
		FallbackCount:                    snapshot.FallbackCount,
		LastCompatibilityProgram:         snapshot.LastCompatibilityProgram,
		LastCompatibilityKind:            snapshot.LastCompatibilityKind,
		LastCompatibilityOperationHash:   snapshot.LastCompatibilityOperationHash,
		LastError:                        snapshot.LastError,
		PersonaOutcomes:                  snapshot.PersonaOutcomes,
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
	if h.cfg == nil {
		h.mu.Unlock()
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	previous := h.cfg.ChatGPTWeb.Sentinel
	candidate := previous
	if replace {
		candidate = config.ChatGPTWebSentinelConfig{}
		candidate.Mode = previous.Mode
		candidate.Remote = previous.Remote
		candidate.GoVMCompatibility = previous.GoVMCompatibility
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
	h.cfg.ChatGPTWeb.Sentinel = candidate
	if !h.persistLocked(c) {
		if h.cfg != nil {
			h.cfg.ChatGPTWeb.Sentinel = previous
		}
		h.mu.Unlock()
		return
	}
	manager := h.authManager
	appliedConfig := h.configSnapshot.Load()
	h.mu.Unlock()
	if manager != nil {
		if registered, ok := manager.Executor(chatgptwebauth.Provider); ok {
			if updater, okUpdater := registered.(chatGPTWebSentinelConfigUpdater); okUpdater {
				updater.UpdateConfig(appliedConfig)
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
	if len(request.Mode) > 0 {
		if bytes.Equal(bytes.TrimSpace(request.Mode), []byte("null")) || json.Unmarshal(request.Mode, &candidate.Mode) != nil {
			return fmt.Errorf("invalid mode")
		}
	}
	if len(request.Remote) > 0 {
		if bytes.Equal(bytes.TrimSpace(request.Remote), []byte("null")) {
			return fmt.Errorf("invalid remote settings")
		}
		decoder := json.NewDecoder(bytes.NewReader(request.Remote))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&candidate.Remote); err != nil {
			return fmt.Errorf("invalid remote settings")
		}
	}
	if len(request.GoVMCompatibility) > 0 {
		merged, err := sentinelcompat.Merge(candidate.GoVMCompatibility, request.GoVMCompatibility)
		if err != nil {
			return fmt.Errorf("invalid go-vm-compatibility: %w", err)
		}
		candidate.GoVMCompatibility = merged
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
