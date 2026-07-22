// Package management provides the management API handlers and middleware
// for configuring the server and managing auth files.
package management

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/buildinfo"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxypool"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/bcrypt"
)

type attemptInfo struct {
	count        int
	blockedUntil time.Time
	lastActivity time.Time // track last activity for cleanup
}

// attemptCleanupInterval controls how often stale IP entries are purged
const attemptCleanupInterval = 1 * time.Hour

// attemptMaxIdleTime controls how long an IP can be idle before cleanup
const attemptMaxIdleTime = 2 * time.Hour

// Handler aggregates config reference, persistence path and helpers.
type Handler struct {
	cfg                     *config.Config
	configFilePath          string
	mu                      sync.Mutex
	codexPlanRefreshMu      sync.Mutex
	codexPlanRefresh        codexPlanTypeRefreshTask
	attemptsMu              sync.Mutex
	failedAttempts          map[string]*attemptInfo // keyed by client IP
	authManager             *coreauth.Manager
	usageStats              *usage.RequestStatistics
	tokenStoreMu            sync.Mutex
	tokenStore              coreauth.Store
	localPassword           string
	allowRemoteOverride     bool
	envSecret               string
	logDir                  string
	postAuthHook            coreauth.PostAuthHook
	authStatusHook          coreauth.AuthStatusHook
	dependencyReconcileHook func(context.Context, string) ([]string, error)
	proxyPoolManager        *proxypool.Manager
	chatGPTWebTasks         *chatGPTWebLoginTaskManager
	chatGPTWebMutationTasks *chatGPTWebMutationTaskManager
	agentIdentityTasks      *codexAgentIdentityTaskManager
	agentIdentityBaseURL    string
	chatGPTWebDependencyMu  sync.Mutex
	cleanupCancel           context.CancelFunc
	cleanupWG               sync.WaitGroup
	cleanupStopOnce         sync.Once
}

// NewHandler creates a new management handler instance.
func NewHandler(cfg *config.Config, configFilePath string, manager *coreauth.Manager) *Handler {
	envSecret, _ := os.LookupEnv("MANAGEMENT_PASSWORD")
	envSecret = strings.TrimSpace(envSecret)

	h := &Handler{
		cfg:                     cfg,
		configFilePath:          configFilePath,
		failedAttempts:          make(map[string]*attemptInfo),
		authManager:             manager,
		usageStats:              usage.GetRequestStatistics(),
		tokenStore:              sdkAuth.GetTokenStore(),
		allowRemoteOverride:     envSecret != "",
		envSecret:               envSecret,
		chatGPTWebTasks:         newChatGPTWebLoginTaskManager(),
		chatGPTWebMutationTasks: newChatGPTWebMutationTaskManager(),
		agentIdentityTasks:      newCodexAgentIdentityTaskManager(),
	}
	cleanupCtx, cleanupCancel := context.WithCancel(context.Background())
	h.cleanupCancel = cleanupCancel
	h.startAttemptCleanup(cleanupCtx)
	return h
}

// startAttemptCleanup launches a background goroutine that periodically
// removes stale IP entries from failedAttempts to prevent memory leaks.
func (h *Handler) startAttemptCleanup(ctx context.Context) {
	h.cleanupWG.Add(1)
	go func() {
		defer h.cleanupWG.Done()
		ticker := time.NewTicker(attemptCleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				h.purgeStaleAttempts()
				h.mu.Lock()
				taskManager := h.chatGPTWebTasks
				h.mu.Unlock()
				if taskManager != nil {
					taskManager.prune()
				}
				h.mu.Lock()
				mutationTaskManager := h.chatGPTWebMutationTasks
				h.mu.Unlock()
				if mutationTaskManager != nil {
					mutationTaskManager.prune()
				}
				h.mu.Lock()
				agentIdentityTasks := h.agentIdentityTasks
				h.mu.Unlock()
				if agentIdentityTasks != nil {
					agentIdentityTasks.prune()
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// Shutdown cancels and waits for provider-owned management tasks.
func (h *Handler) Shutdown(ctx context.Context) error {
	if h == nil {
		return nil
	}
	h.cleanupStopOnce.Do(func() {
		h.mu.Lock()
		cancel := h.cleanupCancel
		h.cleanupCancel = nil
		h.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	})
	h.cleanupWG.Wait()
	h.mu.Lock()
	taskManager := h.chatGPTWebTasks
	mutationTaskManager := h.chatGPTWebMutationTasks
	agentIdentityTasks := h.agentIdentityTasks
	h.mu.Unlock()
	type shutdownResult struct{ err error }
	results := make(chan shutdownResult, 3)
	count := 0
	if taskManager != nil {
		count++
		go func() { results <- shutdownResult{err: taskManager.shutdown(ctx)} }()
	}
	if mutationTaskManager != nil {
		count++
		go func() { results <- shutdownResult{err: mutationTaskManager.shutdown(ctx)} }()
	}
	if agentIdentityTasks != nil {
		count++
		go func() { results <- shutdownResult{err: agentIdentityTasks.shutdown(ctx)} }()
	}
	var shutdownErr error
	for range count {
		shutdownErr = errors.Join(shutdownErr, (<-results).err)
	}
	return shutdownErr
}

// purgeStaleAttempts removes IP entries that have been idle beyond attemptMaxIdleTime
// and whose ban (if any) has expired.
func (h *Handler) purgeStaleAttempts() {
	now := time.Now()
	h.attemptsMu.Lock()
	defer h.attemptsMu.Unlock()
	for ip, ai := range h.failedAttempts {
		// Skip if still banned
		if !ai.blockedUntil.IsZero() && now.Before(ai.blockedUntil) {
			continue
		}
		// Remove if idle too long
		if now.Sub(ai.lastActivity) > attemptMaxIdleTime {
			delete(h.failedAttempts, ip)
		}
	}
}

// NewHandler creates a new management handler instance.
func NewHandlerWithoutConfigFilePath(cfg *config.Config, manager *coreauth.Manager) *Handler {
	return NewHandler(cfg, "", manager)
}

// SetConfig updates the in-memory config reference when the server hot-reloads.
func (h *Handler) SetConfig(cfg *config.Config) {
	if h == nil {
		return
	}
	h.mu.Lock()
	if h.proxyPoolManager != nil {
		if errProxyConfig := h.proxyPoolManager.UpdateConfig(cfg); errProxyConfig != nil {
			log.WithError(errProxyConfig).Error("failed to update proxy pool runtime configuration")
			h.mu.Unlock()
			return
		}
	}
	h.cfg = cfg
	h.mu.Unlock()
}

// SetAuthManager updates the auth manager reference used by management endpoints.
func (h *Handler) SetAuthManager(manager *coreauth.Manager) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.authManager = manager
	h.mu.Unlock()
}

// SetProxyPoolManager updates the structured proxy runtime used by management endpoints.
func (h *Handler) SetProxyPoolManager(manager *proxypool.Manager) {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.proxyPoolManager = manager
	cfg := h.cfg
	if manager != nil {
		if errProxyConfig := manager.UpdateConfig(cfg); errProxyConfig != nil {
			log.WithError(errProxyConfig).Error("failed to initialize proxy pool runtime configuration")
		}
	}
	h.mu.Unlock()
}

// SetUsageStatistics allows replacing the usage statistics reference.
func (h *Handler) SetUsageStatistics(stats *usage.RequestStatistics) { h.usageStats = stats }

func (h *Handler) usageStatisticsFilePath() string {
	if h == nil {
		return usage.StatisticsFilePath(nil)
	}
	h.mu.Lock()
	authDir := ""
	if h.cfg != nil {
		authDir = h.cfg.AuthDir
	}
	h.mu.Unlock()
	return usage.StatisticsFilePath(&config.Config{AuthDir: authDir})
}

// SetLocalPassword configures the runtime-local password accepted for localhost requests.
func (h *Handler) SetLocalPassword(password string) { h.localPassword = password }

// SetLogDirectory updates the directory where main.log should be looked up.
func (h *Handler) SetLogDirectory(dir string) {
	if dir == "" {
		return
	}
	if !filepath.IsAbs(dir) {
		if abs, err := filepath.Abs(dir); err == nil {
			dir = abs
		}
	}
	h.logDir = dir
}

// SetPostAuthHook registers a hook to be called after auth record creation but before persistence.
func (h *Handler) SetPostAuthHook(hook coreauth.PostAuthHook) {
	h.postAuthHook = hook
}

// SetAuthStatusHook registers a hook to be called after auth status changes.
func (h *Handler) SetAuthStatusHook(hook coreauth.AuthStatusHook) {
	h.authStatusHook = hook
}

// SetChatGPTWebDependencyReconcileHook registers the service-level dependency cleanup hook.
func (h *Handler) SetChatGPTWebDependencyReconcileHook(hook func(context.Context, string) ([]string, error)) {
	h.dependencyReconcileHook = hook
}

// Middleware enforces access control for management endpoints.
// All requests (local and remote) require a valid management key.
// Additionally, remote access requires allow-remote-management=true.
func (h *Handler) Middleware() gin.HandlerFunc {
	const maxFailures = 5
	const banDuration = 30 * time.Minute

	return func(c *gin.Context) {
		c.Header("X-CPA-VERSION", buildinfo.Version)
		c.Header("X-CPA-COMMIT", buildinfo.Commit)
		c.Header("X-CPA-BUILD-DATE", buildinfo.BuildDate)

		clientIP := c.ClientIP()
		localClient := clientIP == "127.0.0.1" || clientIP == "::1"
		cfg := h.cfg
		var (
			allowRemote bool
			secretHash  string
		)
		if cfg != nil {
			allowRemote = cfg.RemoteManagement.AllowRemote
			secretHash = cfg.RemoteManagement.SecretKey
		}
		if h.allowRemoteOverride {
			allowRemote = true
		}
		envSecret := h.envSecret

		fail := func() {}
		if !localClient {
			h.attemptsMu.Lock()
			ai := h.failedAttempts[clientIP]
			if ai != nil {
				if !ai.blockedUntil.IsZero() {
					if time.Now().Before(ai.blockedUntil) {
						remaining := time.Until(ai.blockedUntil).Round(time.Second)
						h.attemptsMu.Unlock()
						c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": fmt.Sprintf("IP banned due to too many failed attempts. Try again in %s", remaining)})
						return
					}
					// Ban expired, reset state
					ai.blockedUntil = time.Time{}
					ai.count = 0
				}
			}
			h.attemptsMu.Unlock()

			if !allowRemote {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "remote management disabled"})
				return
			}

			fail = func() {
				h.attemptsMu.Lock()
				aip := h.failedAttempts[clientIP]
				if aip == nil {
					aip = &attemptInfo{}
					h.failedAttempts[clientIP] = aip
				}
				aip.count++
				aip.lastActivity = time.Now()
				if aip.count >= maxFailures {
					aip.blockedUntil = time.Now().Add(banDuration)
					aip.count = 0
				}
				h.attemptsMu.Unlock()
			}
		}
		if secretHash == "" && envSecret == "" {
			c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "remote management key not set"})
			return
		}

		// Accept either Authorization: Bearer <key> or X-Management-Key
		var provided string
		if ah := c.GetHeader("Authorization"); ah != "" {
			parts := strings.SplitN(ah, " ", 2)
			if len(parts) == 2 && strings.ToLower(parts[0]) == "bearer" {
				provided = parts[1]
			} else {
				provided = ah
			}
		}
		if provided == "" {
			provided = c.GetHeader("X-Management-Key")
		}

		if provided == "" {
			if !localClient {
				fail()
			}
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing management key"})
			return
		}

		if localClient {
			if lp := h.localPassword; lp != "" {
				if subtle.ConstantTimeCompare([]byte(provided), []byte(lp)) == 1 {
					c.Next()
					return
				}
			}
		}

		if envSecret != "" && subtle.ConstantTimeCompare([]byte(provided), []byte(envSecret)) == 1 {
			if !localClient {
				h.attemptsMu.Lock()
				if ai := h.failedAttempts[clientIP]; ai != nil {
					ai.count = 0
					ai.blockedUntil = time.Time{}
				}
				h.attemptsMu.Unlock()
			}
			c.Next()
			return
		}

		if secretHash == "" || bcrypt.CompareHashAndPassword([]byte(secretHash), []byte(provided)) != nil {
			if !localClient {
				fail()
			}
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid management key"})
			return
		}

		if !localClient {
			h.attemptsMu.Lock()
			if ai := h.failedAttempts[clientIP]; ai != nil {
				ai.count = 0
				ai.blockedUntil = time.Time{}
			}
			h.attemptsMu.Unlock()
		}

		c.Next()
	}
}

// persist saves the current in-memory config to disk.
func (h *Handler) persist(c *gin.Context) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.persistLocked(c)
}

// persistLocked saves the current in-memory config to disk.
// It expects the caller to hold h.mu.
func (h *Handler) persistLocked(c *gin.Context) bool {
	previousBody, previousExisted, errPreviousBody := h.readPersistedConfigBodyLocked()
	if errPreviousBody != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read current config"})
		return false
	}
	previousCfg, errPreviousConfig := config.LoadConfigOptional(h.configFilePath, true)
	if errPreviousConfig != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load current config"})
		return false
	}
	// Preserve comments when writing
	if err := config.SaveConfigPreserveComments(h.configFilePath, h.cfg); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to save config: %v", err)})
		return false
	}
	if h.proxyPoolManager != nil {
		if errProxyConfig := h.proxyPoolManager.UpdateConfig(h.cfg); errProxyConfig != nil {
			errRollbackFile := h.restorePersistedConfigLocked(previousBody, previousExisted, previousCfg)
			errRollbackRuntime := h.proxyPoolManager.UpdateConfig(previousCfg)
			if errRollbackFile != nil || errRollbackRuntime != nil {
				log.WithError(errors.Join(errProxyConfig, errRollbackFile, errRollbackRuntime)).Error("proxy pool runtime update failed and rollback was incomplete")
				c.JSON(http.StatusInternalServerError, gin.H{"error": "proxy pool runtime update failed and rollback was incomplete"})
				return false
			}
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update proxy pool runtime configuration"})
			return false
		}
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
	return true
}

func (h *Handler) restorePersistedConfigLocked(previousBody []byte, previousExisted bool, previousCfg *config.Config) error {
	if h == nil {
		return nil
	}
	if h.cfg != nil && previousCfg != nil {
		*h.cfg = *previousCfg
	} else {
		h.cfg = previousCfg
	}
	return h.restorePersistedConfigFileLocked(previousBody, previousExisted)
}

func (h *Handler) readPersistedConfigBodyLocked() ([]byte, bool, error) {
	if h == nil {
		return nil, false, nil
	}
	body, errRead := os.ReadFile(h.configFilePath)
	if errRead == nil {
		return body, true, nil
	}
	if errors.Is(errRead, os.ErrNotExist) {
		return nil, false, nil
	}
	return nil, false, errRead
}

func (h *Handler) restorePersistedConfigFileLocked(previousBody []byte, previousExisted bool) error {
	if h == nil {
		return nil
	}
	if previousExisted {
		return WriteConfig(h.configFilePath, previousBody)
	}
	if errRemove := os.Remove(h.configFilePath); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
		return errRemove
	}
	return nil
}

// Helper methods for simple types
func (h *Handler) updateBoolField(c *gin.Context, set func(bool)) {
	var body struct {
		Value *bool `json:"value"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	set(*body.Value)
	h.persist(c)
}

func (h *Handler) updateIntField(c *gin.Context, set func(int)) {
	h.updateIntFieldNormalized(c, nil, set)
}

func (h *Handler) updateIntFieldNormalized(c *gin.Context, normalize func(int) int, set func(int)) {
	var body struct {
		Value *int `json:"value"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	value := *body.Value
	if normalize != nil {
		value = normalize(value)
	}
	set(value)
	h.persist(c)
}

func (h *Handler) updateStringField(c *gin.Context, set func(string)) {
	var body struct {
		Value *string `json:"value"`
	}
	if err := c.ShouldBindJSON(&body); err != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	set(*body.Value)
	h.persist(c)
}
