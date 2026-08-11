package management

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

const (
	latestReleaseURL       = "https://api.github.com/repos/router-for-me/CLIProxyAPI/releases/latest"
	latestReleaseUserAgent = "CLIProxyAPI"
)

type safeRemoteManagementConfig struct {
	LiveLogs config.LiveLogsConfig `json:"live-logs"`
}

type configResponse struct {
	*config.Config
	RemoteManagement safeRemoteManagementConfig `json:"remote-management"`
}

func (h *Handler) GetConfig(c *gin.Context) {
	if h == nil {
		c.JSON(200, gin.H{})
		return
	}
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(200, gin.H{})
		return
	}
	snapshot, errSnapshot := cloneConfigWithMaskedProxyURLs(cfg)
	if errSnapshot != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to snapshot configuration"})
		return
	}
	c.JSON(200, &configResponse{
		Config: &snapshot,
		RemoteManagement: safeRemoteManagementConfig{
			LiveLogs: cfg.RemoteManagement.LiveLogs,
		},
	})
}

type releaseInfo struct {
	TagName string `json:"tag_name"`
	Name    string `json:"name"`
}

// GetLatestVersion returns the latest release version from GitHub without downloading assets.
func (h *Handler) GetLatestVersion(c *gin.Context) {
	client := &http.Client{Timeout: 10 * time.Second}
	proxyURL := ""
	if cfg := h.currentConfig(); cfg != nil {
		proxyURL = strings.TrimSpace(cfg.ProxyURL)
	}
	if proxyURL != "" {
		sdkCfg := &sdkconfig.SDKConfig{ProxyURL: proxyURL}
		util.SetProxy(sdkCfg, client)
	}

	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, latestReleaseURL, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "request_create_failed", "message": err.Error()})
		return
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", latestReleaseUserAgent)

	resp, err := client.Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "request_failed", "message": err.Error()})
		return
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.WithError(errClose).Debug("failed to close latest version response body")
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		c.JSON(http.StatusBadGateway, gin.H{"error": "unexpected_status", "message": fmt.Sprintf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))})
		return
	}

	var info releaseInfo
	if errDecode := json.NewDecoder(resp.Body).Decode(&info); errDecode != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "decode_failed", "message": errDecode.Error()})
		return
	}

	version := strings.TrimSpace(info.TagName)
	if version == "" {
		version = strings.TrimSpace(info.Name)
	}
	if version == "" {
		c.JSON(http.StatusBadGateway, gin.H{"error": "invalid_response", "message": "missing release version"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"latest-version": version})
}

func WriteConfig(path string, data []byte) error {
	data = config.NormalizeCommentIndentation(data)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, errWrite := f.Write(data); errWrite != nil {
		_ = f.Close()
		return errWrite
	}
	if errSync := f.Sync(); errSync != nil {
		_ = f.Close()
		return errSync
	}
	return f.Close()
}

func (h *Handler) PutConfigYAML(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_yaml", "message": "cannot read request body"})
		return
	}
	var cfg config.Config
	if err = yaml.Unmarshal(body, &cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_yaml", "message": err.Error()})
		return
	}
	// Validate config using LoadConfigOptional with optional=false to enforce parsing
	tmpDir := filepath.Dir(h.configFilePath)
	tmpFile, err := os.CreateTemp(tmpDir, "config-validate-*.yaml")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "write_failed", "message": err.Error()})
		return
	}
	tempFile := tmpFile.Name()
	if _, errWrite := tmpFile.Write(body); errWrite != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tempFile)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "write_failed", "message": errWrite.Error()})
		return
	}
	if errClose := tmpFile.Close(); errClose != nil {
		_ = os.Remove(tempFile)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "write_failed", "message": errClose.Error()})
		return
	}
	defer func() {
		_ = os.Remove(tempFile)
	}()
	_, err = config.LoadConfigOptional(tempFile, false)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "invalid_config", "message": err.Error()})
		return
	}
	h.mu.Lock()
	previousBody, errPrevious := os.ReadFile(h.configFilePath)
	previousExisted := errPrevious == nil
	if errPrevious != nil && !errors.Is(errPrevious, os.ErrNotExist) {
		h.mu.Unlock()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "read_failed", "message": "failed to read current config"})
		return
	}
	previousCfg, errPreviousConfig := config.LoadConfigOptional(h.configFilePath, true)
	if errPreviousConfig != nil {
		h.mu.Unlock()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "read_failed", "message": "failed to load current config"})
		return
	}
	if WriteConfig(h.configFilePath, body) != nil {
		h.mu.Unlock()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "write_failed", "message": "failed to write config"})
		return
	}
	// Reload into handler to keep memory in sync
	newCfg, err := config.LoadConfig(h.configFilePath)
	if err != nil {
		errRollback := h.rollbackConfigYAMLLocked(previousBody, previousExisted, previousCfg)
		h.mu.Unlock()
		if errRollback != nil {
			log.WithError(errors.Join(err, errRollback)).Error("config upload reload failed and rollback was incomplete")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "rollback_failed", "message": "config reload failed and rollback was incomplete"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "reload_failed", "message": err.Error()})
		return
	}
	publishedCfg, errPublishedCfg := config.Clone(newCfg)
	if errPublishedCfg != nil {
		errRollback := h.rollbackConfigYAMLLocked(previousBody, previousExisted, previousCfg)
		h.mu.Unlock()
		if errRollback != nil {
			log.WithError(errors.Join(errPublishedCfg, errRollback)).Error("config upload snapshot failed and rollback was incomplete")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "rollback_failed", "message": "config snapshot failed and rollback was incomplete"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "snapshot_failed", "message": errPublishedCfg.Error()})
		return
	}
	h.cfg = newCfg
	h.mu.Unlock()
	result, errApply := h.applyRuntimeConfig(c.Request.Context(), newCfg)
	if errApply != nil {
		h.mu.Lock()
		errRollbackFile := h.rollbackConfigYAMLLocked(previousBody, previousExisted, previousCfg)
		h.mu.Unlock()
		var errRollbackRuntime error
		if previousCfg != nil {
			_, errRollbackRuntime = h.applyRuntimeConfig(context.WithoutCancel(c.Request.Context()), previousCfg)
		}
		if errRollbackFile != nil || errRollbackRuntime != nil {
			log.WithError(errors.Join(errApply, errRollbackFile, errRollbackRuntime)).Error("config upload runtime update failed and rollback was incomplete")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "rollback_failed", "message": "runtime update failed and rollback was incomplete"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "runtime_update_failed", "message": errApply.Error()})
		return
	}
	h.configSnapshot.Store(publishedCfg)
	response := gin.H{"ok": true, "changed": []string{"config"}, "applied": result.Applied}
	if result.RestartRequired {
		response["restart_required"] = true
		response["restart_fields"] = result.RestartFields
	}
	c.JSON(http.StatusOK, response)
}

func (h *Handler) rollbackConfigYAMLLocked(previousBody []byte, previousExisted bool, previousCfg *config.Config) error {
	h.cfg = previousCfg
	if previousExisted {
		return WriteConfig(h.configFilePath, previousBody)
	}
	if errRemove := os.Remove(h.configFilePath); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
		return errRemove
	}
	return nil
}

// GetConfigYAML returns the raw config.yaml file bytes without re-encoding.
// It preserves comments and original formatting/styles.
func (h *Handler) GetConfigYAML(c *gin.Context) {
	data, err := os.ReadFile(h.configFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "not_found", "message": "config file not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "read_failed", "message": err.Error()})
		return
	}
	c.Header("Content-Type", "application/yaml; charset=utf-8")
	c.Header("Cache-Control", "no-store")
	c.Header("X-Content-Type-Options", "nosniff")
	// Write raw bytes as-is
	_, _ = c.Writer.Write(data)
}

// Debug
func (h *Handler) GetDebug(c *gin.Context) {
	cfg := h.currentConfig()
	c.JSON(200, gin.H{"debug": cfg != nil && cfg.Debug})
}
func (h *Handler) PutDebug(c *gin.Context) { h.updateBoolField(c, func(v bool) { h.cfg.Debug = v }) }

// UsageStatisticsEnabled
func (h *Handler) GetUsageStatisticsEnabled(c *gin.Context) {
	cfg := h.currentConfig()
	c.JSON(200, gin.H{"usage-statistics-enabled": cfg != nil && cfg.UsageStatisticsEnabled})
}
func (h *Handler) PutUsageStatisticsEnabled(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.UsageStatisticsEnabled = v })
}

// UsageStatisticsEnabled
func (h *Handler) GetLoggingToFile(c *gin.Context) {
	cfg := h.currentConfig()
	c.JSON(200, gin.H{"logging-to-file": cfg != nil && cfg.LoggingToFile})
}
func (h *Handler) PutLoggingToFile(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.LoggingToFile = v })
}

// LogsMaxTotalSizeMB
func (h *Handler) GetLogsMaxTotalSizeMB(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(200, gin.H{"logs-max-total-size-mb": cfg.LogsMaxTotalSizeMB})
}
func (h *Handler) PutLogsMaxTotalSizeMB(c *gin.Context) {
	var body struct {
		Value *int `json:"value"`
	}
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	value := *body.Value
	if value < 0 {
		value = 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cfg.LogsMaxTotalSizeMB = value
	h.persistLocked(c)
}

// ErrorLogsMaxFiles
func (h *Handler) GetErrorLogsMaxFiles(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(200, gin.H{"error-logs-max-files": cfg.ErrorLogsMaxFiles})
}
func (h *Handler) PutErrorLogsMaxFiles(c *gin.Context) {
	var body struct {
		Value *int `json:"value"`
	}
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	value := *body.Value
	if value < 0 {
		value = 10
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cfg.ErrorLogsMaxFiles = value
	h.persistLocked(c)
}

// Request log
func (h *Handler) GetRequestLog(c *gin.Context) {
	cfg := h.currentConfig()
	c.JSON(200, gin.H{"request-log": cfg != nil && cfg.RequestLog})
}
func (h *Handler) PutRequestLog(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.RequestLog = v })
}

// Request body audit
func (h *Handler) GetRequestBodyAudit(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"request-body-audit": config.NormalizeRequestBodyAudit(cfg.RequestBodyAudit)})
}

func (h *Handler) PutRequestBodyAudit(c *gin.Context) {
	if h == nil || h.currentConfig() == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	var body struct {
		Value *config.RequestBodyAuditConfig `json:"value"`
	}
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cfg.RequestBodyAudit = config.NormalizeRequestBodyAudit(*body.Value)
	h.persistLocked(c)
}

func (h *Handler) GetRequestBodyRelease(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"request-body-release": config.NormalizeRequestBodyRelease(cfg.RequestBodyRelease)})
}

func (h *Handler) PutRequestBodyRelease(c *gin.Context) {
	if h == nil || h.currentConfig() == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	var body struct {
		Value *config.RequestBodyReleaseConfig `json:"value"`
	}
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cfg.RequestBodyRelease = config.NormalizeRequestBodyRelease(*body.Value)
	h.persistLocked(c)
}

// Websocket auth
func (h *Handler) GetWebsocketAuth(c *gin.Context) {
	cfg := h.currentConfig()
	c.JSON(200, gin.H{"ws-auth": cfg != nil && cfg.WebsocketAuth})
}
func (h *Handler) PutWebsocketAuth(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.WebsocketAuth = v })
}

// Request retry
func (h *Handler) GetRequestRetry(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(200, gin.H{"request-retry": cfg.RequestRetry})
}
func (h *Handler) PutRequestRetry(c *gin.Context) {
	h.updateIntFieldNormalized(c, clampNonNegativeInt, func(v int) { h.cfg.RequestRetry = v })
}

func (h *Handler) GetNonRetryableErrors(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"non-retryable-errors": config.NormalizeNonRetryableErrorRules(cfg.NonRetryableErrors)})
}

func (h *Handler) PutNonRetryableErrors(c *gin.Context) {
	if h == nil || h.currentConfig() == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	var body struct {
		Value *[]config.NonRetryableErrorRule `json:"value"`
	}
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cfg.NonRetryableErrors = config.NormalizeNonRetryableErrorRules(*body.Value)
	h.persistLocked(c)
}

func (h *Handler) GetAuthModelExclusions(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"auth-model-exclusions": config.NormalizeAuthModelExclusionRules(cfg.AuthModelExclusions)})
}

func (h *Handler) PutAuthModelExclusions(c *gin.Context) {
	if h == nil || h.currentConfig() == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	var body struct {
		Value *[]config.AuthModelExclusionRule `json:"value"`
	}
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cfg.AuthModelExclusions = config.NormalizeAuthModelExclusionRules(*body.Value)
	h.persistLocked(c)
}

// Max retry credentials
func (h *Handler) GetMaxRetryCredentials(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(200, gin.H{"max-retry-credentials": cfg.MaxRetryCredentials})
}
func (h *Handler) PutMaxRetryCredentials(c *gin.Context) {
	h.updateIntFieldNormalized(c, clampNonNegativeInt, func(v int) { h.cfg.MaxRetryCredentials = v })
}

// Max retry interval
func (h *Handler) GetMaxRetryInterval(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(200, gin.H{"max-retry-interval": cfg.MaxRetryInterval})
}
func (h *Handler) PutMaxRetryInterval(c *gin.Context) {
	h.updateIntFieldNormalized(c, clampNonNegativeInt, func(v int) { h.cfg.MaxRetryInterval = v })
}

// ForceModelPrefix
func (h *Handler) GetForceModelPrefix(c *gin.Context) {
	cfg := h.currentConfig()
	c.JSON(200, gin.H{"force-model-prefix": cfg != nil && cfg.ForceModelPrefix})
}
func (h *Handler) PutForceModelPrefix(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.ForceModelPrefix = v })
}

func normalizeRoutingStrategy(strategy string) (string, bool) {
	return config.NormalizeRoutingStrategy(strategy)
}

func clampNonNegativeInt(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

func normalizeFillFirstRange(value int) int {
	return config.NormalizeFillFirstRange(value)
}

func normalizeFillFirstPerAuthRPM(value int) int {
	return config.NormalizeFillFirstPerAuthRPM(value)
}

func normalizePerAuthRequestLimit(value int) int {
	return config.NormalizePerAuthRequestLimit(value)
}

func normalizePerAuthRequestWindowMinutes(value int) int {
	return config.NormalizePerAuthRequestWindowMinutes(value)
}

func (h *Handler) updateRoutingConfig(c *gin.Context, update func(*config.RoutingConfig)) bool {
	if h == nil || h.currentConfig() == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "config not initialized"})
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	routing := h.cfg.Routing
	update(&routing)
	normalized, errNormalize := config.NormalizeRoutingConfig(routing)
	if errNormalize != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid routing config", "message": errNormalize.Error()})
		return false
	}
	h.cfg.Routing = normalized
	return h.persistLocked(c)
}

// RoutingStrategy
func (h *Handler) GetRoutingStrategy(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	strategy, ok := normalizeRoutingStrategy(cfg.Routing.Strategy)
	if !ok {
		c.JSON(200, gin.H{"strategy": strings.TrimSpace(cfg.Routing.Strategy)})
		return
	}
	c.JSON(200, gin.H{"strategy": strategy})
}
func (h *Handler) PutRoutingStrategy(c *gin.Context) {
	var body struct {
		Value *string `json:"value"`
	}
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	normalized, ok := normalizeRoutingStrategy(*body.Value)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid strategy"})
		return
	}
	if !h.updateRoutingConfig(c, func(routing *config.RoutingConfig) {
		routing.Strategy = normalized
	}) {
		return
	}
}

func (h *Handler) GetRoutingFillFirstRange(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(200, gin.H{"fill-first-range": normalizeFillFirstRange(cfg.Routing.FillFirstRange)})
}

func (h *Handler) PutRoutingFillFirstRange(c *gin.Context) {
	var body struct {
		Value *int `json:"value"`
	}
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if !h.updateRoutingConfig(c, func(routing *config.RoutingConfig) {
		routing.FillFirstRange = normalizeFillFirstRange(*body.Value)
	}) {
		return
	}
}

func (h *Handler) GetRoutingFillFirstPerAuthRPM(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(200, gin.H{"fill-first-per-auth-rpm": normalizeFillFirstPerAuthRPM(cfg.Routing.FillFirstPerAuthRPM)})
}

func (h *Handler) PutRoutingFillFirstPerAuthRPM(c *gin.Context) {
	var body struct {
		Value *int `json:"value"`
	}
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if !h.updateRoutingConfig(c, func(routing *config.RoutingConfig) {
		routing.FillFirstPerAuthRPM = normalizeFillFirstPerAuthRPM(*body.Value)
	}) {
		return
	}
}

func (h *Handler) GetRoutingPerAuthRequestLimit(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(200, gin.H{"per-auth-request-limit": normalizePerAuthRequestLimit(cfg.Routing.PerAuthRequestLimit)})
}

func (h *Handler) PutRoutingPerAuthRequestLimit(c *gin.Context) {
	var body struct {
		Value *int `json:"value"`
	}
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if !h.updateRoutingConfig(c, func(routing *config.RoutingConfig) {
		routing.PerAuthRequestLimit = normalizePerAuthRequestLimit(*body.Value)
	}) {
		return
	}
}

func (h *Handler) GetRoutingPerAuthRequestWindowMinutes(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(200, gin.H{"per-auth-request-window-minutes": normalizePerAuthRequestWindowMinutes(cfg.Routing.PerAuthRequestWindowMinutes)})
}

func (h *Handler) PutRoutingPerAuthRequestWindowMinutes(c *gin.Context) {
	var body struct {
		Value *int `json:"value"`
	}
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if !h.updateRoutingConfig(c, func(routing *config.RoutingConfig) {
		routing.PerAuthRequestWindowMinutes = normalizePerAuthRequestWindowMinutes(*body.Value)
	}) {
		return
	}
}

func (h *Handler) GetRoutingPriorityOverrides(c *gin.Context) {
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(200, gin.H{"priority-overrides": cfg.Routing.PriorityOverrides})
}

func (h *Handler) PutRoutingPriorityOverrides(c *gin.Context) {
	var body struct {
		Value *[]config.RoutingPriorityOverride `json:"value"`
	}
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	if !h.updateRoutingConfig(c, func(routing *config.RoutingConfig) {
		routing.PriorityOverrides = *body.Value
	}) {
		return
	}
}

// Proxy URL
func (h *Handler) GetProxyURL(c *gin.Context) {
	if h == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	cfg := h.currentConfig()
	if cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	proxyURL := proxyutil.MaskProxyURL(cfg.ProxyURL)
	c.JSON(200, gin.H{"proxy-url": proxyURL})
}
func (h *Handler) PutProxyURL(c *gin.Context) {
	var body struct {
		Value *string `json:"value"`
	}
	if errBind := c.ShouldBindJSON(&body); errBind != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	h.mu.Lock()
	previous := h.cfg.ProxyURL
	value := strings.TrimSpace(*body.Value)
	if isMaskedProxyURL(value) && !replaceMaskedProxyRequested(c) {
		if !proxyutil.MaskedProxyURLMatches(value, previous) {
			h.mu.Unlock()
			c.JSON(http.StatusBadRequest, gin.H{"error": "value must contain the complete proxy credential"})
			return
		}
		value = previous
	}
	h.cfg.ProxyURL = value
	if !h.persistLocked(c) {
		h.cfg.ProxyURL = previous
	}
	h.mu.Unlock()
}
func (h *Handler) DeleteProxyURL(c *gin.Context) {
	h.mu.Lock()
	previous := h.cfg.ProxyURL
	h.cfg.ProxyURL = ""
	if !h.persistLocked(c) {
		h.cfg.ProxyURL = previous
	}
	h.mu.Unlock()
}
