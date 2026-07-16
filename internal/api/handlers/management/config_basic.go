package management

import (
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

func (h *Handler) GetConfig(c *gin.Context) {
	if h == nil {
		c.JSON(200, gin.H{})
		return
	}
	h.mu.Lock()
	if h.cfg == nil {
		h.mu.Unlock()
		c.JSON(200, gin.H{})
		return
	}
	snapshot, errSnapshot := cloneConfigWithMaskedProxyURLs(h.cfg)
	h.mu.Unlock()
	if errSnapshot != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to snapshot configuration"})
		return
	}
	c.JSON(200, &snapshot)
}

type releaseInfo struct {
	TagName string `json:"tag_name"`
	Name    string `json:"name"`
}

// GetLatestVersion returns the latest release version from GitHub without downloading assets.
func (h *Handler) GetLatestVersion(c *gin.Context) {
	client := &http.Client{Timeout: 10 * time.Second}
	proxyURL := ""
	if h != nil {
		h.mu.Lock()
		if h.cfg != nil {
			proxyURL = strings.TrimSpace(h.cfg.ProxyURL)
		}
		h.mu.Unlock()
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
	previousCfg := h.cfg
	if WriteConfig(h.configFilePath, body) != nil {
		h.mu.Unlock()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "write_failed", "message": "failed to write config"})
		return
	}
	// Reload into handler to keep memory in sync
	newCfg, err := config.LoadConfig(h.configFilePath)
	if err != nil {
		h.rollbackConfigYAMLLocked(previousBody, previousExisted, previousCfg)
		h.mu.Unlock()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "reload_failed", "message": err.Error()})
		return
	}
	proxyPoolManager := h.proxyPoolManager
	if proxyPoolManager != nil {
		if errProxyConfig := proxyPoolManager.UpdateConfig(newCfg); errProxyConfig != nil {
			h.rollbackConfigYAMLLocked(previousBody, previousExisted, previousCfg)
			if errRollbackRuntime := proxyPoolManager.UpdateConfig(previousCfg); errRollbackRuntime != nil {
				log.WithError(errRollbackRuntime).Error("failed to roll back proxy pool runtime after config upload")
			}
			h.mu.Unlock()
			c.JSON(http.StatusInternalServerError, gin.H{"error": "runtime_update_failed", "message": "failed to update proxy pool runtime"})
			return
		}
	}
	h.cfg = newCfg
	h.mu.Unlock()
	c.JSON(http.StatusOK, gin.H{"ok": true, "changed": []string{"config"}})
}

func (h *Handler) rollbackConfigYAMLLocked(previousBody []byte, previousExisted bool, previousCfg *config.Config) {
	h.cfg = previousCfg
	if previousExisted {
		if errRestore := WriteConfig(h.configFilePath, previousBody); errRestore != nil {
			log.WithError(errRestore).Error("failed to roll back config upload")
		}
		return
	}
	if errRemove := os.Remove(h.configFilePath); errRemove != nil && !errors.Is(errRemove, os.ErrNotExist) {
		log.WithError(errRemove).Error("failed to remove config created by rejected upload")
	}
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
func (h *Handler) GetDebug(c *gin.Context) { c.JSON(200, gin.H{"debug": h.cfg.Debug}) }
func (h *Handler) PutDebug(c *gin.Context) { h.updateBoolField(c, func(v bool) { h.cfg.Debug = v }) }

// UsageStatisticsEnabled
func (h *Handler) GetUsageStatisticsEnabled(c *gin.Context) {
	c.JSON(200, gin.H{"usage-statistics-enabled": h.cfg.UsageStatisticsEnabled})
}
func (h *Handler) PutUsageStatisticsEnabled(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.UsageStatisticsEnabled = v })
}

// UsageStatisticsEnabled
func (h *Handler) GetLoggingToFile(c *gin.Context) {
	c.JSON(200, gin.H{"logging-to-file": h.cfg.LoggingToFile})
}
func (h *Handler) PutLoggingToFile(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.LoggingToFile = v })
}

// LogsMaxTotalSizeMB
func (h *Handler) GetLogsMaxTotalSizeMB(c *gin.Context) {
	c.JSON(200, gin.H{"logs-max-total-size-mb": h.cfg.LogsMaxTotalSizeMB})
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
	h.cfg.LogsMaxTotalSizeMB = value
	h.persist(c)
}

// ErrorLogsMaxFiles
func (h *Handler) GetErrorLogsMaxFiles(c *gin.Context) {
	c.JSON(200, gin.H{"error-logs-max-files": h.cfg.ErrorLogsMaxFiles})
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
	h.cfg.ErrorLogsMaxFiles = value
	h.persist(c)
}

// Request log
func (h *Handler) GetRequestLog(c *gin.Context) { c.JSON(200, gin.H{"request-log": h.cfg.RequestLog}) }
func (h *Handler) PutRequestLog(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.RequestLog = v })
}

// Request body audit
func (h *Handler) GetRequestBodyAudit(c *gin.Context) {
	if h == nil || h.cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"request-body-audit": config.NormalizeRequestBodyAudit(h.cfg.RequestBodyAudit)})
}

func (h *Handler) PutRequestBodyAudit(c *gin.Context) {
	if h == nil || h.cfg == nil {
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
	h.cfg.RequestBodyAudit = config.NormalizeRequestBodyAudit(*body.Value)
	h.persist(c)
}

func (h *Handler) GetRequestBodyRelease(c *gin.Context) {
	if h == nil || h.cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"request-body-release": config.NormalizeRequestBodyRelease(h.cfg.RequestBodyRelease)})
}

func (h *Handler) PutRequestBodyRelease(c *gin.Context) {
	if h == nil || h.cfg == nil {
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
	h.cfg.RequestBodyRelease = config.NormalizeRequestBodyRelease(*body.Value)
	h.persist(c)
}

// Websocket auth
func (h *Handler) GetWebsocketAuth(c *gin.Context) {
	c.JSON(200, gin.H{"ws-auth": h.cfg.WebsocketAuth})
}
func (h *Handler) PutWebsocketAuth(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.WebsocketAuth = v })
}

// Request retry
func (h *Handler) GetRequestRetry(c *gin.Context) {
	c.JSON(200, gin.H{"request-retry": h.cfg.RequestRetry})
}
func (h *Handler) PutRequestRetry(c *gin.Context) {
	h.updateIntFieldNormalized(c, clampNonNegativeInt, func(v int) { h.cfg.RequestRetry = v })
}

func (h *Handler) GetNonRetryableErrors(c *gin.Context) {
	if h == nil || h.cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"non-retryable-errors": config.NormalizeNonRetryableErrorRules(h.cfg.NonRetryableErrors)})
}

func (h *Handler) PutNonRetryableErrors(c *gin.Context) {
	if h == nil || h.cfg == nil {
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
	h.cfg.NonRetryableErrors = config.NormalizeNonRetryableErrorRules(*body.Value)
	h.persist(c)
}

func (h *Handler) GetAuthModelExclusions(c *gin.Context) {
	if h == nil || h.cfg == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "configuration unavailable"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"auth-model-exclusions": config.NormalizeAuthModelExclusionRules(h.cfg.AuthModelExclusions)})
}

func (h *Handler) PutAuthModelExclusions(c *gin.Context) {
	if h == nil || h.cfg == nil {
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
	h.cfg.AuthModelExclusions = config.NormalizeAuthModelExclusionRules(*body.Value)
	h.persist(c)
}

// Max retry credentials
func (h *Handler) GetMaxRetryCredentials(c *gin.Context) {
	c.JSON(200, gin.H{"max-retry-credentials": h.cfg.MaxRetryCredentials})
}
func (h *Handler) PutMaxRetryCredentials(c *gin.Context) {
	h.updateIntFieldNormalized(c, clampNonNegativeInt, func(v int) { h.cfg.MaxRetryCredentials = v })
}

// Max retry interval
func (h *Handler) GetMaxRetryInterval(c *gin.Context) {
	c.JSON(200, gin.H{"max-retry-interval": h.cfg.MaxRetryInterval})
}
func (h *Handler) PutMaxRetryInterval(c *gin.Context) {
	h.updateIntFieldNormalized(c, clampNonNegativeInt, func(v int) { h.cfg.MaxRetryInterval = v })
}

// ForceModelPrefix
func (h *Handler) GetForceModelPrefix(c *gin.Context) {
	c.JSON(200, gin.H{"force-model-prefix": h.cfg.ForceModelPrefix})
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

func (h *Handler) updateRoutingConfig(c *gin.Context, update func(*config.RoutingConfig)) bool {
	if h == nil || h.cfg == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "config not initialized"})
		return false
	}
	routing := h.cfg.Routing
	update(&routing)
	normalized, errNormalize := config.NormalizeRoutingConfig(routing)
	if errNormalize != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid routing config", "message": errNormalize.Error()})
		return false
	}
	h.cfg.Routing = normalized
	return true
}

// RoutingStrategy
func (h *Handler) GetRoutingStrategy(c *gin.Context) {
	strategy, ok := normalizeRoutingStrategy(h.cfg.Routing.Strategy)
	if !ok {
		c.JSON(200, gin.H{"strategy": strings.TrimSpace(h.cfg.Routing.Strategy)})
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
	h.persist(c)
}

func (h *Handler) GetRoutingFillFirstRange(c *gin.Context) {
	c.JSON(200, gin.H{"fill-first-range": normalizeFillFirstRange(h.cfg.Routing.FillFirstRange)})
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
	h.persist(c)
}

func (h *Handler) GetRoutingFillFirstPerAuthRPM(c *gin.Context) {
	c.JSON(200, gin.H{"fill-first-per-auth-rpm": normalizeFillFirstPerAuthRPM(h.cfg.Routing.FillFirstPerAuthRPM)})
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
	h.persist(c)
}

func (h *Handler) GetRoutingPriorityOverrides(c *gin.Context) {
	c.JSON(200, gin.H{"priority-overrides": h.cfg.Routing.PriorityOverrides})
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
	h.persist(c)
}

// Proxy URL
func (h *Handler) GetProxyURL(c *gin.Context) {
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
	proxyURL := proxyutil.MaskProxyURL(h.cfg.ProxyURL)
	h.mu.Unlock()
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
