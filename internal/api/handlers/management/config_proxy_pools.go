package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	log "github.com/sirupsen/logrus"
)

// GetProxyPools returns complete structured proxy pools to authenticated management clients.
func (h *Handler) GetProxyPools(c *gin.Context) {
	var pools []config.ProxyPoolConfig
	if cfg := h.currentConfig(); cfg != nil {
		pools = cloneProxyPools(cfg.ProxyPools)
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, gin.H{"proxy-pools": pools})
}

// PostProxyPool creates one structured proxy pool.
func (h *Handler) PostProxyPool(c *gin.Context) {
	pool, ok := parseProxyPoolBody(c)
	if !ok {
		return
	}
	if containsMaskedProxySecret(pool) && !replaceMaskedProxyRequested(c) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "url-template must contain the complete proxy credential"})
		return
	}

	h.mu.Lock()
	previousPools := h.cfg.ProxyPools
	previousRules := h.cfg.ProxyRules
	for _, existing := range h.cfg.ProxyPools {
		if strings.EqualFold(strings.TrimSpace(existing.Name), strings.TrimSpace(pool.Name)) {
			h.mu.Unlock()
			c.JSON(http.StatusConflict, gin.H{"error": "proxy pool already exists"})
			return
		}
	}
	pools := append(cloneProxyPools(h.cfg.ProxyPools), pool)
	normalizedPools, normalizedRules, errNormalize := config.NormalizeProxyConfiguration(pools, h.cfg.ProxyRules)
	if errNormalize != nil {
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": errNormalize.Error()})
		return
	}
	h.cfg.ProxyPools = normalizedPools
	h.cfg.ProxyRules = normalizedRules
	if !h.persistLocked(c) {
		h.cfg.ProxyPools = previousPools
		h.cfg.ProxyRules = previousRules
	}
	h.mu.Unlock()
}

// PatchProxyPool updates one structured proxy pool.
func (h *Handler) PatchProxyPool(c *gin.Context) {
	type entryPatch struct {
		ID          string  `json:"id"`
		URLTemplate *string `json:"url-template"`
		Ports       *string `json:"ports"`
	}
	var body struct {
		Name                 *string       `json:"name"`
		PlaceholderCharset   *string       `json:"placeholder-charset"`
		CheckIntervalSeconds *int          `json:"check-interval-seconds"`
		BindAttempts         *int          `json:"bind-attempts"`
		SpreadBindings       *bool         `json:"spread-bindings"`
		Entries              *[]entryPatch `json:"entries"`
		DeleteEntryIDs       []string      `json:"delete-entry-ids"`
	}
	if errBind := c.ShouldBindJSON(&body); errBind != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}

	h.mu.Lock()
	pools := cloneProxyPools(h.cfg.ProxyPools)
	index := findProxyPool(pools, c.Param("name"))
	if index < 0 {
		h.mu.Unlock()
		c.JSON(http.StatusNotFound, gin.H{"error": "proxy pool not found"})
		return
	}
	oldName := pools[index].Name
	if body.Name != nil {
		for otherIndex := range pools {
			if otherIndex != index && strings.EqualFold(strings.TrimSpace(pools[otherIndex].Name), strings.TrimSpace(*body.Name)) {
				h.mu.Unlock()
				c.JSON(http.StatusConflict, gin.H{"error": "proxy pool already exists"})
				return
			}
		}
		pools[index].Name = *body.Name
	}
	if body.PlaceholderCharset != nil {
		pools[index].PlaceholderCharset = *body.PlaceholderCharset
	}
	if body.CheckIntervalSeconds != nil {
		pools[index].CheckIntervalSeconds = *body.CheckIntervalSeconds
	}
	if body.BindAttempts != nil {
		pools[index].BindAttempts = *body.BindAttempts
	}
	if body.SpreadBindings != nil {
		pools[index].SpreadBindings = *body.SpreadBindings
	}
	if body.Entries != nil {
		entries := append([]config.ProxyPoolEntryConfig(nil), pools[index].Entries...)
		for _, patch := range *body.Entries {
			existingIndex := findProxyPoolEntry(entries, patch.ID)
			entry := config.ProxyPoolEntryConfig{ID: patch.ID}
			if existingIndex >= 0 {
				entry = entries[existingIndex]
			}
			if patch.URLTemplate != nil {
				candidate := strings.TrimSpace(*patch.URLTemplate)
				if isMaskedProxyURL(candidate) && !replaceMaskedProxyRequested(c) {
					if existingIndex < 0 || !proxyutil.MaskedProxyURLMatches(candidate, entry.URLTemplate) {
						h.mu.Unlock()
						c.JSON(http.StatusBadRequest, gin.H{"error": "url-template must contain the complete proxy credential"})
						return
					}
				} else {
					entry.URLTemplate = candidate
				}
			}
			if patch.Ports != nil {
				entry.Ports = *patch.Ports
			}
			if existingIndex >= 0 {
				entries[existingIndex] = entry
			} else {
				entries = append(entries, entry)
			}
		}
		pools[index].Entries = entries
	}
	for _, entryID := range body.DeleteEntryIDs {
		if entryIndex := findProxyPoolEntry(pools[index].Entries, entryID); entryIndex >= 0 {
			pools[index].Entries = append(pools[index].Entries[:entryIndex], pools[index].Entries[entryIndex+1:]...)
		}
	}
	rules := cloneProxyRules(h.cfg.ProxyRules)
	if body.Name != nil && !strings.EqualFold(strings.TrimSpace(oldName), strings.TrimSpace(*body.Name)) {
		for ruleIndex := range rules {
			for targetIndex := range rules[ruleIndex].Targets {
				if strings.EqualFold(strings.TrimSpace(rules[ruleIndex].Targets[targetIndex].Pool), strings.TrimSpace(oldName)) {
					rules[ruleIndex].Targets[targetIndex].Pool = *body.Name
				}
			}
			if strings.EqualFold(strings.TrimSpace(rules[ruleIndex].Pool), strings.TrimSpace(oldName)) {
				rules[ruleIndex].Pool = *body.Name
			}
		}
	}
	normalizedPools, normalizedRules, errNormalize := config.NormalizeProxyConfiguration(pools, rules)
	if errNormalize != nil {
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": errNormalize.Error()})
		return
	}
	previousPools := h.cfg.ProxyPools
	previousRules := h.cfg.ProxyRules
	h.cfg.ProxyPools = normalizedPools
	h.cfg.ProxyRules = normalizedRules
	renamed := body.Name != nil && !strings.EqualFold(strings.TrimSpace(oldName), strings.TrimSpace(*body.Name))
	if !renamed || h.proxyPoolManager == nil {
		if !h.persistLocked(c) {
			h.cfg.ProxyPools = previousPools
			h.cfg.ProxyRules = previousRules
		}
		h.mu.Unlock()
		return
	}
	if !h.persistProxyPoolRenameLocked(c, oldName, *body.Name) {
		h.cfg.ProxyPools = previousPools
		h.cfg.ProxyRules = previousRules
	}
	h.mu.Unlock()
}

// persistProxyPoolRenameLocked preserves stable bindings while applying the
// same full runtime configuration transaction used by other management writes.
// The caller must hold h.mu; this method returns with h.mu held.
func (h *Handler) persistProxyPoolRenameLocked(c *gin.Context, oldName, newName string) bool {
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
	candidate, errCandidate := config.Clone(h.cfg)
	if errCandidate != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to snapshot runtime configuration"})
		return false
	}
	publishedCandidate, errPublishedCandidate := config.Clone(candidate)
	if errPublishedCandidate != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to snapshot published configuration"})
		return false
	}
	if errSave := config.SaveConfigPreserveComments(h.configFilePath, h.cfg); errSave != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("failed to save config: %v", errSave)})
		return false
	}
	manager := h.proxyPoolManager
	h.mu.Unlock()
	errRename := manager.UpdateConfigWithPoolRename(candidate, oldName, newName)
	var (
		result   config.RuntimeApplyResult
		errApply error
	)
	if errRename == nil {
		result, errApply = h.applyRuntimeConfig(c.Request.Context(), candidate)
	}
	h.mu.Lock()
	if errRename != nil || errApply != nil {
		errPrimary := errors.Join(errRename, errApply)
		errRollbackFile := h.restorePersistedConfigLocked(previousBody, previousExisted, previousCfg)
		h.mu.Unlock()
		var errRollbackRename error
		if errRename == nil {
			errRollbackRename = manager.UpdateConfigWithPoolRename(previousCfg, newName, oldName)
		}
		var errRollbackRuntime error
		if previousCfg != nil {
			_, errRollbackRuntime = h.applyRuntimeConfig(context.WithoutCancel(c.Request.Context()), previousCfg)
		}
		h.mu.Lock()
		if errRollbackFile != nil || errRollbackRename != nil || errRollbackRuntime != nil {
			log.WithError(errors.Join(errPrimary, errRollbackFile, errRollbackRename, errRollbackRuntime)).Error("proxy pool rename failed and rollback was incomplete")
			c.JSON(http.StatusInternalServerError, gin.H{"error": "proxy pool binding migration failed and rollback was incomplete"})
			return false
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to migrate proxy pool bindings"})
		return false
	}
	h.configSnapshot.Store(publishedCandidate)
	response := gin.H{"status": "ok", "applied": result.Applied}
	if result.RestartRequired {
		response["restart_required"] = true
		response["restart_fields"] = result.RestartFields
	}
	c.JSON(http.StatusOK, response)
	return true
}

// DeleteProxyPool removes an unreferenced structured proxy pool.
func (h *Handler) DeleteProxyPool(c *gin.Context) {
	h.mu.Lock()
	index := findProxyPool(h.cfg.ProxyPools, c.Param("name"))
	if index < 0 {
		h.mu.Unlock()
		c.JSON(http.StatusNotFound, gin.H{"error": "proxy pool not found"})
		return
	}
	poolName := h.cfg.ProxyPools[index].Name
	for _, rule := range h.cfg.ProxyRules {
		if proxyRuleReferencesPool(rule, poolName) {
			h.mu.Unlock()
			c.JSON(http.StatusConflict, gin.H{"error": "proxy pool is referenced by a proxy rule"})
			return
		}
	}
	previousPools := cloneProxyPools(h.cfg.ProxyPools)
	h.cfg.ProxyPools = append(h.cfg.ProxyPools[:index], h.cfg.ProxyPools[index+1:]...)
	if !h.persistLocked(c) {
		h.cfg.ProxyPools = previousPools
	}
	h.mu.Unlock()
}

// GetProxyRules returns ordered proxy routing rules.
func (h *Handler) GetProxyRules(c *gin.Context) {
	var rules []config.ProxyRuleConfig
	if cfg := h.currentConfig(); cfg != nil {
		rules = cloneProxyRules(cfg.ProxyRules)
	}
	c.JSON(http.StatusOK, gin.H{"schema_version": 2, "proxy-rules": rules})
}

// PutProxyRules replaces the complete ordered proxy rule list.
func (h *Handler) PutProxyRules(c *gin.Context) {
	rules, ok := parseProxyRulesBody(c)
	if !ok {
		return
	}
	h.mu.Lock()
	normalizedPools, normalizedRules, errNormalize := config.NormalizeProxyConfiguration(h.cfg.ProxyPools, rules)
	if errNormalize != nil {
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": errNormalize.Error()})
		return
	}
	previousPools := h.cfg.ProxyPools
	previousRules := h.cfg.ProxyRules
	h.cfg.ProxyPools = normalizedPools
	h.cfg.ProxyRules = normalizedRules
	c.Set(managementResponseFieldsKey, gin.H{
		"schema_version": 2,
		"proxy-rules":    cloneProxyRules(normalizedRules),
	})
	if !h.persistLocked(c) {
		h.cfg.ProxyPools = previousPools
		h.cfg.ProxyRules = previousRules
	}
	h.mu.Unlock()
}

func parseProxyPoolBody(c *gin.Context) (config.ProxyPoolConfig, bool) {
	data, errRead := c.GetRawData()
	if errRead != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read body"})
		return config.ProxyPoolConfig{}, false
	}
	var pool config.ProxyPoolConfig
	if errDirect := json.Unmarshal(data, &pool); errDirect == nil && strings.TrimSpace(pool.Name) != "" {
		return pool, true
	}
	var wrapped struct {
		Value *config.ProxyPoolConfig `json:"value"`
	}
	if errWrapped := json.Unmarshal(data, &wrapped); errWrapped != nil || wrapped.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return config.ProxyPoolConfig{}, false
	}
	return *wrapped.Value, true
}

func parseProxyRulesBody(c *gin.Context) ([]config.ProxyRuleConfig, bool) {
	data, errRead := c.GetRawData()
	if errRead != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read body"})
		return nil, false
	}
	var rules []config.ProxyRuleConfig
	if errArray := json.Unmarshal(data, &rules); errArray == nil && rules != nil {
		return rules, true
	}
	var wrapped struct {
		SchemaVersion *int                     `json:"schema_version"`
		Items         []config.ProxyRuleConfig `json:"items"`
		Value         []config.ProxyRuleConfig `json:"value"`
		ProxyRules    []config.ProxyRuleConfig `json:"proxy-rules"`
	}
	if errWrapped := json.Unmarshal(data, &wrapped); errWrapped != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return nil, false
	}
	if wrapped.SchemaVersion != nil && *wrapped.SchemaVersion != 2 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported proxy rule schema version"})
		return nil, false
	}
	switch {
	case wrapped.ProxyRules != nil:
		return wrapped.ProxyRules, true
	case wrapped.Items != nil:
		return wrapped.Items, true
	case wrapped.Value != nil:
		return wrapped.Value, true
	default:
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return nil, false
	}
}

func cloneProxyPools(input []config.ProxyPoolConfig) []config.ProxyPoolConfig {
	out := make([]config.ProxyPoolConfig, len(input))
	for index := range input {
		out[index] = input[index]
		out[index].Entries = append([]config.ProxyPoolEntryConfig(nil), input[index].Entries...)
	}
	return out
}

func proxyRuleReferencesPool(rule config.ProxyRuleConfig, poolName string) bool {
	if strings.EqualFold(strings.TrimSpace(rule.Pool), strings.TrimSpace(poolName)) {
		return true
	}
	for _, target := range rule.Targets {
		if strings.EqualFold(strings.TrimSpace(target.Pool), strings.TrimSpace(poolName)) {
			return true
		}
	}
	return false
}

func cloneProxyRules(input []config.ProxyRuleConfig) []config.ProxyRuleConfig {
	out := make([]config.ProxyRuleConfig, len(input))
	for index := range input {
		out[index] = input[index]
		out[index].Targets = append([]config.ProxyRuleTargetConfig(nil), input[index].Targets...)
		out[index].Providers = append([]string(nil), input[index].Providers...)
		out[index].Priorities = append([]int(nil), input[index].Priorities...)
	}
	return out
}

func maskProxyPools(input []config.ProxyPoolConfig) []config.ProxyPoolConfig {
	pools := cloneProxyPools(input)
	for poolIndex := range pools {
		for entryIndex := range pools[poolIndex].Entries {
			pools[poolIndex].Entries[entryIndex].URLTemplate = proxyutil.MaskProxyURL(pools[poolIndex].Entries[entryIndex].URLTemplate)
		}
	}
	return pools
}

func cloneConfigWithMaskedProxyURLs(input *config.Config) (config.Config, error) {
	data, errMarshal := json.Marshal(input)
	if errMarshal != nil {
		return config.Config{}, errMarshal
	}
	var snapshot config.Config
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if errUnmarshal := decoder.Decode(&snapshot); errUnmarshal != nil {
		return config.Config{}, errUnmarshal
	}
	snapshot.ProxyURL = proxyutil.MaskProxyURL(snapshot.ProxyURL)
	snapshot.ProxyPools = maskProxyPools(snapshot.ProxyPools)
	snapshot.GeminiKey = append([]config.GeminiKey(nil), snapshot.GeminiKey...)
	for index := range snapshot.GeminiKey {
		snapshot.GeminiKey[index].ProxyURL = proxyutil.MaskProxyURL(snapshot.GeminiKey[index].ProxyURL)
	}
	snapshot.InteractionsKey = append([]config.GeminiKey(nil), snapshot.InteractionsKey...)
	for index := range snapshot.InteractionsKey {
		snapshot.InteractionsKey[index].ProxyURL = proxyutil.MaskProxyURL(snapshot.InteractionsKey[index].ProxyURL)
	}
	snapshot.CodexKey = append([]config.CodexKey(nil), snapshot.CodexKey...)
	for index := range snapshot.CodexKey {
		snapshot.CodexKey[index].ProxyURL = proxyutil.MaskProxyURL(snapshot.CodexKey[index].ProxyURL)
	}
	snapshot.ClaudeKey = append([]config.ClaudeKey(nil), snapshot.ClaudeKey...)
	for index := range snapshot.ClaudeKey {
		snapshot.ClaudeKey[index].ProxyURL = proxyutil.MaskProxyURL(snapshot.ClaudeKey[index].ProxyURL)
	}
	snapshot.VertexCompatAPIKey = append([]config.VertexCompatKey(nil), snapshot.VertexCompatAPIKey...)
	for index := range snapshot.VertexCompatAPIKey {
		snapshot.VertexCompatAPIKey[index].ProxyURL = proxyutil.MaskProxyURL(snapshot.VertexCompatAPIKey[index].ProxyURL)
	}
	snapshot.OpenAICompatibility = append([]config.OpenAICompatibility(nil), snapshot.OpenAICompatibility...)
	for providerIndex := range snapshot.OpenAICompatibility {
		entries := append([]config.OpenAICompatibilityAPIKey(nil), snapshot.OpenAICompatibility[providerIndex].APIKeyEntries...)
		for entryIndex := range entries {
			entries[entryIndex].ProxyURL = proxyutil.MaskProxyURL(entries[entryIndex].ProxyURL)
		}
		snapshot.OpenAICompatibility[providerIndex].APIKeyEntries = entries
	}
	return snapshot, nil
}

func isMaskedProxyURL(raw string) bool {
	return proxyutil.IsMaskedProxyURL(raw)
}

func containsMaskedProxySecret(pool config.ProxyPoolConfig) bool {
	for _, entry := range pool.Entries {
		if isMaskedProxyURL(entry.URLTemplate) {
			return true
		}
	}
	return false
}

func findProxyPool(pools []config.ProxyPoolConfig, name string) int {
	for index := range pools {
		if strings.EqualFold(strings.TrimSpace(pools[index].Name), strings.TrimSpace(name)) {
			return index
		}
	}
	return -1
}

func findProxyPoolEntry(entries []config.ProxyPoolEntryConfig, id string) int {
	for index := range entries {
		if strings.EqualFold(strings.TrimSpace(entries[index].ID), strings.TrimSpace(id)) {
			return index
		}
	}
	return -1
}
