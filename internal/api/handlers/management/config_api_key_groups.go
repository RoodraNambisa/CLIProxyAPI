package management

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

// GetAPIKeys returns configured client API keys.
func (h *Handler) GetAPIKeys(c *gin.Context) {
	h.mu.Lock()
	keys := append([]string(nil), h.cfg.APIKeys...)
	h.mu.Unlock()
	c.JSON(http.StatusOK, gin.H{"api-keys": keys})
}

// PutAPIKeys replaces client API keys and removes group mappings for deleted keys.
func (h *Handler) PutAPIKeys(c *gin.Context) {
	keys, ok := parseAPIKeyListBody(c)
	if !ok {
		return
	}
	keys, errNormalizeKeys := normalizeAPIKeys(keys)
	if errNormalizeKeys != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errNormalizeKeys.Error()})
		return
	}

	h.mu.Lock()
	groups, errNormalize := config.PruneAPIKeyGroups(h.cfg.APIKeyGroups, keys)
	if errNormalize != nil {
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": errNormalize.Error()})
		return
	}
	h.cfg.APIKeys = append([]string(nil), keys...)
	h.cfg.APIKeyGroups = groups
	h.persistLocked(c)
	h.mu.Unlock()
}

// PatchAPIKeys updates one client API key and migrates its provider group mapping.
func (h *Handler) PatchAPIKeys(c *gin.Context) {
	var body struct {
		Old   *string `json:"old"`
		New   *string `json:"new"`
		Index *int    `json:"index"`
		Value *string `json:"value"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}

	h.mu.Lock()
	keys := append([]string(nil), h.cfg.APIKeys...)
	groups := cloneAPIKeyGroups(h.cfg.APIKeyGroups)
	oldKey := ""
	newKey := ""
	replacementIndex := -1
	switch {
	case body.Index != nil && body.Value != nil && *body.Index >= 0 && *body.Index < len(keys):
		replacementIndex = *body.Index
		oldKey = keys[replacementIndex]
		newKey = strings.TrimSpace(*body.Value)
	case body.Old != nil && body.New != nil:
		lookupKey := strings.TrimSpace(*body.Old)
		newKey = strings.TrimSpace(*body.New)
		if lookupKey == "" || newKey == "" {
			h.mu.Unlock()
			c.JSON(http.StatusBadRequest, gin.H{"error": "api key cannot be empty"})
			return
		}
		for index := range keys {
			if strings.TrimSpace(keys[index]) != lookupKey {
				continue
			}
			replacementIndex = index
			oldKey = keys[index]
			break
		}
		if replacementIndex == -1 {
			h.mu.Unlock()
			c.JSON(http.StatusNotFound, gin.H{"error": "api key not found"})
			return
		}
	default:
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing fields"})
		return
	}
	if newKey == "" {
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": "api key cannot be empty"})
		return
	}
	if containsAPIKeyExcept(keys, newKey, replacementIndex) {
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": "api key already exists"})
		return
	}
	keys[replacementIndex] = newKey
	if oldKey != "" {
		var errMigrate error
		if containsAPIKey(keys, strings.TrimSpace(oldKey)) {
			groups = copyAPIKeyGroup(groups, oldKey, newKey)
		} else {
			groups, errMigrate = migrateAPIKeyGroup(groups, oldKey, newKey)
		}
		if errMigrate != nil {
			h.mu.Unlock()
			c.JSON(http.StatusConflict, gin.H{"error": "api key group already exists for the replacement key"})
			return
		}
	}
	groups, errNormalize := config.PruneAPIKeyGroups(groups, keys)
	if errNormalize != nil {
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": errNormalize.Error()})
		return
	}
	h.cfg.APIKeys = keys
	h.cfg.APIKeyGroups = groups
	h.persistLocked(c)
	h.mu.Unlock()
}

// DeleteAPIKeys removes client API keys and their orphaned provider group mappings.
func (h *Handler) DeleteAPIKeys(c *gin.Context) {
	h.mu.Lock()
	keys := append([]string(nil), h.cfg.APIKeys...)
	deleted := false
	if indexText := c.Query("index"); indexText != "" {
		if index, errIndex := strconv.Atoi(indexText); errIndex == nil && index >= 0 && index < len(keys) {
			keys = append(keys[:index], keys[index+1:]...)
			deleted = true
		}
	}
	if !deleted {
		if value := strings.TrimSpace(c.Query("value")); value != "" {
			filtered := make([]string, 0, len(keys))
			for _, key := range keys {
				if strings.TrimSpace(key) == value {
					deleted = true
					continue
				}
				filtered = append(filtered, key)
			}
			keys = filtered
		}
	}
	if !deleted {
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing index or value"})
		return
	}
	groups, errNormalize := config.PruneAPIKeyGroups(h.cfg.APIKeyGroups, keys)
	if errNormalize != nil {
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": errNormalize.Error()})
		return
	}
	h.cfg.APIKeys = keys
	h.cfg.APIKeyGroups = groups
	h.persistLocked(c)
	h.mu.Unlock()
}

// GetAPIKeyGroups returns provider restrictions for configured API keys.
func (h *Handler) GetAPIKeyGroups(c *gin.Context) {
	h.mu.Lock()
	groups := cloneAPIKeyGroups(h.cfg.APIKeyGroups)
	h.mu.Unlock()
	c.JSON(http.StatusOK, gin.H{"api-key-groups": groups})
}

// PutAPIKeyGroups replaces all API key provider restrictions.
func (h *Handler) PutAPIKeyGroups(c *gin.Context) {
	groups, ok := parseAPIKeyGroupsBody(c)
	if !ok {
		return
	}
	h.mu.Lock()
	normalized, errNormalize := config.NormalizeAPIKeyGroups(groups, h.cfg.APIKeys)
	if errNormalize != nil {
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": errNormalize.Error()})
		return
	}
	h.cfg.APIKeyGroups = normalized
	h.persistLocked(c)
	h.mu.Unlock()
}

// PatchAPIKeyGroups adds, updates, or clears one API key provider restriction.
func (h *Handler) PatchAPIKeyGroups(c *gin.Context) {
	var group config.APIKeyGroup
	if err := c.ShouldBindJSON(&group); err != nil || strings.TrimSpace(group.APIKey) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}

	h.mu.Lock()
	key := strings.TrimSpace(group.APIKey)
	if !containsAPIKey(h.cfg.APIKeys, key) {
		h.mu.Unlock()
		c.JSON(http.StatusNotFound, gin.H{"error": "api key not found"})
		return
	}
	normalized, errNormalize := config.NormalizeAPIKeyGroups([]config.APIKeyGroup{{APIKey: key, Providers: group.Providers}}, h.cfg.APIKeys)
	if errNormalize != nil {
		h.mu.Unlock()
		c.JSON(http.StatusBadRequest, gin.H{"error": errNormalize.Error()})
		return
	}
	groups := deleteAPIKeyGroup(h.cfg.APIKeyGroups, key)
	if len(normalized) > 0 {
		groups = append(groups, normalized[0])
	}
	h.cfg.APIKeyGroups = groups
	h.persistLocked(c)
	h.mu.Unlock()
}

// DeleteAPIKeyGroups clears one API key provider restriction.
func (h *Handler) DeleteAPIKeyGroups(c *gin.Context) {
	key := strings.TrimSpace(c.Query("api-key"))
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "api-key is required"})
		return
	}
	h.mu.Lock()
	h.cfg.APIKeyGroups = deleteAPIKeyGroup(h.cfg.APIKeyGroups, key)
	h.persistLocked(c)
	h.mu.Unlock()
}

func parseAPIKeyListBody(c *gin.Context) ([]string, bool) {
	data, errRead := c.GetRawData()
	if errRead != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read body"})
		return nil, false
	}
	var keys []string
	if errArray := json.Unmarshal(data, &keys); errArray == nil && keys != nil {
		return keys, true
	}
	var object struct {
		Items []string `json:"items"`
	}
	if errObject := json.Unmarshal(data, &object); errObject != nil || object.Items == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return nil, false
	}
	return object.Items, true
}

func parseAPIKeyGroupsBody(c *gin.Context) ([]config.APIKeyGroup, bool) {
	data, errRead := c.GetRawData()
	if errRead != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read body"})
		return nil, false
	}
	var groups []config.APIKeyGroup
	if errArray := json.Unmarshal(data, &groups); errArray == nil && groups != nil {
		return groups, true
	}
	var object struct {
		Items []config.APIKeyGroup `json:"items"`
	}
	if errObject := json.Unmarshal(data, &object); errObject != nil || object.Items == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return nil, false
	}
	return object.Items, true
}

func normalizeAPIKeys(keys []string) ([]string, error) {
	normalized := make([]string, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for index, rawKey := range keys {
		key := strings.TrimSpace(rawKey)
		if key == "" {
			return nil, errAPIKeyEmpty
		}
		if _, exists := seen[key]; exists {
			return nil, errAPIKeyDuplicate
		}
		seen[key] = struct{}{}
		normalized[index] = key
	}
	return normalized, nil
}

func cloneAPIKeyGroups(groups []config.APIKeyGroup) []config.APIKeyGroup {
	if len(groups) == 0 {
		return nil
	}
	cloned := make([]config.APIKeyGroup, len(groups))
	for index, group := range groups {
		cloned[index] = config.APIKeyGroup{APIKey: group.APIKey, Providers: append([]string(nil), group.Providers...)}
	}
	return cloned
}

func migrateAPIKeyGroup(groups []config.APIKeyGroup, oldKey, newKey string) ([]config.APIKeyGroup, error) {
	oldKey = strings.TrimSpace(oldKey)
	newKey = strings.TrimSpace(newKey)
	if oldKey == "" || oldKey == newKey {
		return groups, nil
	}
	oldIndex := -1
	newIndex := -1
	for index := range groups {
		switch strings.TrimSpace(groups[index].APIKey) {
		case oldKey:
			oldIndex = index
		case newKey:
			newIndex = index
		}
	}
	if oldIndex == -1 {
		return groups, nil
	}
	if newKey == "" {
		return deleteAPIKeyGroup(groups, oldKey), nil
	}
	if newKey != "" && newIndex != -1 {
		return nil, errAPIKeyGroupConflict
	}
	groups[oldIndex].APIKey = newKey
	return groups, nil
}

func copyAPIKeyGroup(groups []config.APIKeyGroup, oldKey, newKey string) []config.APIKeyGroup {
	oldKey = strings.TrimSpace(oldKey)
	newKey = strings.TrimSpace(newKey)
	if oldKey == "" || newKey == "" || oldKey == newKey {
		return groups
	}
	oldIndex := -1
	for index := range groups {
		key := strings.TrimSpace(groups[index].APIKey)
		if key == newKey {
			return groups
		}
		if key == oldKey {
			oldIndex = index
		}
	}
	if oldIndex == -1 {
		return groups
	}
	return append(groups, config.APIKeyGroup{
		APIKey:    newKey,
		Providers: append([]string(nil), groups[oldIndex].Providers...),
	})
}

func containsAPIKey(keys []string, target string) bool {
	for _, key := range keys {
		if strings.TrimSpace(key) == target {
			return true
		}
	}
	return false
}

func containsAPIKeyExcept(keys []string, target string, excludedIndex int) bool {
	for index, key := range keys {
		if index != excludedIndex && strings.TrimSpace(key) == target {
			return true
		}
	}
	return false
}

func deleteAPIKeyGroup(groups []config.APIKeyGroup, key string) []config.APIKeyGroup {
	key = strings.TrimSpace(key)
	filtered := make([]config.APIKeyGroup, 0, len(groups))
	for _, group := range groups {
		if strings.TrimSpace(group.APIKey) == key {
			continue
		}
		filtered = append(filtered, group)
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

var (
	errAPIKeyEmpty         = errors.New("api key cannot be empty")
	errAPIKeyDuplicate     = errors.New("api key already exists")
	errAPIKeyGroupConflict = errors.New("api key group conflict")
)
