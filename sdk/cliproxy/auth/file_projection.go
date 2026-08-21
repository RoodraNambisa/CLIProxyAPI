package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	internalcodex "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

// FileAuthProjectionOptions describes the stable runtime view of one persisted
// credential. Store loading and watcher synthesis must use the same projection
// so an unchanged file does not become a runtime replacement during startup.
type FileAuthProjectionOptions struct {
	Config  *internalconfig.Config
	AuthDir string
	Path    string
	Now     time.Time
}

// ApplyFileAuthProjection normalizes a file-backed auth from persisted metadata.
// Runtime-only availability fields are intentionally left to the manager.
func ApplyFileAuthProjection(auth *Auth, opts FileAuthProjectionOptions) error {
	if auth == nil || auth.Metadata == nil {
		return nil
	}
	metadata := auth.Metadata
	provider := strings.ToLower(strings.TrimSpace(metadataStringValue(metadata, "type")))
	if provider == "" {
		provider = strings.ToLower(strings.TrimSpace(auth.Provider))
	}
	if provider == "" {
		return nil
	}

	path := strings.TrimSpace(opts.Path)
	if path == "" && auth.Attributes != nil {
		path = strings.TrimSpace(auth.Attributes["path"])
	}
	if path == "" {
		path = strings.TrimSpace(auth.FileName)
	}
	if path != "" && strings.TrimSpace(opts.AuthDir) != "" && !filepath.IsAbs(path) {
		path = filepath.Join(opts.AuthDir, path)
	}
	if path != "" {
		path = filepath.Clean(path)
	}

	id := strings.TrimSpace(auth.ID)
	if id == "" {
		id = path
		if authDir := strings.TrimSpace(opts.AuthDir); authDir != "" && path != "" {
			if rel, errRel := filepath.Rel(authDir, path); errRel == nil && rel != "" {
				id = rel
			}
		}
	}
	if runtime.GOOS == "windows" {
		id = strings.ToLower(id)
	}

	sourceHash := ""
	if auth.Attributes != nil {
		sourceHash = strings.TrimSpace(auth.Attributes[SourceHashAttributeKey])
	}
	auth.ID = id
	auth.Provider = provider
	auth.FileName = id
	auth.EnsureIndex()
	auth.Label = fileAuthLabel(metadata, provider)
	auth.Prefix = normalizedFileAuthPrefix(metadataStringValue(metadata, "prefix"))
	auth.ProxyURL = metadataStringValue(metadata, "proxy_url")
	auth.Disabled, _ = metadata["disabled"].(bool)
	auth.Status = StatusActive
	auth.StatusMessage = ""
	if auth.Disabled {
		auth.Status = StatusDisabled
	}
	auth.Attributes = make(map[string]string)
	if path != "" && path != "." {
		auth.Attributes["source"] = path
		auth.Attributes["path"] = path
	}
	if sourceHash != "" {
		auth.Attributes[SourceHashAttributeKey] = sourceHash
	} else if errHash := SetCanonicalSourceHashAttribute(auth); errHash != nil {
		return errHash
	}
	if email := strings.TrimSpace(metadataStringValue(metadata, "email")); email != "" {
		auth.Attributes["email"] = email
	}
	if priority, ok := fileAuthPriority(metadata["priority"]); ok {
		auth.Attributes["priority"] = priority
	}
	if note := strings.TrimSpace(metadataStringValue(metadata, "note")); note != "" {
		auth.Attributes["note"] = note
	}
	ApplyFileBackedGeminiAPIKey(auth)
	ApplyCustomHeadersFromMetadata(auth)
	ApplyAuthExcludedModelsMeta(auth, opts.Config, fileAuthExcludedModels(metadata), fileAuthKind(auth))
	if provider == "codex" {
		if planType := internalcodex.EffectivePlanType(metadata); planType != "" {
			auth.Attributes["plan_type"] = planType
		}
	}
	if auth.CreatedAt.IsZero() {
		auth.CreatedAt = opts.Now
	}
	if auth.UpdatedAt.IsZero() {
		auth.UpdatedAt = opts.Now
	}
	normalizeChatGPTWebDependencyState(auth)
	ApplyLifecycleRuntimeState(auth)
	return nil
}

// ApplyAuthExcludedModelsMeta stores the effective excluded-model set on an auth.
func ApplyAuthExcludedModelsMeta(auth *Auth, cfg *internalconfig.Config, perAuth []string, authKind string) {
	if auth == nil || cfg == nil {
		return
	}
	seen := make(map[string]struct{})
	add := func(entries []string) {
		for _, entry := range entries {
			entry = strings.ToLower(strings.TrimSpace(entry))
			if entry != "" {
				seen[entry] = struct{}{}
			}
		}
	}
	add(perAuth)
	if !strings.EqualFold(strings.TrimSpace(authKind), "apikey") && cfg.OAuthExcludedModels != nil {
		add(cfg.OAuthExcludedModels[strings.ToLower(strings.TrimSpace(auth.Provider))])
	}
	combined := make([]string, 0, len(seen))
	for entry := range seen {
		combined = append(combined, entry)
	}
	sort.Strings(combined)
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}
	if hash := hashExcludedModels(combined); hash != "" {
		auth.Attributes["excluded_models_hash"] = hash
	}
	if len(combined) > 0 {
		auth.Attributes["excluded_models"] = strings.Join(combined, ",")
	}
	if authKind = strings.TrimSpace(authKind); authKind != "" {
		auth.Attributes["auth_kind"] = authKind
	}
}

func normalizedFileAuthPrefix(value string) string {
	value = strings.Trim(strings.TrimSpace(value), "/")
	if value == "" || strings.Contains(value, "/") {
		return ""
	}
	return value
}

func fileAuthLabel(metadata map[string]any, provider string) string {
	for _, key := range []string{"label", "email", "project_id"} {
		if value := strings.TrimSpace(metadataStringValue(metadata, key)); value != "" {
			return value
		}
	}
	return provider
}

func metadataStringValue(metadata map[string]any, key string) string {
	value, _ := metadata[key].(string)
	return value
}

func fileAuthPriority(value any) (string, bool) {
	switch typed := value.(type) {
	case int:
		return strconv.Itoa(typed), true
	case int64:
		return strconv.FormatInt(typed, 10), true
	case float64:
		return strconv.Itoa(int(typed)), true
	case string:
		trimmed := strings.TrimSpace(typed)
		if _, errAtoi := strconv.Atoi(trimmed); errAtoi == nil {
			return trimmed, true
		}
	}
	return "", false
}

func fileAuthExcludedModels(metadata map[string]any) []string {
	raw := metadata["excluded_models"]
	if raw == nil {
		raw = metadata["excluded-models"]
	}
	switch values := raw.(type) {
	case []string:
		return append([]string(nil), values...)
	case []any:
		out := make([]string, 0, len(values))
		for _, value := range values {
			if item, ok := value.(string); ok {
				out = append(out, item)
			}
		}
		return out
	default:
		return nil
	}
}

func fileAuthKind(auth *Auth) string {
	if auth != nil && auth.Attributes != nil && strings.TrimSpace(auth.Attributes["api_key"]) != "" {
		return "apikey"
	}
	return "oauth"
}

func hashExcludedModels(models []string) string {
	if len(models) == 0 {
		return ""
	}
	data, _ := json.Marshal(models)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
