package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher/synthesizer"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type patchAuthFileFieldsRequest struct {
	Name       string            `json:"name"`
	Names      []string          `json:"names"`
	Fields     json.RawMessage   `json:"fields"`
	Prefix     *string           `json:"prefix"`
	ProxyURL   *string           `json:"proxy_url"`
	Headers    map[string]string `json:"headers"`
	Priority   *int              `json:"priority"`
	Note       *string           `json:"note"`
	UsingAPI   *bool             `json:"using_api"`
	Websockets *bool             `json:"websockets"`
}

type authFileFieldValues struct {
	prefix          *string
	proxyURL        *string
	headers         map[string]string
	headersSet      bool
	priority        *int
	prioritySet     bool
	note            *string
	usingAPI        *bool
	websockets      *bool
	excludedModels  []string
	excludedSet     bool
	disableCooling  *bool
	legacyHeaderOps bool
}

type authFileFieldPatchFailure struct {
	Name   string `json:"name"`
	Status int    `json:"status"`
	Error  string `json:"error"`
}

// PatchAuthFileFields updates editable fields of one or more auth files.
func (h *Handler) PatchAuthFileFields(c *gin.Context) {
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}

	var req patchAuthFileFieldsRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	if len(req.Fields) > 0 {
		h.patchAuthFileFieldsBatch(c, &req)
		return
	}
	if len(req.Names) > 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "fields is required"})
		return
	}
	h.patchAuthFileFieldsLegacy(c, &req)
}

func (h *Handler) patchAuthFileFieldsLegacy(c *gin.Context, req *patchAuthFileFieldsRequest) {
	name := strings.TrimSpace(req.Name)
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name is required"})
		return
	}

	targetAuth, status, message := h.resolveAuthFileFieldPatchTarget(name)
	if targetAuth == nil {
		c.JSON(status, gin.H{"error": message})
		return
	}
	if (req.UsingAPI != nil || req.Websockets != nil) && !strings.EqualFold(strings.TrimSpace(targetAuth.Provider), "xai") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "using_api and websockets are only supported for xai auth files"})
		return
	}

	values := authFileFieldValues{
		prefix:          req.Prefix,
		proxyURL:        req.ProxyURL,
		note:            req.Note,
		usingAPI:        req.UsingAPI,
		websockets:      req.Websockets,
		legacyHeaderOps: true,
	}
	if len(req.Headers) > 0 {
		values.headers = req.Headers
		values.headersSet = true
	}
	if req.Priority != nil {
		values.prioritySet = true
		if *req.Priority != 0 {
			priority := *req.Priority
			values.priority = &priority
		}
	}
	if !values.hasFields() {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no fields to update"})
		return
	}
	if !values.hasNonHeaderFields() && !legacyAuthHeadersWouldChange(targetAuth, values.headers) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no fields to update"})
		return
	}

	updated, status, message := h.updateAuthFileFields(c.Request.Context(), targetAuth, values)
	if updated == nil {
		c.JSON(status, gin.H{"error": message})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) patchAuthFileFieldsBatch(c *gin.Context, req *patchAuthFileFieldsRequest) {
	values, errDecode := decodeAuthFileFieldValues(req.Fields)
	if errDecode != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errDecode.Error()})
		return
	}

	names := dedupeAuthFilePatchNames(req.Names)
	if len(names) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "names is required"})
		return
	}

	ctx := c.Request.Context()
	files := make([]string, 0, len(names))
	failed := make([]authFileFieldPatchFailure, 0)
	matched := 0
	for _, name := range names {
		targetAuth, status, message := h.resolveAuthFileFieldPatchTarget(name)
		if targetAuth == nil {
			failed = append(failed, authFileFieldPatchFailure{Name: name, Status: status, Error: message})
			continue
		}
		matched++
		if errValidate := validateBatchAuthFileFields(targetAuth, values); errValidate != nil {
			failed = append(failed, authFileFieldPatchFailure{Name: name, Status: http.StatusBadRequest, Error: errValidate.Error()})
			continue
		}
		updated, status, message := h.updateAuthFileFields(ctx, targetAuth, values)
		if updated == nil {
			failed = append(failed, authFileFieldPatchFailure{Name: name, Status: status, Error: message})
			continue
		}
		files = append(files, name)
	}

	response := gin.H{
		"status":  "ok",
		"matched": matched,
		"updated": len(files),
		"files":   files,
		"failed":  failed,
	}
	if len(failed) > 0 {
		response["status"] = "partial"
		c.JSON(http.StatusMultiStatus, response)
		return
	}
	c.JSON(http.StatusOK, response)
}

func decodeAuthFileFieldValues(raw json.RawMessage) (authFileFieldValues, error) {
	var fields map[string]json.RawMessage
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return authFileFieldValues{}, errors.New("fields must be an object")
	}
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return authFileFieldValues{}, errors.New("fields must be an object")
	}

	values := authFileFieldValues{}
	for name, value := range fields {
		switch name {
		case "prefix":
			var decoded string
			if err := decodeNonNullAuthField(value, &decoded); err != nil {
				return authFileFieldValues{}, fmt.Errorf("invalid prefix")
			}
			values.prefix = &decoded
		case "proxy_url":
			var decoded string
			if err := decodeNonNullAuthField(value, &decoded); err != nil {
				return authFileFieldValues{}, fmt.Errorf("invalid proxy_url")
			}
			values.proxyURL = &decoded
		case "headers":
			var decoded map[string]string
			if err := decodeNonNullAuthField(value, &decoded); err != nil || decoded == nil {
				return authFileFieldValues{}, fmt.Errorf("invalid headers")
			}
			normalized, errNormalize := normalizeReplacementAuthHeaders(decoded)
			if errNormalize != nil {
				return authFileFieldValues{}, errNormalize
			}
			values.headers = normalized
			values.headersSet = true
		case "priority":
			values.prioritySet = true
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				values.priority = nil
				continue
			}
			var decoded int
			if err := json.Unmarshal(value, &decoded); err != nil {
				return authFileFieldValues{}, fmt.Errorf("invalid priority")
			}
			values.priority = &decoded
		case "note":
			var decoded string
			if err := decodeNonNullAuthField(value, &decoded); err != nil {
				return authFileFieldValues{}, fmt.Errorf("invalid note")
			}
			values.note = &decoded
		case "using_api":
			var decoded bool
			if err := decodeNonNullAuthField(value, &decoded); err != nil {
				return authFileFieldValues{}, fmt.Errorf("invalid using_api")
			}
			values.usingAPI = &decoded
		case "websockets":
			var decoded bool
			if err := decodeNonNullAuthField(value, &decoded); err != nil {
				return authFileFieldValues{}, fmt.Errorf("invalid websockets")
			}
			values.websockets = &decoded
		case "excluded_models":
			var decoded []string
			if err := decodeNonNullAuthField(value, &decoded); err != nil || decoded == nil {
				return authFileFieldValues{}, fmt.Errorf("invalid excluded_models")
			}
			values.excludedModels = decoded
			values.excludedSet = true
		case "disable_cooling":
			var decoded bool
			if err := decodeNonNullAuthField(value, &decoded); err != nil {
				return authFileFieldValues{}, fmt.Errorf("invalid disable_cooling")
			}
			values.disableCooling = &decoded
		default:
			return authFileFieldValues{}, fmt.Errorf("unsupported field %q", name)
		}
	}
	if !values.hasFields() {
		return authFileFieldValues{}, errors.New("no fields to update")
	}
	return values, nil
}

func decodeNonNullAuthField(raw json.RawMessage, target any) error {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errors.New("null is not allowed")
	}
	return json.Unmarshal(raw, target)
}

func (v authFileFieldValues) hasFields() bool {
	return v.prefix != nil || v.proxyURL != nil || v.headersSet || v.prioritySet || v.note != nil ||
		v.usingAPI != nil || v.websockets != nil || v.excludedSet || v.disableCooling != nil
}

func (v authFileFieldValues) hasNonHeaderFields() bool {
	return v.prefix != nil || v.proxyURL != nil || v.prioritySet || v.note != nil || v.usingAPI != nil ||
		v.websockets != nil || v.excludedSet || v.disableCooling != nil
}

func validateBatchAuthFileFields(auth *coreauth.Auth, values authFileFieldValues) error {
	provider := strings.ToLower(strings.TrimSpace(auth.Provider))
	if values.usingAPI != nil && provider != "xai" {
		return errors.New("using_api is only supported for xai auth files")
	}
	if values.websockets != nil && provider != "xai" && provider != "codex" {
		return errors.New("websockets is only supported for codex and xai auth files")
	}
	return nil
}

func (h *Handler) updateAuthFileFields(ctx context.Context, auth *coreauth.Auth, values authFileFieldValues) (*coreauth.Auth, int, string) {
	h.applyAuthFileFieldValues(auth, values)
	auth.UpdatedAt = time.Now()

	updated, errUpdate := h.authManager.Update(ctx, auth)
	if errUpdate != nil {
		if errors.Is(errUpdate, coreauth.ErrRetiredGeminiCLIAuthReadOnly) {
			return nil, http.StatusGone, errGeminiCLIAuthGone.Error()
		}
		return nil, http.StatusInternalServerError, fmt.Sprintf("failed to update auth: %v", errUpdate)
	}
	if h.authStatusHook != nil {
		h.authStatusHook(ctx, updated.Clone())
	}
	return updated, http.StatusOK, ""
}

func (h *Handler) applyAuthFileFieldValues(auth *coreauth.Auth, values authFileFieldValues) {
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	if auth.Attributes == nil {
		auth.Attributes = make(map[string]string)
	}

	if values.prefix != nil {
		value := strings.TrimSpace(*values.prefix)
		auth.Prefix = value
		setOrDeleteAuthString(auth, "prefix", value)
	}
	if values.proxyURL != nil {
		value := strings.TrimSpace(*values.proxyURL)
		auth.ProxyURL = value
		setOrDeleteAuthString(auth, "proxy_url", value)
	}
	if values.headersSet {
		if values.legacyHeaderOps {
			mergeAuthHeaders(auth, values.headers)
		} else {
			replaceAuthHeaders(auth, values.headers)
		}
	}
	if values.prioritySet {
		if values.priority == nil {
			delete(auth.Metadata, "priority")
			delete(auth.Attributes, "priority")
		} else {
			auth.Metadata["priority"] = *values.priority
			auth.Attributes["priority"] = strconv.Itoa(*values.priority)
		}
	}
	if values.note != nil {
		setOrDeleteAuthString(auth, "note", strings.TrimSpace(*values.note))
	}
	if values.usingAPI != nil {
		auth.Metadata["using_api"] = *values.usingAPI
		auth.Attributes["using_api"] = strconv.FormatBool(*values.usingAPI)
	}
	if values.websockets != nil {
		auth.Metadata["websockets"] = *values.websockets
		auth.Attributes["websockets"] = strconv.FormatBool(*values.websockets)
	}
	if values.excludedSet {
		models := config.NormalizeExcludedModels(values.excludedModels)
		delete(auth.Metadata, "excluded-models")
		if len(models) == 0 {
			delete(auth.Metadata, "excluded_models")
		} else {
			auth.Metadata["excluded_models"] = models
		}
		delete(auth.Attributes, "excluded_models")
		delete(auth.Attributes, "excluded_models_hash")
		authKind := strings.TrimSpace(auth.Attributes["auth_kind"])
		if authKind == "" {
			if metadataKind, ok := auth.Metadata["auth_kind"].(string); ok {
				authKind = strings.TrimSpace(metadataKind)
			}
		}
		if authKind == "" {
			authKind = "oauth"
			if strings.TrimSpace(auth.Attributes["api_key"]) != "" {
				authKind = "apikey"
			}
		}
		if cfg := h.currentConfig(); cfg != nil {
			synthesizer.ApplyAuthExcludedModelsMeta(auth, cfg, models, authKind)
		} else if len(models) > 0 {
			auth.Attributes["excluded_models"] = strings.Join(models, ",")
		}
	}
	if values.disableCooling != nil {
		auth.Metadata["disable_cooling"] = *values.disableCooling
		auth.Attributes["disable_cooling"] = strconv.FormatBool(*values.disableCooling)
	}
}

func setOrDeleteAuthString(auth *coreauth.Auth, name, value string) {
	if value == "" {
		delete(auth.Metadata, name)
		delete(auth.Attributes, name)
		return
	}
	auth.Metadata[name] = value
	auth.Attributes[name] = value
}

func mergeAuthHeaders(auth *coreauth.Auth, updates map[string]string) {
	headers := coreauth.ExtractCustomHeadersFromMetadata(auth.Metadata)
	if headers == nil {
		headers = make(map[string]string)
	}
	for rawName, rawValue := range updates {
		name := strings.TrimSpace(rawName)
		if name == "" {
			continue
		}
		value := strings.TrimSpace(rawValue)
		if value == "" {
			delete(headers, name)
			delete(auth.Attributes, "header:"+name)
			continue
		}
		headers[name] = value
		auth.Attributes["header:"+name] = value
	}
	writeAuthHeaders(auth, headers)
}

func legacyAuthHeadersWouldChange(auth *coreauth.Auth, updates map[string]string) bool {
	headers := coreauth.ExtractCustomHeadersFromMetadata(auth.Metadata)
	for rawName, rawValue := range updates {
		name := strings.TrimSpace(rawName)
		if name == "" {
			continue
		}
		value := strings.TrimSpace(rawValue)
		attributeValue, attributeExists := auth.Attributes["header:"+name]
		if value == "" {
			if _, exists := headers[name]; exists || attributeExists {
				return true
			}
			continue
		}
		metadataValue, metadataExists := headers[name]
		if !metadataExists || metadataValue != value || !attributeExists || attributeValue != value {
			return true
		}
	}
	return false
}

func normalizeReplacementAuthHeaders(headers map[string]string) (map[string]string, error) {
	normalized := make(map[string]string, len(headers))
	seen := make(map[string]struct{}, len(headers))
	for rawName, rawValue := range headers {
		name := strings.TrimSpace(rawName)
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("duplicate header %q", name)
		}
		seen[key] = struct{}{}
		normalized[name] = strings.TrimSpace(rawValue)
	}
	return normalized, nil
}

func replaceAuthHeaders(auth *coreauth.Auth, replacements map[string]string) {
	for name := range auth.Attributes {
		if strings.HasPrefix(name, "header:") {
			delete(auth.Attributes, name)
		}
	}
	headers := make(map[string]string, len(replacements))
	for rawName, rawValue := range replacements {
		name := strings.TrimSpace(rawName)
		value := strings.TrimSpace(rawValue)
		if name == "" || value == "" {
			continue
		}
		headers[name] = value
		auth.Attributes["header:"+name] = value
	}
	writeAuthHeaders(auth, headers)
}

func writeAuthHeaders(auth *coreauth.Auth, headers map[string]string) {
	if len(headers) == 0 {
		delete(auth.Metadata, "headers")
		return
	}
	metadata := make(map[string]any, len(headers))
	for name, value := range headers {
		metadata[name] = value
	}
	auth.Metadata["headers"] = metadata
}

func dedupeAuthFilePatchNames(names []string) []string {
	seen := make(map[string]struct{}, len(names))
	result := make([]string, 0, len(names))
	for _, rawName := range names {
		name := strings.TrimSpace(rawName)
		if name == "" {
			continue
		}
		key := name
		if runtime.GOOS == "windows" {
			key = strings.ToLower(key)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, name)
	}
	return result
}

func (h *Handler) resolveAuthFileFieldPatchTarget(name string) (*coreauth.Auth, int, string) {
	auth := h.findManagedAuth(name)
	if auth != nil {
		if !isRuntimeOnlyAuth(auth) {
			retired, errCheck := h.authBackedByRetiredGeminiCLIFile(auth)
			if errCheck != nil {
				return nil, http.StatusServiceUnavailable, "unable to verify auth file"
			}
			if retired {
				return nil, http.StatusGone, errGeminiCLIAuthGone.Error()
			}
			if backingName, managed := h.managedAuthBackingFileName(auth); managed && !isTopLevelManagedAuthName(backingName) {
				return nil, http.StatusBadRequest, errInvalidAuthFileName.Error()
			}
		}
		return auth, http.StatusOK, ""
	}

	if strings.HasSuffix(strings.ToLower(strings.TrimSpace(name)), ".json") {
		retired, errCheck := h.isRetiredGeminiCLIManagedFile(name)
		if errCheck != nil {
			switch {
			case errors.Is(errCheck, errInvalidAuthFileName):
				return nil, http.StatusBadRequest, errInvalidAuthFileName.Error()
			case errors.Is(errCheck, fs.ErrNotExist):
				return nil, http.StatusNotFound, "auth file not found"
			default:
				return nil, http.StatusServiceUnavailable, "unable to verify auth file"
			}
		}
		if retired {
			return nil, http.StatusGone, errGeminiCLIAuthGone.Error()
		}
		normalizedName, errNormalize := normalizeManagedAuthFileName(name)
		if errNormalize == nil && !isTopLevelManagedAuthName(normalizedName) {
			return nil, http.StatusBadRequest, errInvalidAuthFileName.Error()
		}
	}
	return nil, http.StatusNotFound, "auth file not found"
}
