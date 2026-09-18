package management

import (
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
)

// PutAuthFileContent replaces an existing credential using the editor's original
// snapshot as a precondition. Token refreshes and external edits must not be lost.
func (h *Handler) PutAuthFileContent(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}
	var body struct {
		Name     string          `json:"name"`
		Original json.RawMessage `json:"original"`
		Content  json.RawMessage `json:"content"`
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4<<20)
	if c.ShouldBindJSON(&body) != nil || strings.TrimSpace(body.Name) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name, original and content JSON objects are required"})
		return
	}
	var original, metadata map[string]any
	if json.Unmarshal(body.Original, &original) != nil || original == nil || json.Unmarshal(body.Content, &metadata) != nil || metadata == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "original and content must be JSON objects"})
		return
	}
	auth, status, message := h.resolveAuthFileFieldPatchTarget(strings.TrimSpace(body.Name))
	if auth == nil {
		c.JSON(status, gin.H{"error": message})
		return
	}
	name, managed := h.managedAuthBackingFileName(auth)
	if !managed || !isTopLevelManagedAuthName(name) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "only managed credential files can be edited as JSON"})
		return
	}
	if !h.authManager.SupportsSourceConditionalSave() {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "credential store does not support conditional JSON editing"})
		return
	}
	ctx := c.Request.Context()
	unlockDependency, errDependency := h.chatGPTWebDependencyMu.lock(ctx)
	if errDependency != nil {
		c.JSON(http.StatusRequestTimeout, gin.H{"error": "credential update canceled"})
		return
	}
	defer unlockDependency()
	lockedCtx, unlockAuth, errLock := h.authManager.LockAuthMutation(ctx, auth)
	if errLock != nil {
		c.JSON(http.StatusConflict, gin.H{"error": "credential changed; reopen the editor and retry"})
		return
	}
	defer unlockAuth()
	current, exists := h.authManager.GetByID(auth.ID)
	if !exists || current == nil || coreauth.ChatGPTWebAuthRetainedForDependents(current) {
		c.JSON(http.StatusConflict, gin.H{"error": "credential changed; reopen the editor and retry"})
		return
	}
	file, status, errRead := h.readDownloadAuthFile(name)
	if errRead != nil {
		c.JSON(status, gin.H{"error": "unable to read credential file"})
		return
	}
	expectedHash, _ := coreauth.CanonicalSourceHashFromBytes(body.Original)
	if !coreauth.SourceHashMatchesBytes(expectedHash, file.Data) ||
		!coreauth.SourceHashMatchesBytes(current.Attributes[coreauth.SourceHashAttributeKey], file.Data) {
		c.JSON(http.StatusConflict, gin.H{"error": "credential changed; reopen the editor and retry"})
		return
	}
	if errValidate := h.validateAuthFileContent(current, metadata, body.Content); errValidate != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": errValidate.Error()})
		return
	}
	updated := current.Clone()
	updated.Metadata = metadata
	// A typed token-storage snapshot may contain old secrets or removed fields.
	updated.Storage = nil
	updated.UpdatedAt = time.Now()
	if errProjection := coreauth.ApplyFileAuthProjection(updated, coreauth.FileAuthProjectionOptions{
		Config: h.currentConfig(), Path: current.Attributes["path"], Now: updated.UpdatedAt,
	}); errProjection != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid credential metadata"})
		return
	}
	installed, matched, errUpdate := h.authManager.UpdateIfCurrentSourceHash(lockedCtx, current, updated)
	unlockAuth()
	unlockDependency()
	if errUpdate != nil {
		status := http.StatusInternalServerError
		message := "failed to save credential JSON"
		if outcome, explicit := coreauth.SaveOutcomeFromError(errUpdate); errors.Is(errUpdate, authfileguard.ErrPersistGenerationStale) || (explicit && outcome == coreauth.SaveOutcomeRolledBack) {
			status, message = http.StatusConflict, "credential changed; reopen the editor and retry"
		} else if errors.Is(errUpdate, coreauth.ErrChatGPTWebEmailImmutable) || errors.Is(errUpdate, coreauth.ErrChatGPTWebEmailAlreadyExists) {
			status, message = http.StatusBadRequest, "ChatGPT Web credential email cannot be changed"
		}
		c.JSON(status, gin.H{"error": message})
		return
	}
	if !matched || installed == nil {
		c.JSON(http.StatusConflict, gin.H{"error": "credential changed; reopen the editor and retry"})
		return
	}
	if hook := h.authStatusHookSnapshot(); hook != nil {
		hook(ctx, installed.Clone())
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) validateAuthFileContent(current *coreauth.Auth, metadata map[string]any, raw []byte) error {
	provider, _ := metadata["type"].(string)
	previousProvider, _ := current.Metadata["type"].(string)
	if previousProvider == "" {
		previousProvider = current.Provider
	}
	if provider == "" || !strings.EqualFold(strings.TrimSpace(provider), strings.TrimSpace(previousProvider)) {
		return errors.New("credential type cannot be changed")
	}
	for _, field := range []string{"credential_uid", "source_auth_id", "source_credential_uid", "deletion_state"} {
		if !reflect.DeepEqual(current.Metadata[field], metadata[field]) {
			return errors.New("credential dependency identity cannot be changed in the JSON editor")
		}
	}
	if value, exists := metadata[coreauth.ProxyBindingMemoryKey]; exists && value != nil && !validEditableProxyBinding(value) {
		return errors.New("invalid proxy_binding: expected a versioned node reference or direct binding")
	}
	if metadata[coreauth.ProxyBindingMemoryKey] == nil {
		delete(metadata, coreauth.ProxyBindingMemoryKey)
	}
	if value, exists := metadata["proxy_url"]; exists {
		proxyURL, ok := value.(string)
		if !ok {
			return errors.New("proxy_url must be a string")
		}
		if proxyURL = strings.TrimSpace(proxyURL); proxyURL != "" {
			if _, errParse := proxyutil.Parse(proxyURL); errParse != nil {
				return errors.New("invalid proxy_url")
			}
		}
	}
	if _, errBuild := h.buildAuthFromFileData(current.Attributes["path"], raw); errBuild != nil {
		return errors.New("invalid credential JSON: check credential fields, weight and error rules")
	}
	editableFields := make(map[string]any)
	for _, field := range []string{"prefix", "proxy_url", "headers", "priority", "weight", "note", "base_url", "using_api", "websockets", "excluded_models", "disable_cooling", "login_method", "api798_url", "codex_fingerprint_mode", "request_scoped_errors", "xai_model_catalog_sources", "xai_model_routes"} {
		if value, exists := metadata[field]; exists && !reflect.DeepEqual(current.Metadata[field], value) {
			editableFields[field] = value
		}
	}
	if len(editableFields) > 0 {
		fieldsJSON, _ := json.Marshal(editableFields)
		values, errDecode := decodeAuthFileFieldValues(fieldsJSON)
		if errDecode != nil {
			return errDecode
		}
		candidate := current.Clone()
		candidate.Metadata = metadata
		if errValidate := validateBatchAuthFileFields(candidate, values); errValidate != nil {
			return errValidate
		}
	}
	if !reflect.DeepEqual(current.Metadata[coreauth.RoutingAliasMetadataKey], metadata[coreauth.RoutingAliasMetadataKey]) {
		rawAlias, ok := metadata[coreauth.RoutingAliasMetadataKey].(string)
		if !ok && metadata[coreauth.RoutingAliasMetadataKey] != nil {
			return errors.New("routing_alias must be a string")
		}
		alias, errAlias := coreauth.NormalizeCredentialRoutingAlias(rawAlias)
		if errAlias != nil {
			return errAlias
		}
		if alias != "" {
			found, errTarget := h.authManager.ResolveCredentialTarget(alias)
			var targetError *coreauth.Error
			missing := errors.As(errTarget, &targetError) && targetError.Code == "credential_target_not_found"
			if !missing && (found == nil || found.ID != current.ID) {
				return errors.New("routing alias is already in use")
			}
		}
	}
	return nil
}

func validEditableProxyBinding(value any) bool {
	fields, ok := value.(map[string]any)
	if !ok {
		return false
	}
	for key := range fields {
		switch key {
		case "version", "node_id", "port", "placeholders", "direct":
		default:
			return false
		}
	}
	return coreauth.ReadProxyBindingMemory(&coreauth.Auth{Metadata: map[string]any{coreauth.ProxyBindingMemoryKey: fields}}) != nil
}
