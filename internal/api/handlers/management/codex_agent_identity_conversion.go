package management

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const (
	agentIdentityConversionMaxRequestBytes = 16 << 20
	agentIdentityConversionMaxFileBytes    = 2 << 20
)

var errAgentIdentitySourceChanged = errors.New("source credential changed while conversion was running")

type codexAgentIdentityConversionRequest struct {
	AccessTokens []string `json:"access_tokens"`
	Names        []string `json:"names"`
	TargetMode   string   `json:"target_mode"`
}

type codexAgentIdentityConversionInput struct {
	sourceName string
	name       string
	targetMode string
	token      []byte
	metadata   map[string]any
}

// StartCodexAgentIdentityConversionTask starts a bounded asynchronous conversion task.
func (h *Handler) StartCodexAgentIdentityConversionTask(c *gin.Context) {
	if h == nil || h.authManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "core auth manager unavailable"})
		return
	}
	inputs, errInput := readCodexAgentIdentityConversionInputs(c)
	if errInput != nil {
		status := http.StatusBadRequest
		var maxBytesError *http.MaxBytesError
		if errors.As(errInput, &maxBytesError) || errors.Is(errInput, errChatGPTWebLoginTaskInputTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		c.JSON(status, gin.H{"error": errInput.Error()})
		return
	}
	handedOff := false
	defer func() {
		if !handedOff {
			clearCodexAgentIdentityInputs(inputs)
		}
	}()
	results := make([]codexAgentIdentityTaskResult, len(inputs))
	for index := range inputs {
		results[index] = codexAgentIdentityTaskResult{
			SourceName: inputs[index].sourceName,
			TargetMode: inputs[index].targetMode,
			Stage:      "queued",
			Status:     agentIdentityItemQueued,
		}
	}
	tasks := h.codexAgentIdentityTaskManager()
	task, taskCtx, errCreate := tasks.create(results)
	if errCreate != nil {
		status := http.StatusTooManyRequests
		if errors.Is(errCreate, errAgentIdentityTaskClosed) {
			status = http.StatusServiceUnavailable
		}
		c.JSON(status, gin.H{"error": errCreate.Error()})
		return
	}
	taskCtx = PopulateAuthContext(taskCtx, c)
	handedOff = true
	go h.runCodexAgentIdentityConversionTask(taskCtx, task.ID, inputs, h.authManager)
	c.JSON(http.StatusAccepted, task)
}

// GetCodexAgentIdentityConversionTask returns a task snapshot for polling.
func (h *Handler) GetCodexAgentIdentityConversionTask(c *gin.Context) {
	tasks := h.codexAgentIdentityTaskManager()
	if tasks == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Agent Identity conversion tasks are unavailable"})
		return
	}
	task, ok := tasks.get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "Agent Identity conversion task not found"})
		return
	}
	c.JSON(http.StatusOK, task)
}

// CancelCodexAgentIdentityConversionTask requests cancellation and retains results.
func (h *Handler) CancelCodexAgentIdentityConversionTask(c *gin.Context) {
	tasks := h.codexAgentIdentityTaskManager()
	if tasks == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Agent Identity conversion tasks are unavailable"})
		return
	}
	task, ok := tasks.cancel(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "Agent Identity conversion task not found"})
		return
	}
	c.JSON(http.StatusOK, task)
}

func (h *Handler) codexAgentIdentityTaskManager() *codexAgentIdentityTaskManager {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.agentIdentityTasks == nil {
		h.agentIdentityTasks = newCodexAgentIdentityTaskManager()
	}
	return h.agentIdentityTasks
}

func readCodexAgentIdentityConversionInputs(c *gin.Context) ([]codexAgentIdentityConversionInput, error) {
	if c == nil || c.Request == nil {
		return nil, errors.New("conversion request is required")
	}
	contentType := strings.ToLower(strings.TrimSpace(c.GetHeader("Content-Type")))
	if strings.HasPrefix(contentType, "multipart/form-data") {
		return readCodexAgentIdentityMultipartInputs(c)
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, agentIdentityConversionMaxRequestBytes)
	payload, errRead := ioReadAllLimit(c.Request.Body, agentIdentityConversionMaxRequestBytes)
	if errClose := c.Request.Body.Close(); errRead == nil && errClose != nil {
		errRead = errClose
	}
	c.Request.Body = http.NoBody
	if errRead != nil {
		clear(payload)
		return nil, errRead
	}
	defer clear(payload)
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var request codexAgentIdentityConversionRequest
	if errDecode := decoder.Decode(&request); errDecode != nil {
		return nil, errors.New("invalid Agent Identity conversion request")
	}
	var trailing any
	if errTrailing := decoder.Decode(&trailing); errTrailing != io.EOF {
		return nil, errors.New("invalid Agent Identity conversion request")
	}
	if (len(request.AccessTokens) == 0) == (len(request.Names) == 0) {
		return nil, errors.New("exactly one of access_tokens or names is required")
	}
	targetMode, errMode := normalizeCodexAuthModeTarget(request.TargetMode)
	if errMode != nil {
		return nil, errMode
	}
	if len(request.AccessTokens) > 0 && targetMode != codexauth.AgentIdentityAuthMode {
		return nil, errors.New("access_tokens only supports target_mode agentIdentity")
	}
	if len(request.AccessTokens) > 0 {
		inputs := make([]codexAgentIdentityConversionInput, 0, len(request.AccessTokens))
		for index := range request.AccessTokens {
			token := strings.TrimSpace(request.AccessTokens[index])
			request.AccessTokens[index] = ""
			if token == "" {
				clearCodexAgentIdentityInputs(inputs)
				return nil, errors.New("access_tokens contains an empty token")
			}
			inputs = append(inputs, codexAgentIdentityConversionInput{
				sourceName: fmt.Sprintf("access-token-%d", index+1),
				targetMode: targetMode,
				token:      []byte(token),
				metadata: map[string]any{
					"type":         "codex",
					"access_token": token,
				},
			})
		}
		return inputs, nil
	}
	seen := make(map[string]struct{}, len(request.Names))
	inputs := make([]codexAgentIdentityConversionInput, 0, len(request.Names))
	for _, rawName := range request.Names {
		name := strings.TrimSpace(rawName)
		if name == "" || isUnsafeAuthFileName(name) || !strings.EqualFold(filepath.Ext(name), ".json") {
			return nil, errors.New("names contains an invalid auth file name")
		}
		key := managedAuthNameKey(name)
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		inputs = append(inputs, codexAgentIdentityConversionInput{sourceName: name, name: name, targetMode: targetMode})
	}
	if len(inputs) == 0 {
		return nil, errors.New("at least one source name is required")
	}
	return inputs, nil
}

func readCodexAgentIdentityMultipartInputs(c *gin.Context) ([]codexAgentIdentityConversionInput, error) {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, agentIdentityConversionMaxRequestBytes)
	reader, errReader := c.Request.MultipartReader()
	if errReader != nil {
		return nil, errors.New("multipart form with files fields is required")
	}
	inputs := make([]codexAgentIdentityConversionInput, 0)
	completed := false
	defer func() {
		if !completed {
			clearCodexAgentIdentityInputs(inputs)
		}
		if c.Request.Body != nil {
			_ = c.Request.Body.Close()
			c.Request.Body = http.NoBody
		}
	}()
	for {
		part, errPart := reader.NextPart()
		if errors.Is(errPart, io.EOF) {
			break
		}
		if errPart != nil {
			return nil, errPart
		}
		if part.FormName() == "target_mode" {
			value, errRead := io.ReadAll(io.LimitReader(part, 65))
			errClose := part.Close()
			if errRead != nil {
				return nil, errRead
			}
			if errClose != nil {
				return nil, errClose
			}
			if len(value) > 64 {
				return nil, errors.New("target_mode is too long")
			}
			targetMode, errMode := normalizeCodexAuthModeTarget(string(value))
			if errMode != nil {
				return nil, errMode
			}
			if targetMode != codexauth.AgentIdentityAuthMode {
				return nil, errors.New("multipart files only supports target_mode agentIdentity")
			}
			continue
		}
		if part.FormName() != "file" && part.FormName() != "files" {
			_ = part.Close()
			continue
		}
		payload, errRead := io.ReadAll(io.LimitReader(part, agentIdentityConversionMaxFileBytes+1))
		errClose := part.Close()
		if errRead != nil {
			clear(payload)
			return nil, errRead
		}
		if errClose != nil {
			clear(payload)
			return nil, errClose
		}
		if len(payload) > agentIdentityConversionMaxFileBytes {
			clear(payload)
			return nil, errChatGPTWebLoginTaskInputTooLarge
		}
		fileName := filepath.Base(strings.TrimSpace(part.FileName()))
		if fileName == "." || fileName == "" {
			fileName = fmt.Sprintf("access-token-%d.txt", len(inputs)+1)
		}
		token, metadata, errToken := codexAccessTokenFromConversionFile(payload)
		clear(payload)
		if errToken != nil {
			return nil, fmt.Errorf("%s: %w", fileName, errToken)
		}
		inputs = append(inputs, codexAgentIdentityConversionInput{
			sourceName: fileName,
			targetMode: codexauth.AgentIdentityAuthMode,
			token:      token,
			metadata:   metadata,
		})
	}
	if len(inputs) == 0 {
		return nil, errors.New("at least one access-token file is required")
	}
	completed = true
	return inputs, nil
}

func codexAccessTokenFromConversionFile(data []byte) ([]byte, map[string]any, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, nil, errors.New("file is empty")
	}
	if trimmed[0] != '{' {
		token := append([]byte(nil), trimmed...)
		return token, map[string]any{"type": "codex", "access_token": string(token)}, nil
	}
	var metadata map[string]any
	if err := json.Unmarshal(trimmed, &metadata); err != nil {
		return nil, nil, errors.New("file must contain a raw access token or Codex JSON")
	}
	if codexauth.IsAgentIdentityMetadata(metadata) {
		return nil, nil, errors.New("complete Agent Identity JSON must use the auth-files upload endpoint")
	}
	if provider := strings.TrimSpace(stringValue(metadata, "type")); provider != "" && !strings.EqualFold(provider, "codex") {
		return nil, nil, errors.New("JSON credential is not a Codex credential")
	}
	token := strings.TrimSpace(stringValue(metadata, "access_token"))
	if token == "" {
		return nil, nil, errors.New("Codex JSON does not contain access_token")
	}
	metadata["type"] = "codex"
	metadata["access_token"] = token
	return []byte(token), metadata, nil
}

func clearCodexAgentIdentityInputs(inputs []codexAgentIdentityConversionInput) {
	for index := range inputs {
		clear(inputs[index].token)
		inputs[index].token = nil
		for _, key := range []string{"access_token", "refresh_token", "id_token", "agent_private_key", "task_id"} {
			if inputs[index].metadata != nil {
				inputs[index].metadata[key] = ""
			}
		}
		inputs[index].metadata = nil
	}
}

func normalizeCodexAuthModeTarget(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", strings.ToLower(codexauth.AgentIdentityAuthMode):
		return codexauth.AgentIdentityAuthMode, nil
	case codexauth.OAuthAuthMode:
		return codexauth.OAuthAuthMode, nil
	default:
		return "", errors.New("target_mode must be agentIdentity or oauth")
	}
}

func (h *Handler) runCodexAgentIdentityConversionTask(ctx context.Context, taskID string, inputs []codexAgentIdentityConversionInput, manager *coreauth.Manager) {
	defer clearCodexAgentIdentityInputs(inputs)
	tasks := h.codexAgentIdentityTaskManager()
	if !tasks.start(taskID) {
		tasks.finish(taskID, true)
		return
	}
	jobs := make(chan int)
	var workers sync.WaitGroup
	workerCount := min(agentIdentityTaskWorkers, len(inputs))
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				if !tasks.acquireSlot(ctx) {
					tasks.setResult(taskID, index, canceledAgentIdentityResult(inputs[index]))
					continue
				}
				if !tasks.markRunning(taskID, index) {
					tasks.releaseSlot()
					continue
				}
				result := h.convertCodexAgentIdentity(ctx, taskID, index, inputs[index], manager)
				tasks.setResult(taskID, index, result)
				tasks.releaseSlot()
			}
		}()
	}
sendLoop:
	for index := range inputs {
		select {
		case jobs <- index:
		case <-ctx.Done():
			break sendLoop
		}
	}
	close(jobs)
	workers.Wait()
	tasks.finish(taskID, ctx.Err() != nil)
}

func (h *Handler) convertCodexAgentIdentity(ctx context.Context, taskID string, index int, input codexAgentIdentityConversionInput, manager *coreauth.Manager) codexAgentIdentityTaskResult {
	result := codexAgentIdentityTaskResult{
		SourceName:      input.sourceName,
		TargetMode:      input.targetMode,
		Stage:           "validating",
		ProgressPercent: 5,
		Status:          agentIdentityItemFailed,
	}
	tasks := h.codexAgentIdentityTaskManager()
	var source *coreauth.Auth
	if input.name != "" {
		source = h.findManagedFileAuth(input.name)
		if source == nil {
			return failedAgentIdentityResult(result, "source_not_found", "source credential does not exist")
		}
		if !strings.EqualFold(strings.TrimSpace(source.Provider), "codex") {
			return failedAgentIdentityResult(result, "source_provider_invalid", "source credential is not a Codex credential")
		}
		if strings.TrimSpace(source.Attributes[coreauth.SourceHashAttributeKey]) == "" {
			return failedAgentIdentityResult(result, "source_not_managed", "source credential has no managed source generation")
		}
		result.SourceMode = codexauth.EffectiveAuthMode(source.Metadata)
		result.TargetName = source.FileName
		result.Email = stringValue(source.Metadata, "email")
		result.AccountID = agentIdentityAccountKey(source.Metadata)
		result.PlanType = codexauth.EffectivePlanType(source.Metadata)
		tasks.updateModes(taskID, index, result.SourceMode, result.TargetMode)
		tasks.updateIdentity(taskID, index, result.Email, result.AccountID, result.PlanType)
		if result.SourceMode == result.TargetMode {
			result.Status = agentIdentityItemUnchanged
			return result
		}
		if result.TargetMode == codexauth.OAuthAuthMode {
			return h.convertCodexCredentialToOAuth(ctx, taskID, index, input, result, source, manager)
		}
	} else {
		result.SourceMode = codexauth.OAuthAuthMode
		tasks.updateModes(taskID, index, result.SourceMode, result.TargetMode)
	}
	return h.convertCodexCredentialToAgentIdentity(ctx, taskID, index, input, result, source, manager)
}

func (h *Handler) convertCodexCredentialToOAuth(ctx context.Context, taskID string, index int, input codexAgentIdentityConversionInput, result codexAgentIdentityTaskResult, source *coreauth.Auth, manager *coreauth.Manager) codexAgentIdentityTaskResult {
	if source == nil || !codexauth.OAuthModeAvailable(source.Metadata, time.Now()) {
		return failedAgentIdentityResult(result, "oauth_material_missing", "credential has no usable OAuth token or refresh token")
	}
	tasks := h.codexAgentIdentityTaskManager()
	accessToken := strings.TrimSpace(stringValue(source.Metadata, "access_token"))
	refreshToken := strings.TrimSpace(stringValue(source.Metadata, "refresh_token"))
	if accessToken == "" || codexSourceTokenNeedsRefresh(accessToken, stringValue(source.Metadata, "expired"), time.Now()) {
		if refreshToken == "" {
			return failedAgentIdentityResult(result, "oauth_token_expired", "stored OAuth access token is expired and no refresh token is available")
		}
		if !tasks.updateStage(taskID, index, "refreshing_token", 35) {
			return canceledAgentIdentityResult(input)
		}
		refreshed, errRefresh := h.refreshCodexAgentIdentitySource(ctx, manager, source)
		if errRefresh != nil {
			return failedAgentIdentityResultWithSecret(result, "token_refresh_failed", errRefresh, accessToken, refreshToken)
		}
		source = refreshed
		accessToken = strings.TrimSpace(stringValue(source.Metadata, "access_token"))
		if accessToken == "" {
			return failedAgentIdentityResult(result, "source_token_missing", "Codex refresh returned no access token")
		}
	}
	if !tasks.beginCommit(taskID, index) {
		return canceledAgentIdentityResult(input)
	}
	metadata := cloneStringAnyMap(source.Metadata)
	metadata["auth_mode"] = codexauth.OAuthAuthMode
	installed, errPersist := h.persistCodexAuthModeUpdate(agentIdentityCommitContext(tasks, ctx), manager, source, metadata)
	if errPersist != nil {
		category := "persist_failed"
		if errors.Is(errPersist, errAgentIdentitySourceChanged) {
			category = "source_changed"
		}
		return failedAgentIdentityResultWithSecret(result, category, errPersist, accessToken, refreshToken)
	}
	result.TargetName = installed.FileName
	result.Email = stringValue(installed.Metadata, "email")
	result.AccountID = agentIdentityAccountKey(installed.Metadata)
	result.PlanType = codexauth.EffectivePlanType(installed.Metadata)
	result.Status = agentIdentityItemUpdated
	return result
}

func (h *Handler) convertCodexCredentialToAgentIdentity(ctx context.Context, taskID string, index int, input codexAgentIdentityConversionInput, result codexAgentIdentityTaskResult, source *coreauth.Auth, manager *coreauth.Manager) codexAgentIdentityTaskResult {
	tasks := h.codexAgentIdentityTaskManager()
	if source != nil {
		if stored, errStored := codexauth.ParseAgentIdentityCredential(source.Metadata); errStored == nil {
			result.Email = stored.Email
			result.AccountID = firstNonEmptyValue(stored.ChatGPTAccountID, stored.AccountID)
			result.PlanType = stored.PlanType
			tasks.updateIdentity(taskID, index, result.Email, result.AccountID, result.PlanType)
			if strings.TrimSpace(stored.TaskID) == "" {
				if !tasks.updateStage(taskID, index, "registering_task", 65) {
					return canceledAgentIdentityResult(input)
				}
				taskIDValue, errTask := registerCodexAgentTaskWithRetry(ctx, h.agentIdentityHTTPClient(source), h.codexAgentIdentityBaseURL(), stored)
				if errTask != nil {
					return failedAgentIdentityResult(result, "task_registration_failed", sanitizeAgentIdentityTaskError(errTask))
				}
				stored.TaskID = taskIDValue
			}
			if !tasks.beginCommit(taskID, index) {
				return canceledAgentIdentityResult(input)
			}
			installed, _, errPersist := h.persistCodexAgentIdentity(agentIdentityCommitContext(tasks, ctx), manager, source, nil, stored)
			if errPersist != nil {
				category := "persist_failed"
				if errors.Is(errPersist, errAgentIdentitySourceChanged) {
					category = "source_changed"
				}
				return failedAgentIdentityResultWithSecret(result, category, errPersist, stored.AgentRuntimeID, stored.PrivateKeyPKCS8Base64, stored.TaskID)
			}
			result.TargetName = installed.FileName
			result.Status = agentIdentityItemUpdated
			return result
		}
	}

	sourceRefreshed := false
	accessToken := strings.TrimSpace(string(input.token))
	identityMetadata := input.metadata
	if source != nil {
		accessToken = strings.TrimSpace(stringValue(source.Metadata, "access_token"))
		identityMetadata = source.Metadata
		if accessToken == "" {
			return failedAgentIdentityResult(result, "source_token_missing", "source credential has no access token")
		}
		if codexSourceTokenNeedsRefresh(accessToken, stringValue(source.Metadata, "expired"), time.Now()) {
			if !tasks.updateStage(taskID, index, "refreshing_token", 20) {
				return canceledAgentIdentityResult(input)
			}
			refreshed, errRefresh := h.refreshCodexAgentIdentitySource(ctx, manager, source)
			if errRefresh != nil {
				return failedAgentIdentityResultWithSecret(result, "token_refresh_failed", errRefresh, accessToken, stringValue(source.Metadata, "refresh_token"))
			}
			source = refreshed
			sourceRefreshed = true
			accessToken = strings.TrimSpace(stringValue(source.Metadata, "access_token"))
			identityMetadata = source.Metadata
		}
	}
	identity, errIdentity := codexauth.ParseAccessTokenIdentity(accessToken)
	if errIdentity != nil {
		return failedAgentIdentityResultWithSecret(result, "token_invalid", errIdentity, accessToken)
	}
	mergeAgentIdentitySourceMetadata(&identity, identityMetadata)
	if errRequired := validateAgentIdentityConversionIdentity(identity); errRequired != nil {
		return failedAgentIdentityResultWithSecret(result, "identity_missing", errRequired, accessToken)
	}
	if source == nil {
		matched, errMatch := findUniqueManagedCodexAuthByAccount(manager, identity.ChatGPTAccountID)
		if errMatch != nil {
			return failedAgentIdentityResult(result, "source_ambiguous", errMatch.Error())
		}
		if matched != nil {
			source = matched
			result.SourceMode = codexauth.EffectiveAuthMode(source.Metadata)
			result.TargetName = source.FileName
			tasks.updateModes(taskID, index, result.SourceMode, result.TargetMode)
			identityMetadata = mergeAgentIdentityOperationalMetadata(source.Metadata, identityMetadata)
		}
	}
	result.Email = identity.Email
	result.AccountID = identity.ChatGPTAccountID
	result.PlanType = identity.PlanType
	tasks.updateIdentity(taskID, index, result.Email, result.AccountID, result.PlanType)
	if !tasks.updateStage(taskID, index, "registering_identity", 35) {
		return canceledAgentIdentityResult(input)
	}
	keyMaterial, errKey := codexauth.GenerateAgentIdentityKeyMaterial()
	if errKey != nil {
		return failedAgentIdentityResult(result, "key_generation_failed", "failed to generate Agent Identity key")
	}
	client := h.agentIdentityHTTPClient(source)
	baseURL := h.codexAgentIdentityBaseURL()
	runtimeID, errRegister := registerCodexAgentIdentityWithRetry(ctx, client, baseURL, accessToken, identity.ChatGPTAccountIsFedRAMP, keyMaterial)
	if isAgentIdentityRegistrationStatus(errRegister, http.StatusUnauthorized) && source != nil && !sourceRefreshed && strings.TrimSpace(stringValue(source.Metadata, "refresh_token")) != "" {
		if !tasks.updateStage(taskID, index, "refreshing_token", 45) {
			return canceledAgentIdentityResult(input)
		}
		refreshed, errRefresh := h.refreshCodexAgentIdentitySource(ctx, manager, source)
		if errRefresh != nil {
			return failedAgentIdentityResultWithSecret(result, "token_refresh_failed", errRefresh, accessToken, stringValue(source.Metadata, "refresh_token"))
		}
		source = refreshed
		sourceRefreshed = true
		accessToken = strings.TrimSpace(stringValue(source.Metadata, "access_token"))
		identity, errIdentity = codexauth.ParseAccessTokenIdentity(accessToken)
		if errIdentity != nil {
			return failedAgentIdentityResultWithSecret(result, "token_invalid", errIdentity, accessToken)
		}
		identityMetadata = source.Metadata
		mergeAgentIdentitySourceMetadata(&identity, identityMetadata)
		if errRequired := validateAgentIdentityConversionIdentity(identity); errRequired != nil {
			return failedAgentIdentityResultWithSecret(result, "identity_missing", errRequired, accessToken)
		}
		result.Email = identity.Email
		result.AccountID = identity.ChatGPTAccountID
		result.PlanType = identity.PlanType
		tasks.updateIdentity(taskID, index, result.Email, result.AccountID, result.PlanType)
		if !tasks.updateStage(taskID, index, "registering_identity", 50) {
			return canceledAgentIdentityResult(input)
		}
		client = h.agentIdentityHTTPClient(source)
		runtimeID, errRegister = registerCodexAgentIdentityWithRetry(ctx, client, baseURL, accessToken, identity.ChatGPTAccountIsFedRAMP, keyMaterial)
	}
	if errRegister != nil {
		return failedAgentIdentityResultWithSecret(result, "identity_registration_failed", errRegister, accessToken)
	}
	clear(input.token)
	input.token = nil
	accessToken = ""
	if !tasks.updateStage(taskID, index, "registering_task", 65) {
		return canceledAgentIdentityResult(input)
	}
	credential := codexauth.AgentIdentityCredential{
		AgentRuntimeID:          runtimeID,
		PrivateKeyPKCS8Base64:   keyMaterial.PrivateKeyPKCS8Base64,
		AccountID:               identity.AccountID,
		ChatGPTAccountID:        identity.ChatGPTAccountID,
		ChatGPTUserID:           identity.ChatGPTUserID,
		Email:                   identity.Email,
		PlanType:                identity.PlanType,
		WorkspaceID:             identity.WorkspaceID,
		ChatGPTAccountIsFedRAMP: identity.ChatGPTAccountIsFedRAMP,
	}
	taskIDValue, errTask := registerCodexAgentTaskWithRetry(ctx, client, baseURL, credential)
	if errTask != nil {
		return failedAgentIdentityResult(result, "task_registration_failed", sanitizeAgentIdentityTaskError(errTask))
	}
	credential.TaskID = taskIDValue
	if !tasks.beginCommit(taskID, index) {
		return canceledAgentIdentityResult(input)
	}
	installed, created, errPersist := h.persistCodexAgentIdentity(agentIdentityCommitContext(tasks, ctx), manager, source, identityMetadata, credential)
	if errPersist != nil {
		category := "persist_failed"
		if errors.Is(errPersist, errAgentIdentitySourceChanged) {
			category = "source_changed"
		}
		return failedAgentIdentityResultWithSecret(result, category, errPersist,
			credential.AgentRuntimeID,
			credential.PrivateKeyPKCS8Base64,
			credential.TaskID,
		)
	}
	result.TargetName = installed.FileName
	result.Status = agentIdentityItemUpdated
	if created {
		result.Status = agentIdentityItemCreated
	}
	return result
}

func agentIdentityCommitContext(tasks *codexAgentIdentityTaskManager, requestCtx context.Context) context.Context {
	commitCtx := tasks.lifecycleContext()
	if requestInfo := coreauth.GetRequestInfo(requestCtx); requestInfo != nil {
		commitCtx = coreauth.WithRequestInfo(commitCtx, requestInfo)
	}
	return commitCtx
}

func (h *Handler) refreshCodexAgentIdentitySource(ctx context.Context, manager *coreauth.Manager, expected *coreauth.Auth) (*coreauth.Auth, error) {
	if manager == nil || expected == nil {
		return nil, errors.New("Codex source refresh is unavailable")
	}
	if strings.TrimSpace(stringValue(expected.Metadata, "refresh_token")) == "" {
		return nil, errors.New("source credential has no refresh token")
	}
	executor, ok := manager.Executor("codex")
	if !ok || executor == nil {
		return nil, errors.New("Codex executor is unavailable")
	}
	release, errLock := manager.LockCredentialRefresh(ctx, expected.ID)
	if errLock != nil {
		return nil, errLock
	}
	defer release()
	current, exists := manager.GetByID(expected.ID)
	if !exists || !agentIdentitySourceMatches(expected, current) {
		return nil, errAgentIdentitySourceChanged
	}
	refreshed, errRefresh := executor.Refresh(ctx, current.Clone())
	if errRefresh != nil {
		return nil, errRefresh
	}
	if refreshed == nil || strings.TrimSpace(stringValue(refreshed.Metadata, "access_token")) == "" {
		return nil, errors.New("Codex refresh returned no access token")
	}
	installed, updated, errUpdate := manager.UpdateRefreshedIfCurrent(ctx, current, refreshed)
	if errUpdate != nil {
		return nil, errUpdate
	}
	if !updated || installed == nil {
		return nil, errAgentIdentitySourceChanged
	}
	return installed, nil
}

func registerCodexAgentIdentityWithRetry(ctx context.Context, client *http.Client, baseURL, accessToken string, fedramp bool, keyMaterial codexauth.AgentIdentityKeyMaterial) (string, error) {
	var runtimeID string
	err := codexauth.RetryAgentIdentityRegistration(ctx, 3, func(registerCtx context.Context) error {
		var errRegister error
		runtimeID, errRegister = codexauth.RegisterAgentIdentity(registerCtx, client, baseURL, accessToken, fedramp, keyMaterial)
		return errRegister
	})
	return runtimeID, err
}

func registerCodexAgentTaskWithRetry(ctx context.Context, client *http.Client, baseURL string, credential codexauth.AgentIdentityCredential) (string, error) {
	var taskID string
	err := codexauth.RetryAgentIdentityRegistration(ctx, 3, func(registerCtx context.Context) error {
		var errRegister error
		taskID, errRegister = codexauth.RegisterAgentTask(registerCtx, client, baseURL, credential)
		return errRegister
	})
	return taskID, err
}

func (h *Handler) agentIdentityHTTPClient(auth *coreauth.Auth) *http.Client {
	return &http.Client{Transport: h.apiCallTransport(auth)}
}

func (h *Handler) codexAgentIdentityBaseURL() string {
	if h == nil {
		return codexauth.AgentIdentityAuthAPIBaseURL
	}
	h.mu.Lock()
	baseURL := strings.TrimSpace(h.agentIdentityBaseURL)
	h.mu.Unlock()
	if baseURL == "" {
		return codexauth.AgentIdentityAuthAPIBaseURL
	}
	return baseURL
}

func (h *Handler) persistCodexAgentIdentity(ctx context.Context, manager *coreauth.Manager, source *coreauth.Auth, baseMetadata map[string]any, credential codexauth.AgentIdentityCredential) (*coreauth.Auth, bool, error) {
	if manager == nil {
		return nil, false, errors.New("core auth manager unavailable")
	}
	if source != nil {
		baseMetadata = mergeAgentIdentityOperationalMetadata(source.Metadata, baseMetadata)
	}
	metadata := mergeAgentIdentityOperationalMetadata(baseMetadata, codexauth.AgentIdentityMetadata(credential))
	if source != nil {
		updated, errUpdate := h.persistCodexAuthModeUpdate(ctx, manager, source, metadata)
		return updated, false, errUpdate
	}
	fileName := agentIdentityCredentialFileName(credential)
	path, _, errPath := h.resolveManagedAuthFilePath(fileName)
	if errPath != nil {
		return nil, false, errPath
	}
	authID := h.authIDForPath(path)
	if authID == "" {
		authID = fileName
	}
	disabled, _ := metadata["disabled"].(bool)
	status := coreauth.StatusActive
	if disabled {
		status = coreauth.StatusDisabled
	}
	auth := &coreauth.Auth{
		ID:         authID,
		Provider:   "codex",
		FileName:   fileName,
		Label:      credential.Email,
		Status:     status,
		Disabled:   disabled,
		Attributes: map[string]string{"path": path, "source": path, "plan_type": credential.PlanType},
		Metadata:   metadata,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}
	coreauth.ApplyCustomHeadersFromMetadata(auth)
	installed, errRegister := manager.RegisterIfAbsent(ctx, auth)
	if errors.Is(errRegister, coreauth.ErrAuthAlreadyExists) {
		conflict, exists := manager.GetByID(auth.ID)
		if exists && conflict != nil && strings.EqualFold(strings.TrimSpace(conflict.Provider), "codex") && strings.EqualFold(agentIdentityAccountKey(conflict.Metadata), agentIdentityAccountKey(metadata)) {
			merged := mergeAgentIdentityOperationalMetadata(conflict.Metadata, metadata)
			updated, errUpdate := h.persistCodexAuthModeUpdate(ctx, manager, conflict, merged)
			return updated, false, errUpdate
		}
		return nil, false, errors.New("target credential name is already in use")
	}
	if errRegister != nil {
		return nil, false, errRegister
	}
	if installed == nil {
		return nil, false, errors.New("Agent Identity registration returned no auth")
	}
	return installed, true, nil
}

func (h *Handler) persistCodexAuthModeUpdate(ctx context.Context, manager *coreauth.Manager, expected *coreauth.Auth, metadata map[string]any) (*coreauth.Auth, error) {
	if manager == nil || expected == nil {
		return nil, errAgentIdentitySourceChanged
	}
	if errCurrent := h.verifyAgentIdentitySourceCurrent(manager, expected); errCurrent != nil {
		return nil, errCurrent
	}
	target := expected.Clone()
	target.Provider = "codex"
	target.Metadata = cloneStringAnyMap(metadata)
	target.Label = firstNonEmptyValue(stringValue(metadata, "email"), target.Label)
	target.Unavailable = false
	target.LastError = nil
	target.StatusMessage = ""
	target.NextRetryAfter = time.Time{}
	target.Quota = coreauth.QuotaState{}
	target.ModelStates = nil
	if target.Disabled {
		target.Status = coreauth.StatusDisabled
	} else {
		target.Status = coreauth.StatusActive
	}
	if target.Attributes == nil {
		target.Attributes = make(map[string]string)
	}
	target.Attributes["plan_type"] = codexauth.EffectivePlanType(metadata)
	coreauth.ApplyCustomHeadersFromMetadata(target)
	updateCtx := coreauth.WithForceRuntimeReplacement(coreauth.WithSkipStateCarryForward(ctx))
	updated, ok, errUpdate := manager.UpdateIfCurrent(updateCtx, expected, target)
	if errUpdate != nil {
		return nil, errUpdate
	}
	if !ok || updated == nil {
		return nil, errAgentIdentitySourceChanged
	}
	return updated, nil
}

func (h *Handler) verifyAgentIdentitySourceCurrent(manager *coreauth.Manager, expected *coreauth.Auth) error {
	if manager == nil || expected == nil {
		return errAgentIdentitySourceChanged
	}
	current, exists := manager.GetByID(expected.ID)
	if !exists || !agentIdentitySourceMatches(expected, current) {
		return errAgentIdentitySourceChanged
	}
	expectedHash := strings.TrimSpace(expected.Attributes[coreauth.SourceHashAttributeKey])
	if expectedHash == "" {
		return errAgentIdentitySourceChanged
	}
	data, _, _, errRead := h.readManagedAuthFile(expected.FileName)
	if errRead != nil || !coreauth.SourceHashMatchesBytes(expectedHash, data) {
		return errAgentIdentitySourceChanged
	}
	return nil
}

func agentIdentitySourceMatches(expected, current *coreauth.Auth) bool {
	if expected == nil || current == nil || expected.ID != current.ID || !strings.EqualFold(strings.TrimSpace(current.Provider), "codex") {
		return false
	}
	expectedHash := strings.TrimSpace(expected.Attributes[coreauth.SourceHashAttributeKey])
	currentHash := strings.TrimSpace(current.Attributes[coreauth.SourceHashAttributeKey])
	return expectedHash != "" && expectedHash == currentHash
}

func agentIdentityAccountKey(metadata map[string]any) string {
	if accountID := strings.TrimSpace(stringValue(metadata, "chatgpt_account_id")); accountID != "" {
		return accountID
	}
	accessToken := firstNonEmptyValue(stringValue(metadata, "access_token"), stringValue(metadata, "accessToken"))
	if accessToken != "" {
		if identity, err := codexauth.ParseAccessTokenIdentity(accessToken); err == nil {
			if accountID := strings.TrimSpace(firstNonEmptyValue(identity.ChatGPTAccountID, identity.AccountID)); accountID != "" {
				return accountID
			}
		}
	}
	return strings.TrimSpace(codexauth.EffectiveChatGPTAccountID(metadata))
}

func findUniqueManagedCodexAuthByAccount(manager *coreauth.Manager, accountID string) (*coreauth.Auth, error) {
	if manager == nil || strings.TrimSpace(accountID) == "" {
		return nil, nil
	}
	var match *coreauth.Auth
	for _, candidate := range manager.List() {
		if candidate == nil || !strings.EqualFold(strings.TrimSpace(candidate.Provider), "codex") ||
			strings.TrimSpace(candidate.Attributes[coreauth.SourceHashAttributeKey]) == "" ||
			!strings.EqualFold(agentIdentityAccountKey(candidate.Metadata), accountID) {
			continue
		}
		if match != nil && match.ID != candidate.ID {
			return nil, errors.New("multiple managed Codex credentials match this account; use names to select the target credential")
		}
		match = candidate
	}
	return match, nil
}

func mergeAgentIdentityOperationalMetadata(existing, identity map[string]any) map[string]any {
	merged := make(map[string]any, len(existing)+len(identity))
	for key, value := range existing {
		merged[key] = value
	}
	for key, value := range identity {
		merged[key] = value
	}
	return merged
}

func mergeAgentIdentitySourceMetadata(identity *codexauth.AccessTokenIdentity, metadata map[string]any) {
	if identity == nil {
		return
	}
	if identity.AccountID == "" {
		identity.AccountID = strings.TrimSpace(stringValue(metadata, "account_id"))
	}
	if identity.ChatGPTAccountID == "" {
		identity.ChatGPTAccountID = strings.TrimSpace(stringValue(metadata, "chatgpt_account_id"))
		if identity.ChatGPTAccountID == "" {
			identity.ChatGPTAccountID = identity.AccountID
		}
	}
	if identity.ChatGPTUserID == "" {
		identity.ChatGPTUserID = strings.TrimSpace(stringValue(metadata, "chatgpt_user_id"))
	}
	if identity.Email == "" {
		identity.Email = strings.TrimSpace(stringValue(metadata, "email"))
	}
	if identity.PlanType == "" {
		identity.PlanType = codexauth.EffectivePlanType(metadata)
	}
	if identity.WorkspaceID == "" {
		identity.WorkspaceID = strings.TrimSpace(stringValue(metadata, "workspace_id"))
	}
	if !identity.ChatGPTAccountIsFedRAMP {
		identity.ChatGPTAccountIsFedRAMP = codexauth.ChatGPTAccountIsFedRAMP(metadata)
	}
}

func validateAgentIdentityConversionIdentity(identity codexauth.AccessTokenIdentity) error {
	for key, value := range map[string]string{
		"account_id":         identity.AccountID,
		"chatgpt_account_id": identity.ChatGPTAccountID,
		"chatgpt_user_id":    identity.ChatGPTUserID,
		"email":              identity.Email,
		"plan_type":          identity.PlanType,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("access token does not contain %s", key)
		}
	}
	return nil
}

func agentIdentityCredentialFileName(credential codexauth.AgentIdentityCredential) string {
	accountID := strings.TrimSpace(credential.ChatGPTAccountID)
	if accountID == "" {
		accountID = strings.TrimSpace(credential.AccountID)
	}
	sum := sha256.Sum256([]byte(accountID))
	accountHash := hex.EncodeToString(sum[:6])
	return "codex-" + accountHash + ".json"
}

func isAgentIdentityRegistrationStatus(err error, status int) bool {
	var registrationErr *codexauth.AgentIdentityRegistrationHTTPError
	return errors.As(err, &registrationErr) && registrationErr.HTTPStatus() == status
}

func canceledAgentIdentityResult(input codexAgentIdentityConversionInput) codexAgentIdentityTaskResult {
	return codexAgentIdentityTaskResult{
		SourceName:    input.sourceName,
		TargetMode:    input.targetMode,
		Stage:         "canceled",
		Status:        agentIdentityItemCanceled,
		ErrorCategory: "canceled",
		Error:         "conversion canceled",
	}
}

func failedAgentIdentityResult(result codexAgentIdentityTaskResult, category, message string) codexAgentIdentityTaskResult {
	result.Status = agentIdentityItemFailed
	result.ErrorCategory = category
	result.Error = sanitizeAgentIdentityTaskError(errors.New(message))
	return result
}

func failedAgentIdentityResultWithSecret(result codexAgentIdentityTaskResult, category string, err error, secrets ...string) codexAgentIdentityTaskResult {
	message := "conversion failed"
	if err != nil {
		message = err.Error()
	}
	for _, secret := range secrets {
		if secret = strings.TrimSpace(secret); secret != "" {
			message = strings.ReplaceAll(message, secret, "[redacted]")
		}
	}
	result.Status = agentIdentityItemFailed
	result.ErrorCategory = category
	result.Error = sanitizeAgentIdentityTaskError(errors.New(message))
	return result
}

func sanitizeAgentIdentityTaskError(err error) string {
	if err == nil {
		return "conversion failed"
	}
	message := strings.TrimSpace(err.Error())
	if len(message) > 512 {
		message = message[:512] + "..."
	}
	return message
}
