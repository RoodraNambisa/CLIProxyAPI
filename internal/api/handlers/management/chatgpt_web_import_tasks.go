package management

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const (
	chatGPTWebImportMaxRequestBytes = 32 << 20
	chatGPTWebImportMaxFileBytes    = 1 << 20
	chatGPTWebImportMaxNameBytes    = 1 << 10
	chatGPTWebImportMaxFiles        = 500
)

type chatGPTWebImportInput struct {
	file       string
	targetName string
	data       []byte
}

type chatGPTWebAccountInfoTrigger interface {
	TriggerAccountInfoRefresh(string, bool) bool
}

type chatGPTWebAccountInfoStateTrigger interface {
	TriggerAccountInfoRefreshState(string, bool) string
}

type chatGPTWebImportAccountInfoStateTrigger interface {
	TriggerImportAccountInfoRefreshState(string) string
}

var errChatGPTWebImportIdentityConflict = errors.New("chatgpt web import identity conflict")

func (h *Handler) chatGPTWebMutationTaskManager() *chatGPTWebMutationTaskManager {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.chatGPTWebMutationTasks == nil {
		h.chatGPTWebMutationTasks = newChatGPTWebMutationTaskManager()
	}
	workers := config.DefaultChatGPTWebImportWorkers
	if h.cfg != nil {
		workers = h.cfg.ChatGPTWeb.Import.Resolved().Workers
	}
	h.chatGPTWebMutationTasks.updateWorkerLimit(workers)
	return h.chatGPTWebMutationTasks
}

// StartChatGPTWebImportTask starts a bounded Web credential import task.
func (h *Handler) StartChatGPTWebImportTask(c *gin.Context) {
	executor, manager, errExecutor := h.chatGPTWebImportExecutor()
	if errExecutor != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": errExecutor.Error()})
		return
	}
	inputs, errInput := readChatGPTWebImportInputs(c)
	if errInput != nil {
		status := http.StatusBadRequest
		var maxBytesError *http.MaxBytesError
		if errors.As(errInput, &maxBytesError) || errors.Is(errInput, errChatGPTWebLoginTaskInputTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		c.JSON(status, gin.H{"error": errInput.Error()})
		return
	}
	inputsHandedOff := false
	defer func() {
		if !inputsHandedOff {
			clearChatGPTWebImportInputs(inputs)
		}
	}()
	results := make([]chatGPTWebMutationTaskResult, len(inputs))
	for index := range inputs {
		results[index] = chatGPTWebMutationTaskResult{
			File:       inputs[index].file,
			TargetName: inputs[index].targetName,
			Status:     chatGPTWebLoginResultQueued,
		}
	}
	taskManager := h.chatGPTWebMutationTaskManager()
	task, taskCtx, errCreate := taskManager.create(chatGPTWebMutationTaskImport, results)
	if errCreate != nil {
		status := http.StatusTooManyRequests
		if errors.Is(errCreate, errChatGPTWebLoginTaskClosed) {
			status = http.StatusServiceUnavailable
		}
		c.JSON(status, gin.H{"error": errCreate.Error()})
		return
	}
	taskCtx = PopulateAuthContext(taskCtx, c)
	inputsHandedOff = true
	go h.runChatGPTWebImportTask(taskCtx, task.ID, inputs, executor, manager)
	c.JSON(http.StatusAccepted, task)
}

// GetChatGPTWebImportTask returns one import task snapshot.
func (h *Handler) GetChatGPTWebImportTask(c *gin.Context) {
	taskManager := h.chatGPTWebMutationTaskManager()
	if taskManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "chatgpt web import tasks are unavailable"})
		return
	}
	task, ok := taskManager.get(chatGPTWebMutationTaskImport, c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "chatgpt web import task not found"})
		return
	}
	c.JSON(chatGPTWebMutationTaskHTTPStatus(task), task)
}

// CancelChatGPTWebImportTask requests cancellation without deleting history.
func (h *Handler) CancelChatGPTWebImportTask(c *gin.Context) {
	taskManager := h.chatGPTWebMutationTaskManager()
	if taskManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "chatgpt web import tasks are unavailable"})
		return
	}
	task, ok := taskManager.cancel(chatGPTWebMutationTaskImport, c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "chatgpt web import task not found"})
		return
	}
	c.JSON(http.StatusOK, task)
}

func readChatGPTWebImportInputs(c *gin.Context) ([]chatGPTWebImportInput, error) {
	if c == nil || c.Request == nil {
		return nil, errors.New("multipart request is required")
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, chatGPTWebImportMaxRequestBytes)
	reader, errReader := c.Request.MultipartReader()
	if errReader != nil {
		return nil, errors.New("multipart form with file or files fields is required")
	}
	inputs := make([]chatGPTWebImportInput, 0)
	targetNames := make([]string, 0)
	completed := false
	defer func() {
		if !completed {
			clearChatGPTWebImportInputs(inputs)
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
		field := part.FormName()
		if (field == "name" || field == "names") && part.FileName() == "" {
			value, errRead := io.ReadAll(io.LimitReader(part, chatGPTWebImportMaxNameBytes+1))
			errClose := part.Close()
			if errRead != nil {
				return nil, errRead
			}
			if errClose != nil {
				return nil, errClose
			}
			if len(value) > chatGPTWebImportMaxNameBytes {
				return nil, errors.New("custom credential name is too long")
			}
			targetNames = append(targetNames, string(value))
			continue
		}
		if field != "file" && field != "files" {
			_ = part.Close()
			continue
		}
		if len(inputs) >= chatGPTWebImportMaxFiles {
			_ = part.Close()
			return nil, fmt.Errorf("at most %d credential files are allowed", chatGPTWebImportMaxFiles)
		}
		payload, errRead := io.ReadAll(io.LimitReader(part, chatGPTWebImportMaxFileBytes+1))
		errClose := part.Close()
		if errRead != nil {
			clear(payload)
			clearChatGPTWebImportInputs(inputs)
			return nil, errRead
		}
		if errClose != nil {
			clear(payload)
			clearChatGPTWebImportInputs(inputs)
			return nil, errClose
		}
		if len(payload) > chatGPTWebImportMaxFileBytes {
			clear(payload)
			clearChatGPTWebImportInputs(inputs)
			return nil, errChatGPTWebLoginTaskInputTooLarge
		}
		fileName := filepath.Base(strings.TrimSpace(part.FileName()))
		if fileName == "." || fileName == "" {
			fileName = fmt.Sprintf("credential-%d.json", len(inputs)+1)
		}
		inputs = append(inputs, chatGPTWebImportInput{file: fileName, data: payload})
	}
	if len(inputs) == 0 {
		return nil, errors.New("at least one credential file is required")
	}
	if len(targetNames) > 0 && len(targetNames) != len(inputs) {
		return nil, errors.New("custom credential names must match the uploaded files")
	}
	seenTargetNames := make(map[string]struct{}, len(targetNames))
	for index, value := range targetNames {
		targetName, errName := normalizeChatGPTWebCredentialTargetName(value)
		if errName != nil {
			return nil, fmt.Errorf("custom credential name %d is invalid", index+1)
		}
		if targetName == "" {
			continue
		}
		key := strings.ToLower(targetName)
		if _, exists := seenTargetNames[key]; exists {
			return nil, errors.New("custom credential names must be unique")
		}
		seenTargetNames[key] = struct{}{}
		inputs[index].targetName = targetName
	}
	completed = true
	return inputs, nil
}

func clearChatGPTWebImportInputs(inputs []chatGPTWebImportInput) {
	for index := range inputs {
		clear(inputs[index].data)
		inputs[index].data = nil
	}
}

func (h *Handler) runChatGPTWebImportTask(ctx context.Context, taskID string, inputs []chatGPTWebImportInput, executor chatGPTWebImportExecutor, manager *coreauth.Manager) {
	defer func() {
		clearChatGPTWebImportInputs(inputs)
	}()
	tasks := h.chatGPTWebMutationTaskManager()
	if !tasks.start(chatGPTWebMutationTaskImport, taskID) {
		tasks.finish(chatGPTWebMutationTaskImport, taskID, true)
		return
	}
	jobs := make(chan int)
	var workers sync.WaitGroup
	workerCount := min(config.MaxChatGPTWebImportWorkers, len(inputs))
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				if !tasks.acquireImportSlot(ctx) {
					tasks.setResult(chatGPTWebMutationTaskImport, taskID, index, canceledChatGPTWebImportResult(inputs[index]))
					continue
				}
				if !tasks.markRunning(chatGPTWebMutationTaskImport, taskID, index) {
					tasks.releaseImportSlot()
					continue
				}
				result := h.executeChatGPTWebImport(ctx, inputs[index], executor, manager, func() (context.Context, bool) {
					if !tasks.beginCommit(chatGPTWebMutationTaskImport, taskID, index) {
						return nil, false
					}
					commitCtx := tasks.lifecycleContext()
					if requestInfo := coreauth.GetRequestInfo(ctx); requestInfo != nil {
						commitCtx = coreauth.WithRequestInfo(commitCtx, requestInfo)
					}
					return commitCtx, true
				})
				triggerChatGPTWebAccountInfoRefreshAfterImport(executor, &result)
				tasks.setResult(chatGPTWebMutationTaskImport, taskID, index, result)
				tasks.releaseImportSlot()
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
	tasks.finish(chatGPTWebMutationTaskImport, taskID, ctx.Err() != nil)
}

func triggerChatGPTWebAccountInfoRefreshAfterImport(executor chatGPTWebImportExecutor, result *chatGPTWebMutationTaskResult) {
	if result == nil || result.AccountInfoRefreshState != "queued" {
		return
	}
	// Keep the persisted account-info intent queued until the higher-priority
	// import Session refresh installs a current credential instance.
	if result.SessionRefreshState == "queued" || result.SessionRefreshState == "reused" {
		return
	}
	switch result.Status {
	case "created", "updated", "unchanged":
	default:
		return
	}
	authID := strings.TrimSpace(result.Name)
	if authID == "" {
		return
	}
	// The account-info runtime applies the automatic-refresh switch, TTL,
	// instance isolation, and queue deduplication to this best-effort trigger.
	if trigger, ok := executor.(chatGPTWebImportAccountInfoStateTrigger); ok {
		result.AccountInfoRefreshState = trigger.TriggerImportAccountInfoRefreshState(authID)
		return
	}
	if trigger, ok := executor.(chatGPTWebAccountInfoStateTrigger); ok {
		result.AccountInfoRefreshState = trigger.TriggerAccountInfoRefreshState(authID, false)
		return
	}
	trigger, ok := executor.(chatGPTWebAccountInfoTrigger)
	if !ok || !trigger.TriggerAccountInfoRefresh(authID, false) {
		result.AccountInfoRefreshState = "canceled"
	}
}

func (h *Handler) executeChatGPTWebImport(ctx context.Context, input chatGPTWebImportInput, _ chatGPTWebImportExecutor, manager *coreauth.Manager, beginCommit func() (context.Context, bool)) chatGPTWebMutationTaskResult {
	result := chatGPTWebMutationTaskResult{File: input.file, TargetName: input.targetName, Status: "failed"}
	nameScoped := input.targetName != ""
	credential, errDecode := chatgptwebauth.DecodeImportCredential(input.data)
	if errDecode != nil {
		result.ErrorCategory = "invalid_credential"
		result.Error = "credential JSON is invalid or unsupported"
		result.HTTPStatus = http.StatusBadRequest
		return result
	}
	credential.Email = chatgptwebauth.NormalizeEmail(credential.Email)
	if credential.Email == "" {
		result.ErrorCategory = "identity_missing"
		result.Error = "credential does not identify an email account"
		result.HTTPStatus = http.StatusUnprocessableEntity
		return result
	}
	result.Email = credential.Email
	needsInitialRefresh, errUsable := prepareImportedChatGPTWebCredentialForBackground(credential, time.Now())
	if errUsable != nil {
		result.ErrorCategory = "credential_unavailable"
		result.Error = errUsable.Error()
		result.HTTPStatus = http.StatusUnprocessableEntity
		return result
	}
	maintenance := h.chatGPTWebImportMaintenancePlan(credential, needsInitialRefresh)
	maintenance.apply(credential)
	fileName := input.targetName
	if fileName == "" {
		fileName = chatGPTWebCredentialFileName(credential.Email)
	}
	result.TargetName = fileName
	var (
		existing    *coreauth.Auth
		errExisting error
	)
	if nameScoped {
		existing, errExisting = findNamedChatGPTWebAuth(ctx, manager, fileName, credential.Email)
	} else {
		existing, errExisting = findExistingChatGPTWebAuth(ctx, manager, fileName, credential.Email)
	}
	if errExisting != nil {
		if errors.Is(errExisting, errChatGPTWebCredentialIDOwned) || errors.Is(errExisting, errChatGPTWebCredentialMultiple) {
			result.ErrorCategory = "identity_conflict"
			result.Error = "credential conflicts with an existing account"
			result.HTTPStatus = http.StatusConflict
			return result
		}
		return failedChatGPTWebMutationResult(result, errExisting)
	}
	if existing != nil && !nameScoped {
		candidate := existing.Clone()
		candidate.Metadata = cloneStringAnyMap(existing.Metadata)
		credential.ApplyToMetadata(candidate.Metadata)
		identityChanged := coreauth.ChatGPTWebCredentialRefreshIdentityChanged(existing, candidate)
		if identityChanged {
			result.ErrorCategory = "identity_conflict"
			result.Error = "credential identity conflicts with the existing account"
			result.HTTPStatus = http.StatusConflict
			return result
		}
	}
	reservationMetadata := make(map[string]any)
	credential.ApplyToMetadata(reservationMetadata)
	releaseReservation, reservationConflict := h.chatGPTWebMutationTaskManager().reserveImport(fileName, &coreauth.Auth{
		ID:       fileName,
		Provider: chatgptwebauth.Provider,
		Metadata: reservationMetadata,
	})
	if reservationConflict != "" {
		result.HTTPStatus = http.StatusConflict
		if reservationConflict == "identity" {
			result.ErrorCategory = "identity_conflict"
			result.Error = "credential conflicts with an in-flight account import"
		} else {
			result.ErrorCategory = "credential_changed"
			result.Error = "credential target is being changed by another import"
		}
		return result
	}
	defer releaseReservation()
	if errContext := ctx.Err(); errContext != nil {
		return canceledChatGPTWebImportResult(input)
	}
	if beginCommit == nil {
		result.ErrorCategory = "persist_failed"
		result.Error = "credential persistence is unavailable"
		result.HTTPStatus = http.StatusInternalServerError
		return result
	}
	commitCtx, allowed := beginCommit()
	if !allowed {
		return canceledChatGPTWebImportResult(input)
	}
	ctx = commitCtx
	ctx = coreauth.WithChatGPTWebImportPolicy(ctx, coreauth.ChatGPTWebImportPolicy{
		ValidateModels: maintenance.validateModels,
	})
	installed, status, unchanged, errPersist := h.persistImportedChatGPTWebCredential(ctx, manager, fileName, credential, existing, true, nameScoped)
	if errors.Is(errPersist, errChatGPTWebImportIdentityConflict) || errors.Is(errPersist, errChatGPTWebCredentialIdentityOwned) {
		result.ErrorCategory = "identity_conflict"
		result.Error = "credential conflicts with an existing account"
		result.HTTPStatus = http.StatusConflict
		return result
	}
	if errors.Is(errPersist, errChatGPTWebCredentialChanged) || errors.Is(errPersist, coreauth.ErrAuthAlreadyExists) {
		result.ErrorCategory = "credential_changed"
		result.Error = "credential changed while import was running"
		result.HTTPStatus = http.StatusConflict
		return result
	}
	if errors.Is(errPersist, errChatGPTWebCredentialLookup) {
		return failedChatGPTWebMutationResult(result, errPersist)
	}
	if errPersist != nil {
		return failedChatGPTWebPersistenceResult(result, errPersist, "failed to save chatgpt web credential")
	}
	if unchanged {
		result.Status = "unchanged"
	} else {
		result.Status = status
	}
	result.Name = installed.FileName
	result.AuthIndex = installed.EnsureIndex()
	result.CredentialMode = credential.CredentialMode
	if errConfirm := confirmChatGPTWebImportPersistence(&result, installed, credential); errConfirm != nil {
		return failedChatGPTWebPersistenceResult(result, errConfirm, "persisted credential could not be verified")
	}
	maintenance.assign(&result)
	return result
}

type chatGPTWebImportMaintenancePlan struct {
	refreshSession bool
	validateModels bool
	refreshAccount bool
}

func (plan chatGPTWebImportMaintenancePlan) apply(credential *chatgptwebauth.Credential) {
	if credential == nil {
		return
	}
	credential.ImportSessionPending = plan.refreshSession
	credential.ImportModelsPending = plan.validateModels
	credential.ImportAccountInfoPending = plan.refreshAccount
}

func (plan chatGPTWebImportMaintenancePlan) assign(result *chatGPTWebMutationTaskResult) {
	if result == nil {
		return
	}
	result.SessionRefreshState = "skipped"
	if plan.refreshSession {
		result.SessionRefreshState = "queued"
	}
	result.ModelValidationState = "skipped"
	if plan.validateModels {
		result.ModelValidationState = "queued"
	}
	result.AccountInfoRefreshState = "skipped"
	if plan.refreshAccount {
		result.AccountInfoRefreshState = "queued"
	}
}

func prepareImportedChatGPTWebCredentialForBackground(credential *chatgptwebauth.Credential, now time.Time) (bool, error) {
	if credential == nil {
		return false, errors.New("credential is unavailable")
	}
	hasAccessToken := strings.TrimSpace(credential.AccessToken) != ""
	accessTokenUsable := hasAccessToken
	if expiresAt, known := chatGPTWebImportedCredentialExpiry(credential); known {
		accessTokenUsable = hasAccessToken && expiresAt.After(now)
	}
	refreshable := strings.TrimSpace(credential.RefreshToken) != "" ||
		chatgptwebauth.HasSessionCookieForURL(credential.Cookies, chatgptwebauth.SessionBaseURL)
	if !accessTokenUsable && !refreshable {
		return false, errors.New("credential has no usable access token or complete refresh material")
	}
	credential.LifecycleUpdatedAt = now.UTC().Format(time.RFC3339Nano)
	if accessTokenUsable {
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		credential.LifecycleReason = ""
		return false, nil
	}
	credential.LifecycleState = chatgptwebauth.LifecycleRefreshing
	credential.LifecycleReason = "import_refresh_pending"
	return true, nil
}

func (h *Handler) chatGPTWebImportMaintenancePlan(credential *chatgptwebauth.Credential, needsInitialRefresh bool) chatGPTWebImportMaintenancePlan {
	if credential == nil {
		return chatGPTWebImportMaintenancePlan{}
	}
	h.mu.Lock()
	forceSessionRefresh := true
	importConfig := config.ResolvedChatGPTWebImportConfig{Workers: config.DefaultChatGPTWebImportWorkers}
	autoAccountInfo := true
	if h.cfg != nil {
		forceSessionRefresh = h.cfg.ChatGPTWeb.ForceSessionRefreshOnImportEnabled()
		importConfig = h.cfg.ChatGPTWeb.Import.Resolved()
		autoAccountInfo = h.cfg.ChatGPTWeb.AccountInfo.Resolved().AutomaticRefreshEnabled()
	}
	h.mu.Unlock()
	return chatGPTWebImportMaintenancePlan{
		refreshSession: needsInitialRefresh || credential.RefreshStrategy == chatgptwebauth.RefreshStrategyChatGPTSession && forceSessionRefresh,
		validateModels: importConfig.ValidateModelsAfterUpload,
		refreshAccount: importConfig.RefreshAccountInfoAfterUpload && autoAccountInfo,
	}
}

func confirmChatGPTWebImportPersistence(result *chatGPTWebMutationTaskResult, installed *coreauth.Auth, imported *chatgptwebauth.Credential) error {
	if result == nil || installed == nil || imported == nil {
		return errors.New("persisted credential is unavailable")
	}
	persisted, errParse := chatgptwebauth.ParseCredential(installed.Metadata)
	if errParse != nil {
		return errors.New("persisted credential is invalid")
	}
	result.CredentialSchemaVersion = persisted.CredentialSchemaVersion
	if imported.API798URL != "" {
		if persisted.API798URL != imported.API798URL || persisted.LoginMethod != imported.LoginMethod {
			return errors.New("persisted API798 login settings do not match the imported credential")
		}
		result.PersistedFeatures = append(result.PersistedFeatures, chatgptwebauth.API798LoginFeature)
	}
	if imported.CredentialSchemaVersion == chatgptwebauth.CredentialSchemaVersionWebAuthn {
		if persisted.CredentialSchemaVersion != chatgptwebauth.CredentialSchemaVersionWebAuthn ||
			persisted.WebAuthn == nil || !reflect.DeepEqual(imported.WebAuthn, persisted.WebAuthn) {
			return errors.New("persisted WebAuthn credential does not match the imported credential")
		}
		result.PersistedFeatures = append(result.PersistedFeatures, "webauthn_v1")
		result.WebAuthnV1Persisted = true
	}
	if imported.CredentialSchemaVersion == chatgptwebauth.CredentialSchemaVersionAdvancedAccountSecurity {
		if persisted.CredentialSchemaVersion != chatgptwebauth.CredentialSchemaVersionAdvancedAccountSecurity ||
			persisted.AdvancedAccountSecurity == nil ||
			!reflect.DeepEqual(imported.AdvancedAccountSecurity, persisted.AdvancedAccountSecurity) {
			return errors.New("persisted advanced account security credential does not match the imported credential")
		}
		result.PersistedFeatures = append(result.PersistedFeatures, chatgptwebauth.AdvancedAccountSecurityFeature)
		result.AdvancedSecurityPersisted = true
	}
	return nil
}

func (h *Handler) persistImportedChatGPTWebCredential(ctx context.Context, manager *coreauth.Manager, fileName string, credential *chatgptwebauth.Credential, expected *coreauth.Auth, refreshAware, nameScoped bool) (installed *coreauth.Auth, status string, unchanged bool, err error) {
	if h == nil || manager == nil || credential == nil {
		return nil, "", false, errors.New("credential persistence is unavailable")
	}
	unlockDependency := func() {}
	dependencyLocked := expected != nil || credential.RefreshStrategy == chatgptwebauth.RefreshStrategyCodexSource
	if dependencyLocked {
		var errDependencyLock error
		unlockDependency, errDependencyLock = h.chatGPTWebDependencyMu.lock(ctx)
		if errDependencyLock != nil {
			return nil, "", false, errDependencyLock
		}
	}
	defer unlockDependency()
	var (
		existing    *coreauth.Auth
		errExisting error
	)
	if nameScoped {
		existing, errExisting = findNamedChatGPTWebAuth(ctx, manager, fileName, credential.Email)
	} else {
		existing, errExisting = findExistingChatGPTWebAuth(ctx, manager, fileName, credential.Email)
	}
	if errExisting != nil {
		if errors.Is(errExisting, errChatGPTWebCredentialIDOwned) || errors.Is(errExisting, errChatGPTWebCredentialMultiple) {
			return nil, "", false, errChatGPTWebImportIdentityConflict
		}
		return nil, "", false, errExisting
	}
	if expected == nil && existing != nil || expected != nil && (existing == nil || existing.ID != expected.ID) {
		return nil, "", false, errChatGPTWebCredentialChanged
	}
	persistExpected := existing
	if expected != nil && existing != nil && expected.ID == existing.ID {
		if !chatGPTWebImportCredentialFieldsEqual(expected, existing) {
			return nil, "", false, errChatGPTWebCredentialChanged
		}
		persistExpected = existing
	}
	identityBase := persistExpected
	if expected != nil {
		identityBase = expected
	}
	if identityBase != nil {
		candidate := identityBase.Clone()
		candidate.Metadata = cloneStringAnyMap(identityBase.Metadata)
		credential.ApplyToMetadata(candidate.Metadata)
		identityChanged := coreauth.ChatGPTWebCredentialIdentityChanged(identityBase, candidate)
		if refreshAware {
			identityChanged = coreauth.ChatGPTWebCredentialRefreshIdentityChanged(identityBase, candidate)
		}
		if identityChanged && !nameScoped {
			return nil, "", false, errChatGPTWebImportIdentityConflict
		}
		if !identityChanged {
			current, errParse := chatgptwebauth.ParseCredential(identityBase.Metadata)
			if errParse == nil {
				credential.CredentialUID = current.CredentialUID
			}
		}
	}
	if strings.TrimSpace(credential.CredentialUID) == "" {
		credential.CredentialUID = uuid.NewString()
	}
	preserveImportedChatGPTWebLoginSettings(credential, persistExpected)
	preserveImportedChatGPTWebAuthnRuntimeState(credential, persistExpected)
	preserveImportedChatGPTWebAdvancedAccountSecurityRuntimeState(credential, persistExpected)
	unchanged = existing != nil && importedChatGPTWebCredentialUnchanged(existing, credential)
	status = "created"
	if existing != nil {
		status = "updated"
	}
	if unchanged {
		installed = existing.Clone()
		return installed, status, true, nil
	}
	var oldSourceUID string
	installed, oldSourceUID, err = h.persistChatGPTWebCredentialLocked(ctx, manager, fileName, credential, persistExpected, nil, refreshAware)
	if dependencyLocked {
		unlockDependency()
	}
	if err == nil && oldSourceUID != "" && credential.RefreshStrategy != chatgptwebauth.RefreshStrategyCodexSource {
		h.cleanupRetainedCodexSource(ctx, oldSourceUID)
	}
	return installed, status, false, err
}

func preserveImportedChatGPTWebLoginSettings(imported *chatgptwebauth.Credential, current *coreauth.Auth) {
	if imported == nil || current == nil {
		return
	}
	currentCredential, errParse := chatgptwebauth.ParseCredential(current.Metadata)
	if errParse != nil {
		return
	}
	if imported.API798URL == "" {
		imported.API798URL = currentCredential.API798URL
	}
	if imported.CredentialSchemaVersion == chatgptwebauth.CredentialSchemaVersionAdvancedAccountSecurity {
		imported.LoginMethod = chatgptwebauth.LoginMethodAdvancedSecurityPasskey
		return
	}
	if imported.LoginMethod == chatgptwebauth.LoginMethodAuto && currentCredential.LoginMethod != chatgptwebauth.LoginMethodAuto {
		imported.LoginMethod = currentCredential.LoginMethod
	}
}

func preserveImportedChatGPTWebAdvancedAccountSecurityRuntimeState(imported *chatgptwebauth.Credential, current *coreauth.Auth) {
	if imported == nil || imported.AdvancedAccountSecurity == nil || current == nil {
		return
	}
	currentCredential, errParse := chatgptwebauth.ParseCredential(current.Metadata)
	if errParse != nil || currentCredential.AdvancedAccountSecurity == nil {
		return
	}
	chatgptwebauth.MergeAdvancedAccountSecurityRuntimeState(imported.AdvancedAccountSecurity, currentCredential.AdvancedAccountSecurity)
}

func preserveImportedChatGPTWebAuthnRuntimeState(imported *chatgptwebauth.Credential, current *coreauth.Auth) {
	if imported == nil || imported.WebAuthn == nil || current == nil {
		return
	}
	currentCredential, errParse := chatgptwebauth.ParseCredential(current.Metadata)
	if errParse != nil || currentCredential.WebAuthn == nil ||
		!chatgptwebauth.WebAuthnAuthenticatorMatches(imported.WebAuthn, currentCredential.WebAuthn) {
		return
	}
	if currentCredential.WebAuthn.SignCount > imported.WebAuthn.SignCount {
		imported.WebAuthn.SignCount = currentCredential.WebAuthn.SignCount
	}
	if chatgptwebauth.CompareWebAuthnLastUsedAt(currentCredential.WebAuthn.LastUsedAt, imported.WebAuthn.LastUsedAt) > 0 {
		imported.WebAuthn.LastUsedAt = currentCredential.WebAuthn.LastUsedAt
	}
}

func chatGPTWebImportCredentialFieldsEqual(first, second *coreauth.Auth) bool {
	if first == nil || second == nil {
		return first == second
	}
	firstCredential, firstErr := chatgptwebauth.ParseCredential(first.Metadata)
	secondCredential, secondErr := chatgptwebauth.ParseCredential(second.Metadata)
	return firstErr == nil && secondErr == nil && reflect.DeepEqual(firstCredential, secondCredential)
}

func chatGPTWebImportedCredentialExpiry(credential *chatgptwebauth.Credential) (time.Time, bool) {
	if credential == nil {
		return time.Time{}, false
	}
	if value := strings.TrimSpace(credential.Expired); value != "" {
		parsed, errParse := time.Parse(time.RFC3339, value)
		if errParse != nil {
			return time.Time{}, true
		}
		return parsed, true
	}
	if expiresAt, ok := chatgptwebauth.JWTExpiry(credential.AccessToken); ok {
		return expiresAt, true
	}
	return chatgptwebauth.JWTExpiry(credential.IDToken)
}

func importedChatGPTWebCredentialUnchanged(existing *coreauth.Auth, credential *chatgptwebauth.Credential) bool {
	if existing == nil || credential == nil {
		return false
	}
	before := cloneStringAnyMap(existing.Metadata)
	after := cloneStringAnyMap(existing.Metadata)
	credential.ApplyToMetadata(after)
	for _, key := range []string{"last_refresh_at", "lifecycle_updated_at"} {
		delete(before, key)
		delete(after, key)
	}
	return reflect.DeepEqual(before, after)
}

func cloneStringAnyMap(values map[string]any) map[string]any {
	clone := make(map[string]any, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}

func canceledChatGPTWebImportResult(input chatGPTWebImportInput) chatGPTWebMutationTaskResult {
	return chatGPTWebMutationTaskResult{File: input.file, Status: chatGPTWebLoginResultCanceled, ErrorCategory: "canceled", Error: "import canceled"}
}

func failedChatGPTWebMutationResult(result chatGPTWebMutationTaskResult, err error) chatGPTWebMutationTaskResult {
	if errors.Is(err, context.Canceled) {
		return canceledChatGPTWebMutationResult(result)
	}
	category, message, status, _ := classifyChatGPTWebManagementError(err)
	result.ErrorCategory = category
	result.Error = message
	result.HTTPStatus = status
	if category == "canceled" {
		result.Status = chatGPTWebLoginResultCanceled
	}
	return result
}

func failedChatGPTWebPersistenceResult(result chatGPTWebMutationTaskResult, err error, message string) chatGPTWebMutationTaskResult {
	outcome, explicit := coreauth.SaveOutcomeFromError(err)
	if explicit && (outcome == coreauth.SaveOutcomeUncertain || outcome == coreauth.SaveOutcomeCommitted) ||
		!explicit && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		result.ErrorCategory = "persist_uncertain"
		result.Error = "credential persistence outcome is uncertain"
		result.HTTPStatus = http.StatusServiceUnavailable
		return result
	}
	if errors.Is(err, context.Canceled) {
		result.Status = chatGPTWebLoginResultCanceled
		result.ErrorCategory = "canceled"
		result.Error = "credential persistence was canceled"
		result.HTTPStatus = http.StatusRequestTimeout
		return result
	}
	result.ErrorCategory = "persist_failed"
	result.Error = message
	result.HTTPStatus = http.StatusInternalServerError
	return result
}

func failedChatGPTWebProbeResult(result chatGPTWebMutationTaskResult, err error) chatGPTWebMutationTaskResult {
	if errors.Is(err, context.Canceled) {
		return canceledChatGPTWebMutationResult(result)
	}
	status := chatGPTWebErrorStatus(err)
	path := chatGPTWebErrorRequestPath(err)
	switch {
	case (status == http.StatusUnauthorized || status == http.StatusForbidden) && strings.HasPrefix(path, "/backend-api/models"):
		result.ErrorCategory = "token_incompatible"
		result.Error = "access token is not accepted by ChatGPT Web"
		result.HTTPStatus = http.StatusUnprocessableEntity
	case status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusTooManyRequests || status >= http.StatusInternalServerError || status == 0:
		result.ErrorCategory = "probe_unavailable"
		result.Error = "ChatGPT Web credential validation is temporarily unavailable"
		result.HTTPStatus = http.StatusServiceUnavailable
	default:
		result.ErrorCategory = "probe_failed"
		result.Error = "ChatGPT Web credential validation failed"
		result.HTTPStatus = http.StatusBadGateway
	}
	return result
}

func canceledChatGPTWebMutationResult(result chatGPTWebMutationTaskResult) chatGPTWebMutationTaskResult {
	result.Status = chatGPTWebLoginResultCanceled
	result.ErrorCategory = "canceled"
	result.Error = "operation canceled"
	result.HTTPStatus = http.StatusRequestTimeout
	return result
}

func chatGPTWebErrorStatus(err error) int {
	type statusCoder interface{ StatusCode() int }
	var coded statusCoder
	if errors.As(err, &coded) {
		return coded.StatusCode()
	}
	return 0
}

func chatGPTWebErrorRequestPath(err error) string {
	type pathReporter interface{ ChatGPTWebRequestPath() string }
	var reported pathReporter
	if errors.As(err, &reported) {
		return strings.TrimSpace(reported.ChatGPTWebRequestPath())
	}
	return ""
}
