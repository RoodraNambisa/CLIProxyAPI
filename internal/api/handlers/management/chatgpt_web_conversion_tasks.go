package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

var errChatGPTWebConversionSourceChanged = errors.New("source credential changed")

const chatGPTWebDependencyReservationRenewInterval = 5 * time.Minute

type chatGPTWebConversionRequest struct {
	Names          []string `json:"names"`
	TargetProvider string   `json:"target_provider"`
	Mode           string   `json:"mode"`
	Validate       *bool    `json:"validate"`
}

type chatGPTWebConversionInput struct {
	name     string
	validate bool
}

// StartChatGPTWebConversionTask starts a Codex-to-Web copy task.
func (h *Handler) StartChatGPTWebConversionTask(c *gin.Context) {
	executor, manager, errExecutor := h.chatGPTWebConversionExecutor()
	if errExecutor != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": errExecutor.Error()})
		return
	}
	inputs, errInput := readChatGPTWebConversionInputs(c)
	if errInput != nil {
		status := http.StatusBadRequest
		var maxBytesError *http.MaxBytesError
		if errors.As(errInput, &maxBytesError) || errors.Is(errInput, errChatGPTWebLoginTaskInputTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		c.JSON(status, gin.H{"error": errInput.Error()})
		return
	}
	results := make([]chatGPTWebMutationTaskResult, len(inputs))
	for index := range inputs {
		results[index] = chatGPTWebMutationTaskResult{SourceName: inputs[index].name, Status: chatGPTWebLoginResultQueued}
	}
	tasks := h.chatGPTWebMutationTaskManager()
	task, taskCtx, errCreate := tasks.create(chatGPTWebMutationTaskConversion, results)
	if errCreate != nil {
		status := http.StatusTooManyRequests
		if errors.Is(errCreate, errChatGPTWebLoginTaskClosed) {
			status = http.StatusServiceUnavailable
		}
		c.JSON(status, gin.H{"error": errCreate.Error()})
		return
	}
	taskCtx = PopulateAuthContext(taskCtx, c)
	go h.runChatGPTWebConversionTask(taskCtx, task.ID, inputs, executor, manager)
	c.JSON(http.StatusAccepted, task)
}

// GetChatGPTWebConversionTask returns one conversion task snapshot.
func (h *Handler) GetChatGPTWebConversionTask(c *gin.Context) {
	taskManager := h.chatGPTWebMutationTaskManager()
	if taskManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "chatgpt web conversion tasks are unavailable"})
		return
	}
	task, ok := taskManager.get(chatGPTWebMutationTaskConversion, c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "chatgpt web conversion task not found"})
		return
	}
	c.JSON(chatGPTWebMutationTaskHTTPStatus(task), task)
}

// CancelChatGPTWebConversionTask requests cancellation without deleting history.
func (h *Handler) CancelChatGPTWebConversionTask(c *gin.Context) {
	taskManager := h.chatGPTWebMutationTaskManager()
	if taskManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "chatgpt web conversion tasks are unavailable"})
		return
	}
	task, ok := taskManager.cancel(chatGPTWebMutationTaskConversion, c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "chatgpt web conversion task not found"})
		return
	}
	c.JSON(http.StatusOK, task)
}

func readChatGPTWebConversionInputs(c *gin.Context) ([]chatGPTWebConversionInput, error) {
	if c == nil || c.Request == nil {
		return nil, errors.New("conversion request is required")
	}
	payload, errRead := ioReadAllLimit(c.Request.Body, 1<<20)
	if errRead != nil {
		return nil, errRead
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var request chatGPTWebConversionRequest
	if errDecode := decoder.Decode(&request); errDecode != nil {
		return nil, errors.New("invalid conversion request")
	}
	var trailing any
	if errTrailing := decoder.Decode(&trailing); errTrailing != io.EOF {
		return nil, errors.New("invalid conversion request")
	}
	request.TargetProvider = strings.ToLower(strings.TrimSpace(request.TargetProvider))
	if request.TargetProvider == "" {
		request.TargetProvider = chatgptwebauth.Provider
	}
	if request.TargetProvider != chatgptwebauth.Provider {
		return nil, errors.New("target_provider must be chatgpt-web")
	}
	request.Mode = strings.ToLower(strings.TrimSpace(request.Mode))
	if request.Mode == "" {
		request.Mode = "copy"
	}
	if request.Mode != "copy" {
		return nil, errors.New("mode must be copy")
	}
	validate := true
	if request.Validate != nil {
		validate = *request.Validate
	}
	if !validate {
		return nil, errors.New("validate must be true")
	}
	seen := make(map[string]struct{}, len(request.Names))
	inputs := make([]chatGPTWebConversionInput, 0, len(request.Names))
	for _, name := range request.Names {
		name = strings.TrimSpace(name)
		if name == "" || isUnsafeAuthFileName(name) {
			return nil, errors.New("names contains an invalid auth file name")
		}
		key := managedAuthNameKey(name)
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		inputs = append(inputs, chatGPTWebConversionInput{name: name, validate: validate})
	}
	if len(inputs) == 0 {
		return nil, errors.New("at least one source name is required")
	}
	if len(inputs) > chatGPTWebImportMaxFiles {
		return nil, fmt.Errorf("at most %d source names are allowed", chatGPTWebImportMaxFiles)
	}
	return inputs, nil
}

func (h *Handler) runChatGPTWebConversionTask(ctx context.Context, taskID string, inputs []chatGPTWebConversionInput, executor chatGPTWebConversionExecutor, manager *coreauth.Manager) {
	tasks := h.chatGPTWebMutationTaskManager()
	if !tasks.start(chatGPTWebMutationTaskConversion, taskID) {
		tasks.finish(chatGPTWebMutationTaskConversion, taskID, true)
		return
	}
	jobs := make(chan int)
	var workers sync.WaitGroup
	workerCount := min(chatGPTWebMutationTaskWorkers, len(inputs))
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				if !tasks.acquireSlot(ctx) {
					tasks.setResult(chatGPTWebMutationTaskConversion, taskID, index, canceledChatGPTWebConversionResult(inputs[index]))
					continue
				}
				if !tasks.markRunning(chatGPTWebMutationTaskConversion, taskID, index) {
					tasks.releaseSlot()
					continue
				}
				result := h.executeChatGPTWebConversion(ctx, inputs[index], executor, manager, func() (context.Context, bool) {
					if !tasks.beginCommit(chatGPTWebMutationTaskConversion, taskID, index) {
						return nil, false
					}
					commitCtx := tasks.lifecycleContext()
					if requestInfo := coreauth.GetRequestInfo(ctx); requestInfo != nil {
						commitCtx = coreauth.WithRequestInfo(commitCtx, requestInfo)
					}
					return commitCtx, true
				})
				tasks.setResult(chatGPTWebMutationTaskConversion, taskID, index, result)
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
	tasks.finish(chatGPTWebMutationTaskConversion, taskID, ctx.Err() != nil)
}

func (h *Handler) executeChatGPTWebConversion(ctx context.Context, input chatGPTWebConversionInput, executor chatGPTWebConversionExecutor, manager *coreauth.Manager, beginCommit func() (context.Context, bool)) chatGPTWebMutationTaskResult {
	result := chatGPTWebMutationTaskResult{SourceName: input.name, Status: "failed"}
	source := h.findManagedFileAuth(input.name)
	if source == nil {
		if h.findManagedAuth(input.name) != nil {
			result.ErrorCategory = "source_not_managed"
			result.Error = "source credential is not backed by a managed auth file"
			result.HTTPStatus = http.StatusUnprocessableEntity
			return result
		}
		result.ErrorCategory = "source_not_found"
		result.Error = "source credential does not exist"
		result.HTTPStatus = http.StatusNotFound
		return result
	}
	if strings.TrimSpace(source.Attributes[coreauth.SourceHashAttributeKey]) == "" {
		result.ErrorCategory = "source_not_managed"
		result.Error = "source credential has no managed source generation"
		result.HTTPStatus = http.StatusUnprocessableEntity
		return result
	}
	if !strings.EqualFold(strings.TrimSpace(source.Provider), "codex") {
		result.ErrorCategory = "source_provider_invalid"
		result.Error = "source credential is not a Codex credential"
		result.HTTPStatus = http.StatusUnprocessableEntity
		return result
	}
	if strings.EqualFold(stringValue(source.Metadata, "deletion_state"), "retained_for_dependents") {
		result.ErrorCategory = "source_pending_deletion"
		result.Error = "source credential is pending deletion"
		result.HTTPStatus = http.StatusConflict
		return result
	}
	if chatGPTWebConversionSourceDisabled(source) {
		result.ErrorCategory = "source_disabled"
		result.Error = "source credential is disabled"
		result.HTTPStatus = http.StatusConflict
		return result
	}
	identitySource := source.Clone()
	identitySource.Provider = chatgptwebauth.Provider
	sourceIdentity := coreauth.ChatGPTWebCredentialReferenceValue(identitySource)
	if sourceIdentity == "" || !coreauth.ChatGPTWebCredentialHasStrongIdentity(identitySource) {
		result.ErrorCategory = "source_identity_missing"
		result.Error = "source credential does not contain a stable account or subject identity"
		result.HTTPStatus = http.StatusUnprocessableEntity
		return result
	}
	source, errUID := ensureCodexCredentialUID(ctx, manager, source, sourceIdentity)
	if errUID != nil {
		return failedChatGPTWebConversionSourceUpdate(result, errUID)
	}
	sourceUID := stringValue(source.Metadata, "credential_uid")
	identitySource = source.Clone()
	identitySource.Provider = chatgptwebauth.Provider
	sourceIdentity = coreauth.MergeChatGPTWebCredentialReferenceValues(sourceIdentity, coreauth.ChatGPTWebCredentialReferenceValue(identitySource))
	accessToken := stringValue(source.Metadata, "access_token")
	expired := stringValue(source.Metadata, "expired")
	if accessToken == "" {
		result.ErrorCategory = "source_token_missing"
		result.Error = "source credential has no access token"
		result.HTTPStatus = http.StatusUnprocessableEntity
		return result
	}
	if codexSourceTokenNeedsRefresh(accessToken, expired, time.Now()) {
		refreshed, errRefresh := manager.RefreshLinkedCodexSource(ctx, source.ID, sourceUID, accessToken, sourceIdentity)
		if errRefresh != nil {
			return failedChatGPTWebConversionRefresh(result, errRefresh)
		}
		latestSource, exists := manager.GetByID(source.ID)
		if !exists || latestSource == nil || coreauth.ChatGPTWebCredentialUID(latestSource) != sourceUID ||
			!codexSourceWebIdentityMatches(sourceIdentity, latestSource) {
			return changedChatGPTWebConversionSourceResult(result)
		}
		source = latestSource
		accessToken = strings.TrimSpace(refreshed.AccessToken)
		expired = strings.TrimSpace(refreshed.Expired)
		identitySource = source.Clone()
		identitySource.Provider = chatgptwebauth.Provider
		sourceIdentity = coreauth.MergeChatGPTWebCredentialReferenceValues(sourceIdentity, refreshed.Identity)
	}
	releaseSourceRefresh, errSourceRefreshLock := manager.LockCredentialRefresh(ctx, source.ID)
	if errSourceRefreshLock != nil {
		return failedChatGPTWebConversionRefresh(result, errSourceRefreshLock)
	}
	defer releaseSourceRefresh()
	latestSource, sourceExists := manager.GetByID(source.ID)
	if !sourceExists || latestSource == nil || !strings.EqualFold(strings.TrimSpace(latestSource.Provider), "codex") ||
		coreauth.ChatGPTWebCredentialUID(latestSource) != sourceUID || coreauth.ChatGPTWebAuthRetainedForDependents(latestSource) ||
		chatGPTWebConversionSourceDisabled(latestSource) || !codexSourceWebIdentityMatches(sourceIdentity, latestSource) {
		return changedChatGPTWebConversionSourceResult(result)
	}
	source = latestSource
	accessToken = stringValue(source.Metadata, "access_token")
	expired = stringValue(source.Metadata, "expired")
	if accessToken == "" {
		result.ErrorCategory = "source_token_missing"
		result.Error = "source credential has no access token"
		result.HTTPStatus = http.StatusUnprocessableEntity
		return result
	}
	identitySource = source.Clone()
	identitySource.Provider = chatgptwebauth.Provider
	sourceIdentity = coreauth.MergeChatGPTWebCredentialReferenceValues(sourceIdentity, coreauth.ChatGPTWebCredentialReferenceValue(identitySource))
	email := chatgptwebauth.NormalizeEmail(stringValue(source.Metadata, "email"))
	if email == "" {
		temporary := &chatgptwebauth.Credential{AccessToken: accessToken}
		chatgptwebauth.PopulateCredentialIdentity(temporary)
		email = chatgptwebauth.NormalizeEmail(temporary.Email)
	}
	if email == "" {
		result.ErrorCategory = "source_identity_missing"
		result.Error = "source credential does not identify an email account"
		result.HTTPStatus = http.StatusUnprocessableEntity
		return result
	}
	result.Email = email
	operationCtx, releaseOperation, errOperation := executor.BeginLoginOperation(ctx, email)
	if errOperation != nil {
		return failedChatGPTWebMutationResult(result, errOperation)
	}
	defer releaseOperation()
	ctx = operationCtx
	fileName := chatGPTWebCredentialFileName(email)
	linkedExisting, errLinkedExisting := findChatGPTWebAuthBySourceUID(ctx, manager, sourceUID)
	if errLinkedExisting != nil {
		if errors.Is(errLinkedExisting, errChatGPTWebCredentialMultiple) {
			result.ErrorCategory = "target_conflict"
			result.Error = "multiple linked Web credentials use the same Codex source"
			result.HTTPStatus = http.StatusConflict
			return result
		}
		return failedChatGPTWebMutationResult(result, errLinkedExisting)
	}
	emailExisting, errExisting := findExistingChatGPTWebAuth(ctx, manager, fileName, email)
	if errExisting != nil {
		if errors.Is(errExisting, errChatGPTWebCredentialIDOwned) || errors.Is(errExisting, errChatGPTWebCredentialMultiple) {
			result.ErrorCategory = "target_conflict"
			result.Error = "target credential conflicts with an existing account"
			result.HTTPStatus = http.StatusConflict
			return result
		}
		return failedChatGPTWebMutationResult(result, errExisting)
	}
	if linkedExisting != nil && emailExisting != nil && linkedExisting.ID != emailExisting.ID {
		result.ErrorCategory = "target_conflict"
		result.Error = "Codex source and Web account resolve to different credentials"
		result.HTTPStatus = http.StatusConflict
		return result
	}
	existing := linkedExisting
	if existing == nil {
		existing = emailExisting
	} else if strings.TrimSpace(existing.FileName) != "" {
		fileName = existing.FileName
	}
	targetEmail := email
	if existing != nil {
		current, errParse := chatgptwebauth.ParseCredential(existing.Metadata)
		if errParse != nil || current.RefreshStrategy != chatgptwebauth.RefreshStrategyCodexSource || current.SourceCredentialUID != sourceUID ||
			current.SourceIdentity != "" && !coreauth.ChatGPTWebCredentialReferenceMatches(current.SourceIdentity, identitySource) ||
			current.SourceIdentity == "" && (current.SourceAuthID != source.ID || chatgptwebauth.NormalizeEmail(current.Email) != email) {
			result.ErrorCategory = "target_conflict"
			result.Error = "a native or differently linked Web credential already exists"
			result.HTTPStatus = http.StatusConflict
			return result
		}
		sourceIdentity = coreauth.MergeChatGPTWebCredentialReferenceValues(current.SourceIdentity, sourceIdentity)
		if currentEmail := chatgptwebauth.NormalizeEmail(current.Email); currentEmail != "" {
			targetEmail = currentEmail
		}
	}
	credential := &chatgptwebauth.Credential{
		Type:                chatgptwebauth.Provider,
		CredentialUID:       uuid.NewString(),
		CredentialMode:      chatgptwebauth.CredentialModeLinkedCodex,
		RefreshStrategy:     chatgptwebauth.RefreshStrategyCodexSource,
		SourceAuthID:        source.ID,
		SourceCredentialUID: sourceUID,
		SourceIdentity:      sourceIdentity,
		Email:               targetEmail,
		AccessToken:         accessToken,
		Expired:             expired,
		Cookies:             []chatgptwebauth.Cookie{},
		Persona:             chatgptwebauth.DefaultPersona(),
		LifecycleState:      chatgptwebauth.LifecycleActive,
		LifecycleUpdatedAt:  time.Now().UTC().Format(time.RFC3339),
	}
	if existing != nil {
		if current, errParse := chatgptwebauth.ParseCredential(existing.Metadata); errParse == nil {
			credential.CredentialUID = current.CredentialUID
			credential.Persona = current.Persona
			credential.DeviceID = current.DeviceID
			credential.SessionID = current.SessionID
			credential.Cookies = current.Cookies
		}
	}
	if errIdentity := chatgptwebauth.EnsureCredentialRuntimeIDs(credential, chatgptwebauth.CredentialRuntimeIdentityReader(fileName, credential)); errIdentity != nil {
		result.ErrorCategory = "target_initialization_failed"
		result.Error = "failed to initialize Web credential identity"
		result.HTTPStatus = http.StatusInternalServerError
		return result
	}
	releaseProxyBinding := manager.HoldProxyBinding(source.ID)
	defer releaseProxyBinding()
	resolvedSource, errResolve := manager.ResolveProxyAuth(ctx, source)
	if errResolve != nil {
		return failedChatGPTWebMutationResult(result, errResolve)
	}
	credential.SourceProxyURL = resolvedSource.EffectiveProxyURL()
	probe := &coreauth.Auth{ID: fileName, Provider: chatgptwebauth.Provider, FileName: fileName, ProxyURL: resolvedSource.EffectiveProxyURL(), Metadata: make(map[string]any)}
	credential.ApplyToMetadata(probe.Metadata)
	if input.validate {
		if _, errProbe := executor.FetchModels(ctx, probe); errProbe != nil {
			if resolvedSource.EffectiveProxyBindingID() != "" {
				errProbe = manager.ReportProxyFailure(ctx, resolvedSource, errProbe)
			}
			return failedChatGPTWebProbeResult(result, errProbe)
		}
	}
	sourceGeneration := strings.TrimSpace(source.Attributes[coreauth.SourceHashAttributeKey])
	probeProxyURL := resolvedSource.EffectiveProxyURL()
	probeProxyBindingID := resolvedSource.EffectiveProxyBindingID()
	probeProxyAuthID := resolvedSource.EffectiveProxyAuthID()
	if errContext := ctx.Err(); errContext != nil {
		return canceledChatGPTWebConversionResult(input)
	}
	if beginCommit == nil {
		result.ErrorCategory = "persist_failed"
		result.Error = "credential persistence is unavailable"
		result.HTTPStatus = http.StatusInternalServerError
		return result
	}
	commitCtx, allowed := beginCommit()
	if !allowed {
		return canceledChatGPTWebConversionResult(input)
	}
	h.chatGPTWebDependencyMu.Lock()
	defer h.chatGPTWebDependencyMu.Unlock()
	var installed *coreauth.Auth
	errMutation := manager.WithChatGPTWebDependencyMutation(commitCtx, func(lockedCtx context.Context) error {
		currentSource, sourceExists := manager.GetByID(source.ID)
		currentIdentitySource := currentSource
		if currentIdentitySource != nil {
			currentIdentitySource = currentIdentitySource.Clone()
			currentIdentitySource.Provider = chatgptwebauth.Provider
		}
		if !sourceExists || currentSource == nil || !strings.EqualFold(strings.TrimSpace(currentSource.Provider), "codex") ||
			coreauth.ChatGPTWebCredentialUID(currentSource) != sourceUID || coreauth.ChatGPTWebAuthRetainedForDependents(currentSource) ||
			chatGPTWebConversionSourceDisabled(currentSource) ||
			!coreauth.ChatGPTWebCredentialReferenceMatches(sourceIdentity, currentIdentitySource) ||
			stringValue(currentSource.Metadata, "access_token") != credential.AccessToken ||
			sourceGeneration != "" && strings.TrimSpace(currentSource.Attributes[coreauth.SourceHashAttributeKey]) != sourceGeneration {
			return errChatGPTWebConversionSourceChanged
		}
		persistedAuths, errPersisted := manager.PersistedAuthSnapshot(lockedCtx)
		if errPersisted != nil {
			return errPersisted
		}
		var persistedSource *coreauth.Auth
		for _, candidate := range persistedAuths {
			if candidate != nil && candidate.ID == source.ID {
				persistedSource = candidate
				break
			}
		}
		persistedIdentitySource := persistedSource
		if persistedIdentitySource != nil {
			persistedIdentitySource = persistedIdentitySource.Clone()
			persistedIdentitySource.Provider = chatgptwebauth.Provider
		}
		if persistedSource == nil || !strings.EqualFold(strings.TrimSpace(persistedSource.Provider), "codex") ||
			coreauth.ChatGPTWebCredentialUID(persistedSource) != sourceUID || coreauth.ChatGPTWebAuthRetainedForDependents(persistedSource) ||
			chatGPTWebConversionSourceDisabled(persistedSource) || !coreauth.ChatGPTWebCredentialReferenceMatches(sourceIdentity, persistedIdentitySource) ||
			stringValue(persistedSource.Metadata, "access_token") != credential.AccessToken ||
			sourceGeneration != "" && strings.TrimSpace(persistedSource.Attributes[coreauth.SourceHashAttributeKey]) != sourceGeneration {
			return errChatGPTWebConversionSourceChanged
		}
		currentResolvedSource, errCurrentResolve := manager.ResolveProxyAuth(lockedCtx, currentSource)
		if errCurrentResolve != nil || currentResolvedSource == nil ||
			currentResolvedSource.EffectiveProxyURL() != probeProxyURL ||
			currentResolvedSource.EffectiveProxyBindingID() != probeProxyBindingID ||
			currentResolvedSource.EffectiveProxyAuthID() != probeProxyAuthID {
			return errChatGPTWebConversionSourceChanged
		}
		targetID := fileName
		if existing != nil && strings.TrimSpace(existing.ID) != "" {
			targetID = existing.ID
		}
		reservedSource, reservation, errReserve := manager.ReserveChatGPTWebDependent(
			lockedCtx,
			currentSource,
			targetID,
			credential.CredentialUID,
			time.Now(),
		)
		if errReserve != nil {
			if errors.Is(errReserve, authfileguard.ErrPersistGenerationStale) {
				return errChatGPTWebConversionSourceChanged
			}
			if outcome, explicit := coreauth.SaveOutcomeFromError(errReserve); explicit && outcome == coreauth.SaveOutcomeRolledBack {
				return errChatGPTWebConversionSourceChanged
			}
			return errReserve
		}
		source = reservedSource
		stopReservationLease := startChatGPTWebDependencyReservationLease(
			lockedCtx,
			manager,
			reservedSource.ID,
			sourceUID,
			reservation,
			chatGPTWebDependencyReservationRenewInterval,
		)
		var errPersist error
		installed, errPersist = h.persistConvertedChatGPTWebCredential(lockedCtx, manager, credential, source, existing)
		errLease := stopReservationLease()
		if errPersist != nil {
			outcome, explicit := coreauth.SaveOutcomeFromError(errPersist)
			knownNoWrite := errors.Is(errPersist, errChatGPTWebCredentialChanged) || errors.Is(errPersist, coreauth.ErrAuthAlreadyExists) || errors.Is(errPersist, coreauth.ErrChatGPTWebEmailAlreadyExists)
			if explicit && outcome == coreauth.SaveOutcomeRolledBack || knownNoWrite {
				if errRelease := manager.ReleaseChatGPTWebDependentReservation(lockedCtx, reservedSource.ID, sourceUID, reservation, time.Now()); errRelease != nil {
					log.WithError(errRelease).WithField("source_auth_id", reservedSource.ID).Warn("failed to release rolled-back ChatGPT Web conversion reservation")
				}
			}
			return errPersist
		}
		if errLease != nil {
			log.WithError(errLease).WithFields(log.Fields{
				"source_auth_id": reservedSource.ID,
				"target_auth_id": reservation.AuthID,
			}).Warn("ChatGPT Web conversion dependency reservation renewal stopped")
		}
		persistedAuths, errVerify := manager.PersistedAuthSnapshot(lockedCtx)
		if errVerify != nil {
			return fmt.Errorf("verify linked Web credential persistence: %w", errVerify)
		}
		persistedTarget := false
		for _, candidate := range persistedAuths {
			if candidate != nil && candidate.ID == installed.ID &&
				coreauth.ChatGPTWebLinkedSourceUID(candidate) == sourceUID &&
				coreauth.ChatGPTWebCredentialUID(candidate) == credential.CredentialUID {
				persistedTarget = true
				break
			}
		}
		if !persistedTarget {
			return coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeUncertain, errors.New("linked Web credential is not visible in the persisted auth snapshot"))
		}
		if errFinalize := manager.FinalizeChatGPTWebDependentReservation(lockedCtx, reservedSource.ID, sourceUID, reservation, time.Now()); errFinalize != nil {
			errRollback := rollbackConvertedChatGPTWebCredential(lockedCtx, manager, installed, existing)
			if errRollback != nil {
				log.WithError(errRollback).WithFields(log.Fields{
					"source_auth_id": reservedSource.ID,
					"target_auth_id": installed.ID,
				}).Error("failed to roll back linked Web credential after reservation finalization failure")
				return coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeUncertain, errors.Join(errFinalize, errRollback))
			}
			return errors.Join(errChatGPTWebConversionSourceChanged, errFinalize)
		}
		return nil
	})
	if errors.Is(errMutation, errChatGPTWebConversionSourceChanged) {
		return changedChatGPTWebConversionSourceResult(result)
	}
	if errors.Is(errMutation, errChatGPTWebCredentialChanged) || errors.Is(errMutation, coreauth.ErrAuthAlreadyExists) || errors.Is(errMutation, coreauth.ErrChatGPTWebEmailAlreadyExists) {
		result.ErrorCategory = "target_conflict"
		result.Error = "target Web credential changed while conversion was running"
		result.HTTPStatus = http.StatusConflict
		return result
	}
	if errMutation != nil {
		return failedChatGPTWebPersistenceResult(result, errMutation, "failed to save linked Web credential")
	}
	result.Status = "created"
	if existing != nil {
		result.Status = "updated"
	}
	result.Name = installed.FileName
	result.TargetName = installed.FileName
	result.AuthIndex = installed.EnsureIndex()
	result.CredentialMode = chatgptwebauth.CredentialModeLinkedCodex
	return result
}

func startChatGPTWebDependencyReservationLease(ctx context.Context, manager *coreauth.Manager, sourceID, sourceUID string, reservation coreauth.ChatGPTWebDependencyReservation, interval time.Duration) func() error {
	if interval <= 0 {
		interval = chatGPTWebDependencyReservationRenewInterval
	}
	leaseCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		current := reservation
		for {
			select {
			case <-leaseCtx.Done():
				done <- nil
				return
			case now := <-ticker.C:
				_, renewed, errRenew := manager.RenewChatGPTWebDependentReservation(leaseCtx, sourceID, sourceUID, current, now)
				if errRenew != nil {
					done <- errRenew
					return
				}
				current = renewed
			}
		}
	}()
	var (
		stopOnce sync.Once
		stopErr  error
	)
	return func() error {
		stopOnce.Do(func() {
			cancel()
			stopErr = <-done
		})
		return stopErr
	}
}

func rollbackConvertedChatGPTWebCredential(ctx context.Context, manager *coreauth.Manager, installed, previous *coreauth.Auth) error {
	if manager == nil || installed == nil {
		return errors.New("linked Web credential rollback is unavailable")
	}
	if previous == nil {
		deleted, errDelete := manager.DeleteIfCurrentSourceHash(ctx, installed)
		if errDelete != nil {
			return errDelete
		}
		if !deleted {
			return errChatGPTWebCredentialChanged
		}
		return nil
	}
	var (
		restored *coreauth.Auth
		current  bool
		err      error
	)
	if _, runtimeExists := manager.GetByID(installed.ID); runtimeExists {
		restored, current, err = manager.UpdateIfCurrentSourceHash(ctx, installed, previous)
	} else {
		restored, current, err = manager.UpdatePersistedIfCurrentSourceHash(ctx, installed, previous)
	}
	if err != nil {
		return err
	}
	if !current || restored == nil {
		return errChatGPTWebCredentialChanged
	}
	return nil
}

func chatGPTWebConversionSourceDisabled(source *coreauth.Auth) bool {
	if source == nil {
		return false
	}
	disabled, _ := source.Metadata["disabled"].(bool)
	return source.Disabled || source.Status == coreauth.StatusDisabled || disabled
}

func ensureCodexCredentialUID(ctx context.Context, manager *coreauth.Manager, source *coreauth.Auth, expectedIdentity string) (*coreauth.Auth, error) {
	if source == nil || manager == nil {
		return nil, errors.New("source credential is unavailable")
	}
	releaseRefresh, errRefreshLock := manager.LockCredentialRefreshes(ctx, []string{source.ID})
	if errRefreshLock != nil {
		return nil, errRefreshLock
	}
	defer releaseRefresh()
	latest, exists := manager.GetByID(source.ID)
	if !exists || latest == nil || !strings.EqualFold(strings.TrimSpace(latest.Provider), "codex") ||
		!codexSourceWebIdentityMatches(expectedIdentity, latest) {
		return nil, errChatGPTWebConversionSourceChanged
	}
	if stringValue(latest.Metadata, "credential_uid") != "" {
		return latest, nil
	}
	updated := latest.Clone()
	if updated.Metadata == nil {
		updated.Metadata = make(map[string]any)
	}
	updated.Metadata["credential_uid"] = uuid.NewString()
	installed, current, errUpdate := manager.UpdateIfCurrentSourceHash(ctx, latest, updated)
	if errUpdate != nil {
		return nil, errUpdate
	}
	if current && installed != nil {
		return installed, nil
	}
	latest, exists = manager.GetByID(source.ID)
	if !exists || latest == nil || !strings.EqualFold(strings.TrimSpace(latest.Provider), "codex") ||
		coreauth.ChatGPTWebCredentialUID(latest) == "" || expectedIdentity == "" ||
		!codexSourceWebIdentityMatches(expectedIdentity, latest) {
		return nil, errChatGPTWebConversionSourceChanged
	}
	return latest, nil
}

func findChatGPTWebAuthBySourceUID(ctx context.Context, manager *coreauth.Manager, sourceUID string) (*coreauth.Auth, error) {
	if manager == nil || strings.TrimSpace(sourceUID) == "" {
		return nil, nil
	}
	var match *coreauth.Auth
	auths, errList := manager.CompleteAuthSnapshot(ctx)
	if errList != nil {
		return nil, fmt.Errorf("%w: %w", errChatGPTWebCredentialLookup, errList)
	}
	for _, candidate := range auths {
		if candidate == nil || !strings.EqualFold(strings.TrimSpace(candidate.Provider), chatgptwebauth.Provider) ||
			coreauth.ChatGPTWebLinkedSourceUID(candidate) != sourceUID {
			continue
		}
		if match != nil && match.ID != candidate.ID {
			return nil, errChatGPTWebCredentialMultiple
		}
		match = candidate
	}
	return match, nil
}

func codexSourceWebIdentity(source *coreauth.Auth) string {
	if source == nil {
		return ""
	}
	candidate := source.Clone()
	candidate.Provider = chatgptwebauth.Provider
	return coreauth.ChatGPTWebCredentialReferenceValue(candidate)
}

func codexSourceWebIdentityMatches(reference string, source *coreauth.Auth) bool {
	if source == nil {
		return false
	}
	candidate := source.Clone()
	candidate.Provider = chatgptwebauth.Provider
	return coreauth.ChatGPTWebCredentialReferenceMatches(reference, candidate)
}

func codexSourceTokenNeedsRefresh(accessToken, expired string, now time.Time) bool {
	if expiresAt, ok := chatgptwebauth.JWTExpiry(accessToken); ok {
		return !expiresAt.After(now.Add(chatgptwebauth.DefaultRefreshLead))
	}
	if expiresAt, errParse := time.Parse(time.RFC3339, strings.TrimSpace(expired)); errParse == nil {
		return !expiresAt.After(now.Add(chatgptwebauth.DefaultRefreshLead))
	}
	return false
}

func (h *Handler) persistConvertedChatGPTWebCredential(ctx context.Context, manager *coreauth.Manager, credential *chatgptwebauth.Credential, source, existing *coreauth.Auth) (*coreauth.Auth, error) {
	if credential == nil || manager == nil || source == nil {
		return nil, errors.New("linked credential persistence is unavailable")
	}
	now := time.Now().UTC()
	metadata := make(map[string]any)
	attributes := make(map[string]string)
	if existing != nil {
		metadata = cloneStringAnyMap(existing.Metadata)
		for key, value := range existing.Attributes {
			attributes[key] = value
		}
	}
	credential.ApplyToMetadata(metadata)
	delete(metadata, "priority")
	delete(metadata, "note")
	delete(attributes, "priority")
	delete(attributes, "note")
	for _, key := range []string{"priority", "note"} {
		value := strings.TrimSpace(source.Attributes[key])
		if value == "" {
			if key == "note" {
				value = stringValue(source.Metadata, key)
			} else if rawPriority, ok := source.Metadata[key]; ok {
				value = strings.TrimSpace(fmt.Sprint(rawPriority))
			}
		}
		if value != "" {
			attributes[key] = value
			if key == "priority" {
				if priority, errParse := strconv.Atoi(value); errParse == nil {
					metadata[key] = priority
				}
			} else {
				metadata[key] = value
			}
		}
	}
	if existing != nil {
		updated := existing.Clone()
		updated.Metadata = metadata
		updated.Attributes = attributes
		updated.Label = credential.Email
		updated.UpdatedAt = now
		var (
			installed *coreauth.Auth
			current   bool
			errUpdate error
		)
		if _, runtimeExists := manager.GetByID(existing.ID); runtimeExists {
			installed, current, errUpdate = manager.UpdateIfCurrentSourceHash(ctx, existing, updated)
		} else {
			installed, current, errUpdate = manager.UpdatePersistedIfCurrentSourceHash(ctx, existing, updated)
		}
		if errUpdate != nil {
			if outcome, explicit := coreauth.SaveOutcomeFromError(errUpdate); explicit && outcome == coreauth.SaveOutcomeRolledBack {
				return nil, coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeRolledBack, errChatGPTWebCredentialChanged)
			}
			return nil, errUpdate
		}
		if !current {
			return nil, coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeRolledBack, errChatGPTWebCredentialChanged)
		}
		return installed, nil
	}
	record := &coreauth.Auth{
		ID:         chatGPTWebCredentialFileName(credential.Email),
		Provider:   chatgptwebauth.Provider,
		FileName:   chatGPTWebCredentialFileName(credential.Email),
		Label:      credential.Email,
		Metadata:   metadata,
		Attributes: attributes,
		Status:     coreauth.StatusActive,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if h.postAuthHook != nil {
		if errHook := h.postAuthHook(ctx, record); errHook != nil {
			return nil, errHook
		}
	}
	return manager.RegisterIfAbsent(ctx, record)
}

func failedChatGPTWebConversionRefresh(result chatGPTWebMutationTaskResult, err error) chatGPTWebMutationTaskResult {
	if errors.Is(err, context.Canceled) {
		return canceledChatGPTWebMutationResult(result)
	}
	var coded interface{ ChatGPTWebErrorCode() string }
	if errors.As(err, &coded) {
		category, _, status, _ := classifyChatGPTWebManagementError(err)
		result.ErrorCategory = category
		result.Error = "linked Codex credential cannot refresh this Web credential"
		result.HTTPStatus = status
		return result
	}
	result.ErrorCategory = "source_refresh_unavailable"
	result.Error = "Codex source refresh is temporarily unavailable"
	result.HTTPStatus = http.StatusServiceUnavailable
	return result
}

func failedChatGPTWebConversionSourceUpdate(result chatGPTWebMutationTaskResult, err error) chatGPTWebMutationTaskResult {
	outcome, explicit := coreauth.SaveOutcomeFromError(err)
	if explicit && (outcome == coreauth.SaveOutcomeUncertain || outcome == coreauth.SaveOutcomeCommitted) {
		result.ErrorCategory = "source_persist_uncertain"
		result.Error = "source credential persistence outcome is uncertain"
		result.HTTPStatus = http.StatusServiceUnavailable
		return result
	}
	if errors.Is(err, context.Canceled) {
		return canceledChatGPTWebMutationResult(result)
	}
	if errors.Is(err, errChatGPTWebConversionSourceChanged) || errors.Is(err, authfileguard.ErrPersistGenerationStale) {
		return changedChatGPTWebConversionSourceResult(result)
	}
	result.ErrorCategory = "source_persist_failed"
	result.Error = "failed to persist source credential identity"
	result.HTTPStatus = http.StatusServiceUnavailable
	return result
}

func changedChatGPTWebConversionSourceResult(result chatGPTWebMutationTaskResult) chatGPTWebMutationTaskResult {
	result.ErrorCategory = "source_changed"
	result.Error = "source credential changed during conversion"
	result.HTTPStatus = http.StatusConflict
	return result
}

func canceledChatGPTWebConversionResult(input chatGPTWebConversionInput) chatGPTWebMutationTaskResult {
	return chatGPTWebMutationTaskResult{SourceName: input.name, Status: chatGPTWebLoginResultCanceled, ErrorCategory: "canceled", Error: "conversion canceled"}
}

func ioReadAllLimit(reader io.Reader, limit int64) ([]byte, error) {
	payload, errRead := io.ReadAll(io.LimitReader(reader, limit+1))
	if errRead != nil {
		return nil, errRead
	}
	if int64(len(payload)) > limit {
		return nil, errChatGPTWebLoginTaskInputTooLarge
	}
	return payload, nil
}
