package management

import (
	"context"
	"crypto/sha256"
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
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const (
	chatGPTWebImportMaxRequestBytes = 32 << 20
	chatGPTWebImportMaxFileBytes    = 1 << 20
	chatGPTWebImportMaxFiles        = 500
)

type chatGPTWebImportInput struct {
	file string
	data []byte
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
		results[index] = chatGPTWebMutationTaskResult{File: inputs[index].file, Status: chatGPTWebLoginResultQueued}
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
	workerCount := min(chatGPTWebMutationTaskWorkers, len(inputs))
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				if !tasks.acquireSlot(ctx) {
					tasks.setResult(chatGPTWebMutationTaskImport, taskID, index, canceledChatGPTWebImportResult(inputs[index]))
					continue
				}
				if !tasks.markRunning(chatGPTWebMutationTaskImport, taskID, index) {
					tasks.releaseSlot()
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
				tasks.setResult(chatGPTWebMutationTaskImport, taskID, index, result)
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
	tasks.finish(chatGPTWebMutationTaskImport, taskID, ctx.Err() != nil)
}

func (h *Handler) executeChatGPTWebImport(ctx context.Context, input chatGPTWebImportInput, executor chatGPTWebImportExecutor, manager *coreauth.Manager, beginCommit func() (context.Context, bool)) chatGPTWebMutationTaskResult {
	result := chatGPTWebMutationTaskResult{File: input.file, Status: "failed"}
	credential, errDecode := chatgptwebauth.DecodeImportCredential(input.data)
	if errDecode != nil {
		result.ErrorCategory = "invalid_credential"
		result.Error = "credential JSON is invalid or unsupported"
		result.HTTPStatus = http.StatusBadRequest
		return result
	}
	protectedRefresh := credential.RefreshStrategy == chatgptwebauth.RefreshStrategyWebOAuthRT ||
		credential.RefreshStrategy == chatgptwebauth.RefreshStrategyChatGPTSession
	commitStarted := false
	var commitCtx context.Context
	lockedEmail := chatgptwebauth.NormalizeEmail(credential.Email)
	baseCtx := ctx
	if protectedRefresh {
		releaseRefresh, errRefreshLock := manager.LockCredentialRefreshes(baseCtx, chatGPTWebImportRefreshLockIDs(manager, lockedEmail))
		if errRefreshLock != nil {
			return failedChatGPTWebMutationResult(result, errRefreshLock)
		}
		defer releaseRefresh()
	}
	operationCtx, releaseOperation, errOperation := executor.BeginLoginOperation(baseCtx, chatGPTWebImportOperationKey(credential))
	if errOperation != nil {
		return failedChatGPTWebMutationResult(result, errOperation)
	}
	operationReleased := false
	defer func() {
		if !operationReleased {
			releaseOperation()
		}
	}()
	ctx = operationCtx
	pending := chatGPTWebImportProbeAuth("chatgpt-web-import-"+uuid.NewString(), credential)
	var earlyExisting *coreauth.Auth
	if lockedEmail != "" {
		fileName := chatGPTWebCredentialFileName(lockedEmail)
		var errExisting error
		earlyExisting, errExisting = findExistingChatGPTWebAuth(ctx, manager, fileName, lockedEmail)
		if errExisting != nil {
			if errors.Is(errExisting, errChatGPTWebCredentialIDOwned) || errors.Is(errExisting, errChatGPTWebCredentialMultiple) {
				result.ErrorCategory = "identity_conflict"
				result.Error = "credential conflicts with an existing account"
				result.HTTPStatus = http.StatusConflict
				return result
			}
			return failedChatGPTWebMutationResult(result, errExisting)
		}
		if earlyExisting != nil {
			pending = earlyExisting
		} else {
			pending.ID = fileName
			pending.FileName = fileName
		}
	}
	releaseProxyBinding := manager.HoldProxyBinding(pending.ID)
	defer releaseProxyBinding()
	resolved, errResolve := manager.ResolveProxyAuth(ctx, pending)
	if errResolve != nil {
		return failedChatGPTWebMutationResult(result, errResolve)
	}
	if protectedRefresh {
		if beginCommit == nil {
			result.ErrorCategory = "persist_failed"
			result.Error = "credential persistence is unavailable"
			result.HTTPStatus = http.StatusInternalServerError
			return result
		}
		var allowed bool
		commitCtx, allowed = beginCommit()
		if !allowed {
			return canceledChatGPTWebImportResult(input)
		}
		commitStarted = true
		ctx = commitCtx
	}
	credential, errNormalize := executor.NormalizeImportedCredential(ctx, credential, resolved.EffectiveProxyURL())
	if errNormalize != nil {
		if resolved.EffectiveProxyBindingID() != "" {
			errNormalize = manager.ReportProxyFailure(ctx, resolved, errNormalize)
		}
		return failedChatGPTWebMutationResult(result, errNormalize)
	}
	credential.Email = chatgptwebauth.NormalizeEmail(credential.Email)
	if credential.Email == "" {
		result.ErrorCategory = "identity_missing"
		result.Error = "credential does not identify an email account"
		result.HTTPStatus = http.StatusUnprocessableEntity
		return result
	}
	result.Email = credential.Email
	if protectedRefresh && earlyExisting != nil && lockedEmail != credential.Email {
		result.ErrorCategory = "identity_conflict"
		result.Error = "credential identity changed while refreshing an existing account"
		result.HTTPStatus = http.StatusConflict
		return result
	}
	if lockedEmail != credential.Email {
		releaseOperation()
		operationReleased = true
		finalOperationBaseCtx := baseCtx
		if commitStarted {
			finalOperationBaseCtx = commitCtx
		}
		finalOperationCtx, releaseFinalOperation, errFinalOperation := executor.BeginLoginOperation(finalOperationBaseCtx, credential.Email)
		if errFinalOperation != nil {
			return failedChatGPTWebMutationResult(result, errFinalOperation)
		}
		defer releaseFinalOperation()
		ctx = finalOperationCtx
	}
	fileName := chatGPTWebCredentialFileName(credential.Email)
	existing, errExisting := findExistingChatGPTWebAuth(ctx, manager, fileName, credential.Email)
	if errExisting != nil {
		if errors.Is(errExisting, errChatGPTWebCredentialIDOwned) || errors.Is(errExisting, errChatGPTWebCredentialMultiple) {
			result.ErrorCategory = "identity_conflict"
			result.Error = "credential conflicts with an existing account"
			result.HTTPStatus = http.StatusConflict
			return result
		}
		return failedChatGPTWebMutationResult(result, errExisting)
	}
	finalProxyAuth := chatGPTWebImportProbeAuth(fileName, credential)
	if existing != nil {
		finalProxyAuth = existing.Clone()
		if finalProxyAuth.Metadata == nil {
			finalProxyAuth.Metadata = make(map[string]any)
		}
		credential.ApplyToMetadata(finalProxyAuth.Metadata)
	}
	if !protectedRefresh && finalProxyAuth.ID != pending.ID {
		releaseFinalBinding := manager.HoldProxyBinding(finalProxyAuth.ID)
		defer releaseFinalBinding()
		finalResolved, errFinalResolve := manager.ResolveProxyAuth(ctx, finalProxyAuth)
		if errFinalResolve != nil {
			return failedChatGPTWebMutationResult(result, errFinalResolve)
		}
		resolved = finalResolved
	}
	probe := chatGPTWebImportProbeAuth(fileName, credential)
	probe.ProxyURL = resolved.EffectiveProxyURL()
	if existing != nil {
		candidate := existing.Clone()
		candidate.Metadata = cloneStringAnyMap(existing.Metadata)
		credential.ApplyToMetadata(candidate.Metadata)
		identityChanged := coreauth.ChatGPTWebCredentialIdentityChanged(existing, candidate)
		if protectedRefresh {
			identityChanged = coreauth.ChatGPTWebCredentialRefreshIdentityChanged(existing, candidate)
		}
		if identityChanged {
			result.ErrorCategory = "identity_conflict"
			result.Error = "credential identity conflicts with the existing account"
			result.HTTPStatus = http.StatusConflict
			return result
		}
	}
	var (
		installed *coreauth.Auth
		status    string
		unchanged bool
	)
	_, errProbe := executor.FetchModels(ctx, probe)
	if errProbe != nil {
		if resolved.EffectiveProxyBindingID() != "" {
			errProbe = manager.ReportProxyFailure(ctx, resolved, errProbe)
		}
		if !protectedRefresh {
			return failedChatGPTWebProbeResult(result, errProbe)
		}
		if chatGPTWebProbeRejectsCredential(errProbe) {
			credential.LifecycleState = chatgptwebauth.LifecycleReauthRequired
			credential.LifecycleReason = "reauth_required"
			credential.LifecycleUpdatedAt = time.Now().UTC().Format(time.RFC3339)
		}
	}
	if errContext := ctx.Err(); errContext != nil {
		return canceledChatGPTWebImportResult(input)
	}
	if protectedRefresh {
		var errPersist error
		installed, status, unchanged, errPersist = h.persistImportedChatGPTWebCredential(ctx, manager, fileName, credential, existing, true)
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
		result.Name = installed.FileName
		result.AuthIndex = installed.EnsureIndex()
		result.CredentialMode = credential.CredentialMode
		if errProbe != nil {
			return failedChatGPTWebProbeResult(result, errProbe)
		}
		if unchanged {
			result.Status = "unchanged"
		} else {
			result.Status = status
		}
		return result
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
	commitStarted = true
	ctx = commitCtx
	var errPersist error
	installed, status, unchanged, errPersist = h.persistImportedChatGPTWebCredential(ctx, manager, fileName, credential, existing, false)
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
	return result
}

func (h *Handler) persistImportedChatGPTWebCredential(ctx context.Context, manager *coreauth.Manager, fileName string, credential *chatgptwebauth.Credential, expected *coreauth.Auth, refreshAware bool) (installed *coreauth.Auth, status string, unchanged bool, err error) {
	if h == nil || manager == nil || credential == nil {
		return nil, "", false, errors.New("credential persistence is unavailable")
	}
	h.chatGPTWebDependencyMu.Lock()
	existing, errExisting := findExistingChatGPTWebAuth(ctx, manager, fileName, credential.Email)
	if errExisting != nil {
		h.chatGPTWebDependencyMu.Unlock()
		if errors.Is(errExisting, errChatGPTWebCredentialIDOwned) || errors.Is(errExisting, errChatGPTWebCredentialMultiple) {
			return nil, "", false, errChatGPTWebImportIdentityConflict
		}
		return nil, "", false, errExisting
	}
	if expected == nil && existing != nil || expected != nil && (existing == nil || existing.ID != expected.ID) {
		h.chatGPTWebDependencyMu.Unlock()
		return nil, "", false, errChatGPTWebCredentialChanged
	}
	persistExpected := existing
	if expected != nil && existing != nil && expected.ID == existing.ID {
		if !chatGPTWebImportCredentialFieldsEqual(expected, existing) {
			h.chatGPTWebDependencyMu.Unlock()
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
		if identityChanged {
			h.chatGPTWebDependencyMu.Unlock()
			return nil, "", false, errChatGPTWebImportIdentityConflict
		}
		if current, errParse := chatgptwebauth.ParseCredential(identityBase.Metadata); errParse == nil {
			credential.CredentialUID = current.CredentialUID
		}
	}
	if strings.TrimSpace(credential.CredentialUID) == "" {
		credential.CredentialUID = uuid.NewString()
	}
	unchanged = existing != nil && importedChatGPTWebCredentialUnchanged(existing, credential)
	status = "created"
	if existing != nil {
		status = "updated"
	}
	if unchanged {
		installed = existing.Clone()
		h.chatGPTWebDependencyMu.Unlock()
		return installed, status, true, nil
	}
	var oldSourceUID string
	installed, oldSourceUID, err = h.persistChatGPTWebCredentialLocked(ctx, manager, fileName, credential, persistExpected, nil, refreshAware)
	h.chatGPTWebDependencyMu.Unlock()
	if err == nil && oldSourceUID != "" && credential.RefreshStrategy != chatgptwebauth.RefreshStrategyCodexSource {
		h.cleanupRetainedCodexSource(ctx, oldSourceUID)
	}
	return installed, status, false, err
}

func chatGPTWebImportCredentialFieldsEqual(first, second *coreauth.Auth) bool {
	if first == nil || second == nil {
		return first == second
	}
	firstCredential, firstErr := chatgptwebauth.ParseCredential(first.Metadata)
	secondCredential, secondErr := chatgptwebauth.ParseCredential(second.Metadata)
	return firstErr == nil && secondErr == nil && reflect.DeepEqual(firstCredential, secondCredential)
}

func chatGPTWebProbeRejectsCredential(err error) bool {
	status := chatGPTWebErrorStatus(err)
	return (status == http.StatusUnauthorized || status == http.StatusForbidden) &&
		strings.HasPrefix(chatGPTWebErrorRequestPath(err), "/backend-api/models")
}

func chatGPTWebImportRefreshLockIDs(manager *coreauth.Manager, email string) []string {
	if manager == nil {
		return nil
	}
	email = chatgptwebauth.NormalizeEmail(email)
	ids := make([]string, 0)
	if email != "" {
		ids = append(ids, chatGPTWebCredentialFileName(email))
	}
	for _, candidate := range manager.List() {
		if candidate == nil || !strings.EqualFold(strings.TrimSpace(candidate.Provider), chatgptwebauth.Provider) {
			continue
		}
		if email == "" || chatgptwebauth.NormalizeEmail(authEmail(candidate)) == email {
			ids = append(ids, candidate.ID)
		}
	}
	return ids
}

func chatGPTWebImportOperationKey(credential *chatgptwebauth.Credential) string {
	if credential == nil {
		return "chatgpt-web-import:empty"
	}
	if email := chatgptwebauth.NormalizeEmail(credential.Email); email != "" {
		return email
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(credential.RefreshStrategy))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(credential.RefreshToken))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(credential.AccessToken))
	for index := range credential.Cookies {
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write([]byte(credential.Cookies[index].Name))
		_, _ = hash.Write([]byte{'='})
		_, _ = hash.Write([]byte(credential.Cookies[index].Value))
	}
	return fmt.Sprintf("chatgpt-web-import:%x", hash.Sum(nil))
}

func chatGPTWebImportProbeAuth(id string, credential *chatgptwebauth.Credential) *coreauth.Auth {
	auth := &coreauth.Auth{ID: id, Provider: chatgptwebauth.Provider, FileName: id, Metadata: make(map[string]any)}
	if credential != nil {
		credential.ApplyToMetadata(auth.Metadata)
	}
	return auth
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
