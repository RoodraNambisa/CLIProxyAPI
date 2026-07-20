package management

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxypool"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const (
	chatGPTWebLoginTaskMaxBodyBytes = 1 << 20
	chatGPTWebLoginTaskMaxAccounts  = 500
	chatGPTWebLoginTaskMaxLineBytes = 16 << 10
	chatGPTWebLoginTaskWorkers      = 4
	chatGPTWebLoginTaskMaxRetained  = 100
	chatGPTWebLoginTaskRetention    = 24 * time.Hour

	chatGPTWebLoginTaskQueued              = "queued"
	chatGPTWebLoginTaskRunning             = "running"
	chatGPTWebLoginTaskCanceling           = "canceling"
	chatGPTWebLoginTaskCompleted           = "completed"
	chatGPTWebLoginTaskCompletedWithErrors = "completed_with_errors"
	chatGPTWebLoginTaskCanceled            = "canceled"

	chatGPTWebLoginResultQueued   = "queued"
	chatGPTWebLoginResultRunning  = "running"
	chatGPTWebLoginResultCommit   = "committing"
	chatGPTWebLoginResultSuccess  = "success"
	chatGPTWebLoginResultFailed   = "failed"
	chatGPTWebLoginResultCanceled = "canceled"
)

var (
	errChatGPTWebLoginTaskCapacity       = errors.New("too many retained chatgpt web login tasks")
	errChatGPTWebLoginTaskClosed         = errors.New("chatgpt web login task manager is closed")
	errChatGPTWebLoginEmailBusy          = errors.New("chatgpt web account already has an active login operation")
	errChatGPTWebCredentialChanged       = errors.New("chatgpt web credential changed before persistence")
	errChatGPTWebCredentialIDOwned       = errors.New("chatgpt web credential ID is already owned")
	errChatGPTWebCredentialIdentityOwned = errors.New("chatgpt web account identity is already owned")
	errChatGPTWebCredentialMultiple      = errors.New("multiple chatgpt web credentials use this email")
	errChatGPTWebCredentialLookup        = errors.New("chatgpt web credential lookup failed")
)

type chatGPTWebManagementExecutor interface {
	BeginLoginOperation(context.Context, string) (context.Context, func(), error)
	Login(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error)
	ReloginCurrent(context.Context, *coreauth.Auth) (*coreauth.Auth, bool, error)
}

type chatGPTWebImportExecutor interface {
	BeginLoginOperation(context.Context, string) (context.Context, func(), error)
	NormalizeImportedCredential(context.Context, *chatgptwebauth.Credential, string) (*chatgptwebauth.Credential, error)
	FetchModels(context.Context, *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error)
}

type chatGPTWebConversionExecutor interface {
	BeginLoginOperation(context.Context, string) (context.Context, func(), error)
	FetchModels(context.Context, *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error)
}

type chatGPTWebLoginInput struct {
	Line       int
	Email      string
	Password   string
	TOTPSecret string
}

type chatGPTWebLoginTaskResult struct {
	Line           int    `json:"line"`
	Email          string `json:"email"`
	Status         string `json:"status"`
	Name           string `json:"name,omitempty"`
	AuthIndex      string `json:"auth_index,omitempty"`
	LifecycleState string `json:"lifecycle_state,omitempty"`
	ErrorCategory  string `json:"error_category,omitempty"`
	Error          string `json:"error,omitempty"`
	HTTPStatus     int    `json:"http_status,omitempty"`
}

type chatGPTWebLoginTask struct {
	ID          string                      `json:"id"`
	State       string                      `json:"state"`
	CreatedAt   time.Time                   `json:"created_at"`
	StartedAt   *time.Time                  `json:"started_at,omitempty"`
	CompletedAt *time.Time                  `json:"completed_at,omitempty"`
	Total       int                         `json:"total"`
	Processed   int                         `json:"processed"`
	Succeeded   int                         `json:"succeeded"`
	Failed      int                         `json:"failed"`
	Canceled    int                         `json:"canceled"`
	Results     []chatGPTWebLoginTaskResult `json:"results"`

	cancel context.CancelFunc
	emails []string
}

type chatGPTWebLoginTaskManager struct {
	mu           sync.Mutex
	tasks        map[string]*chatGPTWebLoginTask
	activeEmails map[string]string
	slots        chan struct{}
	now          func() time.Time
	rootCtx      context.Context
	rootCancel   context.CancelFunc
	workers      sync.WaitGroup
	shutdownOnce sync.Once
	shutdownDone chan struct{}
	closed       bool
}

func newChatGPTWebLoginTaskManager() *chatGPTWebLoginTaskManager {
	rootCtx, rootCancel := context.WithCancel(context.Background())
	return &chatGPTWebLoginTaskManager{
		tasks:        make(map[string]*chatGPTWebLoginTask),
		activeEmails: make(map[string]string),
		slots:        make(chan struct{}, chatGPTWebLoginTaskWorkers),
		now:          time.Now,
		rootCtx:      rootCtx,
		rootCancel:   rootCancel,
		shutdownDone: make(chan struct{}),
	}
}

func (h *Handler) chatGPTWebTaskManager() *chatGPTWebLoginTaskManager {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.chatGPTWebTasks == nil {
		h.chatGPTWebTasks = newChatGPTWebLoginTaskManager()
	}
	return h.chatGPTWebTasks
}

func (m *chatGPTWebLoginTaskManager) create(inputs []chatGPTWebLoginInput) (*chatGPTWebLoginTask, context.Context, error) {
	if m == nil {
		return nil, nil, errors.New("chatgpt web login task manager is unavailable")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	if m.closed {
		return nil, nil, errChatGPTWebLoginTaskClosed
	}
	if len(m.tasks) >= chatGPTWebLoginTaskMaxRetained {
		m.pruneOldestTerminalLocked(len(m.tasks) - chatGPTWebLoginTaskMaxRetained + 1)
		if len(m.tasks) >= chatGPTWebLoginTaskMaxRetained {
			return nil, nil, errChatGPTWebLoginTaskCapacity
		}
	}

	id := uuid.NewString()
	emails := make([]string, 0, len(inputs))
	for _, input := range inputs {
		key := normalizeChatGPTWebLoginEmail(input.Email)
		if owner := m.activeEmails[key]; owner != "" {
			return nil, nil, fmt.Errorf("%w: %s", errChatGPTWebLoginEmailBusy, input.Email)
		}
		emails = append(emails, key)
	}
	for _, email := range emails {
		m.activeEmails[email] = id
	}

	now := m.currentTime()
	ctx, cancel := context.WithCancel(m.rootCtx)
	results := make([]chatGPTWebLoginTaskResult, len(inputs))
	for index, input := range inputs {
		results[index] = chatGPTWebLoginTaskResult{
			Line:   input.Line,
			Email:  input.Email,
			Status: chatGPTWebLoginResultQueued,
		}
	}
	task := &chatGPTWebLoginTask{
		ID:        id,
		State:     chatGPTWebLoginTaskQueued,
		CreatedAt: now,
		Total:     len(inputs),
		Results:   results,
		cancel:    cancel,
		emails:    emails,
	}
	m.tasks[id] = task
	m.workers.Add(1)
	return cloneChatGPTWebLoginTask(task), ctx, nil
}

func (m *chatGPTWebLoginTaskManager) start(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.tasks[id]
	if task == nil || task.State != chatGPTWebLoginTaskQueued {
		return false
	}
	now := m.currentTime()
	task.State = chatGPTWebLoginTaskRunning
	task.StartedAt = &now
	return true
}

func (m *chatGPTWebLoginTaskManager) markRunning(id string, index int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.tasks[id]
	if task == nil || index < 0 || index >= len(task.Results) || task.State != chatGPTWebLoginTaskRunning || task.Results[index].Status != chatGPTWebLoginResultQueued {
		return false
	}
	task.Results[index].Status = chatGPTWebLoginResultRunning
	return true
}

func (m *chatGPTWebLoginTaskManager) beginCommit(id string, index int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.tasks[id]
	if m.closed || task == nil || index < 0 || index >= len(task.Results) || task.State != chatGPTWebLoginTaskRunning || task.Results[index].Status != chatGPTWebLoginResultRunning {
		return false
	}
	task.Results[index].Status = chatGPTWebLoginResultCommit
	return true
}

func (m *chatGPTWebLoginTaskManager) setResult(id string, index int, result chatGPTWebLoginTaskResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.tasks[id]
	if task == nil || index < 0 || index >= len(task.Results) {
		return
	}
	previous := task.Results[index].Status
	if previous != chatGPTWebLoginResultQueued && previous != chatGPTWebLoginResultRunning && previous != chatGPTWebLoginResultCommit {
		return
	}
	task.Results[index] = result
	task.Processed++
	switch result.Status {
	case chatGPTWebLoginResultSuccess:
		task.Succeeded++
	case chatGPTWebLoginResultCanceled:
		task.Canceled++
	default:
		task.Failed++
	}
}

func (m *chatGPTWebLoginTaskManager) finish(id string, canceled bool) {
	m.mu.Lock()
	task := m.tasks[id]
	if task == nil || isTerminalChatGPTWebLoginTaskState(task.State) {
		m.mu.Unlock()
		return
	}
	if canceled || task.State == chatGPTWebLoginTaskCanceling {
		for index := range task.Results {
			if task.Results[index].Status != chatGPTWebLoginResultQueued && task.Results[index].Status != chatGPTWebLoginResultRunning {
				continue
			}
			task.Results[index].Status = chatGPTWebLoginResultCanceled
			task.Processed++
			task.Canceled++
		}
	}
	if task.Canceled > 0 {
		task.State = chatGPTWebLoginTaskCanceled
	} else if task.Failed > 0 {
		task.State = chatGPTWebLoginTaskCompletedWithErrors
	} else {
		task.State = chatGPTWebLoginTaskCompleted
	}
	now := m.currentTime()
	task.CompletedAt = &now
	cancel := task.cancel
	task.cancel = nil
	m.releaseTaskEmailsLocked(task)
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (m *chatGPTWebLoginTaskManager) get(id string) (*chatGPTWebLoginTask, bool) {
	if m == nil {
		return nil, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	task := m.tasks[strings.TrimSpace(id)]
	if task == nil {
		return nil, false
	}
	return cloneChatGPTWebLoginTask(task), true
}

func (m *chatGPTWebLoginTaskManager) cancelTask(id string) (*chatGPTWebLoginTask, bool) {
	if m == nil {
		return nil, false
	}
	m.mu.Lock()
	m.pruneLocked()
	task := m.tasks[strings.TrimSpace(id)]
	if task == nil {
		m.mu.Unlock()
		return nil, false
	}
	if isTerminalChatGPTWebLoginTaskState(task.State) {
		snapshot := cloneChatGPTWebLoginTask(task)
		m.mu.Unlock()
		return snapshot, true
	}
	task.State = chatGPTWebLoginTaskCanceling
	cancel := task.cancel
	snapshot := cloneChatGPTWebLoginTask(task)
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return snapshot, true
}

func (m *chatGPTWebLoginTaskManager) prune() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.pruneLocked()
	m.mu.Unlock()
}

func (m *chatGPTWebLoginTaskManager) reserveEmail(email, owner string) error {
	if m == nil {
		return errors.New("chatgpt web login task manager is unavailable")
	}
	key := normalizeChatGPTWebLoginEmail(email)
	m.mu.Lock()
	defer m.mu.Unlock()
	if current := m.activeEmails[key]; current != "" {
		return fmt.Errorf("%w: %s", errChatGPTWebLoginEmailBusy, email)
	}
	m.activeEmails[key] = owner
	return nil
}

func (m *chatGPTWebLoginTaskManager) releaseEmail(email, owner string) {
	if m == nil {
		return
	}
	key := normalizeChatGPTWebLoginEmail(email)
	m.mu.Lock()
	if m.activeEmails[key] == owner {
		delete(m.activeEmails, key)
	}
	m.mu.Unlock()
}

func (m *chatGPTWebLoginTaskManager) releaseTaskEmailsLocked(task *chatGPTWebLoginTask) {
	if task == nil {
		return
	}
	for _, email := range task.emails {
		if m.activeEmails[email] == task.ID {
			delete(m.activeEmails, email)
		}
	}
	task.emails = nil
}

func (m *chatGPTWebLoginTaskManager) pruneLocked() {
	cutoff := m.currentTime().Add(-chatGPTWebLoginTaskRetention)
	for id, task := range m.tasks {
		if task == nil {
			delete(m.tasks, id)
			continue
		}
		if task.CompletedAt != nil && task.CompletedAt.Before(cutoff) {
			m.releaseTaskEmailsLocked(task)
			delete(m.tasks, id)
		}
	}
}

func (m *chatGPTWebLoginTaskManager) pruneOldestTerminalLocked(count int) {
	for count > 0 {
		var oldest *chatGPTWebLoginTask
		for _, task := range m.tasks {
			if task == nil || task.CompletedAt == nil || !isTerminalChatGPTWebLoginTaskState(task.State) {
				continue
			}
			if oldest == nil || task.CompletedAt.Before(*oldest.CompletedAt) {
				oldest = task
			}
		}
		if oldest == nil {
			return
		}
		m.releaseTaskEmailsLocked(oldest)
		delete(m.tasks, oldest.ID)
		count--
	}
}

func (m *chatGPTWebLoginTaskManager) acquireSlot(ctx context.Context) bool {
	if m == nil {
		return false
	}
	select {
	case m.slots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (m *chatGPTWebLoginTaskManager) lifecycleContext() context.Context {
	if m == nil {
		return context.Background()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.rootCtx == nil {
		return context.Background()
	}
	return m.rootCtx
}

func (m *chatGPTWebLoginTaskManager) beginOperation(ctx context.Context) (context.Context, func(), error) {
	if m == nil {
		return nil, nil, errors.New("chatgpt web login task manager is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, nil, errChatGPTWebLoginTaskClosed
	}
	rootCtx := m.rootCtx
	m.workers.Add(1)
	m.mu.Unlock()

	operationCtx, cancel := context.WithCancel(ctx)
	stopRootCancel := context.AfterFunc(rootCtx, cancel)
	if !m.acquireSlot(operationCtx) {
		stopRootCancel()
		cancel()
		m.workers.Done()
		if errContext := operationCtx.Err(); errContext != nil {
			return nil, nil, errContext
		}
		return nil, nil, errors.New("chatgpt web login slot is unavailable")
	}

	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			m.releaseSlot()
			stopRootCancel()
			cancel()
			m.workers.Done()
		})
	}
	return operationCtx, release, nil
}

func (m *chatGPTWebLoginTaskManager) releaseSlot() {
	if m != nil {
		<-m.slots
	}
}

func (m *chatGPTWebLoginTaskManager) taskDone() {
	if m != nil {
		m.workers.Done()
	}
}

func (m *chatGPTWebLoginTaskManager) shutdown(ctx context.Context) error {
	if m == nil {
		return nil
	}
	m.shutdownOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		if m.rootCancel != nil {
			m.rootCancel()
		}
		m.mu.Unlock()
		go func() {
			m.workers.Wait()
			close(m.shutdownDone)
		}()
	})
	if ctx == nil {
		<-m.shutdownDone
		return nil
	}
	select {
	case <-m.shutdownDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *chatGPTWebLoginTaskManager) currentTime() time.Time {
	if m != nil && m.now != nil {
		return m.now().UTC()
	}
	return time.Now().UTC()
}

func cloneChatGPTWebLoginTask(task *chatGPTWebLoginTask) *chatGPTWebLoginTask {
	if task == nil {
		return nil
	}
	clone := *task
	clone.cancel = nil
	clone.emails = nil
	clone.Results = append([]chatGPTWebLoginTaskResult(nil), task.Results...)
	if task.StartedAt != nil {
		startedAt := *task.StartedAt
		clone.StartedAt = &startedAt
	}
	if task.CompletedAt != nil {
		completedAt := *task.CompletedAt
		clone.CompletedAt = &completedAt
	}
	return &clone
}

func isTerminalChatGPTWebLoginTaskState(state string) bool {
	return state == chatGPTWebLoginTaskCompleted || state == chatGPTWebLoginTaskCompletedWithErrors || state == chatGPTWebLoginTaskCanceled
}

func normalizeChatGPTWebLoginEmail(email string) string {
	return chatgptwebauth.NormalizeEmail(email)
}

// StartChatGPTWebLoginTask starts a bounded background login task.
func (h *Handler) StartChatGPTWebLoginTask(c *gin.Context) {
	executor, manager, errExecutor := h.chatGPTWebManagementExecutor()
	if errExecutor != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": errExecutor.Error()})
		return
	}
	inputs, errInput := readChatGPTWebLoginTaskInput(c)
	if errInput != nil {
		status := http.StatusBadRequest
		var maxBytesError *http.MaxBytesError
		if errors.As(errInput, &maxBytesError) || errors.Is(errInput, errChatGPTWebLoginTaskInputTooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		c.JSON(status, gin.H{"error": errInput.Error()})
		return
	}
	taskManager := h.chatGPTWebTaskManager()
	task, taskCtx, errCreate := taskManager.create(inputs)
	if errCreate != nil {
		status := http.StatusConflict
		switch {
		case errors.Is(errCreate, errChatGPTWebLoginTaskClosed):
			status = http.StatusServiceUnavailable
		case errors.Is(errCreate, errChatGPTWebLoginTaskCapacity):
			status = http.StatusTooManyRequests
		}
		c.JSON(status, gin.H{"error": errCreate.Error()})
		return
	}
	taskCtx = PopulateAuthContext(taskCtx, c)
	go func() {
		defer taskManager.taskDone()
		h.runChatGPTWebLoginTask(taskCtx, task.ID, inputs, executor, manager)
	}()
	c.JSON(http.StatusAccepted, task)
}

// GetChatGPTWebLoginTask returns one task snapshot.
func (h *Handler) GetChatGPTWebLoginTask(c *gin.Context) {
	taskManager := h.chatGPTWebTaskManager()
	if taskManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "chatgpt web login tasks are unavailable"})
		return
	}
	task, ok := taskManager.get(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "chatgpt web login task not found"})
		return
	}
	c.JSON(http.StatusOK, task)
}

// CancelChatGPTWebLoginTask cancels pending and in-flight login acquisitions.
func (h *Handler) CancelChatGPTWebLoginTask(c *gin.Context) {
	taskManager := h.chatGPTWebTaskManager()
	if taskManager == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "chatgpt web login tasks are unavailable"})
		return
	}
	task, ok := taskManager.cancelTask(c.Param("id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "chatgpt web login task not found"})
		return
	}
	c.JSON(http.StatusOK, task)
}

// ReloginChatGPTWebAuth performs a synchronous manual re-login for one file.
func (h *Handler) ReloginChatGPTWebAuth(c *gin.Context) {
	name := strings.TrimSpace(c.Param("name"))
	if name == "" || isUnsafeAuthFileName(name) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "valid auth file name is required"})
		return
	}
	auth := h.findManagedAuth(name)
	if auth == nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "auth file not found"})
		return
	}
	if !strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "auth file is not a chatgpt web credential"})
		return
	}
	executor, manager, errExecutor := h.chatGPTWebManagementExecutor()
	if errExecutor != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": errExecutor.Error()})
		return
	}
	email := authEmail(auth)
	if email == "" {
		email = auth.ID
	}
	owner := "manual:" + uuid.NewString()
	taskManager := h.chatGPTWebTaskManager()
	if errReserve := taskManager.reserveEmail(email, owner); errReserve != nil {
		c.JSON(http.StatusConflict, gin.H{"error": errReserve.Error()})
		return
	}
	defer taskManager.releaseEmail(email, owner)
	operationCtx, releaseOperation, errOperation := taskManager.beginOperation(c.Request.Context())
	if errOperation != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(errOperation, context.Canceled) || errors.Is(errOperation, context.DeadlineExceeded) {
			status = http.StatusRequestTimeout
		}
		c.JSON(status, gin.H{"error": "chatgpt web re-login is unavailable"})
		return
	}
	defer releaseOperation()
	releaseProxyBinding := manager.HoldProxyBinding(auth.ID)
	defer releaseProxyBinding()

	updated, current, errRelogin := executor.ReloginCurrent(operationCtx, auth)
	if updated == nil {
		updated, _ = manager.GetByID(auth.ID)
	}
	response := gin.H{"status": "ok"}
	if updated != nil {
		response["auth"] = h.buildAuthFileEntry(updated)
	}
	if errors.Is(errRelogin, chatgptwebauth.ErrCredentialSuperseded) {
		response["status"] = "conflict"
		response["error_category"] = "credential_changed"
		response["error"] = "credential changed while re-login was running"
		c.JSON(http.StatusConflict, response)
		return
	}
	if errRelogin != nil {
		if outcome, explicit := coreauth.SaveOutcomeFromError(errRelogin); explicit {
			switch outcome {
			case coreauth.SaveOutcomeCommitted:
				response["status"] = "ok"
				response["warning"] = "credential was saved with a cleanup warning"
				c.JSON(http.StatusOK, response)
				return
			case coreauth.SaveOutcomeUncertain:
				response["status"] = "failed"
				response["error_category"] = "persist_uncertain"
				response["error"] = "credential persistence outcome is uncertain"
				c.JSON(http.StatusServiceUnavailable, response)
				return
			case coreauth.SaveOutcomeRolledBack:
				response["status"] = "failed"
				response["error_category"] = "persist_failed"
				response["error"] = "failed to save chatgpt web credential"
				c.JSON(http.StatusInternalServerError, response)
				return
			}
		}
		category, message, status, _ := classifyChatGPTWebManagementError(errRelogin)
		response["status"] = "failed"
		response["error_category"] = category
		response["error"] = message
		if !current && errors.Is(errRelogin, context.Canceled) {
			status = http.StatusRequestTimeout
		}
		c.JSON(status, response)
		return
	}
	if !current {
		response["status"] = "conflict"
		response["error_category"] = "credential_changed"
		response["error"] = "credential changed while re-login was running"
		c.JSON(http.StatusConflict, response)
		return
	}
	c.JSON(http.StatusOK, response)
}

func (h *Handler) chatGPTWebManagementExecutor() (chatGPTWebManagementExecutor, *coreauth.Manager, error) {
	registered, manager, errExecutor := h.registeredChatGPTWebExecutor()
	if errExecutor != nil {
		return nil, nil, errExecutor
	}
	executor, ok := registered.(chatGPTWebManagementExecutor)
	if !ok {
		return nil, nil, errors.New("chatgpt web management login is unavailable")
	}
	return executor, manager, nil
}

func (h *Handler) chatGPTWebImportExecutor() (chatGPTWebImportExecutor, *coreauth.Manager, error) {
	registered, manager, errExecutor := h.registeredChatGPTWebExecutor()
	if errExecutor != nil {
		return nil, nil, errExecutor
	}
	executor, ok := registered.(chatGPTWebImportExecutor)
	if !ok {
		return nil, nil, errors.New("chatgpt web credential import is unavailable")
	}
	return executor, manager, nil
}

func (h *Handler) chatGPTWebConversionExecutor() (chatGPTWebConversionExecutor, *coreauth.Manager, error) {
	registered, manager, errExecutor := h.registeredChatGPTWebExecutor()
	if errExecutor != nil {
		return nil, nil, errExecutor
	}
	executor, ok := registered.(chatGPTWebConversionExecutor)
	if !ok {
		return nil, nil, errors.New("chatgpt web credential conversion is unavailable")
	}
	return executor, manager, nil
}

func (h *Handler) registeredChatGPTWebExecutor() (coreauth.ProviderExecutor, *coreauth.Manager, error) {
	if h == nil {
		return nil, nil, errors.New("handler not initialized")
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	if manager == nil {
		return nil, nil, errors.New("core auth manager unavailable")
	}
	registered, ok := manager.Executor(chatgptwebauth.Provider)
	if !ok || registered == nil {
		return nil, nil, errors.New("chatgpt web executor unavailable")
	}
	return registered, manager, nil
}

func (h *Handler) runChatGPTWebLoginTask(ctx context.Context, taskID string, inputs []chatGPTWebLoginInput, executor chatGPTWebManagementExecutor, manager *coreauth.Manager) {
	taskManager := h.chatGPTWebTaskManager()
	if !taskManager.start(taskID) {
		taskManager.finish(taskID, true)
		return
	}
	jobs := make(chan int)
	var workers sync.WaitGroup
	workerCount := min(chatGPTWebLoginTaskWorkers, len(inputs))
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				if !taskManager.acquireSlot(ctx) {
					taskManager.setResult(taskID, index, canceledChatGPTWebLoginTaskResult(inputs[index]))
					continue
				}
				if !taskManager.markRunning(taskID, index) {
					taskManager.releaseSlot()
					continue
				}
				if errContext := ctx.Err(); errContext != nil {
					taskManager.setResult(taskID, index, canceledChatGPTWebLoginTaskResult(inputs[index]))
					taskManager.releaseSlot()
					continue
				}
				result := h.executeChatGPTWebLogin(ctx, inputs[index], executor, manager, func() (context.Context, bool) {
					if !taskManager.beginCommit(taskID, index) {
						return nil, false
					}
					commitCtx := taskManager.lifecycleContext()
					if requestInfo := coreauth.GetRequestInfo(ctx); requestInfo != nil {
						commitCtx = coreauth.WithRequestInfo(commitCtx, requestInfo)
					}
					return commitCtx, true
				})
				taskManager.setResult(taskID, index, result)
				taskManager.releaseSlot()
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
	for index := range inputs {
		inputs[index].Password = ""
		inputs[index].TOTPSecret = ""
	}
	taskManager.finish(taskID, ctx.Err() != nil)
}

func (h *Handler) executeChatGPTWebLogin(ctx context.Context, input chatGPTWebLoginInput, executor chatGPTWebManagementExecutor, manager *coreauth.Manager, beginCommit func() (context.Context, bool)) chatGPTWebLoginTaskResult {
	result := chatGPTWebLoginTaskResult{
		Line:   input.Line,
		Email:  input.Email,
		Status: chatGPTWebLoginResultFailed,
	}
	operationCtx, releaseOperation, errOperation := executor.BeginLoginOperation(ctx, input.Email)
	if errOperation != nil {
		result.ErrorCategory, result.Error, result.HTTPStatus, result.LifecycleState = classifyChatGPTWebManagementError(errOperation)
		if result.ErrorCategory == "canceled" {
			result.Status = chatGPTWebLoginResultCanceled
		}
		return result
	}
	defer releaseOperation()
	ctx = operationCtx

	fileName := chatGPTWebCredentialFileName(input.Email)
	existing, errExisting := findExistingChatGPTWebAuth(ctx, manager, fileName, input.Email)
	if errExisting != nil {
		if errors.Is(errExisting, errChatGPTWebCredentialIDOwned) {
			result.ErrorCategory = "credential_id_conflict"
			result.Error = "credential name is already used by another account"
			result.HTTPStatus = http.StatusConflict
		} else if errors.Is(errExisting, errChatGPTWebCredentialMultiple) {
			result.ErrorCategory = "credential_ambiguous"
			result.Error = "multiple chatgpt web credentials use this email"
			result.HTTPStatus = http.StatusConflict
		} else {
			result.ErrorCategory, result.Error, result.HTTPStatus, result.LifecycleState = classifyChatGPTWebManagementError(errExisting)
			if result.ErrorCategory == "canceled" {
				result.Status = chatGPTWebLoginResultCanceled
			}
		}
		return result
	}
	pending := existing
	var existingCredential *chatgptwebauth.Credential
	if pending == nil {
		pending = &coreauth.Auth{
			ID:       fileName,
			Provider: chatgptwebauth.Provider,
			FileName: fileName,
			Label:    input.Email,
			Metadata: map[string]any{"type": chatgptwebauth.Provider, "email": input.Email},
		}
	} else {
		existingCredential, _ = chatgptwebauth.ParseCredential(pending.Metadata)
	}
	releaseProxyBinding := manager.HoldProxyBinding(pending.ID)
	defer releaseProxyBinding()
	resolved, errResolve := manager.ResolveProxyAuth(ctx, pending)
	if errResolve != nil {
		result.ErrorCategory, result.Error, result.HTTPStatus, result.LifecycleState = classifyChatGPTWebManagementError(errResolve)
		if result.ErrorCategory == "canceled" {
			result.Status = chatGPTWebLoginResultCanceled
		}
		return result
	}
	credential, errLogin := executor.Login(ctx, chatgptwebauth.LoginInput{
		Email:      input.Email,
		Password:   input.Password,
		TOTPSecret: input.TOTPSecret,
		ProxyURL:   resolved.EffectiveProxyURL(),
		Credential: existingCredential,
	})
	if errors.Is(ctx.Err(), context.Canceled) {
		return canceledChatGPTWebLoginTaskResult(input)
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) && !errors.Is(errLogin, context.DeadlineExceeded) {
		errLogin = errors.Join(errLogin, context.DeadlineExceeded)
	}
	if errLogin != nil && resolved.EffectiveProxyBindingID() != "" {
		errLogin = manager.ReportProxyFailure(ctx, resolved, errLogin)
	}

	var installed *coreauth.Auth
	if shouldPersistChatGPTWebLoginCredential(credential, errLogin) {
		if beginCommit == nil {
			result.ErrorCategory = "persist_failed"
			result.Error = "credential persistence is unavailable"
			result.HTTPStatus = http.StatusInternalServerError
			return result
		}
		persistCtx, allowed := beginCommit()
		if !allowed {
			return canceledChatGPTWebLoginTaskResult(input)
		}
		var errPersist error
		installed, errPersist = h.persistChatGPTWebLoginCredential(persistCtx, manager, fileName, credential, existing, errLogin)
		if errPersist != nil {
			outcome, explicit := coreauth.SaveOutcomeFromError(errPersist)
			// A committed post-auth hook does not imply that manager registration completed.
			if (explicit && (outcome == coreauth.SaveOutcomeUncertain || outcome == coreauth.SaveOutcomeCommitted)) ||
				(!explicit && (errors.Is(errPersist, context.Canceled) || errors.Is(errPersist, context.DeadlineExceeded))) {
				result.ErrorCategory = "persist_uncertain"
				result.Error = "credential persistence outcome is uncertain"
				result.HTTPStatus = http.StatusServiceUnavailable
				result.LifecycleState = string(chatgptwebauth.SafeLifecycleState(string(credential.LifecycleState)))
				return result
			}
			if errors.Is(errPersist, context.Canceled) || errors.Is(errPersist, context.DeadlineExceeded) {
				canceled := canceledChatGPTWebLoginTaskResult(input)
				canceled.LifecycleState = string(chatgptwebauth.SafeLifecycleState(string(credential.LifecycleState)))
				return canceled
			}
			if errors.Is(errPersist, errChatGPTWebCredentialIdentityOwned) {
				result.ErrorCategory = "identity_conflict"
				result.Error = "credential identity is already used by another account file"
				result.HTTPStatus = http.StatusConflict
			} else if errors.Is(errPersist, errChatGPTWebCredentialLookup) {
				result.ErrorCategory = "credential_lookup_failed"
				result.Error = "unable to inspect existing credentials"
				result.HTTPStatus = http.StatusServiceUnavailable
			} else if errors.Is(errPersist, coreauth.ErrChatGPTWebEmailAlreadyExists) {
				result.ErrorCategory = "credential_ambiguous"
				result.Error = "multiple chatgpt web credentials use this email"
				result.HTTPStatus = http.StatusConflict
			} else if errors.Is(errPersist, coreauth.ErrAuthAlreadyExists) || errors.Is(errPersist, errChatGPTWebCredentialChanged) {
				result.ErrorCategory = "credential_changed"
				result.Error = "credential changed while login was running"
				result.HTTPStatus = http.StatusConflict
			} else {
				result.ErrorCategory = "persist_failed"
				result.Error = "failed to save chatgpt web credential"
				result.HTTPStatus = http.StatusInternalServerError
			}
			if credential != nil {
				result.LifecycleState = string(chatgptwebauth.SafeLifecycleState(string(credential.LifecycleState)))
			}
			return result
		}
		result.Name = installed.FileName
		result.AuthIndex = installed.EnsureIndex()
		result.LifecycleState = installed.LifecycleState()
	}
	if errLogin != nil {
		result.ErrorCategory, result.Error, result.HTTPStatus, result.LifecycleState = classifyChatGPTWebManagementError(errLogin)
		if result.ErrorCategory == "canceled" {
			result.Status = chatGPTWebLoginResultCanceled
		}
		if installed != nil {
			result.LifecycleState = installed.LifecycleState()
		}
		return result
	}
	if credential == nil || installed == nil {
		result.ErrorCategory = "login_failed"
		result.Error = "chatgpt web login returned no credential"
		result.HTTPStatus = http.StatusBadGateway
		return result
	}
	result.Status = chatGPTWebLoginResultSuccess
	result.ErrorCategory = ""
	result.Error = ""
	result.HTTPStatus = 0
	return result
}

func (h *Handler) persistChatGPTWebLoginCredential(ctx context.Context, manager *coreauth.Manager, fileName string, credential *chatgptwebauth.Credential, existing *coreauth.Auth, loginErr error) (*coreauth.Auth, error) {
	return h.persistChatGPTWebCredential(ctx, manager, fileName, credential, existing, loginErr, false)
}

func (h *Handler) persistChatGPTWebCredential(ctx context.Context, manager *coreauth.Manager, fileName string, credential *chatgptwebauth.Credential, existing *coreauth.Auth, loginErr error, refreshAware bool) (*coreauth.Auth, error) {
	h.chatGPTWebDependencyMu.Lock()
	installed, oldSourceUID, errPersist := h.persistChatGPTWebCredentialLocked(ctx, manager, fileName, credential, existing, loginErr, refreshAware)
	h.chatGPTWebDependencyMu.Unlock()
	if errPersist == nil && oldSourceUID != "" && credential != nil && credential.RefreshStrategy != chatgptwebauth.RefreshStrategyCodexSource {
		h.cleanupRetainedCodexSource(ctx, oldSourceUID)
	}
	return installed, errPersist
}

func (h *Handler) persistChatGPTWebCredentialLocked(ctx context.Context, manager *coreauth.Manager, fileName string, credential *chatgptwebauth.Credential, existing *coreauth.Auth, loginErr error, refreshAware bool) (*coreauth.Auth, string, error) {
	if credential == nil {
		return nil, "", errors.New("chatgpt web credential is nil")
	}
	if manager == nil {
		return nil, "", errors.New("core auth manager unavailable")
	}
	excludedID := ""
	if existing != nil {
		excludedID = existing.ID
	}
	owner, errOwner := chatGPTWebStrongIdentityOwner(ctx, manager, credential, excludedID)
	if errOwner != nil {
		return nil, "", errOwner
	}
	if owner != nil {
		return nil, "", errChatGPTWebCredentialIdentityOwned
	}
	if strings.TrimSpace(credential.CredentialUID) == "" && existing != nil {
		if current, errParse := chatgptwebauth.ParseCredential(existing.Metadata); errParse == nil {
			credential.CredentialUID = strings.TrimSpace(current.CredentialUID)
		}
	}
	if strings.TrimSpace(credential.CredentialUID) == "" {
		credential.CredentialUID = uuid.NewString()
	}
	oldSourceUID := linkedSourceUID(existing)
	now := time.Now().UTC()
	lifecycleState := string(chatgptwebauth.SafeLifecycleState(string(credential.LifecycleState)))
	lifecycleReason := chatgptwebauth.SafeLifecycleReason(credential.LifecycleReason)
	if existing != nil {
		record := existing.Clone()
		if record.Metadata == nil {
			record.Metadata = make(map[string]any)
		}
		if loginErr == nil {
			credential.ApplyToMetadata(record.Metadata)
		} else {
			record.Metadata["lifecycle_state"] = lifecycleState
			record.Metadata["lifecycle_reason"] = lifecycleReason
			record.Metadata["lifecycle_updated_at"] = credential.LifecycleUpdatedAt
		}
		if record.Disabled {
			record.Status = coreauth.StatusDisabled
		} else {
			record.Status = coreauth.RuntimeStatusForLifecycle(lifecycleState)
		}
		record.StatusMessage = lifecycleReason
		record.UpdatedAt = now
		var (
			installed *coreauth.Auth
			current   bool
			errUpdate error
		)
		if _, runtimeExists := manager.GetByID(existing.ID); !runtimeExists {
			installed, current, errUpdate = manager.UpdatePersistedIfCurrentSourceHash(ctx, existing, record)
		} else if refreshAware {
			installed, current, errUpdate = manager.UpdateRefreshedIfCurrent(ctx, existing, record)
		} else {
			installed, current, errUpdate = manager.UpdateIfCurrent(ctx, existing, record)
		}
		if errUpdate != nil {
			return nil, "", errUpdate
		}
		if !current {
			return nil, "", errChatGPTWebCredentialChanged
		}
		return installed, oldSourceUID, nil
	}

	metadata := make(map[string]any)
	credential.ApplyToMetadata(metadata)
	record := &coreauth.Auth{
		ID:            fileName,
		Provider:      chatgptwebauth.Provider,
		FileName:      fileName,
		Label:         strings.TrimSpace(credential.Email),
		Metadata:      metadata,
		Status:        coreauth.RuntimeStatusForLifecycle(lifecycleState),
		StatusMessage: lifecycleReason,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if h.postAuthHook != nil {
		if errHook := h.postAuthHook(ctx, record); errHook != nil {
			return nil, "", fmt.Errorf("post-auth hook failed: %w", errHook)
		}
	}
	installed, errRegister := manager.RegisterIfAbsent(ctx, record)
	return installed, "", errRegister
}

func findExistingChatGPTWebAuth(ctx context.Context, manager *coreauth.Manager, fileName, email string) (*coreauth.Auth, error) {
	if manager == nil {
		return nil, nil
	}
	email = normalizeChatGPTWebLoginEmail(email)
	var match *coreauth.Auth
	auths, errList := manager.CompleteAuthSnapshot(ctx)
	if errList != nil {
		return nil, fmt.Errorf("%w: %w", errChatGPTWebCredentialLookup, errList)
	}
	for _, candidate := range auths {
		if candidate != nil && candidate.ID == fileName {
			if !strings.EqualFold(strings.TrimSpace(candidate.Provider), chatgptwebauth.Provider) {
				return nil, fmt.Errorf("%w: another provider", errChatGPTWebCredentialIDOwned)
			}
			if normalizeChatGPTWebLoginEmail(authEmail(candidate)) != email {
				return nil, fmt.Errorf("%w: another chatgpt web account", errChatGPTWebCredentialIDOwned)
			}
			match = candidate
		}
		if candidate == nil || !strings.EqualFold(strings.TrimSpace(candidate.Provider), chatgptwebauth.Provider) || normalizeChatGPTWebLoginEmail(authEmail(candidate)) != email {
			continue
		}
		if match != nil && match.ID != candidate.ID {
			return nil, errChatGPTWebCredentialMultiple
		}
		match = candidate
	}
	return match, nil
}

func chatGPTWebStrongIdentityOwner(ctx context.Context, manager *coreauth.Manager, credential *chatgptwebauth.Credential, excludedID string) (*coreauth.Auth, error) {
	if manager == nil || credential == nil {
		return nil, nil
	}
	metadata := make(map[string]any)
	credential.ApplyToMetadata(metadata)
	incoming := &coreauth.Auth{Provider: chatgptwebauth.Provider, Metadata: metadata}
	if !coreauth.ChatGPTWebCredentialHasStrongIdentity(incoming) {
		return nil, nil
	}
	auths, errSnapshot := manager.CompleteAuthSnapshot(ctx)
	if errSnapshot != nil {
		return nil, fmt.Errorf("%w: %w", errChatGPTWebCredentialLookup, errSnapshot)
	}
	reference := coreauth.NewChatGPTWebCredentialReference(incoming)
	for _, candidate := range auths {
		if candidate == nil || candidate.ID == excludedID ||
			!strings.EqualFold(strings.TrimSpace(candidate.Provider), chatgptwebauth.Provider) ||
			!coreauth.ChatGPTWebCredentialHasStrongIdentity(candidate) {
			continue
		}
		if reference.Matches(candidate) {
			return candidate, nil
		}
	}
	return nil, nil
}

func shouldPersistChatGPTWebLoginCredential(credential *chatgptwebauth.Credential, errLogin error) bool {
	if credential == nil {
		return false
	}
	if errLogin == nil {
		return credential.LifecycleState == chatgptwebauth.LifecycleActive
	}
	switch credential.LifecycleState {
	case chatgptwebauth.LifecycleDead, chatgptwebauth.LifecycleInteractionRequired, chatgptwebauth.LifecycleReauthRequired:
		return true
	default:
		return false
	}
}

func chatGPTWebCredentialFileName(email string) string {
	return chatgptwebauth.CredentialFileName(email)
}

func canceledChatGPTWebLoginTaskResult(input chatGPTWebLoginInput) chatGPTWebLoginTaskResult {
	return chatGPTWebLoginTaskResult{
		Line:          input.Line,
		Email:         input.Email,
		Status:        chatGPTWebLoginResultCanceled,
		ErrorCategory: "canceled",
		Error:         "login canceled",
	}
}

func classifyChatGPTWebManagementError(err error) (category, message string, status int, lifecycle string) {
	if err == nil {
		return "", "", http.StatusOK, ""
	}
	switch {
	case errors.Is(err, context.Canceled):
		return "canceled", "login canceled", http.StatusRequestTimeout, ""
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout", "login timed out", http.StatusGatewayTimeout, ""
	case errors.Is(err, errChatGPTWebCredentialLookup):
		return "credential_lookup_failed", "unable to inspect existing credentials", http.StatusServiceUnavailable, ""
	}
	var unavailable *proxypool.UnavailableError
	if errors.As(err, &unavailable) {
		return "proxy_unavailable", "no proxy node is currently available", http.StatusServiceUnavailable, ""
	}
	if authError, ok := chatgptwebauth.AsAuthError(err); ok {
		category = safeChatGPTWebErrorCategory(authError.Code)
		message = safeChatGPTWebErrorMessage(category)
		status = chatGPTWebManagementAuthStatus(authError, category)
		state := authError.State
		if state == "" {
			state = authError.LifecycleState
		}
		return category, message, status, string(chatgptwebauth.SafeLifecycleState(string(state)))
	}
	var coded interface{ ChatGPTWebErrorCode() string }
	if errors.As(err, &coded) {
		category = safeChatGPTWebErrorCategory(coded.ChatGPTWebErrorCode())
		status = http.StatusUnprocessableEntity
		switch category {
		case "source_auth_missing", "source_auth_disabled", "source_auth_replaced", "source_auth_changed", "source_identity_changed", "source_identity_mismatch":
			status = http.StatusConflict
		case "source_refresh_unavailable":
			status = http.StatusServiceUnavailable
		}
		return category, safeChatGPTWebErrorMessage(category), status, string(chatgptwebauth.LifecycleReauthRequired)
	}
	return "login_failed", "chatgpt web login failed", http.StatusBadGateway, ""
}

func chatGPTWebManagementAuthStatus(authError *chatgptwebauth.AuthError, category string) int {
	if authError == nil {
		return http.StatusBadGateway
	}
	if authError.Retryable {
		if authError.StatusCode == http.StatusTooManyRequests {
			return http.StatusTooManyRequests
		}
		return http.StatusServiceUnavailable
	}
	switch category {
	case "account_deleted", "account_deactivated":
		return http.StatusGone
	case "email_otp_required",
		"sms_otp_required",
		"passkey_required",
		"browser_confirmation_required",
		"turnstile_required",
		"arkose_required",
		"interaction_required",
		"access_denied":
		return http.StatusConflict
	case "invalid_password",
		"invalid_totp",
		"invalid_totp_secret",
		"totp_required",
		"totp_factor_missing",
		"missing_credentials",
		"authorization_completion_required",
		"refresh_token_missing",
		"access_token_missing",
		"invalid_grant",
		"app_session_terminated",
		"credential_invalid":
		return http.StatusUnprocessableEntity
	}
	status := authError.StatusCode
	if status < 400 || status > 599 {
		return http.StatusUnprocessableEntity
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return http.StatusUnprocessableEntity
	}
	if status == http.StatusNotFound || status >= http.StatusInternalServerError {
		return http.StatusBadGateway
	}
	return status
}

func safeChatGPTWebErrorCategory(value string) string {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "rate_limited", "credential_cooldown":
		return normalized
	}
	safe := chatgptwebauth.SafeLifecycleReason(normalized)
	if safe == "" {
		return "authentication_failed"
	}
	return safe
}

func safeChatGPTWebErrorMessage(category string) string {
	switch category {
	case "account_deleted":
		return "account is deleted"
	case "account_deactivated":
		return "account is deactivated"
	case "invalid_password":
		return "password was rejected"
	case "invalid_totp", "invalid_totp_secret":
		return "TOTP verification failed"
	case "totp_required":
		return "a TOTP secret is required"
	case "email_otp_required", "sms_otp_required", "passkey_required", "browser_confirmation_required", "interaction_required", "access_denied":
		return "additional user verification is required"
	case "rate_limited":
		return "credential was rate limited"
	case "credential_cooldown":
		return "credential is cooling down"
	default:
		return "chatgpt web authentication failed"
	}
}

var errChatGPTWebLoginTaskInputTooLarge = errors.New("chatgpt web login file is too large")

func readChatGPTWebLoginTaskInput(c *gin.Context) ([]chatGPTWebLoginInput, error) {
	if c == nil || c.Request == nil {
		return nil, errors.New("request is unavailable")
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, chatGPTWebLoginTaskMaxBodyBytes)
	mediaType, _, errMediaType := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if errMediaType != nil && strings.TrimSpace(c.GetHeader("Content-Type")) != "" {
		return nil, errors.New("invalid content type")
	}

	var data []byte
	var errRead error
	if mediaType == "multipart/form-data" {
		file, _, errFile := c.Request.FormFile("file")
		if errFile != nil {
			var maxBytesError *http.MaxBytesError
			if errors.As(errFile, &maxBytesError) {
				return nil, errFile
			}
			return nil, errors.New("file is required")
		}
		data, errRead = readLimitedChatGPTWebLoginInput(file)
		if errClose := file.Close(); errRead == nil && errClose != nil {
			errRead = errClose
		}
	} else {
		data, errRead = readLimitedChatGPTWebLoginInput(c.Request.Body)
	}
	if errRead != nil {
		return nil, errRead
	}
	return parseChatGPTWebLoginInputs(data)
}

func readLimitedChatGPTWebLoginInput(reader io.Reader) ([]byte, error) {
	data, errRead := io.ReadAll(io.LimitReader(reader, chatGPTWebLoginTaskMaxBodyBytes+1))
	if errRead != nil {
		return nil, errRead
	}
	if len(data) > chatGPTWebLoginTaskMaxBodyBytes {
		return nil, errChatGPTWebLoginTaskInputTooLarge
	}
	return data, nil
}

func parseChatGPTWebLoginInputs(data []byte) ([]chatGPTWebLoginInput, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, errors.New("login file is empty")
	}
	if bytes.IndexByte(data, 0) >= 0 {
		return nil, errors.New("login file contains invalid data")
	}
	inputs := make([]chatGPTWebLoginInput, 0, chatGPTWebLoginTaskMaxAccounts)
	seen := make(map[string]int, chatGPTWebLoginTaskMaxAccounts)
	const delimiter = "---"
	for lineNumber := 1; len(data) > 0; lineNumber++ {
		rawLine := data
		if lineEnd := bytes.IndexByte(data, '\n'); lineEnd >= 0 {
			rawLine = data[:lineEnd]
			data = data[lineEnd+1:]
		} else {
			data = nil
		}
		rawLine = bytes.TrimSuffix(rawLine, []byte{'\r'})
		if len(bytes.TrimSpace(rawLine)) == 0 {
			continue
		}
		if len(rawLine) > chatGPTWebLoginTaskMaxLineBytes {
			return nil, fmt.Errorf("line %d is too long", lineNumber)
		}
		line := string(rawLine)
		first := strings.Index(line, delimiter)
		last := strings.LastIndex(line, delimiter)
		if first <= 0 || last <= first {
			return nil, fmt.Errorf("line %d must use email---password---totp_secret", lineNumber)
		}
		email := strings.Clone(strings.TrimSpace(line[:first]))
		password := line[first+len(delimiter) : last]
		totpSecret := strings.TrimSpace(line[last+len(delimiter):])
		if email == "" || strings.TrimSpace(password) == "" {
			return nil, fmt.Errorf("line %d has an empty email or password", lineNumber)
		}
		key := normalizeChatGPTWebLoginEmail(email)
		if previous := seen[key]; previous > 0 {
			return nil, fmt.Errorf("line %d duplicates email from line %d", lineNumber, previous)
		}
		if len(inputs) >= chatGPTWebLoginTaskMaxAccounts {
			return nil, fmt.Errorf("login file exceeds %d accounts", chatGPTWebLoginTaskMaxAccounts)
		}
		seen[key] = lineNumber
		inputs = append(inputs, chatGPTWebLoginInput{
			Line:       lineNumber,
			Email:      email,
			Password:   password,
			TOTPSecret: totpSecret,
		})
	}
	if len(inputs) == 0 {
		return nil, errors.New("login file contains no accounts")
	}
	return inputs, nil
}
