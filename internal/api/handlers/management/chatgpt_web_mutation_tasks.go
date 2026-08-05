package management

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const (
	chatGPTWebMutationTaskImport     = "import"
	chatGPTWebMutationTaskConversion = "conversion"
	chatGPTWebMutationTaskMaxActive  = 4
	chatGPTWebConversionTaskWorkers  = 4
)

type chatGPTWebMutationTaskResult struct {
	File                    string   `json:"file,omitempty"`
	SourceName              string   `json:"source_name,omitempty"`
	Email                   string   `json:"email,omitempty"`
	Status                  string   `json:"status"`
	Name                    string   `json:"name,omitempty"`
	TargetName              string   `json:"target_name,omitempty"`
	AuthIndex               string   `json:"auth_index,omitempty"`
	CredentialMode          string   `json:"credential_mode,omitempty"`
	CredentialSchemaVersion int      `json:"credential_schema_version,omitempty"`
	PersistedFeatures       []string `json:"persisted_features,omitempty"`
	WebAuthnV1Persisted     bool     `json:"webauthn_v1_persisted,omitempty"`
	ErrorCategory           string   `json:"error_category,omitempty"`
	Error                   string   `json:"error,omitempty"`
	HTTPStatus              int      `json:"http_status,omitempty"`
	SessionRefreshState     string   `json:"session_refresh_state,omitempty"`
	ModelValidationState    string   `json:"model_validation_state,omitempty"`
	AccountInfoRefreshState string   `json:"account_info_refresh_state,omitempty"`
}

type chatGPTWebMutationTask struct {
	ID          string                         `json:"id"`
	Kind        string                         `json:"kind"`
	State       string                         `json:"state"`
	CreatedAt   time.Time                      `json:"created_at"`
	StartedAt   *time.Time                     `json:"started_at,omitempty"`
	CompletedAt *time.Time                     `json:"completed_at,omitempty"`
	Total       int                            `json:"total"`
	Processed   int                            `json:"processed"`
	Succeeded   int                            `json:"succeeded"`
	Failed      int                            `json:"failed"`
	Canceled    int                            `json:"canceled"`
	Results     []chatGPTWebMutationTaskResult `json:"results"`

	cancel context.CancelFunc
}

type chatGPTWebImportRuntimeSnapshot struct {
	QueuedEntries  int `json:"queued_entries"`
	RunningEntries int `json:"running_entries"`
	ActiveWorkers  int `json:"active_workers"`
	WorkerLimit    int `json:"worker_limit"`
}

type chatGPTWebImportReservation struct {
	target string
	auth   *coreauth.Auth
}

type chatGPTWebMutationTaskManager struct {
	mu              sync.Mutex
	tasks           map[string]map[string]*chatGPTWebMutationTask
	slotMu          sync.Mutex
	slotCond        *sync.Cond
	workerLimit     int
	activeSlots     int
	nextTicket      uint64
	servingTicket   uint64
	canceledTickets map[uint64]struct{}
	conversionSlots chan struct{}
	reservationSeq  uint64
	reservations    map[uint64]chatGPTWebImportReservation
	now             func() time.Time
	rootCtx         context.Context
	rootCancel      context.CancelFunc
	workers         sync.WaitGroup
	shutdownOnce    sync.Once
	shutdownDone    chan struct{}
	closed          bool
}

func newChatGPTWebMutationTaskManager() *chatGPTWebMutationTaskManager {
	rootCtx, rootCancel := context.WithCancel(context.Background())
	manager := &chatGPTWebMutationTaskManager{
		tasks: map[string]map[string]*chatGPTWebMutationTask{
			chatGPTWebMutationTaskImport:     {},
			chatGPTWebMutationTaskConversion: {},
		},
		workerLimit:     config.DefaultChatGPTWebImportWorkers,
		canceledTickets: make(map[uint64]struct{}),
		conversionSlots: make(chan struct{}, chatGPTWebConversionTaskWorkers),
		reservations:    make(map[uint64]chatGPTWebImportReservation),
		now:             time.Now,
		rootCtx:         rootCtx,
		rootCancel:      rootCancel,
		shutdownDone:    make(chan struct{}),
	}
	manager.slotCond = sync.NewCond(&manager.slotMu)
	return manager
}

func (m *chatGPTWebMutationTaskManager) reserveImport(target string, auth *coreauth.Auth) (func(), string) {
	if m == nil || auth == nil {
		return func() {}, ""
	}
	target = strings.TrimSpace(target)
	m.mu.Lock()
	for _, current := range m.reservations {
		if target != "" && strings.EqualFold(target, current.target) {
			m.mu.Unlock()
			return nil, "target"
		}
		if current.auth != nil && coreauth.ChatGPTWebCredentialIdentityConflict(current.auth, auth) {
			m.mu.Unlock()
			return nil, "identity"
		}
	}
	m.reservationSeq++
	reservationID := m.reservationSeq
	m.reservations[reservationID] = chatGPTWebImportReservation{target: target, auth: auth.Clone()}
	m.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			delete(m.reservations, reservationID)
			m.mu.Unlock()
		})
	}, ""
}

func (m *chatGPTWebMutationTaskManager) create(kind string, results []chatGPTWebMutationTaskResult) (*chatGPTWebMutationTask, context.Context, error) {
	if m == nil {
		return nil, nil, errors.New("chatgpt web task manager is unavailable")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked(kind)
	if m.closed {
		return nil, nil, errChatGPTWebLoginTaskClosed
	}
	tasks := m.tasks[kind]
	if tasks == nil {
		return nil, nil, errors.New("unsupported chatgpt web task kind")
	}
	if m.activeTaskCountLocked() >= chatGPTWebMutationTaskMaxActive {
		return nil, nil, errChatGPTWebLoginTaskCapacity
	}
	if len(tasks) >= chatGPTWebLoginTaskMaxRetained {
		m.pruneOldestTerminalLocked(kind, len(tasks)-chatGPTWebLoginTaskMaxRetained+1)
		if len(tasks) >= chatGPTWebLoginTaskMaxRetained {
			return nil, nil, errChatGPTWebLoginTaskCapacity
		}
	}
	ctx, cancel := context.WithCancel(m.rootCtx)
	task := &chatGPTWebMutationTask{
		ID:        uuid.NewString(),
		Kind:      kind,
		State:     chatGPTWebLoginTaskQueued,
		CreatedAt: m.currentTime(),
		Total:     len(results),
		Results:   append([]chatGPTWebMutationTaskResult(nil), results...),
		cancel:    cancel,
	}
	tasks[task.ID] = task
	m.workers.Add(1)
	return cloneChatGPTWebMutationTask(task), ctx, nil
}

func (m *chatGPTWebMutationTaskManager) activeTaskCountLocked() int {
	count := 0
	for _, tasks := range m.tasks {
		for _, task := range tasks {
			if task != nil && !isTerminalChatGPTWebLoginTaskState(task.State) {
				count++
			}
		}
	}
	return count
}

func (m *chatGPTWebMutationTaskManager) start(kind, id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.taskLocked(kind, id)
	if task == nil || task.State != chatGPTWebLoginTaskQueued {
		return false
	}
	now := m.currentTime()
	task.State = chatGPTWebLoginTaskRunning
	task.StartedAt = &now
	return true
}

func (m *chatGPTWebMutationTaskManager) markRunning(kind, id string, index int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.taskLocked(kind, id)
	if task == nil || task.State != chatGPTWebLoginTaskRunning || index < 0 || index >= len(task.Results) || task.Results[index].Status != chatGPTWebLoginResultQueued {
		return false
	}
	task.Results[index].Status = chatGPTWebLoginResultRunning
	return true
}

func (m *chatGPTWebMutationTaskManager) beginCommit(kind, id string, index int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.taskLocked(kind, id)
	if m.closed || task == nil || task.State != chatGPTWebLoginTaskRunning || index < 0 || index >= len(task.Results) || task.Results[index].Status != chatGPTWebLoginResultRunning {
		return false
	}
	task.Results[index].Status = chatGPTWebLoginResultCommit
	return true
}

func (m *chatGPTWebMutationTaskManager) setResult(kind, id string, index int, result chatGPTWebMutationTaskResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.taskLocked(kind, id)
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
	case "created", "updated", "unchanged":
		task.Succeeded++
	case chatGPTWebLoginResultCanceled:
		task.Canceled++
	default:
		task.Failed++
	}
}

func (m *chatGPTWebMutationTaskManager) finish(kind, id string, canceled bool) {
	m.mu.Lock()
	task := m.taskLocked(kind, id)
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
	now := m.currentTime()
	task.CompletedAt = &now
	switch {
	case task.Canceled == task.Total:
		task.State = chatGPTWebLoginTaskCanceled
	case task.Failed > 0 || task.Canceled > 0:
		task.State = chatGPTWebLoginTaskCompletedWithErrors
	default:
		task.State = chatGPTWebLoginTaskCompleted
	}
	cancel := task.cancel
	task.cancel = nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	m.workers.Done()
}

func (m *chatGPTWebMutationTaskManager) get(kind, id string) (*chatGPTWebMutationTask, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked(kind)
	task := m.taskLocked(kind, id)
	return cloneChatGPTWebMutationTask(task), task != nil
}

func (m *chatGPTWebMutationTaskManager) cancel(kind, id string) (*chatGPTWebMutationTask, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.taskLocked(kind, id)
	if task == nil {
		return nil, false
	}
	if isTerminalChatGPTWebLoginTaskState(task.State) {
		return cloneChatGPTWebMutationTask(task), true
	}
	task.State = chatGPTWebLoginTaskCanceling
	if task.cancel != nil {
		task.cancel()
	}
	return cloneChatGPTWebMutationTask(task), true
}

func (m *chatGPTWebMutationTaskManager) acquireImportSlot(ctx context.Context) bool {
	if m == nil {
		return false
	}
	m.slotMu.Lock()
	ticket := m.nextTicket
	m.nextTicket++
	stopCancel := context.AfterFunc(ctx, func() {
		m.slotMu.Lock()
		if ticket >= m.servingTicket {
			m.canceledTickets[ticket] = struct{}{}
		}
		m.advanceCanceledTicketsLocked()
		m.slotCond.Broadcast()
		m.slotMu.Unlock()
	})
	defer stopCancel()
	for {
		m.advanceCanceledTicketsLocked()
		if ctx.Err() != nil {
			if ticket >= m.servingTicket {
				m.canceledTickets[ticket] = struct{}{}
			}
			m.advanceCanceledTicketsLocked()
			m.slotCond.Broadcast()
			m.slotMu.Unlock()
			return false
		}
		if ticket == m.servingTicket && m.activeSlots < m.workerLimit {
			m.servingTicket++
			m.activeSlots++
			m.advanceCanceledTicketsLocked()
			m.slotCond.Broadcast()
			m.slotMu.Unlock()
			return true
		}
		m.slotCond.Wait()
	}
}

func (m *chatGPTWebMutationTaskManager) releaseImportSlot() {
	if m == nil {
		return
	}
	m.slotMu.Lock()
	if m.activeSlots > 0 {
		m.activeSlots--
	}
	m.slotCond.Broadcast()
	m.slotMu.Unlock()
}

func (m *chatGPTWebMutationTaskManager) acquireConversionSlot(ctx context.Context) bool {
	if m == nil {
		return false
	}
	select {
	case m.conversionSlots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (m *chatGPTWebMutationTaskManager) releaseConversionSlot() {
	if m == nil {
		return
	}
	select {
	case <-m.conversionSlots:
	default:
	}
}

func (m *chatGPTWebMutationTaskManager) advanceCanceledTicketsLocked() {
	for {
		if _, canceled := m.canceledTickets[m.servingTicket]; !canceled {
			return
		}
		delete(m.canceledTickets, m.servingTicket)
		m.servingTicket++
	}
}

func (m *chatGPTWebMutationTaskManager) updateWorkerLimit(workers int) {
	if m == nil {
		return
	}
	if workers < 1 {
		workers = config.DefaultChatGPTWebImportWorkers
	}
	m.slotMu.Lock()
	m.workerLimit = workers
	m.slotCond.Broadcast()
	m.slotMu.Unlock()
}

func (m *chatGPTWebMutationTaskManager) workerLimitSnapshot() int {
	if m == nil {
		return config.DefaultChatGPTWebImportWorkers
	}
	m.slotMu.Lock()
	workers := m.workerLimit
	m.slotMu.Unlock()
	return workers
}

func (m *chatGPTWebMutationTaskManager) importRuntimeSnapshot() chatGPTWebImportRuntimeSnapshot {
	if m == nil {
		return chatGPTWebImportRuntimeSnapshot{}
	}
	m.mu.Lock()
	snapshot := chatGPTWebImportRuntimeSnapshot{}
	for _, task := range m.tasks[chatGPTWebMutationTaskImport] {
		if task == nil || isTerminalChatGPTWebLoginTaskState(task.State) {
			continue
		}
		for index := range task.Results {
			switch task.Results[index].Status {
			case chatGPTWebLoginResultQueued:
				snapshot.QueuedEntries++
			case chatGPTWebLoginResultRunning, chatGPTWebLoginResultCommit:
				snapshot.RunningEntries++
			}
		}
	}
	m.mu.Unlock()
	m.slotMu.Lock()
	snapshot.ActiveWorkers = m.activeSlots
	snapshot.WorkerLimit = m.workerLimit
	m.slotMu.Unlock()
	return snapshot
}

func (m *chatGPTWebMutationTaskManager) lifecycleContext() context.Context {
	if m == nil || m.rootCtx == nil {
		return context.Background()
	}
	return m.rootCtx
}

func (m *chatGPTWebMutationTaskManager) prune() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for kind := range m.tasks {
		m.pruneLocked(kind)
	}
}

func (m *chatGPTWebMutationTaskManager) pruneLocked(kind string) {
	cutoff := m.currentTime().Add(-chatGPTWebLoginTaskRetention)
	for id, task := range m.tasks[kind] {
		if task.CompletedAt != nil && task.CompletedAt.Before(cutoff) {
			delete(m.tasks[kind], id)
		}
	}
}

func (m *chatGPTWebMutationTaskManager) pruneOldestTerminalLocked(kind string, count int) {
	tasks := m.tasks[kind]
	for count > 0 {
		var oldest *chatGPTWebMutationTask
		for _, task := range tasks {
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
		delete(tasks, oldest.ID)
		count--
	}
}

func (m *chatGPTWebMutationTaskManager) shutdown(ctx context.Context) error {
	if m == nil {
		return nil
	}
	m.shutdownOnce.Do(func() {
		m.mu.Lock()
		m.closed = true
		m.rootCancel()
		m.mu.Unlock()
		go func() {
			m.workers.Wait()
			close(m.shutdownDone)
		}()
	})
	select {
	case <-m.shutdownDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *chatGPTWebMutationTaskManager) taskLocked(kind, id string) *chatGPTWebMutationTask {
	return m.tasks[kind][id]
}

func (m *chatGPTWebMutationTaskManager) currentTime() time.Time {
	if m.now != nil {
		return m.now()
	}
	return time.Now()
}

func cloneChatGPTWebMutationTask(task *chatGPTWebMutationTask) *chatGPTWebMutationTask {
	if task == nil {
		return nil
	}
	clone := *task
	clone.Results = append([]chatGPTWebMutationTaskResult(nil), task.Results...)
	clone.cancel = nil
	return &clone
}

func chatGPTWebMutationTaskHTTPStatus(task *chatGPTWebMutationTask) int {
	if task != nil && isTerminalChatGPTWebLoginTaskState(task.State) && (task.Failed > 0 || task.Canceled > 0) {
		return 207
	}
	return 200
}
