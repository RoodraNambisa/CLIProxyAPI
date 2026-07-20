package management

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	chatGPTWebMutationTaskImport     = "import"
	chatGPTWebMutationTaskConversion = "conversion"
	chatGPTWebMutationTaskWorkers    = 4
	chatGPTWebMutationTaskMaxActive  = 4
)

type chatGPTWebMutationTaskResult struct {
	File           string `json:"file,omitempty"`
	SourceName     string `json:"source_name,omitempty"`
	Email          string `json:"email,omitempty"`
	Status         string `json:"status"`
	Name           string `json:"name,omitempty"`
	TargetName     string `json:"target_name,omitempty"`
	AuthIndex      string `json:"auth_index,omitempty"`
	CredentialMode string `json:"credential_mode,omitempty"`
	ErrorCategory  string `json:"error_category,omitempty"`
	Error          string `json:"error,omitempty"`
	HTTPStatus     int    `json:"http_status,omitempty"`
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

type chatGPTWebMutationTaskManager struct {
	mu           sync.Mutex
	tasks        map[string]map[string]*chatGPTWebMutationTask
	slots        chan struct{}
	now          func() time.Time
	rootCtx      context.Context
	rootCancel   context.CancelFunc
	workers      sync.WaitGroup
	shutdownOnce sync.Once
	shutdownDone chan struct{}
	closed       bool
}

func newChatGPTWebMutationTaskManager() *chatGPTWebMutationTaskManager {
	rootCtx, rootCancel := context.WithCancel(context.Background())
	return &chatGPTWebMutationTaskManager{
		tasks: map[string]map[string]*chatGPTWebMutationTask{
			chatGPTWebMutationTaskImport:     {},
			chatGPTWebMutationTaskConversion: {},
		},
		slots:        make(chan struct{}, chatGPTWebMutationTaskWorkers),
		now:          time.Now,
		rootCtx:      rootCtx,
		rootCancel:   rootCancel,
		shutdownDone: make(chan struct{}),
	}
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

func (m *chatGPTWebMutationTaskManager) acquireSlot(ctx context.Context) bool {
	select {
	case m.slots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (m *chatGPTWebMutationTaskManager) releaseSlot() { <-m.slots }

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
