package management

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
)

const (
	agentIdentityTaskQueued              = "queued"
	agentIdentityTaskRunning             = "running"
	agentIdentityTaskCanceling           = "canceling"
	agentIdentityTaskCompleted           = "completed"
	agentIdentityTaskCompletedWithErrors = "completed_with_errors"
	agentIdentityTaskCanceled            = "canceled"

	agentIdentityItemQueued    = "queued"
	agentIdentityItemRunning   = "running"
	agentIdentityItemFailed    = "failed"
	agentIdentityItemCanceled  = "canceled"
	agentIdentityItemCreated   = "created"
	agentIdentityItemUpdated   = "updated"
	agentIdentityItemUnchanged = "unchanged"

	agentIdentityTaskWorkers     = 4
	agentIdentityTaskMaxActive   = 4
	agentIdentityTaskMaxRetained = 32
	agentIdentityTaskRetention   = 24 * time.Hour
)

var (
	errAgentIdentityTaskCapacity = errors.New("Agent Identity conversion task capacity reached")
	errAgentIdentityTaskClosed   = errors.New("Agent Identity conversion tasks are unavailable")
)

type codexAgentIdentityTaskResult struct {
	SourceName      string `json:"source_name"`
	TargetName      string `json:"target_name,omitempty"`
	SourceMode      string `json:"source_mode,omitempty"`
	TargetMode      string `json:"target_mode"`
	Email           string `json:"email,omitempty"`
	AccountID       string `json:"account_id,omitempty"`
	PlanType        string `json:"plan_type,omitempty"`
	Stage           string `json:"stage"`
	ProgressPercent int    `json:"progress_percent"`
	Status          string `json:"status"`
	ErrorCategory   string `json:"error_category,omitempty"`
	Error           string `json:"error,omitempty"`
}

type codexAgentIdentityTask struct {
	ID              string                         `json:"id"`
	Status          string                         `json:"status"`
	CreatedAt       time.Time                      `json:"created_at"`
	StartedAt       *time.Time                     `json:"started_at,omitempty"`
	CompletedAt     *time.Time                     `json:"completed_at,omitempty"`
	Total           int                            `json:"total"`
	Processed       int                            `json:"processed"`
	Succeeded       int                            `json:"succeeded"`
	Failed          int                            `json:"failed"`
	Canceled        int                            `json:"canceled"`
	ProgressPercent int                            `json:"progress_percent"`
	Results         []codexAgentIdentityTaskResult `json:"results"`

	cancel        context.CancelFunc
	progressTotal int64
}

type codexAgentIdentityTaskManager struct {
	mu           sync.Mutex
	tasks        map[string]*codexAgentIdentityTask
	slots        chan struct{}
	now          func() time.Time
	rootCtx      context.Context
	rootCancel   context.CancelFunc
	workers      sync.WaitGroup
	shutdownOnce sync.Once
	shutdownDone chan struct{}
	closed       bool
}

func newCodexAgentIdentityTaskManager() *codexAgentIdentityTaskManager {
	rootCtx, rootCancel := context.WithCancel(context.Background())
	return &codexAgentIdentityTaskManager{
		tasks:        make(map[string]*codexAgentIdentityTask),
		slots:        make(chan struct{}, agentIdentityTaskWorkers),
		now:          time.Now,
		rootCtx:      rootCtx,
		rootCancel:   rootCancel,
		shutdownDone: make(chan struct{}),
	}
}

func (m *codexAgentIdentityTaskManager) create(results []codexAgentIdentityTaskResult) (*codexAgentIdentityTask, context.Context, error) {
	if m == nil {
		return nil, nil, errAgentIdentityTaskClosed
	}
	if len(results) == 0 {
		return nil, nil, errAgentIdentityTaskCapacity
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	if m.closed {
		return nil, nil, errAgentIdentityTaskClosed
	}
	if m.activeCountLocked() >= agentIdentityTaskMaxActive {
		return nil, nil, errAgentIdentityTaskCapacity
	}
	if len(m.tasks) >= agentIdentityTaskMaxRetained {
		m.pruneOldestTerminalLocked(len(m.tasks) - agentIdentityTaskMaxRetained + 1)
		if len(m.tasks) >= agentIdentityTaskMaxRetained {
			return nil, nil, errAgentIdentityTaskCapacity
		}
	}
	ctx, cancel := context.WithCancel(m.rootCtx)
	task := &codexAgentIdentityTask{
		ID:        uuid.NewString(),
		Status:    agentIdentityTaskQueued,
		CreatedAt: m.currentTime(),
		Total:     len(results),
		Results:   append([]codexAgentIdentityTaskResult(nil), results...),
		cancel:    cancel,
	}
	m.tasks[task.ID] = task
	m.workers.Add(1)
	return cloneCodexAgentIdentityTask(task), ctx, nil
}

func (m *codexAgentIdentityTaskManager) start(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.tasks[id]
	if task == nil || task.Status != agentIdentityTaskQueued {
		return false
	}
	now := m.currentTime()
	task.Status = agentIdentityTaskRunning
	task.StartedAt = &now
	return true
}

func (m *codexAgentIdentityTaskManager) markRunning(id string, index int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.tasks[id]
	if task == nil || task.Status != agentIdentityTaskRunning || index < 0 || index >= len(task.Results) || task.Results[index].Status != agentIdentityItemQueued {
		return false
	}
	task.Results[index].Status = agentIdentityItemRunning
	m.updateStageLocked(task, index, "validating", 5)
	return true
}

func (m *codexAgentIdentityTaskManager) updateStage(id string, index int, stage string, progress int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.tasks[id]
	if task == nil || task.Status != agentIdentityTaskRunning || index < 0 || index >= len(task.Results) || task.Results[index].Status != agentIdentityItemRunning {
		return false
	}
	m.updateStageLocked(task, index, stage, progress)
	return true
}

func (m *codexAgentIdentityTaskManager) updateIdentity(id string, index int, email, accountID, planType string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.tasks[id]
	if task == nil || index < 0 || index >= len(task.Results) {
		return
	}
	task.Results[index].Email = email
	task.Results[index].AccountID = accountID
	task.Results[index].PlanType = planType
}

func (m *codexAgentIdentityTaskManager) updateModes(id string, index int, sourceMode, targetMode string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.tasks[id]
	if task == nil || index < 0 || index >= len(task.Results) {
		return
	}
	task.Results[index].SourceMode = sourceMode
	task.Results[index].TargetMode = targetMode
}

func (m *codexAgentIdentityTaskManager) beginCommit(id string, index int) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.tasks[id]
	if m.closed || task == nil || task.Status != agentIdentityTaskRunning || index < 0 || index >= len(task.Results) || task.Results[index].Status != agentIdentityItemRunning {
		return false
	}
	m.updateStageLocked(task, index, "persisting", 85)
	return true
}

func (m *codexAgentIdentityTaskManager) setResult(id string, index int, result codexAgentIdentityTaskResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.tasks[id]
	if task == nil || index < 0 || index >= len(task.Results) {
		return
	}
	previous := task.Results[index].Status
	if previous != agentIdentityItemQueued && previous != agentIdentityItemRunning {
		return
	}
	if result.ProgressPercent < task.Results[index].ProgressPercent {
		result.ProgressPercent = task.Results[index].ProgressPercent
		if result.Status != agentIdentityItemCanceled {
			result.Stage = task.Results[index].Stage
		}
	}
	if result.Stage == "" {
		result.Stage = task.Results[index].Stage
	}
	result.ProgressPercent = 100
	if result.Stage == "" || result.Status == agentIdentityItemCreated || result.Status == agentIdentityItemUpdated || result.Status == agentIdentityItemUnchanged {
		result.Stage = "completed"
	}
	previousProgress := task.Results[index].ProgressPercent
	task.Results[index] = result
	task.progressTotal += int64(result.ProgressPercent - previousProgress)
	task.Processed++
	switch result.Status {
	case agentIdentityItemCreated, agentIdentityItemUpdated, agentIdentityItemUnchanged:
		task.Succeeded++
	case agentIdentityItemCanceled:
		task.Canceled++
	default:
		task.Failed++
	}
	m.recalculateProgressLocked(task)
}

func (m *codexAgentIdentityTaskManager) finish(id string, canceled bool) {
	m.mu.Lock()
	task := m.tasks[id]
	if task == nil || isTerminalAgentIdentityTaskStatus(task.Status) {
		m.mu.Unlock()
		return
	}
	if canceled || task.Status == agentIdentityTaskCanceling {
		for index := range task.Results {
			if task.Results[index].Status != agentIdentityItemQueued && task.Results[index].Status != agentIdentityItemRunning {
				continue
			}
			previousProgress := task.Results[index].ProgressPercent
			task.Results[index].Status = agentIdentityItemCanceled
			task.Results[index].Stage = "canceled"
			task.Results[index].ErrorCategory = "canceled"
			task.Results[index].Error = "conversion canceled"
			task.progressTotal += int64(100 - previousProgress)
			task.Results[index].ProgressPercent = 100
			task.Processed++
			task.Canceled++
		}
	}
	now := m.currentTime()
	task.CompletedAt = &now
	task.ProgressPercent = 100
	switch {
	case task.Canceled == task.Total:
		task.Status = agentIdentityTaskCanceled
	case task.Failed > 0 || task.Canceled > 0:
		task.Status = agentIdentityTaskCompletedWithErrors
	default:
		task.Status = agentIdentityTaskCompleted
	}
	cancel := task.cancel
	task.cancel = nil
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	m.workers.Done()
}

func (m *codexAgentIdentityTaskManager) get(id string) (*codexAgentIdentityTask, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
	task := m.tasks[id]
	return cloneCodexAgentIdentityTask(task), task != nil
}

func (m *codexAgentIdentityTaskManager) cancel(id string) (*codexAgentIdentityTask, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := m.tasks[id]
	if task == nil {
		return nil, false
	}
	if isTerminalAgentIdentityTaskStatus(task.Status) {
		return cloneCodexAgentIdentityTask(task), true
	}
	task.Status = agentIdentityTaskCanceling
	if task.cancel != nil {
		task.cancel()
	}
	return cloneCodexAgentIdentityTask(task), true
}

func (m *codexAgentIdentityTaskManager) acquireSlot(ctx context.Context) bool {
	select {
	case m.slots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (m *codexAgentIdentityTaskManager) releaseSlot() { <-m.slots }

func (m *codexAgentIdentityTaskManager) lifecycleContext() context.Context {
	if m == nil || m.rootCtx == nil {
		return context.Background()
	}
	return m.rootCtx
}

func (m *codexAgentIdentityTaskManager) prune() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pruneLocked()
}

func (m *codexAgentIdentityTaskManager) shutdown(ctx context.Context) error {
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

func (m *codexAgentIdentityTaskManager) updateStageLocked(task *codexAgentIdentityTask, index int, stage string, progress int) {
	if progress < task.Results[index].ProgressPercent {
		progress = task.Results[index].ProgressPercent
	}
	if progress > 100 {
		progress = 100
	}
	previousProgress := task.Results[index].ProgressPercent
	task.Results[index].Stage = stage
	task.Results[index].ProgressPercent = progress
	task.progressTotal += int64(progress - previousProgress)
	m.recalculateProgressLocked(task)
}

func (m *codexAgentIdentityTaskManager) recalculateProgressLocked(task *codexAgentIdentityTask) {
	if task == nil || task.Total == 0 {
		return
	}
	progress := int(task.progressTotal / int64(task.Total))
	if progress > task.ProgressPercent {
		task.ProgressPercent = progress
	}
}

func (m *codexAgentIdentityTaskManager) activeCountLocked() int {
	count := 0
	for _, task := range m.tasks {
		if task != nil && !isTerminalAgentIdentityTaskStatus(task.Status) {
			count++
		}
	}
	return count
}

func (m *codexAgentIdentityTaskManager) pruneLocked() {
	cutoff := m.currentTime().Add(-agentIdentityTaskRetention)
	for id, task := range m.tasks {
		if task != nil && task.CompletedAt != nil && task.CompletedAt.Before(cutoff) {
			delete(m.tasks, id)
		}
	}
}

func (m *codexAgentIdentityTaskManager) pruneOldestTerminalLocked(count int) {
	for count > 0 {
		var oldest *codexAgentIdentityTask
		for _, task := range m.tasks {
			if task == nil || task.CompletedAt == nil || !isTerminalAgentIdentityTaskStatus(task.Status) {
				continue
			}
			if oldest == nil || task.CompletedAt.Before(*oldest.CompletedAt) {
				oldest = task
			}
		}
		if oldest == nil {
			return
		}
		delete(m.tasks, oldest.ID)
		count--
	}
}

func (m *codexAgentIdentityTaskManager) currentTime() time.Time {
	if m.now != nil {
		return m.now()
	}
	return time.Now()
}

func cloneCodexAgentIdentityTask(task *codexAgentIdentityTask) *codexAgentIdentityTask {
	if task == nil {
		return nil
	}
	clone := *task
	clone.Results = append([]codexAgentIdentityTaskResult(nil), task.Results...)
	clone.cancel = nil
	return &clone
}

func isTerminalAgentIdentityTaskStatus(status string) bool {
	switch status {
	case agentIdentityTaskCompleted, agentIdentityTaskCompletedWithErrors, agentIdentityTaskCanceled:
		return true
	default:
		return false
	}
}
