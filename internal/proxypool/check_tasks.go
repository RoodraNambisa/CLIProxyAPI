package proxypool

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
)

const (
	CheckTaskStatusQueued    = "queued"
	CheckTaskStatusRunning   = "running"
	CheckTaskStatusCompleted = "completed"
	CheckTaskStatusFailed    = "failed"

	maxProxyCheckTaskRecords = 64
	maxProxyCheckTaskResults = 256
)

var (
	ErrCheckTaskConflict = errors.New("proxy check task already active")
	ErrCheckTaskCapacity = errors.New("proxy check task capacity reached")
)

type checkTaskState struct {
	snapshot CheckTask
}

// CheckTaskConflictError identifies the already active task for one pool.
type CheckTaskConflictError struct {
	TaskID string
}

func (err *CheckTaskConflictError) Error() string { return ErrCheckTaskConflict.Error() }
func (err *CheckTaskConflictError) Unwrap() error { return ErrCheckTaskConflict }

// StartCheckTask creates a request-independent asynchronous pool check.
func (m *Manager) StartCheckTask(poolName string, sample int) (CheckTask, error) {
	if m == nil {
		return CheckTask{}, errors.New("proxy pool manager unavailable")
	}
	poolKey := strings.ToLower(strings.TrimSpace(poolName))
	if poolKey == "" || !m.poolExists(poolKey) {
		return CheckTask{}, errors.New("proxy pool not found")
	}
	if sample < 0 {
		return CheckTask{}, errors.New("proxy check sample cannot be negative")
	}

	m.taskMu.Lock()
	m.pruneCheckTasksLocked()
	if taskID := m.taskByPool[poolKey]; taskID != "" {
		if task := m.checkTasks[taskID]; task != nil && checkTaskActive(task.snapshot.Status) {
			snapshot := cloneCheckTask(task.snapshot)
			m.taskMu.Unlock()
			return snapshot, &CheckTaskConflictError{TaskID: taskID}
		}
		delete(m.taskByPool, poolKey)
	}
	if len(m.checkTasks) >= maxProxyCheckTaskRecords {
		m.taskMu.Unlock()
		return CheckTask{}, ErrCheckTaskCapacity
	}
	taskID, errID := newCheckTaskID()
	if errID != nil {
		m.taskMu.Unlock()
		return CheckTask{}, errors.New("failed to create proxy check task")
	}
	task := &checkTaskState{snapshot: CheckTask{
		ID:        taskID,
		Pool:      strings.TrimSpace(poolName),
		Status:    CheckTaskStatusQueued,
		CreatedAt: m.now().UTC(),
	}}
	m.checkTasks[taskID] = task
	m.taskByPool[poolKey] = taskID
	m.taskOrder = append(m.taskOrder, taskID)
	taskContext := m.ensureCheckTaskContextLocked()
	m.taskWG.Add(1)
	snapshot := cloneCheckTask(task.snapshot)
	m.taskMu.Unlock()

	go m.runCheckTask(taskContext, poolKey, taskID, sample)
	return snapshot, nil
}

// CheckTasks returns the active and recent tasks for one pool, newest first.
func (m *Manager) CheckTasks(poolName string) []CheckTask {
	if m == nil {
		return nil
	}
	poolKey := strings.ToLower(strings.TrimSpace(poolName))
	m.taskMu.Lock()
	defer m.taskMu.Unlock()
	out := make([]CheckTask, 0)
	for index := len(m.taskOrder) - 1; index >= 0; index-- {
		task := m.checkTasks[m.taskOrder[index]]
		if task == nil || strings.ToLower(strings.TrimSpace(task.snapshot.Pool)) != poolKey {
			continue
		}
		out = append(out, cloneCheckTask(task.snapshot))
	}
	return out
}

// CheckTask returns one task when it belongs to the requested pool.
func (m *Manager) CheckTask(poolName, taskID string) (CheckTask, bool) {
	if m == nil {
		return CheckTask{}, false
	}
	poolKey := strings.ToLower(strings.TrimSpace(poolName))
	m.taskMu.Lock()
	defer m.taskMu.Unlock()
	task := m.checkTasks[strings.TrimSpace(taskID)]
	if task == nil || strings.ToLower(strings.TrimSpace(task.snapshot.Pool)) != poolKey {
		return CheckTask{}, false
	}
	return cloneCheckTask(task.snapshot), true
}

func (m *Manager) runCheckTask(ctx context.Context, poolKey, taskID string, sample int) {
	defer m.taskWG.Done()
	observer := &poolCheckObserver{
		prepared: func(total, bound, sampled int) {
			m.updateCheckTask(taskID, func(task *CheckTask) {
				now := m.now().UTC()
				task.Status = CheckTaskStatusRunning
				task.StartedAt = &now
				task.Total = total
				task.Bound = bound
				task.Sampled = sampled
			})
		},
		started: func() {
			m.updateCheckTask(taskID, func(task *CheckTask) { task.Running++ })
		},
		result: func(result CheckResult) {
			m.updateCheckTask(taskID, func(task *CheckTask) {
				if task.Running > 0 {
					task.Running--
				}
				task.Completed++
				if result.OK {
					task.Succeeded++
				} else {
					task.Failed++
				}
				appendCheckTaskResult(task, result)
			})
		},
	}
	_, errCheck := m.checkPool(ctx, poolKey, sample, observer, false)
	m.taskMu.Lock()
	if task := m.checkTasks[taskID]; task != nil {
		now := m.now().UTC()
		task.snapshot.Running = 0
		task.snapshot.CompletedAt = &now
		if errCheck != nil {
			task.snapshot.Status = CheckTaskStatusFailed
			task.snapshot.ErrorCode = classifyCheckTaskError(errCheck)
		} else {
			task.snapshot.Status = CheckTaskStatusCompleted
		}
	}
	if m.taskByPool[poolKey] == taskID {
		delete(m.taskByPool, poolKey)
	}
	m.taskMu.Unlock()
}

func (m *Manager) updateCheckTask(taskID string, update func(*CheckTask)) {
	if m == nil || update == nil {
		return
	}
	m.taskMu.Lock()
	if task := m.checkTasks[taskID]; task != nil {
		update(&task.snapshot)
	}
	m.taskMu.Unlock()
}

func appendCheckTaskResult(task *CheckTask, result CheckResult) {
	if task == nil {
		return
	}
	if len(task.Results) < maxProxyCheckTaskResults {
		task.Results = append(task.Results, result)
		return
	}
	task.ResultsTruncated = true
	if result.OK && result.Bound {
		return
	}
	for index := range task.Results {
		if task.Results[index].OK && task.Results[index].Bound {
			task.Results[index] = result
			return
		}
	}
}

func (m *Manager) ensureCheckTaskContextLocked() context.Context {
	if m.taskContext == nil || m.taskContext.Err() != nil {
		m.taskContext, m.taskCancel = context.WithCancel(context.Background())
	}
	return m.taskContext
}

func (m *Manager) stopCheckTasks() {
	if m == nil {
		return
	}
	m.taskMu.Lock()
	if m.taskCancel != nil {
		m.taskCancel()
	}
	m.taskMu.Unlock()
	m.taskWG.Wait()
	m.taskMu.Lock()
	m.taskContext = nil
	m.taskCancel = nil
	m.taskMu.Unlock()
}

func (m *Manager) pruneCheckTasksLocked() {
	for len(m.checkTasks) >= maxProxyCheckTaskRecords {
		removed := false
		for index, taskID := range m.taskOrder {
			task := m.checkTasks[taskID]
			if task != nil && checkTaskActive(task.snapshot.Status) {
				continue
			}
			delete(m.checkTasks, taskID)
			m.taskOrder = append(m.taskOrder[:index], m.taskOrder[index+1:]...)
			removed = true
			break
		}
		if !removed {
			return
		}
	}
}

func (m *Manager) poolExists(poolKey string) bool {
	snapshot := m.snapshot()
	if snapshot == nil {
		return false
	}
	_, exists := snapshot.pools[strings.ToLower(strings.TrimSpace(poolKey))]
	return exists
}

func checkTaskActive(status string) bool {
	return status == CheckTaskStatusQueued || status == CheckTaskStatusRunning
}

func classifyCheckTaskError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "proxy_check_canceled"
	case errors.Is(err, errProxyConfigurationChanged):
		return "proxy_configuration_changed"
	default:
		return "proxy_check_failed"
	}
}

func cloneCheckTask(task CheckTask) CheckTask {
	task.Results = append([]CheckResult(nil), task.Results...)
	return task
}

func newCheckTaskID() (string, error) {
	var bytes [12]byte
	if _, errRead := rand.Read(bytes[:]); errRead != nil {
		return "", errRead
	}
	return hex.EncodeToString(bytes[:]), nil
}
