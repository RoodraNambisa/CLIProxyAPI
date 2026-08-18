package management

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

const usagePruneRecentTaskLimit = 16

var errUsagePruneTasksClosed = errors.New("usage prune task manager is closed")

type usagePrunePolicy struct {
	OlderThanDays       int   `json:"older_than_days"`
	MaxStorageMegabytes int64 `json:"max_storage_megabytes"`
}

type usagePruneTaskSnapshot struct {
	TaskID              string           `json:"task_id"`
	Status              string           `json:"status"`
	CreatedAt           time.Time        `json:"created_at"`
	StartedAt           *time.Time       `json:"started_at,omitempty"`
	CompletedAt         *time.Time       `json:"completed_at,omitempty"`
	Policy              usagePrunePolicy `json:"policy"`
	SafeErrorCode       string           `json:"safe_error_code,omitempty"`
	Processed           int              `json:"processed"`
	Pruned              int              `json:"pruned"`
	Saved               bool             `json:"saved"`
	StorageBytesBefore  int64            `json:"storage_bytes_before"`
	StorageBytesAfter   int64            `json:"storage_bytes_after"`
	DetailCountBefore   int              `json:"detail_count_before"`
	DetailCountAfter    int              `json:"detail_count_after"`
	TotalRequestsBefore int64            `json:"total_requests_before"`
	TotalRequestsAfter  int64            `json:"total_requests_after"`
}

type usagePruneTaskHistory struct {
	Active *usagePruneTaskSnapshot  `json:"active,omitempty"`
	Recent []usagePruneTaskSnapshot `json:"recent"`
}

type usagePruneOutcome struct {
	SafeErrorCode       string
	Processed           int
	Pruned              int
	Saved               bool
	StorageBytesBefore  int64
	StorageBytesAfter   int64
	DetailCountBefore   int
	DetailCountAfter    int
	TotalRequestsBefore int64
	TotalRequestsAfter  int64
}

type usagePruneRunner func(context.Context, usagePrunePolicy) usagePruneOutcome

type usagePruneTask struct {
	snapshot usagePruneTaskSnapshot
	done     chan struct{}
}

type usagePruneTaskManager struct {
	mu      sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	closing bool
	active  *usagePruneTask
	recent  []usagePruneTaskSnapshot
}

func newUsagePruneTaskManager() *usagePruneTaskManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &usagePruneTaskManager{ctx: ctx, cancel: cancel, recent: make([]usagePruneTaskSnapshot, 0, usagePruneRecentTaskLimit)}
}

func (manager *usagePruneTaskManager) submit(policy usagePrunePolicy, runner usagePruneRunner) (usagePruneTaskSnapshot, bool, error) {
	if manager == nil || runner == nil {
		return usagePruneTaskSnapshot{}, false, errUsagePruneTasksClosed
	}
	manager.mu.Lock()
	if manager.closing || manager.ctx.Err() != nil {
		manager.mu.Unlock()
		return usagePruneTaskSnapshot{}, false, errUsagePruneTasksClosed
	}
	if manager.active != nil {
		existing := manager.active.snapshot
		manager.mu.Unlock()
		return existing, true, nil
	}
	task := &usagePruneTask{
		snapshot: usagePruneTaskSnapshot{
			TaskID:    uuid.NewString(),
			Status:    "queued",
			CreatedAt: time.Now().UTC(),
			Policy:    policy,
		},
		done: make(chan struct{}),
	}
	manager.active = task
	snapshot := task.snapshot
	manager.mu.Unlock()
	go manager.run(task, runner)
	return snapshot, false, nil
}

func (manager *usagePruneTaskManager) run(task *usagePruneTask, runner usagePruneRunner) {
	manager.mu.Lock()
	if manager.active != task {
		manager.mu.Unlock()
		close(task.done)
		return
	}
	startedAt := time.Now().UTC()
	task.snapshot.Status = "running"
	task.snapshot.StartedAt = &startedAt
	manager.mu.Unlock()

	outcome := runUsagePruneSafely(manager.ctx, task.snapshot.Policy, runner)

	manager.mu.Lock()
	completedAt := time.Now().UTC()
	task.snapshot.CompletedAt = &completedAt
	task.snapshot.SafeErrorCode = strings.TrimSpace(outcome.SafeErrorCode)
	task.snapshot.Processed = outcome.Processed
	task.snapshot.Pruned = outcome.Pruned
	task.snapshot.Saved = outcome.Saved
	task.snapshot.StorageBytesBefore = outcome.StorageBytesBefore
	task.snapshot.StorageBytesAfter = outcome.StorageBytesAfter
	task.snapshot.DetailCountBefore = outcome.DetailCountBefore
	task.snapshot.DetailCountAfter = outcome.DetailCountAfter
	task.snapshot.TotalRequestsBefore = outcome.TotalRequestsBefore
	task.snapshot.TotalRequestsAfter = outcome.TotalRequestsAfter
	if task.snapshot.SafeErrorCode == "" {
		task.snapshot.Status = "completed"
	} else {
		task.snapshot.Status = "failed"
	}
	manager.recent = append([]usagePruneTaskSnapshot{task.snapshot}, manager.recent...)
	if len(manager.recent) > usagePruneRecentTaskLimit {
		manager.recent = manager.recent[:usagePruneRecentTaskLimit]
	}
	if manager.active == task {
		manager.active = nil
	}
	manager.mu.Unlock()
	close(task.done)
}

func runUsagePruneSafely(ctx context.Context, policy usagePrunePolicy, runner usagePruneRunner) (outcome usagePruneOutcome) {
	defer func() {
		if recover() != nil {
			log.Error("usage history prune task panicked")
			outcome = usagePruneOutcome{SafeErrorCode: "usage_prune_failed"}
		}
	}()
	return runner(ctx, policy)
}

func (manager *usagePruneTaskManager) get(taskID string) (usagePruneTaskSnapshot, bool) {
	if manager == nil {
		return usagePruneTaskSnapshot{}, false
	}
	taskID = strings.TrimSpace(taskID)
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.active != nil && manager.active.snapshot.TaskID == taskID {
		return manager.active.snapshot, true
	}
	for _, snapshot := range manager.recent {
		if snapshot.TaskID == taskID {
			return snapshot, true
		}
	}
	return usagePruneTaskSnapshot{}, false
}

func (manager *usagePruneTaskManager) history() usagePruneTaskHistory {
	history := usagePruneTaskHistory{Recent: []usagePruneTaskSnapshot{}}
	if manager == nil {
		return history
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.active != nil {
		active := manager.active.snapshot
		history.Active = &active
	}
	history.Recent = append(history.Recent, manager.recent...)
	return history
}

func (manager *usagePruneTaskManager) shutdown(ctx context.Context) error {
	if manager == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	manager.mu.Lock()
	if !manager.closing {
		manager.closing = true
		manager.cancel()
	}
	var done <-chan struct{}
	if manager.active != nil {
		done = manager.active.done
	}
	manager.mu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Handler) usagePruneTaskManagerSnapshot() *usagePruneTaskManager {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.usagePruneTasks == nil {
		h.usagePruneTasks = newUsagePruneTaskManager()
	}
	return h.usagePruneTasks
}

func (h *Handler) usagePruneRunnerSnapshot() usagePruneRunner {
	if h == nil {
		return nil
	}
	h.mu.Lock()
	runner := h.usagePruneRunner
	h.mu.Unlock()
	if runner != nil {
		return runner
	}
	return h.executeUsagePrune
}

func (h *Handler) executeUsagePrune(ctx context.Context, policy usagePrunePolicy) usagePruneOutcome {
	outcome := usagePruneOutcome{}
	if h == nil {
		outcome.SafeErrorCode = "usage_statistics_unavailable"
		return outcome
	}
	if restore := h.usageRestoreStatusSnapshot(); restore.Active {
		outcome.SafeErrorCode = "usage_restore_in_progress"
		return outcome
	}
	stats := h.usageStatisticsSnapshot()
	if stats == nil {
		outcome.SafeErrorCode = "usage_statistics_unavailable"
		return outcome
	}
	if errBarrier := coreusage.DefaultManager().Barrier(ctx); errBarrier != nil {
		if errors.Is(errBarrier, context.Canceled) || errors.Is(errBarrier, context.DeadlineExceeded) {
			outcome.SafeErrorCode = "usage_prune_canceled"
		} else {
			outcome.SafeErrorCode = "usage_queue_unavailable"
		}
		return outcome
	}
	if restore := h.usageRestoreStatusSnapshot(); restore.Active {
		outcome.SafeErrorCode = "usage_restore_in_progress"
		return outcome
	}
	beforeStorage, errBeforeStorage := usage.InspectStatisticsStorage(h.usageStatisticsFilePath())
	if errBeforeStorage != nil {
		outcome.SafeErrorCode = "usage_storage_inspect_failed"
		return outcome
	}
	before := stats.Meta()
	outcome.Processed = stats.DetailCount()
	outcome.DetailCountBefore = outcome.Processed
	outcome.TotalRequestsBefore = before.TotalRequests
	outcome.StorageBytesBefore = beforeStorage.TotalBytes
	result, errPrune := usage.PruneAndPersistRequestStatistics(h.usageStatisticsFilePath(), stats, usage.PersistencePolicy{
		DetailRetentionDays: policy.OlderThanDays,
		MaxBytes:            historyMegabytesToBytes(policy.MaxStorageMegabytes),
	})
	outcome.Pruned = result.Pruned
	outcome.Saved = result.Saved
	if errPrune != nil {
		log.WithError(errPrune).Warn("usage history prune task failed")
		after := stats.Meta()
		outcome.DetailCountAfter = stats.DetailCount()
		outcome.TotalRequestsAfter = after.TotalRequests
		if afterStorage, errInspect := usage.InspectStatisticsStorage(h.usageStatisticsFilePath()); errInspect == nil {
			outcome.StorageBytesAfter = afterStorage.TotalBytes
		}
		outcome.SafeErrorCode = "usage_prune_failed"
		return outcome
	}
	afterStorage, errAfterStorage := usage.InspectStatisticsStorage(h.usageStatisticsFilePath())
	if errAfterStorage != nil {
		outcome.SafeErrorCode = "usage_storage_inspect_failed"
		return outcome
	}
	after := stats.Meta()
	outcome.StorageBytesAfter = afterStorage.TotalBytes
	outcome.DetailCountAfter = stats.DetailCount()
	outcome.TotalRequestsAfter = after.TotalRequests
	return outcome
}

// GetUsagePruneTask returns one asynchronous Usage cleanup task.
func (h *Handler) GetUsagePruneTask(c *gin.Context) {
	manager := h.usagePruneTaskManagerSnapshot()
	snapshot, ok := manager.get(c.Param("task_id"))
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "usage prune task not found", "code": "usage_prune_task_not_found"})
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, snapshot)
}
