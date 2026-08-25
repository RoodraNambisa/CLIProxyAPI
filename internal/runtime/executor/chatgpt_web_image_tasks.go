package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

const (
	chatGPTWebImageTaskWarningAge       = 15 * time.Minute
	chatGPTWebImageCanceledTombstoneTTL = 10 * time.Minute
	chatGPTWebImageCanceledTombstoneMax = 2048
)

// ChatGPTWebImageTaskSnapshot contains only bounded, non-sensitive task data.
type ChatGPTWebImageTaskSnapshot struct {
	ID                          string     `json:"id"`
	Status                      string     `json:"status"`
	Stage                       string     `json:"stage"`
	StartedAt                   time.Time  `json:"started_at"`
	DurationMilliseconds        int64      `json:"duration_milliseconds"`
	LastProgressAt              time.Time  `json:"last_progress_at"`
	LastProgressAgeMilliseconds int64      `json:"last_progress_age_milliseconds"`
	LastPollCompletedAt         *time.Time `json:"last_poll_completed_at,omitempty"`
	PollsInFlight               int        `json:"polls_in_flight"`
	CredentialFingerprint       string     `json:"credential_fingerprint"`
	Canceling                   bool       `json:"canceling"`
	CancellationRequestedAt     *time.Time `json:"cancellation_requested_at,omitempty"`
	Over15Minutes               bool       `json:"over_15_minutes"`
}

// ChatGPTWebImageTaskListSnapshot is the management view of active tasks.
type ChatGPTWebImageTaskListSnapshot struct {
	CollectedAt      time.Time                     `json:"collected_at"`
	Active           int                           `json:"active"`
	Canceling        int                           `json:"canceling"`
	ActiveOver15Min  int                           `json:"active_over_15_minutes"`
	RegistryCapacity int                           `json:"registry_capacity"`
	Tasks            []ChatGPTWebImageTaskSnapshot `json:"tasks"`
}

// ChatGPTWebImageTaskCancelResult describes an idempotent management cancel.
type ChatGPTWebImageTaskCancelResult struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type chatGPTWebImageTaskEntry struct {
	id                    string
	stage                 string
	startedAt             time.Time
	lastProgressAt        time.Time
	lastPollCompletedAt   time.Time
	credentialFingerprint string
	pollsInFlight         int
	canceling             bool
	cancellationAt        time.Time
	cancel                context.CancelCauseFunc
}

type chatGPTWebImageTaskRegistry struct {
	mu            sync.Mutex
	active        map[string]*chatGPTWebImageTaskEntry
	canceled      map[string]time.Time
	canceledOrder []string
	now           func() time.Time
}

type chatGPTWebImageTaskHandle struct {
	registry *chatGPTWebImageTaskRegistry
	id       string
	once     sync.Once
}

type chatGPTWebImageTaskContextKey struct{}

var (
	defaultChatGPTWebImageTaskRegistry = newChatGPTWebImageTaskRegistry(time.Now)
	chatGPTWebImageTaskFingerprintSalt = uuid.NewString()
)

var errChatGPTWebImageTaskCanceledByAdmin = errors.New("chatgpt web image task canceled by administrator")

type chatGPTWebImageTaskCanceledError struct {
	cause error
}

func (err *chatGPTWebImageTaskCanceledError) Error() string {
	payload, _ := json.Marshal(map[string]any{"error": map[string]any{
		"message":       "chatgpt web image task was canceled by an administrator",
		"type":          "request_canceled",
		"code":          "chatgpt_web_image_task_canceled",
		"failure_stage": "canceled",
	}})
	return string(payload)
}

func (err *chatGPTWebImageTaskCanceledError) Unwrap() error {
	if err == nil || err.cause == nil {
		return context.Canceled
	}
	return err.cause
}

func (*chatGPTWebImageTaskCanceledError) StatusCode() int { return 499 }

func (*chatGPTWebImageTaskCanceledError) SkipAuthResult() bool { return true }

func (*chatGPTWebImageTaskCanceledError) RetryOtherAuth() bool { return false }

func (*chatGPTWebImageTaskCanceledError) ExecutionResultErrorCode() string {
	return "chatgpt_web_image_task_canceled"
}

func (*chatGPTWebImageTaskCanceledError) ChatGPTWebFailureStage() string { return "canceled" }

func newChatGPTWebImageTaskRegistry(now func() time.Time) *chatGPTWebImageTaskRegistry {
	if now == nil {
		now = time.Now
	}
	return &chatGPTWebImageTaskRegistry{
		active:   make(map[string]*chatGPTWebImageTaskEntry),
		canceled: make(map[string]time.Time),
		now:      now,
	}
}

func beginChatGPTWebImageTask(ctx context.Context, authID string) (context.Context, *chatGPTWebImageTaskHandle) {
	return defaultChatGPTWebImageTaskRegistry.begin(ctx, authID)
}

func (registry *chatGPTWebImageTaskRegistry) begin(ctx context.Context, authID string) (context.Context, *chatGPTWebImageTaskHandle) {
	if ctx == nil {
		ctx = context.Background()
	}
	taskCtx, cancel := context.WithCancelCause(ctx)
	id := uuid.NewString()
	now := registry.now()
	entry := &chatGPTWebImageTaskEntry{
		id:                    id,
		stage:                 "preparing",
		startedAt:             now,
		lastProgressAt:        now,
		credentialFingerprint: chatGPTWebImageCredentialFingerprint(authID),
		cancel:                cancel,
	}
	registry.mu.Lock()
	registry.cleanupCanceledLocked(now)
	registry.active[id] = entry
	registry.mu.Unlock()
	handle := &chatGPTWebImageTaskHandle{registry: registry, id: id}
	return context.WithValue(taskCtx, chatGPTWebImageTaskContextKey{}, handle), handle
}

func chatGPTWebImageTaskHandleFromContext(ctx context.Context) *chatGPTWebImageTaskHandle {
	if ctx == nil {
		return nil
	}
	handle, _ := ctx.Value(chatGPTWebImageTaskContextKey{}).(*chatGPTWebImageTaskHandle)
	return handle
}

func withChatGPTWebImageTaskHandle(ctx context.Context, handle *chatGPTWebImageTaskHandle) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if handle == nil {
		return ctx
	}
	return context.WithValue(ctx, chatGPTWebImageTaskContextKey{}, handle)
}

func setChatGPTWebImageTaskStage(ctx context.Context, stage string) {
	if handle := chatGPTWebImageTaskHandleFromContext(ctx); handle != nil {
		handle.setStage(stage)
	}
}

func beginChatGPTWebImageTaskPoll(ctx context.Context) func(bool) {
	handle := chatGPTWebImageTaskHandleFromContext(ctx)
	if handle == nil {
		return func(bool) {}
	}
	handle.beginPoll()
	var once sync.Once
	return func(canceled bool) {
		once.Do(func() { handle.finishPoll(canceled) })
	}
}

func (handle *chatGPTWebImageTaskHandle) setStage(stage string) {
	if handle == nil || handle.registry == nil {
		return
	}
	stage = strings.TrimSpace(stage)
	if stage == "" {
		return
	}
	now := handle.registry.now()
	handle.registry.mu.Lock()
	if entry := handle.registry.active[handle.id]; entry != nil && !entry.canceling {
		entry.stage = stage
		entry.lastProgressAt = now
	}
	handle.registry.mu.Unlock()
}

func (handle *chatGPTWebImageTaskHandle) beginPoll() {
	if handle == nil || handle.registry == nil {
		return
	}
	now := handle.registry.now()
	handle.registry.mu.Lock()
	if entry := handle.registry.active[handle.id]; entry != nil {
		entry.pollsInFlight++
		if !entry.canceling {
			entry.stage = "polling"
		}
		entry.lastProgressAt = now
	}
	handle.registry.mu.Unlock()
}

func (handle *chatGPTWebImageTaskHandle) finishPoll(canceled bool) {
	if handle == nil || handle.registry == nil {
		return
	}
	now := handle.registry.now()
	handle.registry.mu.Lock()
	if entry := handle.registry.active[handle.id]; entry != nil {
		entry.pollsInFlight = max(0, entry.pollsInFlight-1)
		entry.lastProgressAt = now
		if !canceled {
			entry.lastPollCompletedAt = now
		}
		if !entry.canceling && entry.pollsInFlight == 0 {
			entry.stage = "poll_wait"
		}
	}
	handle.registry.mu.Unlock()
}

func (handle *chatGPTWebImageTaskHandle) finish() {
	if handle == nil || handle.registry == nil {
		return
	}
	handle.once.Do(func() {
		handle.registry.mu.Lock()
		entry := handle.registry.active[handle.id]
		delete(handle.registry.active, handle.id)
		handle.registry.mu.Unlock()
		if entry != nil && entry.cancel != nil && !entry.canceling {
			entry.cancel(nil)
		}
	})
}

// ChatGPTWebImageTasksSnapshot returns active image operations without upstream identities.
func ChatGPTWebImageTasksSnapshot() ChatGPTWebImageTaskListSnapshot {
	return defaultChatGPTWebImageTaskRegistry.snapshot()
}

// CancelChatGPTWebImageTask cancels an active task or acknowledges a recent repeated cancel.
func CancelChatGPTWebImageTask(id string) (ChatGPTWebImageTaskCancelResult, bool) {
	return defaultChatGPTWebImageTaskRegistry.cancelTask(id)
}

func (registry *chatGPTWebImageTaskRegistry) snapshot() ChatGPTWebImageTaskListSnapshot {
	now := registry.now()
	registry.mu.Lock()
	registry.cleanupCanceledLocked(now)
	result := ChatGPTWebImageTaskListSnapshot{
		CollectedAt:      now.UTC(),
		Active:           len(registry.active),
		RegistryCapacity: max(1, coreexecutor.ChatGPTWebImageExecutionAdmissionSnapshot().Limit),
		Tasks:            make([]ChatGPTWebImageTaskSnapshot, 0, len(registry.active)),
	}
	for _, entry := range registry.active {
		duration := max(time.Duration(0), now.Sub(entry.startedAt))
		progressAge := max(time.Duration(0), now.Sub(entry.lastProgressAt))
		snapshot := ChatGPTWebImageTaskSnapshot{
			ID:                          entry.id,
			Status:                      "running",
			Stage:                       entry.stage,
			StartedAt:                   entry.startedAt.UTC(),
			DurationMilliseconds:        duration.Milliseconds(),
			LastProgressAt:              entry.lastProgressAt.UTC(),
			LastProgressAgeMilliseconds: progressAge.Milliseconds(),
			PollsInFlight:               entry.pollsInFlight,
			CredentialFingerprint:       entry.credentialFingerprint,
			Canceling:                   entry.canceling,
			Over15Minutes:               duration >= chatGPTWebImageTaskWarningAge,
		}
		if !entry.lastPollCompletedAt.IsZero() {
			completedAt := entry.lastPollCompletedAt.UTC()
			snapshot.LastPollCompletedAt = &completedAt
		}
		if entry.canceling {
			snapshot.Status = "canceling"
			snapshot.Stage = "canceling"
			result.Canceling++
			canceledAt := entry.cancellationAt.UTC()
			snapshot.CancellationRequestedAt = &canceledAt
		}
		if snapshot.Over15Minutes {
			result.ActiveOver15Min++
		}
		result.Tasks = append(result.Tasks, snapshot)
	}
	registry.mu.Unlock()
	sort.Slice(result.Tasks, func(left, right int) bool {
		return result.Tasks[left].StartedAt.Before(result.Tasks[right].StartedAt)
	})
	return result
}

func (registry *chatGPTWebImageTaskRegistry) cancelTask(id string) (ChatGPTWebImageTaskCancelResult, bool) {
	id = strings.TrimSpace(id)
	if id == "" {
		return ChatGPTWebImageTaskCancelResult{}, false
	}
	now := registry.now()
	registry.mu.Lock()
	registry.cleanupCanceledLocked(now)
	if entry := registry.active[id]; entry != nil {
		if !entry.canceling {
			entry.canceling = true
			entry.cancellationAt = now
			entry.stage = "canceling"
			entry.lastProgressAt = now
			registry.canceled[id] = now
			registry.canceledOrder = append(registry.canceledOrder, id)
			registry.cleanupCanceledLocked(now)
		}
		cancel := entry.cancel
		registry.mu.Unlock()
		if cancel != nil {
			cancel(errChatGPTWebImageTaskCanceledByAdmin)
		}
		return ChatGPTWebImageTaskCancelResult{ID: id, Status: "canceling"}, true
	}
	if _, exists := registry.canceled[id]; exists {
		registry.mu.Unlock()
		return ChatGPTWebImageTaskCancelResult{ID: id, Status: "already_canceled"}, true
	}
	registry.mu.Unlock()
	return ChatGPTWebImageTaskCancelResult{}, false
}

func (registry *chatGPTWebImageTaskRegistry) cleanupCanceledLocked(now time.Time) {
	for len(registry.canceledOrder) > 0 {
		id := registry.canceledOrder[0]
		canceledAt, exists := registry.canceled[id]
		if exists && now.Sub(canceledAt) < chatGPTWebImageCanceledTombstoneTTL && len(registry.canceled) <= chatGPTWebImageCanceledTombstoneMax {
			break
		}
		registry.canceledOrder = registry.canceledOrder[1:]
		delete(registry.canceled, id)
	}
}

func chatGPTWebImageCredentialFingerprint(authID string) string {
	digest := sha256.Sum256([]byte(chatGPTWebImageTaskFingerprintSalt + "\x00" + strings.TrimSpace(authID)))
	return "auth_" + hex.EncodeToString(digest[:6])
}

func normalizeChatGPTWebImageTaskCancellation(ctx context.Context, err error) error {
	if err == nil || ctx == nil || !errors.Is(context.Cause(ctx), errChatGPTWebImageTaskCanceledByAdmin) {
		return err
	}
	var existing *chatGPTWebImageTaskCanceledError
	if errors.As(err, &existing) {
		return err
	}
	return &chatGPTWebImageTaskCanceledError{cause: err}
}
