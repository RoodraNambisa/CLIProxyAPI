package chatgptweb

import (
	"errors"
	"time"
)

// ErrAccountInfoTaskLimitReached reports that the bounded task registry is full.
var ErrAccountInfoTaskLimitReached = errors.New("chatgpt web account info active task limit reached")

// ErrAccountInfoAutoRefreshDisabled reports that a non-forced refresh was rejected by configuration.
var ErrAccountInfoAutoRefreshDisabled = errors.New("chatgpt web account info automatic refresh is disabled")

const (
	AccountInfoTaskQueued              = "queued"
	AccountInfoTaskRunning             = "running"
	AccountInfoTaskCanceling           = "canceling"
	AccountInfoTaskCompleted           = "completed"
	AccountInfoTaskCompletedWithErrors = "completed_with_errors"
	AccountInfoTaskCanceled            = "canceled"

	AccountInfoResultQueued    = "queued"
	AccountInfoResultRunning   = "running"
	AccountInfoResultRetrying  = "retrying"
	AccountInfoResultUpdated   = "updated"
	AccountInfoResultUnchanged = "unchanged"
	AccountInfoResultFresh     = "fresh"
	AccountInfoResultPartial   = "partial"
	AccountInfoResultFailed    = "failed"
	AccountInfoResultCanceled  = "canceled"

	AccountInfoRecoveryIdle                   = "idle"
	AccountInfoRecoveryAutoRetrying           = "auto_retrying"
	AccountInfoRecoveryManualChecking         = "manual_checking"
	AccountInfoRecoveryManualRequired         = "manual_recovery_required"
	AccountInfoRecoveryReloginPending         = "relogin_pending"
	AccountInfoRecoveryReauthRequired         = "reauth_required"
	AccountInfoRecoveryInteractionRequired    = "interaction_required"
	AccountInfoRecoveryMaxAutomaticRetryCount = 3
)

// AccountInfoRuntimeSnapshot is safe for the management API.
type AccountInfoRuntimeSnapshot struct {
	Busy                       int                              `json:"busy"`
	Queued                     int                              `json:"queued"`
	ImmediateQueued            int                              `json:"immediate_queued"`
	Scheduled                  int                              `json:"scheduled"`
	RetryScheduled             int                              `json:"retry_scheduled"`
	TaskRetryScheduled         int                              `json:"task_retry_scheduled"`
	TransientRecoveryScheduled int                              `json:"transient_recovery_scheduled"`
	QuotaRecoveryScheduled     int                              `json:"quota_recovery_scheduled"`
	PeriodicReviewScheduled    int                              `json:"periodic_review_scheduled"`
	PeriodicPending            int                              `json:"periodic_pending"`
	PeriodicNextAt             time.Time                        `json:"periodic_next_at,omitempty"`
	MaxAutomaticAttempts       int                              `json:"max_automatic_attempts"`
	Inflight                   int                              `json:"inflight"`
	RefreshCount               uint64                           `json:"refresh_count"`
	RetryCount                 uint64                           `json:"retry_count"`
	FailedCount                uint64                           `json:"failed_count"`
	LastError                  string                           `json:"last_error"`
	LastFailure                string                           `json:"last_failure"`
	LastFailureAt              time.Time                        `json:"last_failure_at,omitempty"`
	LastSuccessAt              time.Time                        `json:"last_success_at,omitempty"`
	FailureCounts              map[string]uint64                `json:"failure_counts"`
	RecoveryStateCounts        map[string]int                   `json:"recovery_state_counts"`
	BackgroundRelogin          BackgroundReloginRuntimeSnapshot `json:"background_relogin"`
}

// BackgroundReloginRuntimeSnapshot reports bounded queue activity without
// exposing credential material.
type BackgroundReloginRuntimeSnapshot struct {
	Queued       int    `json:"queued"`
	Delayed      int    `json:"delayed"`
	Running      int    `json:"running"`
	Promoted     uint64 `json:"promoted"`
	Deduplicated uint64 `json:"deduplicated"`
	Canceled     uint64 `json:"canceled"`
}

// AccountInfoAuthRuntimeState contains transient refresh state for one auth.
type AccountInfoAuthRuntimeState struct {
	Refreshing          bool      `json:"refreshing"`
	NextRefreshAt       time.Time `json:"next_refresh_at,omitempty"`
	LastError           string    `json:"last_error,omitempty"`
	RecoveryState       string    `json:"recovery_state,omitempty"`
	RecoveryAttempts    int       `json:"recovery_attempts,omitempty"`
	RecoveryMaxAttempts int       `json:"recovery_max_attempts,omitempty"`
	RecoveryStopReason  string    `json:"recovery_stop_reason,omitempty"`
	LastFailure         string    `json:"last_failure,omitempty"`
	LastFailureAt       time.Time `json:"last_failure_at,omitempty"`
	LastSuccessAt       time.Time `json:"last_success_at,omitempty"`
	ConsecutiveFailures int       `json:"consecutive_failures,omitempty"`
}

// AccountInfoRefreshTarget maps a management filename to a runtime auth ID.
type AccountInfoRefreshTarget struct {
	Name           string `json:"name"`
	AuthID         string `json:"-"`
	AuthInstanceID string `json:"-"`
	AuthIndex      string `json:"auth_index,omitempty"`
}

// AccountInfoRefreshResult is one non-sensitive task item.
type AccountInfoRefreshResult struct {
	Name      string `json:"name"`
	AuthIndex string `json:"auth_index,omitempty"`
	Status    string `json:"status"`
	Attempts  int    `json:"attempts,omitempty"`
	Error     string `json:"error,omitempty"`
}

// AccountInfoRefreshTask is an immutable management snapshot.
type AccountInfoRefreshTask struct {
	ID          string                     `json:"id"`
	State       string                     `json:"state"`
	Force       bool                       `json:"force"`
	CreatedAt   time.Time                  `json:"created_at"`
	StartedAt   *time.Time                 `json:"started_at,omitempty"`
	CompletedAt *time.Time                 `json:"completed_at,omitempty"`
	Total       int                        `json:"total"`
	Processed   int                        `json:"processed"`
	Succeeded   int                        `json:"succeeded"`
	Partial     int                        `json:"partial"`
	Failed      int                        `json:"failed"`
	Canceled    int                        `json:"canceled"`
	Results     []AccountInfoRefreshResult `json:"results"`
}
