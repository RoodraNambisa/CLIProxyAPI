package chatgptweb

import (
	"errors"
	"time"
)

// ErrAccountInfoTaskLimitReached reports that the bounded task registry is full.
var ErrAccountInfoTaskLimitReached = errors.New("chatgpt web account info active task limit reached")

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
)

// AccountInfoRuntimeSnapshot is safe for the management API.
type AccountInfoRuntimeSnapshot struct {
	Busy         int    `json:"busy"`
	Queued       int    `json:"queued"`
	Scheduled    int    `json:"scheduled"`
	Inflight     int    `json:"inflight"`
	RefreshCount uint64 `json:"refresh_count"`
	RetryCount   uint64 `json:"retry_count"`
	FailedCount  uint64 `json:"failed_count"`
	LastError    string `json:"last_error"`
}

// AccountInfoAuthRuntimeState contains transient refresh state for one auth.
type AccountInfoAuthRuntimeState struct {
	Refreshing    bool      `json:"refreshing"`
	NextRefreshAt time.Time `json:"next_refresh_at,omitempty"`
	LastError     string    `json:"last_error,omitempty"`
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
