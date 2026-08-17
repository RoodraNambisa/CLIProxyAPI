package usage

import "time"

// RestoreRuntimeSnapshot describes the background snapshot restore without
// exposing storage paths or request details.
type RestoreRuntimeSnapshot struct {
	Enabled       bool       `json:"enabled"`
	Status        string     `json:"status"`
	Active        bool       `json:"active"`
	Applied       bool       `json:"applied"`
	NeedsSidecar  bool       `json:"needs_sidecar"`
	StartedAt     *time.Time `json:"started_at,omitempty"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
	Added         int64      `json:"added"`
	Skipped       int64      `json:"skipped"`
	SafeErrorCode string     `json:"error_code,omitempty"`
}
