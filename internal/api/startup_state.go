package api

import (
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	StartupPhaseStarting         = "starting"
	StartupPhaseListenerReady    = "listener_ready"
	StartupPhaseAuthLoading      = "auth_loading"
	StartupPhaseRoutingBootstrap = "routing_bootstrap"
	StartupPhaseReady            = "ready"
	StartupPhaseFailed           = "failed"

	StartupStatusInitializing = "initializing"
	StartupStatusReady        = "ready"
	StartupStatusDegraded     = "degraded"
	StartupStatusFailed       = "failed"
)

// StartupIssueSnapshot identifies one safe, low-cardinality startup issue.
type StartupIssueSnapshot struct {
	Stage    string `json:"stage"`
	Code     string `json:"code"`
	Severity string `json:"severity"`
}

// StartupStageSnapshot describes one low-cardinality startup stage without
// exposing credential or storage details.
type StartupStageSnapshot struct {
	Name           string     `json:"name"`
	Status         string     `json:"status"`
	StartedAt      time.Time  `json:"started_at"`
	CompletedAt    *time.Time `json:"completed_at,omitempty"`
	DurationMillis int64      `json:"duration_milliseconds"`
	Processed      int64      `json:"processed,omitempty"`
	Skipped        int64      `json:"skipped,omitempty"`
	SafeErrorCode  string     `json:"error_code,omitempty"`
}

// StartupSnapshot is the safe management and readiness view of startup.
type StartupSnapshot struct {
	Phase     string                 `json:"phase"`
	Status    string                 `json:"status"`
	Ready     bool                   `json:"ready"`
	Degraded  bool                   `json:"degraded"`
	StartedAt time.Time              `json:"started_at"`
	UpdatedAt time.Time              `json:"updated_at"`
	Issues    []StartupIssueSnapshot `json:"issues,omitempty"`
	Stages    []StartupStageSnapshot `json:"stages"`
}

// StartupState coordinates the listener/readiness boundary and startup
// diagnostics. It is safe for concurrent HTTP reads and service updates.
type StartupState struct {
	mu        sync.RWMutex
	ready     bool
	degraded  bool
	phase     string
	startedAt time.Time
	updatedAt time.Time
	stages    []StartupStageSnapshot
	issues    []StartupIssueSnapshot
}

// NewStartupState creates a state that rejects proxy traffic until MarkReady.
func NewStartupState() *StartupState {
	now := time.Now().UTC()
	return &StartupState{
		phase:     StartupPhaseStarting,
		startedAt: now,
		updatedAt: now,
	}
}

func newReadyStartupState() *StartupState {
	state := NewStartupState()
	state.MarkReady()
	return state
}

// Ready reports whether proxy routes may accept work.
func (state *StartupState) Ready() bool {
	if state == nil {
		return true
	}
	state.mu.RLock()
	ready := state.ready
	state.mu.RUnlock()
	return ready
}

// SetPhase updates the current low-cardinality startup phase.
func (state *StartupState) SetPhase(phase string) {
	if state == nil {
		return
	}
	phase = strings.TrimSpace(phase)
	if phase == "" {
		return
	}
	now := time.Now().UTC()
	state.mu.Lock()
	state.phase = phase
	state.updatedAt = now
	state.mu.Unlock()
}

// MarkReady opens proxy routes after the essential routing bootstrap.
func (state *StartupState) MarkReady() {
	if state == nil {
		return
	}
	now := time.Now().UTC()
	state.mu.Lock()
	state.ready = true
	state.phase = StartupPhaseReady
	state.updatedAt = now
	state.mu.Unlock()
}

// AddIssue records a low-cardinality startup issue. Warning issues put a ready
// service into degraded state; error issues are surfaced without opening proxy
// traffic until MarkReady is explicitly called.
func (state *StartupState) AddIssue(stage, code, severity string) {
	if state == nil {
		return
	}
	stage = strings.TrimSpace(stage)
	code = strings.TrimSpace(code)
	severity = strings.ToLower(strings.TrimSpace(severity))
	if code == "" {
		return
	}
	if severity != "error" {
		severity = "warning"
	}
	now := time.Now().UTC()
	state.mu.Lock()
	for _, issue := range state.issues {
		if issue.Stage == stage && issue.Code == code && issue.Severity == severity {
			state.mu.Unlock()
			return
		}
	}
	state.issues = append(state.issues, StartupIssueSnapshot{Stage: stage, Code: code, Severity: severity})
	if severity == "warning" {
		state.degraded = true
	}
	state.updatedAt = now
	state.mu.Unlock()
}

// MarkFailed leaves proxy routes closed while keeping management diagnostics
// available.
func (state *StartupState) MarkFailed() {
	if state == nil {
		return
	}
	now := time.Now().UTC()
	state.mu.Lock()
	state.ready = false
	state.phase = StartupPhaseFailed
	state.updatedAt = now
	state.mu.Unlock()
}

// BeginStage starts a timed startup stage and returns an idempotent finisher.
// safeErrorCode must be a low-cardinality classification, never raw error text.
func (state *StartupState) BeginStage(name string) func(processed int64, safeErrorCode string) {
	finish := state.BeginReportedStage(name)
	return func(processed int64, safeErrorCode string) {
		finish(processed, 0, safeErrorCode)
	}
}

// BeginReportedStage starts a stage that also reports safely skipped records.
func (state *StartupState) BeginReportedStage(name string) func(processed, skipped int64, safeErrorCode string) {
	if state == nil {
		return func(int64, int64, string) {}
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return func(int64, int64, string) {}
	}
	startedAt := time.Now().UTC()
	state.mu.Lock()
	index := len(state.stages)
	state.stages = append(state.stages, StartupStageSnapshot{
		Name:      name,
		Status:    "running",
		StartedAt: startedAt,
	})
	state.updatedAt = startedAt
	state.mu.Unlock()

	var once sync.Once
	return func(processed, skipped int64, safeErrorCode string) {
		once.Do(func() {
			completedAt := time.Now().UTC()
			safeErrorCode = strings.TrimSpace(safeErrorCode)
			status := "completed"
			if safeErrorCode != "" {
				status = "failed"
			}
			duration := completedAt.Sub(startedAt)
			state.mu.Lock()
			if index < len(state.stages) {
				stage := &state.stages[index]
				stage.Status = status
				stage.CompletedAt = &completedAt
				stage.DurationMillis = duration.Milliseconds()
				stage.Processed = processed
				stage.Skipped = skipped
				stage.SafeErrorCode = safeErrorCode
			}
			state.updatedAt = completedAt
			state.mu.Unlock()

			fields := log.Fields{
				"stage":       name,
				"duration_ms": duration.Milliseconds(),
				"processed":   processed,
				"skipped":     skipped,
				"outcome":     status,
			}
			if safeErrorCode != "" {
				fields["error_code"] = safeErrorCode
			}
			log.WithFields(fields).Info("startup stage completed")
		})
	}
}

// Snapshot returns a detached startup state view.
func (state *StartupState) Snapshot() StartupSnapshot {
	if state == nil {
		return StartupSnapshot{Phase: StartupPhaseReady, Status: StartupStatusReady, Ready: true}
	}
	state.mu.RLock()
	status := StartupStatusInitializing
	if state.phase == StartupPhaseFailed {
		status = StartupStatusFailed
	} else if state.ready && state.degraded {
		status = StartupStatusDegraded
	} else if state.ready {
		status = StartupStatusReady
	}
	snapshot := StartupSnapshot{
		Phase:     state.phase,
		Status:    status,
		Ready:     state.ready,
		Degraded:  state.degraded,
		StartedAt: state.startedAt,
		UpdatedAt: state.updatedAt,
		Issues:    append([]StartupIssueSnapshot(nil), state.issues...),
		Stages:    append([]StartupStageSnapshot(nil), state.stages...),
	}
	state.mu.RUnlock()
	return snapshot
}
