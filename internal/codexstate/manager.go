// Package codexstate manages opt-in, account/model-scoped response state in memory.
package codexstate

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	log "github.com/sirupsen/logrus"
)

// Credential contains routing identities only, never bearer tokens.
type Credential struct{ ID, Name, Owner, Instance, Model, Route, Plan string }
type Result struct {
	State, Model, Answer string
	Completed            bool
	Status               int
	Tokens               int64
	FailureReason        string
}
type Probe func(context.Context, Credential, config.CodexStateOverrideConfig) (Result, error)
type Snapshot struct {
	Model               string    `json:"model"`
	Status              string    `json:"status"`
	Length              int       `json:"length"`
	Digest              string    `json:"digest,omitempty"`
	ExpiresAt           time.Time `json:"expires_at,omitzero"`
	NextAttempt         time.Time `json:"next_attempt,omitzero"`
	LastUsed            time.Time `json:"last_used,omitzero"`
	Attempts            uint64    `json:"attempts"`
	Acquired            uint64    `json:"acquired"`
	Uses                uint64    `json:"uses"`
	CurrentUses         uint64    `json:"current_uses"`
	Completed           uint64    `json:"completed"`
	Misses              uint64    `json:"misses"`
	Tokens              int64     `json:"acquisition_tokens"`
	LastError           string    `json:"last_error,omitempty"`
	LastStatus          int       `json:"last_status,omitempty"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	Exhausted           bool      `json:"exhausted"`
	Invalidations       uint64    `json:"invalidations"`
	LastInvalidation    string    `json:"last_invalidation,omitempty"`
	ManualOnly          bool      `json:"manual_only,omitempty"`
}
type entry struct {
	credential Credential
	Snapshot
	state                string
	valueVersion         uint64
	busy, paused, manual bool
	failures             int
	cancel               context.CancelFunc
}
type Manager struct {
	mu               sync.Mutex
	wg               sync.WaitGroup
	cfg              config.CodexStateOverrideConfig
	entries          map[string]*entry
	version          uint64
	running          int
	nextValueVersion uint64
}

var Default = New()

func New() *Manager           { return &Manager{entries: make(map[string]*entry)} }
func key(c Credential) string { return c.ID + "\x00" + c.Owner + "\x00" + c.Model }

// Sync retires removed/replaced credentials and cancels obsolete acquisition tasks.
func (m *Manager) Sync(cfg config.CodexStateOverrideConfig, credentials []Credential, manualScopes ...Credential) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cfg = cfg.Resolved()
	changed := !reflect.DeepEqual(cfg, m.cfg)
	if changed {
		m.version++
		for _, e := range m.entries {
			if e.cancel != nil {
				e.cancel()
			}
			e.busy = false
			e.cancel = nil
		}
		if !reflect.DeepEqual(cfg.Lengths, m.cfg.Lengths) || !reflect.DeepEqual(cfg.MatchModel, m.cfg.MatchModel) || cfg.Prompt != m.cfg.Prompt || cfg.ResponseContains != m.cfg.ResponseContains || cfg.TTLMinutes != m.cfg.TTLMinutes || !reflect.DeepEqual(cfg.ModelOverrides, m.cfg.ModelOverrides) || !reflect.DeepEqual(cfg.PlanLengths, m.cfg.PlanLengths) {
			for _, e := range m.entries {
				e.state = ""
				e.Length = 0
				e.CurrentUses = 0
				e.Digest = ""
				e.ExpiresAt = time.Time{}
			}
		}
		m.cfg = cfg
	}
	if !cfg.Enabled {
		credentials = nil
		manualScopes = nil
	}
	// Keep explicitly requested diagnostic pairs while their credential/model scope
	// remains eligible. They never expand the scheduler's registered model catalog.
	registered := make(map[string]bool, len(credentials))
	for _, c := range credentials {
		registered[key(c)] = true
	}
	scopes := make(map[string]Credential, len(manualScopes))
	for _, c := range manualScopes {
		scopes[c.ID+"\x00"+c.Owner] = c
	}
	for k, e := range m.entries {
		if !e.ManualOnly || registered[k] {
			continue
		}
		if c, ok := scopes[e.credential.ID+"\x00"+e.credential.Owner]; ok && (len(cfg.Models) == 0 || slices.Contains(cfg.Models, e.Model) || slices.Contains(cfg.Models, e.credential.Route)) {
			c.Model, c.Route = e.Model, e.credential.Route
			credentials = append(credentials, c)
		}
	}
	wanted := make(map[string]bool, len(credentials))
	for _, c := range credentials {
		k := key(c)
		wanted[k] = true
		if old := m.entries[k]; old != nil {
			if registered[k] {
				old.ManualOnly = false
			}
			if old.credential.Instance == c.Instance && old.credential.Plan == c.Plan {
				old.credential = c
				continue
			}
			if old.cancel != nil {
				old.cancel()
			}
			// Token refresh replaces the runtime instance, not the account/model state.
			// Publish a new entry so an old in-flight acquisition cannot update it.
			next := *old
			next.credential = c
			next.busy = false
			next.cancel = nil
			if old.credential.Plan != c.Plan {
				next.state, next.Digest = "", ""
				next.Length, next.CurrentUses = 0, 0
				next.ExpiresAt, next.NextAttempt = time.Time{}, time.Time{}
				next.failures, next.ConsecutiveFailures = 0, 0
				next.Exhausted, next.LastError = false, ""
				next.manual = old.manual || old.busy
			}
			m.entries[k] = &next
			continue
		}
		m.entries[k] = &entry{credential: c, Snapshot: Snapshot{Model: c.Model, Status: "missing"}}
	}
	for k, e := range m.entries {
		if !wanted[k] {
			if e.cancel != nil {
				e.cancel()
			}
			delete(m.entries, k)
		}
	}
}

// QueueManual creates a bounded diagnostic entry without registering a model.
// The caller must validate current credential and configured model scope first.
func (m *Manager) QueueManual(c Credential) (bool, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.cfg.Enabled {
		return false, 0
	}
	e := m.entries[key(c)]
	if e == nil {
		count := 0
		for _, existing := range m.entries {
			if existing.credential.ID == c.ID && existing.ManualOnly {
				count++
			}
		}
		if count >= 256 {
			return false, 0
		}
		e = &entry{credential: c, Snapshot: Snapshot{Model: c.Model, ManualOnly: true}}
		m.entries[key(c)] = e
	}
	if e.credential.Instance != c.Instance || e.credential.Plan != c.Plan {
		return false, e.Acquired
	}
	e.ManualOnly = true
	e.credential = c
	if !e.busy {
		e.manual, e.paused = true, false
		e.NextAttempt = time.Time{}
		e.failures, e.ConsecutiveFailures = 0, 0
		e.Exhausted = false
	}
	return true, e.Acquired
}

func (m *Manager) HasManual(c Credential) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[key(c)]
	return m.cfg.Enabled && e != nil && e.ManualOnly && e.credential.Instance == c.Instance && e.credential.Plan == c.Plan
}

// Pick freezes a value for an outgoing attempt. Acquiring a replacement never blocks inference.
func (m *Manager) Pick(c Credential, clientState string, now time.Time) (string, string, bool) {
	value, policy, _, eligible := m.PickVersion(c, clientState, now)
	return value, policy, eligible
}

func (m *Manager) PickVersion(c Credential, clientState string, now time.Time) (string, string, uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[key(c)]
	if !m.cfg.Enabled || e == nil || e.credential.Instance != c.Instance || e.credential.Plan != c.Plan || e.paused {
		return "", "", 0, false
	}
	e.LastUsed = now
	if m.cfg.Mode == "missing" && strings.TrimSpace(clientState) != "" {
		return "", "", 0, true
	}
	if e.state != "" && now.Before(e.ExpiresAt) {
		e.Uses++
		e.CurrentUses++
		return e.state, "", e.valueVersion, true
	}
	e.Misses++
	return "", m.cfg.MissingPolicy, 0, true
}

// ObserveResponse invalidates only the value used by this request. Versions also
// distinguish equal opaque values acquired at different times (the ABA case).
// A nonempty reason still identifies a rejected old connection when a newer cache value exists.
func (m *Manager) ObserveResponse(c Credential, version uint64, value, returnedModel string, returnedLength int) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[key(c)]
	if !m.cfg.Enabled || version == 0 || e == nil || e.paused || e.credential.Plan != c.Plan || (!m.cfg.InvalidateOnModelMismatch && !m.cfg.InvalidateOnStateLengthMismatch) {
		return ""
	}
	reason := ""
	if m.cfg.InvalidateOnModelMismatch && returnedModel != "" && returnedModel != c.Model {
		reason = "response_model_mismatch"
	} else if m.cfg.InvalidateOnStateLengthMismatch && returnedLength > 0 {
		lengths := m.cfg.ForCredential(c.Plan, c.Model).Lengths
		if len(lengths) > 0 && !slices.Contains(lengths, returnedLength) {
			reason = "response_state_length_mismatch"
		}
	}
	if reason == "" {
		return ""
	}
	if e.valueVersion != version || e.state != value || e.state == "" {
		return reason
	}
	e.state, e.Digest = "", ""
	e.valueVersion, e.CurrentUses, e.Length = 0, 0, 0
	e.ExpiresAt = time.Time{}
	e.Invalidations++
	e.LastInvalidation = reason
	// Keep failure limits and backoff. An already-running renewal supplies the replacement.
	if !e.busy && !e.Exhausted && !e.ManualOnly {
		e.manual = true
	}
	log.WithFields(log.Fields{"auth_id": c.ID, "model": c.Model, "reason": reason, "state_version": version, "returned_state_length": returnedLength}).Warn("Codex managed state invalidated by upstream response")
	return reason
}

func (m *Manager) Complete(c Credential, state string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e := m.entries[key(c)]; e != nil && e.credential.Instance == c.Instance && e.state == state {
		e.Completed++
	}
}

func (m *Manager) WatchesResponses() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg.Enabled && (m.cfg.InvalidateOnModelMismatch || m.cfg.InvalidateOnStateLengthMismatch)
}

func (m *Manager) Snapshots(id string, now time.Time) []Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := []Snapshot{}
	for _, e := range m.entries {
		if e.credential.ID != id {
			continue
		}
		s := e.Snapshot
		switch {
		case e.paused:
			s.Status = "paused"
		case e.busy:
			s.Status = "acquiring"
		case e.manual:
			s.Status = "queued"
		case e.state != "" && now.Before(e.ExpiresAt):
			s.Status = "valid"
		case e.Exhausted:
			s.Status = "exhausted"
		case !e.ExpiresAt.IsZero():
			s.Status = "expired"
		case e.LastError != "":
			s.Status = "failed"
		default:
			s.Status = "missing"
		}
		result = append(result, s)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Model < result[j].Model })
	return result
}

// Action queues work; management requests never wait for an upstream response.
func (m *Manager) Action(id, model, action string) bool {
	matched, _ := m.ActionWithBaseline(id, model, action)
	return matched
}

// ActionWithBaseline reads the acquisition counter under the same lock used to
// queue work, so a concurrent renewal cannot be mistaken for this manual action.
func (m *Manager) ActionWithBaseline(id, model, action string) (bool, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	matched := false
	var previousAcquired uint64
	for _, e := range m.entries {
		if e.credential.ID != id || (model != "" && model != e.Model) {
			continue
		}
		matched = true
		previousAcquired += e.Acquired
		switch action {
		case "acquire":
			if e.busy {
				continue
			}
			e.manual = true
			e.paused = false
			e.NextAttempt = time.Time{}
			e.failures = 0
			e.Exhausted = false
			e.ConsecutiveFailures = 0
		case "pause":
			e.paused = true
			e.manual = false
			if e.cancel != nil {
				e.cancel()
			}
		case "resume":
			e.paused = false
		case "clear":
			if e.cancel != nil {
				e.cancel()
			}
			copyEntry := *e
			copyEntry.cancel = nil
			copyEntry.busy = false
			copyEntry.manual = false
			m.entries[key(e.credential)] = &copyEntry
			e = &copyEntry
			e.state = ""
			e.Length = 0
			e.Digest = ""
			e.ExpiresAt = time.Time{}
			e.CurrentUses = 0
		}
	}
	return matched, previousAcquired
}

// Tick starts at most the configured concurrency; tasks are deduplicated per pair.
func (m *Manager) Tick(ctx context.Context, now time.Time, probe Probe) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.cfg.Enabled || probe == nil || ctx.Err() != nil {
		return
	}
	keys := make([]string, 0, len(m.entries))
	for k := range m.entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if m.running >= m.cfg.Concurrency {
			break
		}
		e := m.entries[k]
		if e.busy || e.paused || e.Exhausted || now.Before(e.NextAttempt) {
			continue
		}
		active := !e.ManualOnly && (m.cfg.Acquisition == "all" || (m.cfg.Acquisition == "active" && !e.LastUsed.IsZero() && now.Sub(e.LastUsed) <= time.Duration(m.cfg.ActiveMinutes)*time.Minute))
		if !e.manual && (!active || (e.state != "" && now.Before(e.ExpiresAt.Add(-time.Duration(m.cfg.RefreshBeforeMinutes)*time.Minute)))) {
			continue
		}
		taskCtx, cancel := context.WithCancel(ctx)
		e.cancel = cancel
		e.busy = true
		e.manual = false
		e.Attempts++
		m.running++
		m.wg.Add(1)
		version, cfg, credential := m.version, m.cfg.ForCredential(e.credential.Plan, e.Model), e.credential
		go func() {
			defer m.wg.Done()
			defer cancel()
			result, err := probe(taskCtx, credential, cfg)
			m.finish(k, e, version, taskCtx.Err(), result, err, time.Now())
		}()
	}
}

// Wait joins canceled workers after disabling acquisition.
func (m *Manager) Wait() { m.wg.Wait() }

func (m *Manager) finish(k string, expected *entry, version uint64, canceled error, result Result, err error, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.running--
	e := m.entries[k]
	if e != expected || version != m.version {
		return
	}
	e.busy = false
	e.cancel = nil
	if canceled != nil || e.paused {
		return
	}
	e.Tokens += result.Tokens
	e.LastStatus = result.Status
	reason := ""
	validation := m.cfg.ForCredential(e.credential.Plan, e.Model)
	switch {
	case err != nil:
		reason = result.FailureReason
		if reason == "" {
			reason = "acquisition_request_failed"
		}
	case !result.Completed:
		reason = "response_not_completed"
	case result.State == "" || len(result.State) > 8192 || strings.ContainsAny(result.State, "\r\n\x00"):
		reason = "invalid_or_missing_state"
	case len(validation.Lengths) > 0 && !slices.Contains(validation.Lengths, len(result.State)):
		reason = "state_length_mismatch"
	case *validation.MatchModel && result.Model != e.Model:
		reason = "response_model_mismatch"
	case validation.ResponseContains != "" && !strings.Contains(result.Answer, validation.ResponseContains):
		reason = "response_text_mismatch"
	}
	if reason != "" {
		e.LastError = reason
		e.failures++
		e.ConsecutiveFailures = e.failures
		delay := time.Duration(m.cfg.RetrySeconds) * time.Second * time.Duration(1<<min(e.failures-1, 6))
		if e.failures >= m.cfg.MaxAttempts {
			e.Exhausted = true
			e.NextAttempt = time.Time{}
		} else {
			e.NextAttempt = now.Add(delay)
		}
		log.WithFields(log.Fields{"auth_id": e.credential.ID, "model": e.Model, "reason": reason, "consecutive_failures": e.ConsecutiveFailures, "paused_after_failure_limit": e.Exhausted}).Warn("Codex managed state acquisition rejected")
		return
	}
	e.state = result.State
	m.nextValueVersion++
	e.valueVersion = m.nextValueVersion
	e.Length = len(result.State)
	d := sha256.Sum256([]byte(result.State))
	e.Digest = hex.EncodeToString(d[:8])
	e.ExpiresAt = now.Add(time.Duration(m.cfg.TTLMinutes) * time.Minute)
	e.Acquired++
	e.CurrentUses = 0
	e.failures = 0
	e.ConsecutiveFailures = 0
	e.Exhausted = false
	e.LastError = ""
	e.NextAttempt = time.Time{}
}

// ExpandProxy generates one fixed substitution per placeholder width in a task.
func ExpandProxy(template string) (string, error) {
	values := map[string]string{}
	var randomErr error
	out := config.CodexStateProxyPlaceholder.ReplaceAllStringFunc(template, func(match string) string {
		if value, ok := values[match]; ok {
			return value
		}
		n, _ := strconv.Atoi(strings.Trim(match, "{}"))
		if n < 1 || n > 64 {
			randomErr = fmt.Errorf("invalid proxy placeholder")
			return ""
		}
		var b strings.Builder
		for range n {
			v, err := rand.Int(rand.Reader, big.NewInt(10))
			if err != nil {
				randomErr = err
				return ""
			}
			b.WriteByte(byte('0' + v.Int64()))
		}
		values[match] = b.String()
		return b.String()
	})
	return out, randomErr
}
