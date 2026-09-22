package codexstate

import (
	"context"
	"net/http"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementdiag"
)

type cookieGroup struct {
	owner            string
	main, candidate  *CookieBundle
	version          uint64
	candidateVersion uint64
	work             *entry
	observation      string
	pending          map[string]bool
}
type cookieCandidateKey struct{}

// PublishCookieCandidate reports progress without exposing cookie values to management APIs.
func PublishCookieCandidate(ctx context.Context, b *CookieBundle) {
	if fn, _ := ctx.Value(cookieCandidateKey{}).(func(*CookieBundle)); fn != nil {
		fn(b)
	}
}

func ValidateAcquisition(p config.CodexStateOverrideConfig, model string, r Result, now time.Time) string {
	p = p.Resolved()
	if !r.Completed {
		return "response_not_completed"
	}
	if p.CookieOnly() {
		if r.Cookies == nil || r.Cookies.Select(r.Cookies.Origin, now, p).Header == "" {
			return "invalid_or_missing_cookie"
		}
	} else if r.State == "" || len(r.State) > 8192 || strings.ContainsAny(r.State, "\r\n\x00") {
		return "invalid_or_missing_state"
	}
	if len(p.Lengths) > 0 {
		if r.State == "" {
			if p.MissingReturnedState == "reject" {
				return "missing_returned_state"
			}
		} else {
			matched := false
			for _, n := range p.Lengths {
				matched = matched || n == len(r.State)
			}
			if !matched {
				return "state_length_mismatch"
			}
		}
	}
	if *p.MatchModel && r.Model != model {
		return "response_model_mismatch"
	}
	if p.ResponseContains != "" && !strings.Contains(r.Answer, p.ResponseContains) {
		return "response_text_mismatch"
	}
	return ""
}

// cookieEntriesLocked orders pending demands by the same rule precedence as routing.
func (m *Manager) cookieEntriesLocked(id string) []*entry {
	var entries []*entry
	for _, e := range m.entries {
		if e.credential.ID == id && e.policy.CookieOnly() {
			entries = append(entries, e)
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		_, a, _ := m.cfg.PolicyFor(entries[i].credential.Scope())
		_, b, _ := m.cfg.PolicyFor(entries[j].credential.Scope())
		if a.RuleIndex != b.RuleIndex {
			return a.RuleIndex < b.RuleIndex
		}
		return entries[i].Model < entries[j].Model
	})
	return entries
}

func (m *Manager) syncCookiesLocked() {
	if m.cookies == nil {
		m.cookies = map[string]*cookieGroup{}
	}
	wanted := map[string]bool{}
	for _, e := range m.entries {
		if e.policy.CookieOnly() {
			wanted[e.credential.ID] = true
		}
	}
	for id := range wanted {
		entries := m.cookieEntriesLocked(id)
		selected := entries[0]
		g := m.cookies[id]
		if g == nil || g.owner != selected.credential.Owner {
			if g != nil && g.work.cancel != nil {
				g.work.cancel()
			}
			m.cookies[id] = &cookieGroup{owner: selected.credential.Owner, work: &entry{credential: selected.credential, policy: selected.policy, Snapshot: Snapshot{Model: selected.Model, Status: "missing"}}}
			continue
		}
		validModels := map[string]bool{}
		for _, e := range entries {
			validModels[e.Model] = true
		}
		for model := range g.pending {
			if !validModels[model] {
				delete(g.pending, model)
			}
		}
		w := g.work
		for _, e := range entries {
			if e.Model == w.Model {
				selected = e
				break
			}
		}
		if w.credential.Instance != selected.credential.Instance || w.Model != selected.Model || !reflect.DeepEqual(w.policy, selected.policy) {
			if w.cancel != nil {
				w.cancel()
			}
			next := *w
			next.credential, next.policy = selected.credential, selected.policy
			next.Model, next.RuleID, next.RuleName = selected.Model, selected.RuleID, selected.RuleName
			next.ManualOnly = selected.ManualOnly
			next.busy, next.cancel = false, nil
			next.manual = w.manual || w.busy
			// Ordinary token or proxy updates retain the credential's cookies. A new
			// validation contract cannot silently inherit an unverified acquisition.
			if w.Model == selected.Model && (!sameCookieValidation(w.policy, selected.policy)) {
				g.main = nil
				g.version = 0
				g.observation = "rules_changed"
				next.resetRetryCycle()
			}
			g.candidate = nil
			g.work = &next
		}
	}
	for id, g := range m.cookies {
		if !wanted[id] {
			if g.work.cancel != nil {
				g.work.cancel()
			}
			delete(m.cookies, id)
		}
	}
}

func sameCookieValidation(a, b config.CodexStateOverrideConfig) bool {
	return reflect.DeepEqual(a.Lengths, b.Lengths) && reflect.DeepEqual(a.MatchModel, b.MatchModel) && a.Prompt == b.Prompt && a.ResponseContains == b.ResponseContains && a.CookieVerifyAfterAcquire == b.CookieVerifyAfterAcquire && a.MissingReturnedState == b.MissingReturnedState
}

func (m *Manager) CookieSnapshot(id string, now time.Time) *CookieSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.cookies[id]
	if g == nil {
		return nil
	}
	w := g.work
	s := &CookieSnapshot{Snapshot: w.Snapshot, Main: g.main.snapshot(g.version, now, w.policy), Candidate: g.candidate.snapshot(g.candidateVersion, now, w.policy), Observation: g.observation}
	s.AllowedLengths = append([]int(nil), w.policy.Lengths...)
	s.LastReturnedLength = cloneCookieInt(w.LastReturnedLength)
	s.InvalidationLength = cloneCookieInt(w.InvalidationLength)
	switch {
	case w.paused:
		s.Status = "paused"
	case w.busy:
		s.Status = "acquiring"
	case w.manual || len(g.pending) > 0 && !w.Exhausted && !w.RoundWaiting && !now.Before(w.NextAttempt):
		s.Status = "queued"
	case g.main != nil && g.main.Select(g.main.Origin, now, w.policy).Header != "":
		s.Status = "valid"
	case w.RoundWaiting:
		s.Status = "retry_wait"
	case w.Exhausted:
		s.Status = "exhausted"
	case g.main != nil:
		if end := g.main.localExpiry(w.policy); !end.IsZero() && !now.Before(end) {
			s.Status = "local_expired"
		} else {
			s.Status = "expired"
		}
	case w.LastError != "":
		s.Status = "failed"
	default:
		s.Status = "missing"
	}
	return s
}
func cloneCookieInt(p *int) *int {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func (m *Manager) PickCookie(c Credential, rawURL string, now time.Time, p config.CodexStateOverrideConfig) (CookieSelection, string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.cookies[c.ID]
	if !m.cfg.Enabled || g == nil || g.owner != c.Owner {
		return CookieSelection{}, p.MissingPolicy, true
	}
	w := g.work
	configured, _, _ := m.resolvePolicy(m.cfg, c)
	if w.paused || !sameCookieValidation(configured, p) {
		return CookieSelection{}, p.MissingPolicy, true
	}
	if e := m.entries[key(c)]; e != nil {
		e.LastUsed = now
	}
	w.LastUsed = now
	selection := g.main.Select(rawURL, now, p)
	if selection.Header != "" {
		selection.Version = g.version
		w.Uses++
		w.CurrentUses++
		return selection, "", true
	}
	w.Misses++
	return selection, p.MissingPolicy, true
}

func (m *Manager) CookieAction(id, model, action string) (bool, uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer m.publishAvailabilityLocked()
	g := m.cookies[id]
	if g == nil {
		return false, 0
	}
	w := g.work
	if model != "" && action == "acquire" {
		found := false
		for _, e := range m.entries {
			if e.credential.ID == id && e.Model == model && e.policy.CookieOnly() {
				found = true
				if !w.busy {
					w.credential = e.credential
					w.Model = model
					w.policy = e.policy
				}
				break
			}
		}
		if !found {
			return false, w.Acquired
		}
		if w.busy && model != w.Model {
			if g.pending == nil {
				g.pending = map[string]bool{}
			}
			g.pending[model] = true
		}
	}
	previous := w.Acquired
	switch action {
	case "acquire":
		if !w.busy {
			w.manual = true
			w.paused = false
			w.resetRetryCycle()
		}
	case "pause":
		clear(g.pending)
		w.paused = true
		w.manual = false
		if w.cancel != nil {
			w.cancel()
		}
	case "resume":
		w.paused = false
	case "clear":
		clear(g.pending)
		if w.cancel != nil {
			w.cancel()
		}
		copy := *w
		copy.busy = false
		copy.cancel = nil
		copy.manual = false
		copy.ExpiresAt = time.Time{}
		copy.CurrentUses = 0
		copy.LastError = ""
		copy.resetRetryCycle()
		g.work = &copy
		g.main = nil
		g.candidate = nil
		g.version = 0
		g.observation = "cleared"
	default:
		return false, previous
	}
	return true, previous
}

func (m *Manager) tickCookiesLocked(ctx context.Context, now time.Time, probe Probe, capacity int) {
	ids := make([]string, 0, len(m.cookies))
	for id := range m.cookies {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if m.running >= min(m.cfg.Concurrency, capacity) {
			return
		}
		g := m.cookies[id]
		w := g.work
		if w.busy || w.paused || w.Exhausted || now.Before(w.NextAttempt) {
			continue
		}
		candidates := m.cookieEntriesLocked(id)
		var selected *entry
		for _, e := range candidates {
			manual := w.manual && w.Model == e.Model || g.pending[e.Model]
			valid := g.main != nil && g.main.Select(g.main.Origin, now, e.policy).Header != ""
			active := !e.ManualOnly && (e.policy.Acquisition == "all" || e.policy.Acquisition == "active" && (!e.LastUsed.IsZero() && now.Sub(e.LastUsed) <= time.Duration(e.policy.ActiveMinutes)*time.Minute || e.policy.MissingPolicy == "hide" && !valid))
			recovering := w.automaticRounds() && (w.RoundWaiting || w.RetryRoundsUsed > 0)
			if !manual && !active && !recovering {
				continue
			}
			end := g.main.refreshDeadline(e.policy, now)
			if !manual && !recovering && valid && (end.IsZero() || now.Before(end)) {
				continue
			}
			selected = e
			break
		}
		if selected == nil {
			continue
		}
		delete(g.pending, selected.Model)
		w.credential = selected.credential
		w.Model = selected.Model
		w.policy = selected.policy
		w.ManualOnly = selected.ManualOnly
		w.RuleID = selected.RuleID
		w.RuleName = selected.RuleName
		w.RetrySeconds = w.policy.RetrySeconds
		w.MaxAttempts = w.policy.MaxAttempts
		w.RetryRoundIntervalMinutes = w.policy.RetryRoundIntervalMinutes
		w.MaxRetryRounds = w.policy.MaxRetryRounds
		if w.RoundWaiting {
			w.RetryRoundsUsed++
			w.RoundWaiting = false
			w.failures = 0
			w.ConsecutiveFailures = 0
			w.lastFailure = time.Time{}
			w.NextAttempt = time.Time{}
		}
		task, cancel := context.WithCancel(ctx)
		w.cancel = cancel
		w.busy = true
		w.manual = false
		w.Attempts++
		m.running++
		m.wg.Add(1)
		policy, credential := w.policy, w.credential
		task = context.WithValue(task, cookieCandidateKey{}, func(b *CookieBundle) {
			m.mu.Lock()
			defer m.mu.Unlock()
			if m.cookies[id] == g && g.work == w && !w.paused {
				m.nextValueVersion++
				g.candidateVersion = m.nextValueVersion
				g.candidate = cloneCookieBundle(b)
			}
		})
		go func() {
			defer m.wg.Done()
			defer cancel()
			r, err := probe(task, credential, policy)
			m.finishCookie(id, g, w, task.Err(), r, err, time.Now())
		}()
	}
}

func (m *Manager) finishCookie(id string, expected *cookieGroup, work *entry, canceled error, r Result, err error, now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer m.publishAvailabilityLocked()
	m.running--
	g := m.cookies[id]
	if g != expected || g.work != work {
		return
	}
	w := g.work
	w.busy = false
	w.cancel = nil
	g.candidate = nil
	if canceled != nil || w.paused {
		return
	}
	w.Tokens += r.Tokens
	w.LastStatus = r.Status
	w.LastReturnedLength = new(len(r.State))
	w.LastReturnedModel, _ = managementdiag.ProcessText(r.Model, "safe", 256)
	reason := ValidateAcquisition(w.policy, w.Model, r, now)
	if err != nil {
		reason = r.FailureReason
		if reason == "" {
			reason = "acquisition_request_failed"
		}
	}
	if reason != "" {
		w.LastError = reason
		w.lastFailure = now
		w.failures++
		w.ConsecutiveFailures = w.failures
		w.scheduleRetry()
		return
	}
	m.nextValueVersion++
	g.version = m.nextValueVersion
	g.main = cloneCookieBundle(r.Cookies)
	g.observation = "matches_rules"
	w.Acquired++
	w.CurrentUses = 0
	w.LastError = ""
	w.ExpiresAt = g.main.expiry(w.policy)
	w.resetRetryCycle()
}

func (m *Manager) ObserveCookie(c Credential, used CookieSelection, p config.CodexStateOverrideConfig, headers http.Header, model string, completed bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer m.publishAvailabilityLocked()
	g := m.cookies[c.ID]
	if g == nil || g.owner != c.Owner || g.version != used.Version || used.Version == 0 || g.main == nil {
		return false
	}
	w := g.work
	reason, mismatch := "", ""
	now := time.Now()
	if headers != nil {
		length := len(headers.Get("X-Codex-Turn-State"))
		w.LastReturnedLength = new(length)
		if completed && len(p.Lengths) > 0 {
			if length == 0 && p.MissingReturnedState == "reject" {
				mismatch = "missing_returned_state"
			}
			if length > 0 && !slices.Contains(p.Lengths, length) {
				mismatch = "response_state_length_mismatch"
			}
			if p.InvalidateOnStateLengthMismatch {
				reason = mismatch
			}
		}
		updates := CaptureCookies(used.URL, headers, now)
		if updates != nil {
			for _, update := range updates.Members {
				key := update.key()
				found := false
				for i, current := range g.main.Members {
					if current.key() != key {
						continue
					}
					found = true
					if used.Members[key] != current.Version {
						break
					}
					if update.Cookie.MaxAge < 0 || !update.Cookie.Expires.IsZero() && !now.Before(update.Cookie.Expires) {
						if RouteCookieName(update.Cookie.Name) {
							reason = "cookie_deleted"
						}
						g.main.Members[i] = update
						g.main.Members[i].Version = current.Version + 1
					} else if update.Cookie.Name == "__cf_bm" {
						g.main.Members[i] = update
						g.main.Members[i].Version = current.Version + 1
					}
					break
				}
				// Missing members may be added only once. A later response from the same
				// old request has no matching member version and cannot overwrite them.
				if !found && update.Cookie.Name == "__cf_bm" && used.Members[key] == 0 {
					g.main.Members = append(g.main.Members, update)
				}
			}
		}
	}
	if model != "" {
		w.LastReturnedModel, _ = managementdiag.ProcessText(model, "safe", 256)
	}
	if completed {
		w.Completed++
		if model != "" && model != c.Model && (p.MatchModel == nil || *p.MatchModel || p.InvalidateOnModelMismatch) {
			mismatch = "response_model_mismatch"
			if p.InvalidateOnModelMismatch {
				reason = mismatch
			}
		}
	}
	if reason == "" {
		if completed {
			g.observation = "matches_rules"
			if mismatch != "" {
				g.observation = mismatch
			} else if model == "" && (p.MatchModel == nil || *p.MatchModel) {
				g.observation = "missing_returned_model"
			}
		}
		return false
	}
	g.main = nil
	g.version = 0
	g.observation = reason
	w.Invalidations++
	w.LastInvalidation = reason
	w.InvalidationModel = w.LastReturnedModel
	w.InvalidationLength = cloneCookieInt(w.LastReturnedLength)
	if !w.busy && !w.Exhausted && !w.ManualOnly && !w.RoundWaiting {
		w.manual = true
	}
	return true
}

func (m *Manager) CookieCandidate(c Credential, target string, now time.Time, p config.CodexStateOverrideConfig) CookieSelection {
	m.mu.Lock()
	defer m.mu.Unlock()
	if g := m.cookies[c.ID]; g != nil && g.owner == c.Owner && g.candidate != nil {
		selection := g.candidate.Select(target, now, p)
		selection.Version = g.candidateVersion
		return selection
	}
	return CookieSelection{}
}

// QueueManualStrategy keeps diagnostic Cookie acquisition outside normal routing.
func (m *Manager) QueueManualStrategy(c Credential, strategy string) (bool, uint64) {
	ok, previous := m.QueueManual(c, strategy)
	if !ok || strategy != "cookie-only" {
		return ok, previous
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.entries[key(c)]
	if e == nil || e.busy {
		return false, previous
	}
	e.manualStrategy = strategy
	e.policy.Strategy = strategy
	m.syncCookiesLocked()
	g := m.cookies[c.ID]
	if g.work.busy {
		if c.Model != g.work.Model {
			if g.pending == nil {
				g.pending = map[string]bool{}
			}
			g.pending[c.Model] = true
		}
		return true, g.work.Acquired
	}
	g.work.credential = c
	g.work.Model = c.Model
	g.work.policy = e.policy
	g.work.ManualOnly = true
	g.work.manual = true
	g.work.paused = false
	g.work.resetRetryCycle()
	return true, g.work.Acquired
}
