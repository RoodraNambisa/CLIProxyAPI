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
	ruleOrder         int
	pool              string
	acquireCredential Credential
	acquirePolicy     config.CodexStateOverrideConfig
	backups           []cookieBackup
	backupTarget      int
	filling           bool
	promotions        uint64
	owner             string
	main, candidate   *CookieBundle
	version           uint64
	candidateVersion  uint64
	work              *entry
	observation       string
	pending           map[string]bool
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
	if p.CheckReturnedLength() {
		if r.State == "" {
			if p.MissingReturnedState == "reject" {
				return "missing_returned_state"
			}
		} else if !config.CodexReturnedLengthAccepted(len(r.State), p.ReturnedLengthMode, p.Lengths) {
			return "state_length_mismatch"
		}
	}
	if *p.MatchModel && !config.CodexReturnedModelAccepted(model, r.Model, p.AcceptedReturnedModels) {
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
		if e.policy.CookieOnly() && cookiePoolKey(e.credential.ID, CookiePool(e.credential, e.policy)) == id {
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
			wanted[cookiePoolKey(e.credential.ID, CookiePool(e.credential, e.policy))] = true
		}
	}
	for id := range wanted {
		entries := m.cookieEntriesLocked(id)
		selected := entries[0]
		_, firstRule, _ := m.cfg.PolicyFor(selected.credential.Scope())
		target := 0
		for _, e := range entries {
			target = max(target, e.policy.CookieBackupCount)
		}
		g := m.cookies[id]
		if g == nil || g.owner != selected.credential.Owner {
			if g != nil && g.work.cancel != nil {
				g.work.cancel()
			}
			acquireCredential, acquirePolicy := CookieAcquisition(m.cfg, selected.credential, selected.policy)
			m.cookies[id] = &cookieGroup{ruleOrder: firstRule.RuleIndex, pool: CookiePool(selected.credential, selected.policy), acquireCredential: acquireCredential, acquirePolicy: acquirePolicy, owner: selected.credential.Owner, backupTarget: target, work: &entry{credential: selected.credential, policy: selected.policy, Snapshot: Snapshot{Model: selected.Model, Status: "missing"}}}
			continue
		}
		g.ruleOrder = firstRule.RuleIndex
		g.backupTarget = target
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
		acquireCredential, acquirePolicy := CookieAcquisition(m.cfg, selected.credential, selected.policy)
		if w.credential.Instance != selected.credential.Instance || w.Model != selected.Model || !reflect.DeepEqual(w.policy, selected.policy) || g.acquireCredential.Model != acquireCredential.Model || !reflect.DeepEqual(g.acquirePolicy, acquirePolicy) {
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
			if w.Model == selected.Model && (!sameCookieValidation(w.policy, selected.policy) || g.acquireCredential.Model != acquireCredential.Model || !sameCookieValidation(g.acquirePolicy, acquirePolicy)) {
				g.main = nil
				g.backups = nil
				g.version = 0
				g.observation = "rules_changed"
				next.resetRetryCycle()
			}
			g.candidate = nil
			g.work = &next
		}
		g.acquireCredential, g.acquirePolicy = acquireCredential, acquirePolicy
		g.pruneBackups(time.Now())
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
	return a.CookiePoolMode == b.CookiePoolMode && a.CookiePoolGroup == b.CookiePoolGroup && a.CookieAcquisitionModel == b.CookieAcquisitionModel && a.ReturnedLengthMode == b.ReturnedLengthMode && reflect.DeepEqual(a.AcceptedReturnedModels, b.AcceptedReturnedModels) && reflect.DeepEqual(a.Lengths, b.Lengths) && reflect.DeepEqual(a.MatchModel, b.MatchModel) && a.Prompt == b.Prompt && a.ResponseContains == b.ResponseContains && a.CookieVerifyAfterAcquire == b.CookieVerifyAfterAcquire && a.MissingReturnedState == b.MissingReturnedState
}

func (m *Manager) cookieSnapshotLocked(id string, now time.Time) *CookieSnapshot {
	g := m.cookies[id]
	if g == nil {
		return nil
	}
	w := g.work
	s := &CookieSnapshot{Snapshot: w.Snapshot, Pool: g.pool, AcquisitionModel: g.acquireCredential.Model, SharedModels: []string{}, Main: g.main.snapshot(g.version, now, w.policy), Candidate: g.candidate.snapshot(g.candidateVersion, now, w.policy), Observation: g.observation, BackupTarget: g.backupTarget, Promotions: g.promotions, Backups: []*CookieBundleSnapshot{}}
	for _, b := range g.backups {
		if b.bundle.Select(b.bundle.Origin, now, w.policy).Header != "" {
			s.Backups = append(s.Backups, b.bundle.snapshot(b.version, now, w.policy))
		}
	}
	for _, e := range m.cookieEntriesLocked(id) {
		s.SharedModels = append(s.SharedModels, e.Model)
	}
	s.AllowedLengths = append([]int(nil), g.acquirePolicy.Lengths...)
	s.LengthMode = g.acquirePolicy.ReturnedLengthMode
	s.LastReturnedLength = cloneCookieInt(w.LastReturnedLength)
	s.InvalidationLength = cloneCookieInt(w.InvalidationLength)
	switch {
	case w.paused:
		s.Status = "paused"
	case w.busy:
		s.Status = "acquiring"
	case w.manual || len(g.pending) > 0 && !w.Exhausted && !w.RoundWaiting && !now.Before(w.NextAttempt):
		s.Status = "queued"
	case g.main != nil && g.main.Select(g.main.Origin, now, w.policy).Header != "" || len(s.Backups) > 0:
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
	g := m.cookies[cookiePoolKey(c.ID, CookiePool(c, p))]
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
	if selection.Header == "" && m.promoteCookie(g, rawURL, now, p) {
		selection = g.main.Select(rawURL, now, p)
		m.publishAvailabilityLocked()
	}
	if selection.Header != "" {
		selection.Version, selection.Pool = g.version, g.pool
		w.Uses++
		w.CurrentUses++
		return selection, "", true
	}
	w.Misses++
	return selection, p.MissingPolicy, true
}

func (m *Manager) cookieActionLocked(id, model, action string) (bool, uint64) {
	g := m.cookies[id]
	if g == nil {
		return false, 0
	}
	w := g.work
	if model != "" && action == "acquire" {
		found := false
		for _, e := range m.entries {
			if cookiePoolKey(e.credential.ID, CookiePool(e.credential, e.policy)) == id && e.Model == model && e.policy.CookieOnly() {
				found = true
				if !w.busy {
					w.credential = e.credential
					w.Model = model
					w.policy = e.policy
					g.acquireCredential, g.acquirePolicy = CookieAcquisition(m.cfg, e.credential, e.policy)
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
		g.backups = nil
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
	busyCredentials := map[string]bool{}
	for id, g := range m.cookies {
		ids = append(ids, id)
		if g.work.busy {
			busyCredentials[g.work.credential.ID] = true
		}
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := m.cookies[ids[i]], m.cookies[ids[j]]
		if a.work.credential.ID != b.work.credential.ID {
			return a.work.credential.ID < b.work.credential.ID
		}
		if a.ruleOrder != b.ruleOrder {
			return a.ruleOrder < b.ruleOrder
		}
		return ids[i] < ids[j]
	})
	for _, id := range ids {
		if m.running >= min(m.cfg.Concurrency, capacity) {
			return
		}
		g := m.cookies[id]
		w := g.work
		g.pruneBackups(now)
		if len(g.backups) > 0 && (g.main == nil || g.main.Select(g.main.Origin, now, w.policy).Header == "") {
			m.promoteCookie(g, g.backups[0].bundle.Origin, now, w.policy)
		}
		if w.busy || busyCredentials[w.credential.ID] || w.paused || w.Exhausted || now.Before(w.NextAttempt) {
			continue
		}
		candidates := m.cookieEntriesLocked(id)
		var selected *entry
		filling := false
		for _, e := range candidates {
			manual := w.manual && w.Model == e.Model || g.pending[e.Model]
			valid := g.main != nil && g.main.Select(g.main.Origin, now, e.policy).Header != ""
			active := !e.ManualOnly && (e.policy.Acquisition == "all" || e.policy.Acquisition == "active" && (!e.LastUsed.IsZero() && now.Sub(e.LastUsed) <= time.Duration(e.policy.ActiveMinutes)*time.Minute || e.policy.MissingPolicy == "hide" && !valid))
			recovering := w.automaticRounds() && (w.RoundWaiting || w.RetryRoundsUsed > 0)
			if !manual && !active && !recovering {
				continue
			}
			end := g.main.refreshDeadline(e.policy, now)
			needsRefresh := !valid || !end.IsZero() && !now.Before(end)
			needsBackups := g.readyBackups(now, e.policy) < g.backupTarget
			if !manual && !recovering && !needsRefresh && !needsBackups {
				continue
			}
			filling = !manual && !needsRefresh && needsBackups
			selected = e
			break
		}
		if selected == nil {
			continue
		}
		delete(g.pending, selected.Model)
		g.filling = filling
		w.credential = selected.credential
		w.Model = selected.Model
		w.policy = selected.policy
		g.acquireCredential, g.acquirePolicy = CookieAcquisition(m.cfg, selected.credential, selected.policy)
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
		busyCredentials[w.credential.ID] = true
		w.manual = false
		w.Attempts++
		m.running++
		m.wg.Add(1)
		policy, credential := g.acquirePolicy, g.acquireCredential
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
	reason := ValidateAcquisition(g.acquirePolicy, g.acquireCredential.Model, r, now)
	if err != nil {
		reason = r.FailureReason
		if reason == "" {
			reason = "acquisition_request_failed"
		}
	}
	g.pruneBackups(now)
	if reason == "" && g.backupTarget > 0 && g.duplicate(r.Cookies, now) {
		reason = "duplicate_cookie"
	}
	if reason != "" {
		w.LastError = reason
		w.lastFailure = now
		w.failures++
		w.ConsecutiveFailures = w.failures
		w.scheduleRetry()
		return
	}
	bundle := cloneCookieBundle(r.Cookies)
	mainValid := g.main != nil && g.main.Select(g.main.Origin, now, w.policy).Header != ""
	if g.filling && mainValid && g.backupTarget > 0 {
		m.nextValueVersion++
		g.backups = append(g.backups, cookieBackup{bundle, m.nextValueVersion})
	} else {
		if mainValid && g.backupTarget > 0 {
			end := g.main.refreshDeadline(w.policy, now)
			if end.IsZero() || now.Before(end) {
				g.backups = append(g.backups, cookieBackup{g.main, g.version})
			}
		}
		m.activateCookie(g, bundle)
	}
	g.pruneBackups(now)
	g.observation = "matches_rules"
	w.Acquired++
	w.LastError = ""
	w.resetRetryCycle()
}

func (m *Manager) ObserveCookie(c Credential, used CookieSelection, p config.CodexStateOverrideConfig, headers http.Header, model string, completed bool) bool {
	return m.ObserveCookieEvidence(c, used, p, headers, model, completed, false)
}

func (m *Manager) ObserveCookieEvidence(c Credential, used CookieSelection, p config.CodexStateOverrideConfig, headers http.Header, model string, completed, evidence bool) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	defer m.publishAvailabilityLocked()
	g := m.cookies[cookiePoolKey(c.ID, used.Pool)]
	if g == nil || g.owner != c.Owner || used.Version == 0 {
		return false
	}
	bundle, backupIndex := g.main, -1
	if g.version != used.Version {
		bundle = nil
		for i, spare := range g.backups {
			if spare.version == used.Version {
				bundle, backupIndex = spare.bundle, i
				break
			}
		}
	}
	if bundle == nil {
		return false
	}
	w := g.work
	reason, mismatch := "", ""
	now := time.Now()
	if headers != nil {
		length := len(headers.Get("X-Codex-Turn-State"))
		w.LastReturnedLength = new(length)
		if (completed || evidence) && p.CheckReturnedLength() {
			if length == 0 && p.MissingReturnedState == "reject" {
				mismatch = "missing_returned_state"
			}
			if length > 0 && !config.CodexReturnedLengthAccepted(length, p.ReturnedLengthMode, p.Lengths) {
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
				for i, current := range bundle.Members {
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
						bundle.Members[i] = update
						bundle.Members[i].Version = current.Version + 1
					} else if update.Cookie.Name == "__cf_bm" {
						bundle.Members[i] = update
						bundle.Members[i].Version = current.Version + 1
					}
					break
				}
				// Missing members may be added only once. A later response from the same
				// old request has no matching member version and cannot overwrite them.
				if !found && update.Cookie.Name == "__cf_bm" && used.Members[key] == 0 {
					bundle.Members = append(bundle.Members, update)
				}
			}
		}
	}
	if model != "" {
		w.LastReturnedModel, _ = managementdiag.ProcessText(model, "safe", 256)
	}
	if completed {
		w.Completed++
	}
	if completed || evidence {
		if model != "" && !config.CodexReturnedModelAccepted(c.Model, model, p.AcceptedReturnedModels) && (p.MatchModel == nil || *p.MatchModel || p.InvalidateOnModelMismatch) {
			mismatch = "response_model_mismatch"
			if p.InvalidateOnModelMismatch {
				reason = mismatch
			}
		}
	}
	if reason == "" {
		if completed || evidence {
			g.observation = "matches_rules"
			if mismatch != "" {
				g.observation = mismatch
			} else if model == "" && (p.MatchModel == nil || *p.MatchModel) {
				g.observation = "missing_returned_model"
			}
		}
		return false
	}
	if backupIndex >= 0 {
		g.backups = slices.Delete(g.backups, backupIndex, backupIndex+1)
	} else {
		g.main = nil
		g.version = 0
	}
	g.observation = reason
	w.Invalidations++
	w.LastInvalidation = reason
	w.InvalidationModel = w.LastReturnedModel
	w.InvalidationLength = cloneCookieInt(w.LastReturnedLength)
	if backupIndex < 0 {
		m.promoteCookie(g, used.URL, now, p)
	}
	if g.main == nil && !w.busy && !w.Exhausted && !w.ManualOnly && !w.RoundWaiting {
		w.manual = true
	}
	return true
}

func (m *Manager) CookieCandidate(c Credential, target string, now time.Time, p config.CodexStateOverrideConfig) CookieSelection {
	m.mu.Lock()
	defer m.mu.Unlock()
	if g := m.cookies[cookiePoolKey(c.ID, CookiePool(c, p))]; g != nil && g.owner == c.Owner && g.candidate != nil {
		selection := g.candidate.Select(target, now, p)
		selection.Version, selection.Pool = g.candidateVersion, g.pool
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
	g := m.cookies[cookiePoolKey(c.ID, CookiePool(c, e.policy))]
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
	g.acquireCredential, g.acquirePolicy = CookieAcquisition(m.cfg, c, e.policy)
	g.work.ManualOnly = true
	g.work.manual = true
	g.work.paused = false
	g.work.resetRetryCycle()
	return true, g.work.Acquired
}
