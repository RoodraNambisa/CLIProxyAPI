package auth

import (
	"context"
	"slices"
	"strings"
	"sync"

	internalcodex "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func ResponseGuardCredentialScope(a *Auth, model string) config.CodexStateScope {
	if a == nil {
		return config.CodexStateScope{}
	}
	plan := a.Attributes["plan_type"]
	if plan == "" {
		plan = internalcodex.EffectivePlanType(a.Metadata)
	}
	return config.CodexStateScope{ID: a.ID, ShortID: a.Index, Name: a.FileName, Plan: plan, Model: thinking.ParseSuffix(model).ModelName, Priority: authPriority(a)}
}

func (m *Manager) withResponseGuardAttempt(ctx context.Context, a *Auth, req core.Request, opts core.Options) core.Options {
	opts.ResponseGuard = nil
	if a == nil || a.ExecutionProvider() != "codex" || opts.Alt == "responses/compact" {
		return opts
	}
	policy := m.selectionPolicy(ctx)
	if policy == nil {
		return opts
	}
	opts = ensureClientResponseModelMetadata(opts, req.Model)
	requested, _ := opts.Metadata[clientResponseModelKey].(string)
	scope := ResponseGuardCredentialScope(a, req.Model)
	scope.Aliases = []string{requested, thinking.ParseSuffix(requested).ModelName}
	resolved := policy.codexResponseGuard.PolicyFor(scope)
	var clear func()
	if resolved.Mode == "enforce" {
		clear = m.captureResponseGuardAffinity(ctx, a, req.Model, opts, resolved.ClearAffinity)
	}
	var rejected sync.Once
	var previous *core.ResponseGuardRecord
	opts.ResponseGuard = core.NewResponseGuardAttempt(policy.codexResponseGuard, requested, core.NextResponseGuardAttempt(ctx), func(r core.ResponseGuardRecord) {
		if r.Outcome == "blocked" || r.Outcome == "aborted" {
			rejected.Do(func() {
				core.ExcludeResponseGuardAuth(ctx, a.ID)
				if !core.SingleAttempt(ctx) && clear != nil {
					clear()
				}
			})
		}
		m.recordResponseGuard(a, r, previous)
		copy := r
		previous = &copy
	})
	return opts
}

func (m *Manager) captureResponseGuardAffinity(ctx context.Context, a *Auth, model string, opts core.Options, mode string) func() {
	if mode == "none" || core.SingleAttempt(ctx) {
		return nil
	}
	s, ok := m.selectorForContext(ctx).(*SessionAffinitySelector)
	if !ok || s == nil {
		return nil
	}
	var releases []func()
	if mode == "credential" {
		releases = append(releases, s.cache.captureGuardInvalidation(a.ID, nil))
		if s.historyMatcher != nil {
			releases = append(releases, s.historyMatcher.CaptureInvalidation("", nil, a.ID))
		}
	} else {
		provider := "codex"
		if snapshot := sessionBindingSnapshotFromOptions(opts, s.cache); snapshot != nil {
			provider, model = snapshot.provider, snapshot.model
			releases = append(releases, func() { snapshot.release(a.ID) })
		}
		primary, fallback := s.sessionIDs(ctx, opts)
		keys := []string{}
		for _, id := range []string{primary, fallback} {
			if id != "" {
				keys = append(keys, provider+"::"+id+"::"+canonicalModelKey(model))
			}
		}
		if len(keys) > 0 {
			releases = append(releases, s.cache.captureGuardInvalidation(a.ID, keys))
		}
		if namespace, history, ok := s.historyRequest(ctx, provider, model, opts); ok {
			releases = append(releases, s.historyMatcher.CaptureInvalidation(namespace, history, a.ID))
		}
	}
	return func() {
		m.mu.RLock()
		defer m.mu.RUnlock()
		current := m.auths[a.ID]
		if current == nil || current.RuntimeInstanceID() != a.RuntimeInstanceID() || m.sessionCleanupPendingLocked(a.ID) {
			return
		}
		for _, release := range releases {
			if release != nil {
				release()
			}
		}
	}
}

func (c *SessionCache) captureGuardInvalidation(authID string, keys []string) func() {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	versions := map[string]uint64{}
	for key, entry := range c.entries {
		if entry.authID == authID && (keys == nil || slices.Contains(keys, key)) {
			versions[key] = entry.bindingVersion
		}
	}
	c.mu.RUnlock()
	return func() {
		for key, version := range versions {
			c.invalidateBindingIfVersion(key, authID, version)
		}
	}
}

func (m *Manager) recordResponseGuard(a *Auth, r core.ResponseGuardRecord, previous *core.ResponseGuardRecord) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	current := m.auths[a.ID]
	if current == nil || current.RuntimeInstanceID() != a.RuntimeInstanceID() {
		return
	}
	stats := &m.responseModelStats
	stats.mu.Lock()
	defer stats.mu.Unlock()
	if stats.auths == nil {
		stats.auths = map[string]*responseModelAuthStats{}
	}
	entry := stats.auths[a.ID]
	if entry == nil {
		entry = &responseModelAuthStats{}
		stats.auths[a.ID] = entry
	}
	if stats.since.IsZero() {
		stats.since = r.At
	}
	index := -1
	for i := range entry.recent {
		if old := entry.recent[i].Validation; old != nil && old.ID == r.ID {
			index = i
			break
		}
	}
	blocked := func(v *core.ResponseGuardRecord) bool {
		return v != nil && (v.Outcome == "blocked" || v.Outcome == "aborted")
	}
	observed := func(v *core.ResponseGuardRecord) bool { return v != nil && v.Outcome == "observed" }
	if blocked(&r) && !blocked(previous) {
		entry.blocked++
	}
	if observed(&r) && !observed(previous) {
		entry.observed++
	}
	if observed(previous) && !observed(&r) && entry.observed > 0 {
		entry.observed--
	}
	if r.Rewritten && (previous == nil || !previous.Rewritten) {
		entry.total++
	}
	if index < 0 {
		if previous != nil {
			return
		}
		entry.recent = append(entry.recent, ResponseModelRewriteRecord{})
		index = len(entry.recent) - 1
	}
	entry.recent[index] = ResponseModelRewriteRecord{At: r.At, RequestedModel: r.RequestedModel, OriginalModel: r.OriginalModel, ResponseModel: r.ResponseModel, Rule: r.RewriteRule, Stream: r.Stream, Validation: &r}
	if len(entry.recent) > responseModelRecentLimit {
		entry.recent = slices.Clone(entry.recent[len(entry.recent)-responseModelRecentLimit:])
	}
}

// ResponseGuardMayEnforce avoids committing non-streaming keepalives before a
// matching credential can be selected. It is conservative across rule scopes.
func (m *Manager) ResponseGuardMayEnforce(ctx context.Context) bool {
	if m == nil {
		return false
	}
	if core.SingleAttempt(ctx) || sdkaccess.CredentialTargetAuthID(ctx) != "" {
		return sdkaccess.CredentialTargetAppliesResponseGuard(ctx)
	}
	policy := m.selectionPolicy(ctx)
	if policy == nil {
		return false
	}
	c := policy.codexResponseGuard
	if !c.Enabled {
		return false
	}
	if c.Mode != nil && *c.Mode == "enforce" {
		return true
	}
	if c.Rules != nil {
		for _, r := range *c.Rules {
			if r.Enabled != nil && !*r.Enabled {
				continue
			}
			if r.Settings.Mode != nil && *r.Settings.Mode == "enforce" {
				return true
			}
			for _, o := range r.ModelOverrides {
				if (o.Enabled == nil || *o.Enabled) && o.Settings.Mode != nil && *o.Settings.Mode == "enforce" {
					return true
				}
			}
		}
	}
	return strings.EqualFold(core.ResponseGuardMode(ctx), "enforce")
}
