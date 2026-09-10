package auth

import (
	"context"
	"sync"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

const sessionAffinityResultSnapshotKey = "__session_affinity_result_snapshot"

// Each attempt observes its binding before selection. Its own selection commit
// advances the observation; an unrelated rebind makes a later result a no-op.
type sessionBindingSnapshot struct {
	mu                   sync.Mutex
	cache                *SessionCache
	provider, model, key string
	version              uint64
}

func (s *SessionAffinitySelector) captureBindingSnapshot(ctx context.Context, provider, model string, opts core.Options) *sessionBindingSnapshot {
	if s == nil || s.cache == nil {
		return nil
	}
	primary, _ := s.sessionIDs(ctx, opts)
	if primary == "" {
		return nil
	}
	model = canonicalModelKey(model)
	key := provider + "::" + primary + "::" + model
	return &sessionBindingSnapshot{cache: s.cache, provider: provider, model: model, key: key, version: s.cache.bindingVersion(key)}
}

func (m *Manager) withSessionAffinityResultSnapshot(ctx context.Context, providers []string, model string, opts core.Options) core.Options {
	selector := m.selectorForContext(ctx)
	var snapshot *sessionBindingSnapshot
	if observer, ok := selector.(interface {
		captureBindingSnapshot(context.Context, string, string, core.Options) *sessionBindingSnapshot
	}); ok {
		snapshot = observer.captureBindingSnapshot(ctx, affinityProviderKey(providers), selectionArgForSelector(selector, model), opts)
	}
	if snapshot == nil && opts.Metadata[sessionAffinityResultSnapshotKey] == nil {
		return opts
	}
	metadata := make(map[string]any, len(opts.Metadata)+1)
	for key, value := range opts.Metadata {
		metadata[key] = value
	}
	delete(metadata, sessionAffinityResultSnapshotKey)
	if snapshot != nil {
		metadata[sessionAffinityResultSnapshotKey] = snapshot
	}
	opts.Metadata = metadata
	return opts
}

func sessionBindingSnapshotFromOptions(opts core.Options, cache *SessionCache) *sessionBindingSnapshot {
	snapshot, _ := opts.Metadata[sessionAffinityResultSnapshotKey].(*sessionBindingSnapshot)
	if snapshot == nil || snapshot.cache != cache {
		return nil
	}
	return snapshot
}

func (s *sessionBindingSnapshot) bind(authID string) sessionMutation {
	s.mu.Lock()
	defer s.mu.Unlock()
	mutation := s.cache.setWithRollbackIfVersion(s.key, authID, s.version)
	if mutation.version != 0 {
		s.version = mutation.bindingVersion
	}
	return mutation
}
