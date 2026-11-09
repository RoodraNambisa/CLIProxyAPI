package auth

import (
	"context"
	"strings"
	"sync"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type webImageAggregationKey struct{}

type webImageAggregationSelection struct {
	mu   sync.Mutex
	used map[string]struct{}
}

// WithChatGPTWebImageAggregation prefers distinct credentials within a Web
// image batch, while preserving targeted credentials and session affinity.
func WithChatGPTWebImageAggregation(ctx context.Context) context.Context {
	return context.WithValue(ctx, webImageAggregationKey{}, &webImageAggregationSelection{used: make(map[string]struct{})})
}

func (m *Manager) pickWebImageAggregation(ctx context.Context, providers []string, model string, opts core.Options, tried map[string]struct{}, allowed []func(*Auth) bool) (*Auth, ProviderExecutor, string, error, bool) {
	group, _ := ctx.Value(webImageAggregationKey{}).(*webImageAggregationSelection)
	if group == nil || len(providers) != 1 || providers[0] != "chatgpt-web" || authenticatedModelTarget(ctx, opts) != "" || pinnedAuthIDFromMetadata(opts.Metadata) != "" {
		return nil, nil, "", nil, false
	}
	if _, affinity := m.selectorForContext(ctx).(*SessionAffinitySelector); affinity {
		return nil, nil, "", nil, false
	}
	// Serialize only selection, never credential queueing or upstream execution.
	group.mu.Lock()
	defer group.mu.Unlock()
	ctx = context.WithValue(ctx, webImageAggregationKey{}, (*webImageAggregationSelection)(nil))
	preferred := make(map[string]struct{}, len(tried)+len(group.used))
	for id := range tried {
		preferred[id] = struct{}{}
	}
	for id := range group.used {
		preferred[id] = struct{}{}
	}
	auth, executor, provider, err := m.pickNextMixedImmediate(ctx, providers, model, opts, preferred, allowed...)
	if err != nil && ctx.Err() == nil && len(group.used) > 0 {
		// A small pool may reuse a credential, subject to its normal admission.
		auth, executor, provider, err = m.pickNextMixedImmediate(ctx, providers, model, opts, tried, allowed...)
	}
	if auth != nil && err == nil {
		group.used[auth.ID] = struct{}{}
	}
	return auth, executor, provider, err, true
}

// HasProviderCredentials conservatively includes disabled credentials when
// deciding whether a request can enter the Web-only aggregation path.
func (m *Manager) HasProviderCredentials(provider string) bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.providerAuthIDs[strings.ToLower(strings.TrimSpace(provider))]) > 0
}
