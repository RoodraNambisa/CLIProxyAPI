package auth

import (
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func requestScopedConfig(action string) *config.Config {
	return &config.Config{OAuthRequestScopedErrors: map[string][]config.RequestScopedErrorRule{
		" CoDeX ": {{Status: 400, Match: []string{"fixture"}, Action: action}},
	}}
}

func TestRequestScopedErrorPolicyPreservesRequestAndSelectorSnapshots(t *testing.T) {
	m := NewManager(nil, nil, nil)
	cfg := requestScopedConfig("stop")
	m.SetConfig(cfg)
	ctx := m.WithRoutingPolicySnapshot(t.Context())
	captured := m.requestOAuthErrorRules(ctx, "codex")
	cfg.OAuthRequestScopedErrors[" CoDeX "][0].Match[0] = "changed"
	if action, ok := captured.Match(400, "fixture"); !ok || action != config.RequestScopedActionStop {
		t.Fatal("published rules retained mutable configuration")
	}
	m.SetSelector(&FillFirstSelector{})
	if m.requestOAuthErrorRules(t.Context(), "codex") != captured {
		t.Fatal("selector-only change recompiled or lost error rules")
	}
	m.SetConfig(requestScopedConfig("continue"))
	if action, _ := m.requestOAuthErrorRules(ctx, "codex").Match(400, "fixture"); action != config.RequestScopedActionStop {
		t.Fatal("in-flight request adopted new rules")
	}
	if action, _ := m.requestOAuthErrorRules(t.Context(), "codex").Match(400, "fixture"); action != config.RequestScopedActionContinue {
		t.Fatal("new request missed rules")
	}
	m.SetConfig(&config.Config{})
	if m.requestOAuthErrorRules(t.Context(), "codex") != nil {
		t.Fatal("cleared provider rules remained active")
	}
	if action, _ := captured.Match(400, "fixture"); action != config.RequestScopedActionStop {
		t.Fatal("clearing mutated in-flight policy")
	}
}

func TestRequestScopedErrorPolicyRejectsInvalidUpdatesAtomically(t *testing.T) {
	for _, replaceSelector := range []bool{false, true} {
		m := NewManager(nil, nil, nil)
		m.SetConfig(requestScopedConfig("stop"))
		oldPolicy, oldConfig := m.routingPolicy.Load(), m.currentConfig()
		if replaceSelector {
			m.SetConfigAndSelector(requestScopedConfig("invalid"), &FillFirstSelector{})
		} else {
			m.SetConfig(requestScopedConfig("invalid"))
		}
		if m.routingPolicy.Load() != oldPolicy || m.currentConfig() != oldConfig || m.selectorForContext(t.Context()) != oldPolicy.selector {
			t.Fatal("invalid config partially published")
		}
	}
}

func TestRequestScopedErrorPolicyConcurrentReload(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetConfig(requestScopedConfig("stop"))
	ctx := m.WithRoutingPolicySnapshot(t.Context())
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				if action, _ := m.requestOAuthErrorRules(ctx, "codex").Match(400, "fixture"); action != config.RequestScopedActionStop {
					t.Error("old snapshot changed")
				}
				current := m.WithRoutingPolicySnapshot(t.Context())
				action, ok := m.requestOAuthErrorRules(current, "codex").Match(400, "fixture")
				if !ok || (action != config.RequestScopedActionStop && action != config.RequestScopedActionContinue) {
					t.Error("incomplete policy")
				}
			}
		})
	}
	for range 20 {
		m.SetConfig(requestScopedConfig("continue"))
		m.SetConfigAndSelector(requestScopedConfig("stop"), &FillFirstSelector{})
	}
	wg.Wait()
}
