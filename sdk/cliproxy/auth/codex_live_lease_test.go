package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func newCodexLiveLeaseFixture(t *testing.T) (*Manager, *Auth, *authFallbackExecutor) {
	t.Helper()
	m := NewManager(nil, &FillFirstSelector{}, nil)
	m.SetConfig(&config.Config{Routing: config.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 1}})
	e := &authFallbackExecutor{id: "codex"}
	m.RegisterExecutor(e)
	a := &Auth{ID: "lease-" + t.Name(), Provider: "codex", Metadata: map[string]any{"access_token": "fixture", "email": "fixture@example.invalid"}}
	registerFallbackAuthForModel(t, m, a, "live-model")
	current, _ := m.GetByID(a.ID)
	return m, current, e
}

func TestCodexLiveLeaseReservesUntilUpstreamAndClosesIdempotently(t *testing.T) {
	for _, connect := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancelled-setup", true: "connection-attempt"}[connect], func(t *testing.T) {
			m, a, e := newCodexLiveLeaseFixture(t)
			lease, errAcquire := m.AcquireCodexLive(t.Context(), "live-model", core.Options{})
			if errAcquire != nil {
				t.Fatal(errAcquire)
			}
			if len(e.ExecuteCalls()) != 0 {
				t.Fatal("selection executed an upstream request")
			}
			if lease.Model() != "live-model" || lease.CloneAuth().RuntimeInstanceID() != a.RuntimeInstanceID() {
				t.Fatal("selection lost the model or credential instance")
			}
			if available, _ := m.authRequestLimiter().availableAt(a.ID, m.routingAuthRequestLimitPolicyForAuth(a), time.Now()); available {
				t.Fatal("selection did not reserve capacity")
			}
			if connect {
				if errCommit := lease.CommitUpstream(); errCommit != nil {
					t.Fatal(errCommit)
				}
				if errCommit := lease.CommitUpstream(); errCommit != nil {
					t.Fatal("repeated commit was not idempotent")
				}
			}
			lease.Close()
			lease.Close()
			if !errors.Is(lease.CommitUpstream(), context.Canceled) || lease.Context().Err() == nil {
				t.Fatal("closed lease remains usable")
			}
			if available, _ := m.authRequestLimiter().availableAt(a.ID, m.routingAuthRequestLimitPolicyForAuth(a), time.Now()); available == connect {
				t.Fatal("closing did not preserve the reservation commit boundary")
			}
			a.instanceState.mu.Lock()
			remaining := len(a.instanceState.executions)
			a.instanceState.mu.Unlock()
			if remaining != 0 {
				t.Fatal("lease retained a runtime execution")
			}
		})
	}
}

func TestCodexLiveLeaseRetirementAndCallerCancellation(t *testing.T) {
	for _, action := range []string{"cancel", "delete", "replace"} {
		t.Run(action, func(t *testing.T) {
			m, a, _ := newCodexLiveLeaseFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			lease, errAcquire := m.AcquireCodexLive(ctx, "live-model", core.Options{})
			if errAcquire != nil {
				t.Fatal(errAcquire)
			}
			t.Cleanup(lease.Close)
			switch action {
			case "cancel":
				cancel()
			case "delete":
				if errDelete := m.Delete(t.Context(), a.ID); errDelete != nil {
					t.Fatal(errDelete)
				}
			case "replace":
				a.Metadata["access_token"] = "replacement-fixture"
				if _, errUpdate := m.Update(t.Context(), a); errUpdate != nil {
					t.Fatal(errUpdate)
				}
			}
			select {
			case <-lease.Context().Done():
			case <-time.After(time.Second):
				t.Fatal("session context was not cancelled")
			}
			if lease.CommitUpstream() == nil {
				t.Fatal("cancelled session committed capacity")
			}
			deadline := time.After(time.Second)
			for {
				m.resultProducerMu.Lock()
				remaining := len(m.resultProducers)
				m.resultProducerMu.Unlock()
				if remaining == 0 {
					break
				}
				select {
				case <-deadline:
					t.Fatal("cancelled lease did not automatically release its producer")
				case <-time.After(time.Millisecond):
				}
			}
		})
	}
}

type liveLeaseProxyResolver struct {
	resolve func(context.Context, *Auth) (ResolvedProxy, error)
}

func (r liveLeaseProxyResolver) Resolve(ctx context.Context, a *Auth) (ResolvedProxy, error) {
	return r.resolve(ctx, a)
}

func (liveLeaseProxyResolver) ReportFailure(_ context.Context, _ *Auth, err error) error { return err }

func TestCodexLiveLeaseProxySnapshotAndPreAcquisitionRetirement(t *testing.T) {
	for _, mode := range []string{"proxy", "failure", "replace"} {
		t.Run(mode, func(t *testing.T) {
			m, a, _ := newCodexLiveLeaseFixture(t)
			proxyError := errors.New("fixture proxy unavailable")
			m.SetProxyResolver(liveLeaseProxyResolver{resolve: func(ctx context.Context, selected *Auth) (ResolvedProxy, error) {
				switch mode {
				case "failure":
					return ResolvedProxy{}, proxyError
				case "replace":
					next := selected.Clone()
					next.Metadata["access_token"] = "replacement-fixture"
					if _, errUpdate := m.Update(ctx, next); errUpdate != nil {
						return ResolvedProxy{}, errUpdate
					}
				}
				return ResolvedProxy{URL: "http://proxy.example.invalid:8080", BindingID: "fixture-binding"}, nil
			}})
			lease, errAcquire := m.AcquireCodexLive(t.Context(), "live-model", core.Options{})
			if mode == "proxy" {
				if errAcquire != nil {
					t.Fatal(errAcquire)
				}
				defer lease.Close()
				if lease.CloneAuth().RuntimeProxyURL != "http://proxy.example.invalid:8080" || a.ProxyURL != "" {
					t.Fatal("runtime proxy snapshot was lost or changed the credential")
				}
				return
			}
			if lease != nil || errAcquire == nil {
				t.Fatal("failed setup returned a lease")
			}
			if mode == "failure" && !errors.Is(errAcquire, proxyError) {
				t.Fatal("proxy failure cause was lost")
			}
			if mode == "replace" && !isRuntimeAuthInstanceRetiredError(errAcquire) {
				t.Fatal("retired selection did not fail before acquisition")
			}
			if available, _ := m.authRequestLimiter().availableAt(a.ID, m.routingAuthRequestLimitPolicyForAuth(a), time.Now()); !available {
				t.Fatal("failed setup consumed capacity")
			}
		})
	}
}

func TestCodexLiveLeaseMapsAliasesAndRetainsExecutorSnapshot(t *testing.T) {
	m, a, executor := newCodexLiveLeaseFixture(t)
	m.SetOAuthModelAlias(map[string][]config.OAuthModelAlias{"codex": {{Name: "live-upstream", Alias: "live-model"}}})
	lease, errAcquire := m.AcquireCodexLive(t.Context(), "live-model", core.Options{})
	if errAcquire != nil {
		t.Fatal(errAcquire)
	}
	defer lease.Close()
	if lease.Model() != "live-upstream" {
		t.Fatal("model alias was not resolved")
	}
	m.RegisterExecutor(&authFallbackExecutor{id: "codex"})
	if lease.executor != executor {
		t.Fatal("executor snapshot changed after replacement")
	}
	copy := lease.CloneAuth()
	copy.Metadata["access_token"] = "edited-fixture"
	if lease.CloneAuth().Metadata["access_token"] != a.Metadata["access_token"] {
		t.Fatal("exported clone changed the retained credential")
	}
}

func TestCodexLiveLeaseRejectsMissingModelAndCancelledSelection(t *testing.T) {
	m, _, _ := newCodexLiveLeaseFixture(t)
	for _, model := range []string{"", "unknown-model"} {
		if lease, errAcquire := m.AcquireCodexLive(t.Context(), model, core.Options{}); errAcquire == nil || lease != nil {
			t.Fatal("missing or unauthorized model was selected")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if lease, errAcquire := m.AcquireCodexLive(ctx, "live-model", core.Options{}); !errors.Is(errAcquire, context.Canceled) || lease != nil {
		t.Fatal("cancelled request selected a credential")
	}
	m.stopAcceptingResultPersistenceProducers()
	if lease, errAcquire := m.AcquireCodexLive(t.Context(), "live-model", core.Options{}); errAcquire == nil || lease != nil {
		t.Fatal("closing manager accepted a new session")
	}
}

func TestCodexLiveLeaseConcurrentCommitAndClose(t *testing.T) {
	for range 20 {
		m, _, _ := newCodexLiveLeaseFixture(t)
		lease, errAcquire := m.AcquireCodexLive(t.Context(), "live-model", core.Options{})
		if errAcquire != nil {
			t.Fatal(errAcquire)
		}
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() { _ = lease.CommitUpstream() })
			wg.Go(lease.Close)
		}
		wg.Wait()
		if lease.Context().Err() == nil || lease.CommitUpstream() == nil {
			t.Fatal("concurrent close left a usable lease")
		}
	}
}
