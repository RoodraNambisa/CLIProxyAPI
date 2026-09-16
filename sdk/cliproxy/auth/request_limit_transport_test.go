package auth

import (
	"context"
	"fmt"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type transportRequestLimitExecutor struct {
	ProviderExecutor
	localModel string
}

func (*transportRequestLimitExecutor) DeferAuthRequestCommitUntilUpstream() bool { return true }

func (e *transportRequestLimitExecutor) mark(ctx context.Context, req core.Request) {
	if req.Model != e.localModel {
		core.MarkUpstreamAttempt(ctx)
	}
}

func (e *transportRequestLimitExecutor) Execute(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (core.Response, error) {
	e.mark(ctx, req)
	return e.ProviderExecutor.Execute(ctx, auth, req, opts)
}

func (e *transportRequestLimitExecutor) ExecuteStream(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	e.mark(ctx, req)
	return e.ProviderExecutor.ExecuteStream(ctx, auth, req, opts)
}

func (e *transportRequestLimitExecutor) CountTokens(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (core.Response, error) {
	e.mark(ctx, req)
	return e.ProviderExecutor.CountTokens(ctx, auth, req, opts)
}

func runRequestLimitOperation(ctx context.Context, manager *Manager, provider, model, operation string) error {
	req := core.Request{Model: model}
	switch operation {
	case "execute":
		_, err := manager.Execute(ctx, []string{provider}, req, core.Options{})
		return err
	case "count":
		_, err := manager.ExecuteCount(ctx, []string{provider}, req, core.Options{})
		return err
	default:
		result, err := manager.ExecuteStream(ctx, []string{provider}, req, core.Options{})
		if result != nil {
			for chunk := range result.Chunks {
				if chunk.Err != nil {
					err = chunk.Err
				}
			}
		}
		return err
	}
}

func TestTransportRequestLimitReleasesLocalModelPoolFailures(t *testing.T) {
	const alias = "transport-limited-model-pool"
	models := []internalconfig.OpenAICompatibilityModel{{Name: "first-upstream", Alias: alias}, {Name: "second-upstream", Alias: alias}}
	for _, operation := range []string{"execute", "count", "stream"} {
		for _, firstLocal := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/local=%t", operation, firstLocal), func(t *testing.T) {
				failures := map[string]error{"first-upstream": requestLimitRetryError{}}
				fixture := &openAICompatPoolExecutor{id: "pool", executeErrors: failures, countErrors: failures, streamFirstErrors: failures}
				manager := newOpenAICompatPoolTestManager(t, alias, models, fixture)
				wrapped := &transportRequestLimitExecutor{ProviderExecutor: fixture}
				if firstLocal {
					wrapped.localModel = "first-upstream"
				}
				manager.RegisterExecutor(wrapped)
				enableManagerRequestLimitForTest(t, manager, 1)
				err := runRequestLimitOperation(t.Context(), manager, "pool", alias, operation)
				if firstLocal && err != nil {
					t.Fatalf("local failure consumed capacity needed by next model: %v", err)
				}
				if !firstLocal && !core.IsUpstreamAttemptError(err) {
					t.Fatalf("lost the upstream cause after exhausting capacity: %v", err)
				}
				var attempts []string
				switch operation {
				case "execute":
					attempts = fixture.ExecuteModels()
				case "count":
					attempts = fixture.CountModels()
				default:
					attempts = fixture.StreamModels()
				}
				wantAttempts := 1
				if firstLocal {
					wantAttempts = 2
				}
				if len(attempts) != wantAttempts {
					t.Fatalf("model attempts = %v, want %d", attempts, wantAttempts)
				}
				if metrics := manager.RequestExecutionMetrics(); metrics.UpstreamCommitted != 1 {
					t.Fatalf("upstream commit count = %d, want 1", metrics.UpstreamCommitted)
				}
			})
		}
	}
}

func TestTransportRequestLimitCountsUnauthorizedReplay(t *testing.T) {
	for _, operation := range []string{"execute", "count", "stream"} {
		t.Run(operation, func(t *testing.T) {
			manager, fixture, primary, backup, model := newAntigravityUnauthorizedFixture(t, false)
			manager.RegisterExecutor(&transportRequestLimitExecutor{ProviderExecutor: fixture})
			fixed := enableManagerRequestLimitForTest(t, manager, 1)
			policy := manager.routingAuthRequestLimitPolicyForAuth(backup)
			if acquired, _ := manager.authRequestLimiter().tryAcquireAt(backup.ID, policy, fixed); !acquired {
				t.Fatal("failed to consume backup request quota")
			}
			if err := runRequestLimitOperation(t.Context(), manager, "antigravity", model, operation); !core.IsUpstreamAttemptError(err) || statusCodeFromError(err) != 401 {
				t.Fatalf("lost original 401 after exhausting request limit: %v", err)
			}
			fixture.mu.Lock()
			calls := len(fixture.executeCalls) + len(fixture.countCalls) + len(fixture.streamCalls)
			refreshes := fixture.refreshCalls
			fixture.mu.Unlock()
			usage := manager.authRequestLimiter().usageAt(primary.ID, manager.routingAuthRequestLimitPolicyForAuth(primary), fixed)
			if calls != 1 || refreshes != 1 || usage.remaining != 0 {
				t.Fatalf("401 replay bypassed limit: calls=%d refreshes=%d remaining=%d", calls, refreshes, usage.remaining)
			}
		})
	}
}

func TestTransportRequestLimitCoversCreditsFallback(t *testing.T) {
	const model = "claude-credits-local-limit"
	for _, stream := range []bool{false, true} {
		for _, local := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/local=%t", stream, local), func(t *testing.T) {
				manager := NewManager(nil, &FillFirstSelector{}, nil)
				fixture := &antigravityCreditsBudgetExecutor{}
				wrapped := &transportRequestLimitExecutor{ProviderExecutor: fixture}
				if local {
					wrapped.localModel = model
				}
				manager.RegisterExecutor(wrapped)
				manager.SetRetryConfig(0, 0, 1)
				credential := &Auth{ID: t.Name(), Provider: "antigravity"}
				if _, err := manager.Register(WithSkipPersist(t.Context()), credential); err != nil {
					t.Fatal(err)
				}
				registry.GetGlobalRegistry().RegisterClient(credential.ID, credential.Provider, []*registry.ModelInfo{{ID: model}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(credential.ID) })
				fixed := enableManagerRequestLimitForTest(t, manager, 1)
				if stream {
					_, _, _ = manager.tryAntigravityCreditsExecuteStream(t.Context(), core.Request{Model: model}, core.Options{})
				} else {
					_, _, _ = manager.tryAntigravityCreditsExecute(t.Context(), core.Request{Model: model}, core.Options{})
				}
				wantRemaining := 0
				if local {
					wantRemaining = 1
				}
				usage := manager.authRequestLimiter().usageAt(credential.ID, manager.routingAuthRequestLimitPolicyForAuth(credential), fixed)
				if usage.remaining != wantRemaining || len(fixture.executeAuthIDs)+len(fixture.streamAuthIDs) != 1 {
					t.Fatalf("credits capacity remaining = %d, want %d", usage.remaining, wantRemaining)
				}
			})
		}
	}
}
