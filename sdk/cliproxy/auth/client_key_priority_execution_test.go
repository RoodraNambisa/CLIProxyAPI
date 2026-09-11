package auth

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type clientPriorityExecutor struct {
	schedulerTestExecutor
	calls   []int
	onFirst func()
}

func (e *clientPriorityExecutor) attempt(credential *Auth) error {
	e.calls = append(e.calls, authPriority(credential))
	if len(e.calls) == 1 {
		e.onFirst()
		return &Error{Code: "server_error", Message: "fixture failure", HTTPStatus: 500}
	}
	return nil
}

func (e *clientPriorityExecutor) Execute(_ context.Context, credential *Auth, _ core.Request, _ core.Options) (core.Response, error) {
	return core.Response{Payload: []byte(`{"ok":true}`)}, e.attempt(credential)
}

func (e *clientPriorityExecutor) CountTokens(ctx context.Context, credential *Auth, req core.Request, opts core.Options) (core.Response, error) {
	return e.Execute(ctx, credential, req, opts)
}

func (e *clientPriorityExecutor) ExecuteStream(_ context.Context, credential *Auth, _ core.Request, _ core.Options) (*core.StreamResult, error) {
	if err := e.attempt(credential); err != nil {
		return nil, err
	}
	chunks := make(chan core.StreamChunk, 1)
	chunks <- core.SuccessfulStreamTerminalChunk()
	close(chunks)
	return &core.StreamResult{Chunks: chunks}, nil
}

func TestClientKeyPriorityRetriesKeepSnapshotAcrossConfigUpdate(t *testing.T) {
	for _, operation := range []string{"execute", "count", "stream"} {
		t.Run(operation, func(t *testing.T) {
			m := NewManager(nil, &RoundRobinSelector{}, nil)
			m.SetConfig(clientPriorityConfig([]int{1}, nil))
			m.SetRetryConfig(1, 0, 0)
			executor := &clientPriorityExecutor{onFirst: func() { m.SetConfig(clientPriorityConfig([]int{10}, nil)) }}
			m.RegisterExecutor(executor)
			for index, priority := range []int{1, 1, 10} {
				registerFallbackAuthForModel(t, m, &Auth{ID: fmt.Sprintf("priority-retry-%s-%d", operation, index), Provider: "test", Attributes: map[string]string{"priority": fmt.Sprint(priority)}}, "priority-model")
			}
			run := func() error {
				ctx := clientPriorityContext(t, "priority-fixture")
				req := core.Request{Model: "priority-model", Payload: []byte(`{}`)}
				switch operation {
				case "execute":
					_, err := m.Execute(ctx, []string{"test"}, req, core.Options{})
					return err
				case "count":
					_, err := m.ExecuteCount(ctx, []string{"test"}, req, core.Options{})
					return err
				default:
					result, err := m.ExecuteStream(ctx, []string{"test"}, req, core.Options{})
					if err == nil {
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								err = chunk.Err
							}
						}
					}
					return err
				}
			}
			if err := run(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(executor.calls, []int{1, 1}) {
				t.Fatal("retry escaped the original key policy", executor.calls)
			}
			if err := run(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(executor.calls, []int{1, 1, 10}) {
				t.Fatal("new request did not use the updated key policy", executor.calls)
			}
		})
	}
}

func TestClientKeyPriorityBoundSessionCannotEscapeRestrictions(t *testing.T) {
	for _, failover := range []bool{false, true} {
		m, selector := newAcrossPriorityFixture(t, "round-robin", true, failover)
		cfg := clientPriorityConfig([]int{0}, nil)
		cfg.Routing.SessionAffinity = true
		cfg.Routing.SessionAffinityAcrossPriorities = true
		cfg.Routing.SessionAffinityFailover = &failover
		m.SetConfig(cfg)
		ctx := clientPriorityContext(t, "priority-fixture")
		opts := core.Options{Headers: http.Header{"Session-Id": {"bound-priority-fixture"}}}
		selector.BindSession(ctx, "test", "", opts, "high")
		selected, _, _, err := m.pickNextMixed(ctx, []string{"test"}, "", opts, nil)
		if failover {
			if err != nil || selected == nil || selected.ID != "low" {
				t.Fatal("failover did not stay inside the permitted priority", err)
			}
		} else if err == nil || selected != nil {
			t.Fatal("strict binding escaped the permitted priority")
		}
	}
}

func TestClientKeyPriorityAppliesToLiveLeaseAndCreditsFallback(t *testing.T) {
	m, _, _ := newCodexLiveLeaseFixture(t)
	m.SetConfig(clientPriorityConfig([]int{1}, nil))
	if lease, err := m.AcquireCodexLive(clientPriorityContext(t, "priority-fixture"), "live-model", core.Options{}); err == nil || lease != nil {
		t.Fatal("Live lease bypassed the client key restriction")
	}
	m.SetConfig(clientPriorityConfig([]int{0}, nil))
	lease, err := m.AcquireCodexLive(clientPriorityContext(t, "priority-fixture"), "live-model", core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	m.SetConfig(clientPriorityConfig(nil, []int{0}))
	if m.ClientAPIKeyAllowsCredential(clientPriorityContext(t, "priority-fixture"), lease.CloneAuth()) {
		t.Fatal("new sideband was allowed on an excluded lease")
	}
	if lease.Context().Err() != nil {
		t.Fatal("policy update interrupted an already established lease")
	}

	m = NewManager(nil, &RoundRobinSelector{}, nil)
	m.SetConfig(clientPriorityConfig([]int{1}, nil))
	m.RegisterExecutor(&authFallbackExecutor{id: "antigravity"})
	for _, priority := range []int{1, 10} {
		registerFallbackAuthForModel(t, m, &Auth{ID: fmt.Sprintf("priority-credits-%d", priority), Provider: "antigravity", Attributes: map[string]string{"priority": fmt.Sprint(priority)}}, "claude-priority")
	}
	ctx := m.WithRoutingPolicySnapshot(clientPriorityContext(t, "priority-fixture"))
	selected, err := m.pickAntigravityCreditsCandidate(ctx, "claude-priority", core.Options{}, newRequestRoundState(), 0)
	if err != nil || selected == nil || authPriority(selected.auth) != 1 {
		t.Fatal("credits fallback escaped client priority restrictions", err)
	}
}

func TestClientKeyPriorityRemovedAuthenticatedKeyCannotBecomeUnrestricted(t *testing.T) {
	m := NewManager(nil, &RoundRobinSelector{}, nil)
	m.SetConfig(clientPriorityConfig([]int{1}, nil))
	ctx := clientPriorityContext(t, "priority-fixture")
	ctx.Value("gin").(*gin.Context).Set("accessProvider", sdkaccess.DefaultAccessProviderName)
	m.SetConfig(&config.Config{})
	if clientKeyPriorityAllowed(m.WithRoutingPolicySnapshot(ctx), &Auth{}) {
		t.Fatal("deleted authenticated key became unrestricted before selection")
	}
}
