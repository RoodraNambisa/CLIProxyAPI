package auth

import (
	"context"
	"testing"
	"time"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type timeoutPreparer struct {
	ProviderExecutor
	provider string
	executed int
	block    bool
}

func (e *timeoutPreparer) Identifier() string { return e.provider }
func (e *timeoutPreparer) PrepareProviderRequest(ctx context.Context, _ core.Request, _ core.Options, _ core.RequestOperation) (any, error) {
	if e.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return nil, nil
}
func (e *timeoutPreparer) Execute(context.Context, *Auth, core.Request, core.Options) (core.Response, error) {
	e.executed++
	return core.Response{Payload: []byte(`{"output":[]}`)}, nil
}

func TestImageRequestTimeoutPreflightCanStillSelectUnlimitedProvider(t *testing.T) {
	m := NewManager(nil, nil, nil)
	web := &timeoutPreparer{provider: "chatgpt-web", block: true}
	codex := &timeoutPreparer{provider: "codex"}
	m.RegisterExecutor(web)
	m.RegisterExecutor(codex)
	registerFallbackAuthForModel(t, m, &Auth{ID: "timeout-codex", Provider: "codex"}, "timeout-model")
	b := core.NewImageRequestBudget(t.Context(), time.Now().Add(-2*time.Second), 0, time.Second)
	defer b.Close()
	ctx, cancel := b.Bind(t.Context())
	defer cancel()
	_, err := m.Execute(ctx, []string{"chatgpt-web", "codex"}, core.Request{Model: "timeout-model"}, core.Options{Metadata: map[string]any{core.ImageRequestBudgetMetadataKey: b}})
	if err != nil {
		t.Fatal(err)
	}
	if web.executed != 0 || codex.executed != 1 {
		t.Fatalf("wrong execution counts Web=%d Codex=%d", web.executed, codex.executed)
	}
}

func TestImageRequestTimeoutAllPreflightExpired(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.RegisterExecutor(&timeoutPreparer{provider: "chatgpt-web", block: true})
	b := core.NewImageRequestBudget(t.Context(), time.Now().Add(-2*time.Second), 0, time.Second)
	defer b.Close()
	ctx, cancel := b.Bind(t.Context())
	defer cancel()
	_, err := m.Execute(ctx, []string{"chatgpt-web"}, core.Request{Model: "timeout-model"}, core.Options{Metadata: map[string]any{core.ImageRequestBudgetMetadataKey: b}})
	if !core.IsImageRequestTimeout(err) {
		t.Fatalf("preflight error: %v", err)
	}
}

type timeoutProxyResolver struct{ reported bool }

func (r *timeoutProxyResolver) Resolve(context.Context, *Auth) (ResolvedProxy, error) {
	return ResolvedProxy{}, nil
}
func (r *timeoutProxyResolver) ReportFailure(_ context.Context, _ *Auth, err error) error {
	r.reported = true
	return err
}
func TestImageRequestTimeoutDoesNotFaultProxy(t *testing.T) {
	m := NewManager(nil, nil, nil)
	resolver := &timeoutProxyResolver{}
	m.proxyResolver = resolver
	err := &core.ImageRequestTimeoutError{Limit: time.Second}
	got := m.reportProxyFailure(t.Context(), &Auth{ID: "timeout-auth", RuntimeProxyBindingID: "fixture"}, err)
	if got != err || resolver.reported {
		t.Fatal("local timeout reported as proxy failure")
	}
}
