package executor

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

type mixedProviderUsageExecutor struct {
	id  string
	err error
}

type mixedProviderStreamUsageExecutor struct {
	id                    string
	bootstrap             map[string]error
	accepted              map[string]error
	delayedFailureAuthID  string
	releaseDelayedFailure <-chan struct{}
	delayedFailureDone    chan struct{}
	delayedCompletionID   string
	releaseCompletion     <-chan struct{}
}

func (e *mixedProviderStreamUsageExecutor) Identifier() string { return e.id }

func (e *mixedProviderStreamUsageExecutor) Execute(context.Context, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("execute not implemented")
}

func (e *mixedProviderStreamUsageExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	reporter := helps.NewExecutorUsageReporter(ctx, e, req.Model, auth)
	reporter.SetExecutionDiagnostics(opts.ExecutionDiagnostics)
	chunks := make(chan cliproxyexecutor.StreamChunk, 2)
	if streamErr := e.bootstrap[auth.ID]; streamErr != nil {
		chunks <- cliproxyexecutor.StreamChunk{Err: streamErr}
		close(chunks)
		if auth.ID == e.delayedFailureAuthID && e.releaseDelayedFailure != nil {
			go func() {
				<-e.releaseDelayedFailure
				reporter.PublishFailure(ctx, streamErr)
				if e.delayedFailureDone != nil {
					close(e.delayedFailureDone)
				}
			}()
		} else {
			reporter.PublishFailure(ctx, streamErr)
		}
		return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
	}
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("data: {\"delta\":\"ok\"}\n\n")}
	finish := func() {
		if streamErr := e.accepted[auth.ID]; streamErr != nil {
			reporter.PublishFailure(ctx, streamErr)
			chunks <- cliproxyexecutor.StreamChunk{Err: streamErr}
		} else {
			reporter.Publish(ctx, coreusage.Detail{InputTokens: 1, OutputTokens: 1, TotalTokens: 2})
		}
		close(chunks)
	}
	if auth.ID == e.delayedCompletionID && e.releaseCompletion != nil {
		go func() {
			<-e.releaseCompletion
			finish()
		}()
	} else {
		finish()
	}
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *mixedProviderStreamUsageExecutor) Refresh(_ context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	return auth, nil
}

func (e *mixedProviderStreamUsageExecutor) CountTokens(context.Context, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("count not implemented")
}

func (e *mixedProviderStreamUsageExecutor) HttpRequest(context.Context, *cliproxyauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("http request not implemented")
}

func (e *mixedProviderUsageExecutor) Identifier() string { return e.id }

func (e *mixedProviderUsageExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (response cliproxyexecutor.Response, err error) {
	reporter := helps.NewExecutorUsageReporter(ctx, e, req.Model, auth)
	reporter.SetExecutionDiagnostics(opts.ExecutionDiagnostics)
	if e.id == "chatgpt-web" {
		reporter.SetRequestUsageOutcome(opts.UsageOutcome)
	}
	if e.err != nil {
		reporter.PublishFailure(ctx, e.err)
		return cliproxyexecutor.Response{}, e.err
	}
	reporter.Publish(ctx, coreusage.Detail{InputTokens: 1, OutputTokens: 1, TotalTokens: 2})
	return cliproxyexecutor.Response{Payload: []byte(e.id)}, nil
}

func (e *mixedProviderUsageExecutor) ExecuteStream(context.Context, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, errors.New("stream not implemented")
}

func (e *mixedProviderUsageExecutor) Refresh(_ context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	return auth, nil
}

func (e *mixedProviderUsageExecutor) CountTokens(context.Context, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("count not implemented")
}

func (e *mixedProviderUsageExecutor) HttpRequest(context.Context, *cliproxyauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("http request not implemented")
}

type mixedProviderUsagePlugin struct {
	authIDs map[string]struct{}
	records chan coreusage.Record
}

func (p *mixedProviderUsagePlugin) HandleUsage(_ context.Context, record coreusage.Record) {
	if _, ok := p.authIDs[record.AuthID]; ok {
		p.records <- record
	}
}

func registerMixedProviderUsageAuth(t *testing.T, manager *cliproxyauth.Manager, provider, authID, model, priority string) {
	t.Helper()
	registry.GetGlobalRegistry().RegisterClient(authID, provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
	_, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), &cliproxyauth.Auth{
		ID:         authID,
		Provider:   provider,
		Status:     cliproxyauth.StatusActive,
		Attributes: map[string]string{"priority": priority},
		Metadata:   map[string]any{"lifecycle_state": cliproxyauth.LifecycleStateActive},
	})
	if errRegister != nil {
		t.Fatalf("Register(%s) error = %v", authID, errRegister)
	}
}

func collectMixedProviderUsageRecords(t *testing.T, records <-chan coreusage.Record) []coreusage.Record {
	t.Helper()
	select {
	case record := <-records:
		got := []coreusage.Record{record}
		select {
		case extra := <-records:
			got = append(got, extra)
		case <-time.After(75 * time.Millisecond):
		}
		return got
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for usage record")
		return nil
	}
}

func TestManagerMixedProviderFailureThenSuccessPublishesOnePrimaryUsageRecord(t *testing.T) {
	const (
		model       = "mixed-provider-usage-success"
		failedID    = "mixed-provider-usage-nonweb-failed"
		succeededID = "mixed-provider-usage-web-succeeded"
	)
	records := make(chan coreusage.Record, 2)
	coreusage.RegisterPlugin(&mixedProviderUsagePlugin{
		authIDs: map[string]struct{}{failedID: {}, succeededID: {}},
		records: records,
	})
	manager := cliproxyauth.NewManager(nil, &cliproxyauth.FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(&mixedProviderUsageExecutor{id: "mixed-nonweb", err: &cliproxyauth.Error{Code: "upstream_failed", Message: "failed", HTTPStatus: http.StatusInternalServerError}})
	manager.RegisterExecutor(&mixedProviderUsageExecutor{id: "chatgpt-web"})
	registerMixedProviderUsageAuth(t, manager, "mixed-nonweb", failedID, model, "10")
	registerMixedProviderUsageAuth(t, manager, "chatgpt-web", succeededID, model, "0")

	response, errExecute := manager.Execute(t.Context(), []string{"mixed-nonweb", "chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if string(response.Payload) != "chatgpt-web" {
		t.Fatalf("response provider = %q, want chatgpt-web", response.Payload)
	}
	got := collectMixedProviderUsageRecords(t, records)
	if len(got) != 1 || got[0].Failed || got[0].AuthID != succeededID {
		t.Fatalf("usage records = %+v, want one final success", got)
	}
}

func TestManagerMixedProviderFailuresPublishOnlyFinalPrimaryUsageRecord(t *testing.T) {
	const (
		model    = "mixed-provider-usage-failure"
		webID    = "mixed-provider-usage-web-failed"
		nonWebID = "mixed-provider-usage-nonweb-final"
	)
	records := make(chan coreusage.Record, 2)
	coreusage.RegisterPlugin(&mixedProviderUsagePlugin{
		authIDs: map[string]struct{}{webID: {}, nonWebID: {}},
		records: records,
	})
	manager := cliproxyauth.NewManager(nil, &cliproxyauth.FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(&mixedProviderUsageExecutor{id: "chatgpt-web", err: &cliproxyauth.Error{Code: "web_failed", Message: "failed", HTTPStatus: http.StatusInternalServerError}})
	manager.RegisterExecutor(&mixedProviderUsageExecutor{id: "mixed-nonweb", err: &cliproxyauth.Error{Code: "nonweb_failed", Message: "failed", HTTPStatus: http.StatusBadGateway}})
	registerMixedProviderUsageAuth(t, manager, "chatgpt-web", webID, model, "10")
	registerMixedProviderUsageAuth(t, manager, "mixed-nonweb", nonWebID, model, "0")

	_, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web", "mixed-nonweb"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errExecute == nil {
		t.Fatal("Execute() error = nil, want final provider failure")
	}
	got := collectMixedProviderUsageRecords(t, records)
	if len(got) != 1 || !got[0].Failed || got[0].AuthID != nonWebID {
		t.Fatalf("usage records = %+v, want one final failure", got)
	}
}

func TestManagerStreamBootstrapFailureThenSuccessPublishesOnlySuccess(t *testing.T) {
	const (
		provider    = "mixed-stream"
		model       = "mixed-stream-bootstrap-fallback"
		failedID    = "mixed-stream-bootstrap-failed"
		succeededID = "mixed-stream-bootstrap-succeeded"
	)
	records := make(chan coreusage.Record, 2)
	coreusage.RegisterPlugin(&mixedProviderUsagePlugin{
		authIDs: map[string]struct{}{failedID: {}, succeededID: {}},
		records: records,
	})
	manager := cliproxyauth.NewManager(nil, &cliproxyauth.FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(&mixedProviderStreamUsageExecutor{
		id: provider,
		bootstrap: map[string]error{
			failedID: &cliproxyauth.Error{Code: "bootstrap_failed", Message: "failed", HTTPStatus: http.StatusBadGateway},
		},
		accepted: map[string]error{},
	})
	registerMixedProviderUsageAuth(t, manager, provider, failedID, model, "10")
	registerMixedProviderUsageAuth(t, manager, provider, succeededID, model, "0")

	result, errStream := manager.ExecuteStream(t.Context(), []string{provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errStream != nil {
		t.Fatalf("ExecuteStream() error = %v", errStream)
	}
	for range result.Chunks {
	}
	got := collectMixedProviderUsageRecords(t, records)
	if len(got) != 1 || got[0].Failed || got[0].AuthID != succeededID {
		t.Fatalf("usage records = %+v, want one final stream success", got)
	}
}

func TestManagerLateFailureFromSupersededStreamDoesNotOverrideAcceptedSuccess(t *testing.T) {
	const (
		provider    = "mixed-stream-late-failure"
		model       = "mixed-stream-late-failure-fallback"
		failedID    = "mixed-stream-late-failure-first"
		succeededID = "mixed-stream-late-failure-final"
	)
	records := make(chan coreusage.Record, 2)
	coreusage.RegisterPlugin(&mixedProviderUsagePlugin{
		authIDs: map[string]struct{}{failedID: {}, succeededID: {}},
		records: records,
	})
	releaseLateFailure := make(chan struct{})
	lateFailureDone := make(chan struct{})
	releaseAcceptedCompletion := make(chan struct{})
	manager := cliproxyauth.NewManager(nil, &cliproxyauth.FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(&mixedProviderStreamUsageExecutor{
		id: provider,
		bootstrap: map[string]error{
			failedID: &cliproxyauth.Error{Code: "bootstrap_failed", Message: "failed", HTTPStatus: http.StatusBadGateway},
		},
		accepted:              map[string]error{},
		delayedFailureAuthID:  failedID,
		releaseDelayedFailure: releaseLateFailure,
		delayedFailureDone:    lateFailureDone,
		delayedCompletionID:   succeededID,
		releaseCompletion:     releaseAcceptedCompletion,
	})
	registerMixedProviderUsageAuth(t, manager, provider, failedID, model, "10")
	registerMixedProviderUsageAuth(t, manager, provider, succeededID, model, "0")

	result, errStream := manager.ExecuteStream(t.Context(), []string{provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errStream != nil {
		t.Fatalf("ExecuteStream() error = %v", errStream)
	}
	close(releaseLateFailure)
	select {
	case <-lateFailureDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for superseded stream failure")
	}
	select {
	case record := <-records:
		t.Fatalf("superseded stream published usage before accepted attempt completed: %+v", record)
	default:
	}
	close(releaseAcceptedCompletion)
	for range result.Chunks {
	}
	got := collectMixedProviderUsageRecords(t, records)
	if len(got) != 1 || got[0].Failed || got[0].AuthID != succeededID {
		t.Fatalf("usage records = %+v, want one accepted stream success", got)
	}
}

func TestManagerAcceptedStreamEarlyFailurePublishesOneFailure(t *testing.T) {
	const (
		provider = "mixed-stream-accepted"
		model    = "mixed-stream-accepted-failure"
		authID   = "mixed-stream-accepted-failed"
	)
	records := make(chan coreusage.Record, 2)
	coreusage.RegisterPlugin(&mixedProviderUsagePlugin{
		authIDs: map[string]struct{}{authID: {}},
		records: records,
	})
	manager := cliproxyauth.NewManager(nil, &cliproxyauth.FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(&mixedProviderStreamUsageExecutor{
		id:        provider,
		bootstrap: map[string]error{},
		accepted: map[string]error{
			authID: &cliproxyauth.Error{Code: "stream_failed", Message: "failed", HTTPStatus: http.StatusBadGateway},
		},
	})
	registerMixedProviderUsageAuth(t, manager, provider, authID, model, "0")

	result, errStream := manager.ExecuteStream(t.Context(), []string{provider}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errStream != nil {
		t.Fatalf("ExecuteStream() error = %v", errStream)
	}
	for range result.Chunks {
	}
	got := collectMixedProviderUsageRecords(t, records)
	if len(got) != 1 || !got[0].Failed || got[0].AuthID != authID {
		t.Fatalf("usage records = %+v, want one accepted stream failure", got)
	}
}
