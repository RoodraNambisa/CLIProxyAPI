package executor

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestCodexPrepareProviderRequestDisabled(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{})
	prepared, err := executor.PrepareProviderRequest(t.Context(), cliproxyexecutor.Request{}, cliproxyexecutor.Options{}, cliproxyexecutor.RequestOperationExecute)
	if err != nil {
		t.Fatalf("PrepareProviderRequest() error = %v", err)
	}
	if prepared != nil {
		t.Fatalf("PrepareProviderRequest() = %#v, want nil", prepared)
	}
}

func TestCodexPrepareProviderRequestUsesStablePromptCacheIdentity(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{SpoofSessionIdentity: true}})
	req := cliproxyexecutor.Request{Payload: []byte(`{"prompt_cache_key":"shared-cache"}`)}

	first := prepareCodexSessionIdentityForTest(t, executor, contextWithCodexTestAPIKey("tenant-a"), req, cliproxyexecutor.Options{})
	second := prepareCodexSessionIdentityForTest(t, executor, contextWithCodexTestAPIKey("tenant-a"), req, cliproxyexecutor.Options{})
	otherTenant := prepareCodexSessionIdentityForTest(t, executor, contextWithCodexTestAPIKey("tenant-b"), req, cliproxyexecutor.Options{})

	if first.ThreadID != second.ThreadID {
		t.Fatalf("same tenant prompt cache thread IDs differ: %q != %q", first.ThreadID, second.ThreadID)
	}
	if first.ThreadID == otherTenant.ThreadID {
		t.Fatalf("different tenants share thread ID %q", first.ThreadID)
	}
	if first.TurnID == second.TurnID {
		t.Fatalf("independent requests share turn ID %q", first.TurnID)
	}
}

func TestCodexPrepareProviderRequestKeepsExplicitUUID(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{SpoofSessionIdentity: true}})
	explicit := uuid.NewString()
	req := cliproxyexecutor.Request{Payload: []byte(`{"prompt_cache_key":"` + explicit + `"}`)}
	prepared := prepareCodexSessionIdentityForTest(t, executor, t.Context(), req, cliproxyexecutor.Options{})
	if prepared.ThreadID != explicit || prepared.SessionID != explicit || prepared.WindowID != explicit+":0" {
		t.Fatalf("prepared identity = %#v, want explicit UUID %q", prepared, explicit)
	}
}

func TestCodexPrepareProviderRequestUsesExecutionSessionAndRequestID(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{SpoofSessionIdentity: true}})
	executionSessionID := uuid.NewString()
	wsPrepared := prepareCodexSessionIdentityForTest(t, executor, t.Context(), cliproxyexecutor.Request{}, cliproxyexecutor.Options{
		Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: executionSessionID},
	})
	if wsPrepared.ThreadID != executionSessionID {
		t.Fatalf("websocket ThreadID = %q, want %q", wsPrepared.ThreadID, executionSessionID)
	}

	ctx := logging.WithRequestID(t.Context(), "request-123")
	first := prepareCodexSessionIdentityForTest(t, executor, ctx, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
	second := prepareCodexSessionIdentityForTest(t, executor, ctx, cliproxyexecutor.Request{}, cliproxyexecutor.Options{})
	if first.ThreadID != second.ThreadID {
		t.Fatalf("same request ID produced different threads: %q != %q", first.ThreadID, second.ThreadID)
	}
}

func TestCodexPrepareProviderRequestClassifiesKindsAndSkipsCount(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{Codex: config.CodexConfig{SpoofSessionIdentity: true}})
	compact := prepareCodexSessionIdentityForTest(t, executor, t.Context(), cliproxyexecutor.Request{}, cliproxyexecutor.Options{Alt: "responses/compact"})
	if compact.RequestKind != "compaction" {
		t.Fatalf("compact RequestKind = %q, want compaction", compact.RequestKind)
	}
	prewarm := prepareCodexSessionIdentityForTest(t, executor, t.Context(), cliproxyexecutor.Request{Payload: []byte(`{"generate":false}`)}, cliproxyexecutor.Options{})
	if prewarm.RequestKind != "prewarm" {
		t.Fatalf("prewarm RequestKind = %q, want prewarm", prewarm.RequestKind)
	}
	prepared, err := executor.PrepareProviderRequest(t.Context(), cliproxyexecutor.Request{}, cliproxyexecutor.Options{}, cliproxyexecutor.RequestOperationCount)
	if err != nil || prepared != nil {
		t.Fatalf("count preparation = %#v, %v; want nil, nil", prepared, err)
	}
}

func prepareCodexSessionIdentityForTest(t *testing.T, executor *CodexExecutor, ctx context.Context, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) codexPreparedSessionIdentity {
	t.Helper()
	prepared, err := executor.PrepareProviderRequest(ctx, req, opts, cliproxyexecutor.RequestOperationExecute)
	if err != nil {
		t.Fatalf("PrepareProviderRequest() error = %v", err)
	}
	identity, ok := prepared.(codexPreparedSessionIdentity)
	if !ok {
		t.Fatalf("PrepareProviderRequest() type = %T, want codexPreparedSessionIdentity", prepared)
	}
	return identity
}

func contextWithCodexTestAPIKey(apiKey string) context.Context {
	ginContext, _ := gin.CreateTestContext(httptest.NewRecorder())
	ginContext.Set("apiKey", apiKey)
	return context.WithValue(context.Background(), "gin", ginContext)
}
