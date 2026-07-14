package handlers

import (
	"reflect"
	"testing"

	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"golang.org/x/net/context"
)

func TestRequestExecutionMetadataIncludesExecutionSessionWithoutIdempotencyKey(t *testing.T) {
	ctx := WithExecutionSessionID(context.Background(), "session-1")

	meta := requestExecutionMetadata(ctx)
	if got := meta[coreexecutor.ExecutionSessionMetadataKey]; got != "session-1" {
		t.Fatalf("ExecutionSessionMetadataKey = %v, want %q", got, "session-1")
	}
	if _, ok := meta[idempotencyKeyMetadataKey]; ok {
		t.Fatalf("unexpected idempotency key in metadata: %v", meta[idempotencyKeyMetadataKey])
	}
}

func TestAdjustExecutionProvidersForInteractions(t *testing.T) {
	providers := adjustExecutionProvidersForEntryProtocol("interactions", []string{"antigravity", "gemini-interactions", "gemini"})
	want := []string{"gemini-interactions", "antigravity", "gemini"}
	if !reflect.DeepEqual(providers, want) {
		t.Fatalf("providers = %#v, want %#v", providers, want)
	}
}

func TestAdjustExecutionProvidersExcludesInteractionsFromUnsupportedProtocol(t *testing.T) {
	providers := adjustExecutionProvidersForEntryProtocol("codex", []string{"gemini-interactions", "codex"})
	want := []string{"codex"}
	if !reflect.DeepEqual(providers, want) {
		t.Fatalf("providers = %#v, want %#v", providers, want)
	}
}

func TestRequestExecutionMetadataIncludesInteractionsRouteValues(t *testing.T) {
	ctx := WithInteractionsAPIMetadata(context.Background(), "v1", "2026-07-01")

	meta := requestExecutionMetadata(ctx)
	if got := meta[coreexecutor.InteractionsAPIVersionMetadataKey]; got != "v1" {
		t.Fatalf("InteractionsAPIVersionMetadataKey = %v, want v1", got)
	}
	if got := meta[coreexecutor.InteractionsAPIRevisionMetadataKey]; got != "2026-07-01" {
		t.Fatalf("InteractionsAPIRevisionMetadataKey = %v, want 2026-07-01", got)
	}
}
