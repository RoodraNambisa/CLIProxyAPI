package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func affinityCallerContext(t *testing.T, principal, allowed string) context.Context {
	t.Helper()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	c.Request.Header.Set("Session-Id", "fixture-parent")
	if principal != "" {
		c.Set("apiKey", principal)
		c.Set("accessProvider", "fixture-authentication")
		c.Set("accessMetadata", map[string]string{"allowed_providers": allowed})
	}
	return context.WithValue(t.Context(), "gin", c)
}

func TestAffinityIdentityCapturesTrustedScopeAndDetachesBody(t *testing.T) {
	ctx := affinityCallerContext(t, "caller-a", "codex")
	parent := captureAffinityIdentity(ctx, core.Request{}, core.Options{})
	payload := []byte(`{"thread_id":"fixture-child","parent_thread_id":"fixture-parent"}`)
	opts := core.Options{OriginalRequest: payload, Metadata: map[string]any{"unknown": true}}
	frozen := withAffinityIdentity(ctx, core.Request{}, opts)
	if _, exists := opts.Metadata[affinityIdentityMetadataKey]; exists {
		t.Fatal("snapshot mutated caller metadata")
	}
	child := captureAffinityIdentity(ctx, core.Request{}, frozen)
	if child.identity.SessionID != "codex:fixture-child" || child.identity.ParentSessionID != parent.identity.SessionID || !child.identity.IsSubagent || child.scope != parent.scope {
		t.Fatal("child did not retain the scoped explicit parent")
	}
	clear(payload)
	frozen.OriginalRequest = nil
	if next := captureAffinityIdentity(affinityCallerContext(t, "caller-b", "xai"), core.Request{}, withAffinityIdentity(ctx, core.Request{}, frozen)); next != child {
		t.Fatal("released body or subsequent authentication state changed the logical request")
	}
	for _, other := range []context.Context{affinityCallerContext(t, "caller-b", "codex"), affinityCallerContext(t, "caller-a", "xai")} {
		otherParent := captureAffinityIdentity(other, core.Request{}, core.Options{})
		if scopedAffinityID(parent.scope, parent.identity.SessionID) == scopedAffinityID(otherParent.scope, otherParent.identity.SessionID) {
			t.Fatal("caller or authorization boundary was merged")
		}
	}
}

func TestAffinityIdentitySDKScopeAndUnverifiedFallback(t *testing.T) {
	anonymous := affinityCallerContext(t, "", "")
	c := anonymous.Value("gin").(*gin.Context)
	c.Request.Header.Set("X-Caller-Scope", "forged")
	opts := core.Options{OriginalRequest: []byte(`{"caller_scope":"forged"}`)}
	if got := captureAffinityIdentity(anonymous, core.Request{}, opts); got.scope != "" || got.identity.SessionID != "" {
		t.Fatal("unverified client data established a shared scope")
	}
	opts.Headers = http.Header{"Session-Id": {"sdk-session"}}
	opts.Metadata = map[string]any{core.CallerScopeMetadataKey: "caller/authorized-provider-set"}
	got := captureAffinityIdentity(t.Context(), core.Request{}, opts)
	if len(got.scope) != 64 || got.identity.SessionID != "codex:sdk-session" {
		t.Fatal("trusted SDK scope or headers were lost")
	}
	if scopedAffinityID("", "id") != "" || scopedAffinityID("scope", "") != "" || scopedAffinityID("a::b", "c") == scopedAffinityID("a", "b::c") {
		t.Fatal("scope framing permits an empty or ambiguous identity")
	}
	trusted := affinityCallerContext(t, "caller-a", "codex")
	first := captureAffinityIdentity(trusted, core.Request{}, opts)
	opts.Metadata[core.CallerScopeMetadataKey] = "changed-sdk-scope"
	if next := captureAffinityIdentity(trusted, core.Request{}, opts); next.scope != first.scope {
		t.Fatal("SDK metadata overrode middleware authentication")
	}
}
