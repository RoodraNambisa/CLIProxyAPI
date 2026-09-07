package util

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestAuthenticatedSessionScopeUsesOnlyTrustedIdentity(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	c.Request.Header.Set("Authorization", "Bearer fixture-principal")
	c.Request.Header.Set("X-Caller-Scope", "forged")
	ctx := context.WithValue(t.Context(), "gin", c)
	if AuthenticatedSessionScope(ctx) != "" || AuthenticatedSessionScope(nil) != "" || AuthenticatedSessionScope(t.Context()) != "" {
		t.Fatal("an unverified header or absent context established a caller scope")
	}
	c.Set("apiKey", "fixture-principal")
	if AuthenticatedSessionScope(ctx) != "" {
		t.Fatal("an unknown authentication provider established a shared scope")
	}
	c.Set("accessProvider", "fixture-provider")
	c.Set("accessMetadata", map[string]string{"allowed_providers": "codex", "role": "reader"})
	scope := AuthenticatedSessionScope(ctx)
	if len(scope) != 64 || strings.Contains(scope, "fixture") {
		t.Fatal("scope did not detach and hash authentication information")
	}
	c.Set("accessMetadata", map[string]string{"role": "reader", "allowed_providers": "codex"})
	if AuthenticatedSessionScope(ctx) != scope {
		t.Fatal("map order changed caller identity")
	}
	for _, change := range []struct {
		key   string
		value any
	}{
		{"apiKey", "different-principal"},
		{"accessProvider", "different-provider"},
		{"accessMetadata", map[string]string{"allowed_providers": "xai", "role": "reader"}},
	} {
		old, _ := c.Get(change.key)
		c.Set(change.key, change.value)
		if next := AuthenticatedSessionScope(ctx); next == "" || next == scope {
			t.Fatal("caller, provider or authorization boundary was merged")
		}
		c.Set(change.key, old)
	}
	c.Set("accessMetadata", map[string]any{"unsupported": make(chan struct{})})
	if AuthenticatedSessionScope(ctx) != "" {
		t.Fatal("unrecognized authorization metadata produced a shared scope")
	}
}
