package cliproxy

import (
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestRequestScopedErrorConfigRejectsBuildAndRuntimeSideEffects(t *testing.T) {
	requested := &config.Config{OAuthRequestScopedErrors: map[string][]config.RequestScopedErrorRule{
		"codex": {{Status: 400, MatchRegexr: []string{"["}, Action: "stop"}},
	}}
	if _, err := NewBuilder().WithConfig(requested).WithConfigPath(filepath.Join(t.TempDir(), "config.yaml")).Build(); err == nil {
		t.Fatal("builder accepted invalid rules")
	}
	service := &Service{cfg: &config.Config{RequestRetry: 1}}
	result, err := service.ApplyRuntimeConfig(t.Context(), requested)
	if err == nil || result.Applied || service.currentConfig().RequestRetry != 1 || service.runtimeConfigHashed {
		t.Fatal("invalid rules applied runtime side effects")
	}
	requested.OAuthRequestScopedErrors["codex"][0].MatchRegexr[0] = "^fixture$"
	result, err = service.ApplyRuntimeConfig(t.Context(), requested)
	if err != nil || !result.Applied {
		t.Fatalf("valid update: %v", err)
	}
	requested.OAuthRequestScopedErrors["codex"][0].MatchRegexr[0] = "changed"
	if service.currentConfig().OAuthRequestScopedErrors["codex"][0].MatchRegexr[0] != "^fixture$" {
		t.Fatal("caller mutation changed installed rules")
	}
}
