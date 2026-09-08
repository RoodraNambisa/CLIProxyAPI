package cliproxy

import (
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestModelThinkingRejectsBuildAndRuntimeSideEffects(t *testing.T) {
	requested := &config.Config{CodexKey: []config.CodexKey{{Models: []config.CodexModel{{Name: "upstream", Thinking: &registry.ThinkingSupport{Levels: []string{"invalid"}}}}}}}
	if _, err := NewBuilder().WithConfig(requested).WithConfigPath(filepath.Join(t.TempDir(), "config.yaml")).Build(); err == nil {
		t.Fatal("builder accepted invalid thinking")
	}
	service := &Service{cfg: &config.Config{RequestRetry: 1}}
	result, err := service.ApplyRuntimeConfig(t.Context(), requested)
	if err == nil || result.Applied || service.currentConfig().RequestRetry != 1 || service.runtimeConfigHashed {
		t.Fatal("invalid thinking changed runtime state")
	}
	requested.CodexKey[0].Models[0].Thinking.Levels[0] = "high"
	result, err = service.ApplyRuntimeConfig(t.Context(), requested)
	if err != nil || !result.Applied {
		t.Fatalf("valid thinking update failed: %v", err)
	}
	requested.CodexKey[0].Models[0].Thinking.Levels[0] = "low"
	if service.currentConfig().CodexKey[0].Models[0].Thinking.Levels[0] != "high" {
		t.Fatal("caller mutation changed installed thinking")
	}
}
