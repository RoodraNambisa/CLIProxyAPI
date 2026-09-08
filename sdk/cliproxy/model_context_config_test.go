package cliproxy

import (
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestModelContextLengthRejectsBuildAndRuntimeSideEffects(t *testing.T) {
	requested := &config.Config{CodexKey: []config.CodexKey{{Models: []config.CodexModel{{Name: "upstream", MaxContextLength: -1}}}}}
	if _, err := NewBuilder().WithConfig(requested).WithConfigPath(filepath.Join(t.TempDir(), "config.yaml")).Build(); err == nil {
		t.Fatal("builder accepted an invalid model context length")
	}
	service := &Service{cfg: &config.Config{RequestRetry: 1}}
	result, err := service.ApplyRuntimeConfig(t.Context(), requested)
	if err == nil || result.Applied || service.currentConfig().RequestRetry != 1 || service.runtimeConfigHashed {
		t.Fatal("invalid context length changed runtime state")
	}
	requested.CodexKey[0].Models[0].MaxContextLength = config.MaxModelContextLength
	result, err = service.ApplyRuntimeConfig(t.Context(), requested)
	if err != nil || !result.Applied {
		t.Fatalf("valid update failed: %v", err)
	}
	requested.CodexKey[0].Models[0].MaxContextLength = 0
	if service.currentConfig().CodexKey[0].Models[0].MaxContextLength != config.MaxModelContextLength {
		t.Fatal("caller mutation changed the installed model declaration")
	}
}
