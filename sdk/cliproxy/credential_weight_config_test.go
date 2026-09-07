package cliproxy

import (
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestCredentialWeightInvalidConfigCannotBuildOrReplaceRuntime(t *testing.T) {
	invalid := config.MaxCredentialWeight + 1
	requested := &config.Config{
		RequestRetry: 4,
		CodexKey:     []config.CodexKey{{APIKey: "test", BaseURL: "https://example.test", Weight: &invalid}},
	}
	if _, err := NewBuilder().WithConfig(requested).WithConfigPath(filepath.Join(t.TempDir(), "config.yaml")).Build(); err == nil {
		t.Fatal("builder accepted an invalid credential weight")
	}
	service := &Service{cfg: &config.Config{RequestRetry: 1}}
	result, err := service.ApplyRuntimeConfig(t.Context(), requested)
	if err == nil || result.Applied || service.currentConfig().RequestRetry != 1 || service.runtimeConfigHashed {
		t.Fatal("invalid weight changed runtime state before rejection")
	}
	zero := 0
	requested.CodexKey[0].Weight = &zero
	result, err = service.ApplyRuntimeConfig(t.Context(), requested)
	if err != nil || !result.Applied {
		t.Fatalf("valid config after rejection: %v", err)
	}
	zero = 9
	if got := service.currentConfig(); got.CodexKey[0].Weight == nil || *got.CodexKey[0].Weight != 0 {
		t.Fatal("caller mutation changed the installed weight snapshot")
	}
}
