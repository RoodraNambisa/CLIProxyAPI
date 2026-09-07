package cliproxy

import (
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestCredentialRequestRetryInvalidConfigCannotBuildOrReplaceRuntime(t *testing.T) {
	large := int64(config.MaxCredentialRequestRetry) + 1
	if int64(int(large)) != large {
		t.Skip("Go int is 32 bits")
	}
	invalid := int(large)
	requested := &config.Config{RequestRetry: 4, CodexKey: []config.CodexKey{{APIKey: "test", BaseURL: "https://example.test", RequestRetry: &invalid}}}
	if _, err := NewBuilder().WithConfig(requested).WithConfigPath(filepath.Join(t.TempDir(), "config.yaml")).Build(); err == nil {
		t.Fatal("builder accepted an invalid credential retry limit")
	}
	service := &Service{cfg: &config.Config{RequestRetry: 1}}
	result, err := service.ApplyRuntimeConfig(t.Context(), requested)
	if err == nil || result.Applied || service.currentConfig().RequestRetry != 1 || service.runtimeConfigHashed {
		t.Fatal("invalid retry limit changed runtime state before rejection")
	}
	zero := 0
	requested.CodexKey[0].RequestRetry = &zero
	result, err = service.ApplyRuntimeConfig(t.Context(), requested)
	if err != nil || !result.Applied {
		t.Fatalf("valid config after rejection: %v", err)
	}
	zero = 9
	if got := service.currentConfig(); got.CodexKey[0].RequestRetry == nil || *got.CodexKey[0].RequestRetry != 0 {
		t.Fatal("caller mutation changed the installed retry override")
	}
}
