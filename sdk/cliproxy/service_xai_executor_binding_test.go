package cliproxy

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestServiceBindsXAIAutoExecutorAndModels(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	auth := &coreauth.Auth{ID: "xai-service-auth", Provider: "xai", Status: coreauth.StatusActive}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })

	service.ensureExecutorsForAuth(auth)
	registered, ok := service.coreManager.Executor("xai")
	if !ok {
		t.Fatal("xai executor was not registered")
	}
	if _, ok = registered.(*executor.XAIAutoExecutor); !ok {
		t.Fatalf("xai executor type = %T, want *executor.XAIAutoExecutor", registered)
	}

	service.registerModelsForAuth(auth)
	if !GlobalModelRegistry().ClientSupportsModel(auth.ID, "grok-imagine-image") {
		t.Fatal("xai image model was not registered")
	}
}
