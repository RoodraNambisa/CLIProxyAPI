package cliproxy

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher/synthesizer"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestXAINativeKeysSynthesizeAndRegisterConfiguredModels(t *testing.T) {
	cfg := &config.Config{XAIKey: []config.XAIKey{{APIKey: "fixture-key", BaseURL: "https://api.x.ai/v1", Prefix: "tenant", Headers: map[string]string{"X-Configured": "yes"}, Models: []config.CodexModel{{Name: "grok-4.6", Alias: "native-fixture"}}}}}
	auths, err := synthesizer.NewConfigSynthesizer().Synthesize(&synthesizer.SynthesisContext{Config: cfg, Now: time.Now(), IDGenerator: synthesizer.NewStableIDGenerator()})
	if err != nil || len(auths) != 1 {
		t.Fatalf("synthesis: %v", err)
	}
	auth := auths[0]
	if auth.Provider != "xai" || auth.Attributes["api_key"] != "fixture-key" || auth.Attributes["header:X-Configured"] != "yes" || auth.Attributes["runtime_only"] != "true" {
		t.Fatal("native key projection lost settings")
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	service := &Service{cfg: cfg, coreManager: manager}
	service.registerModelsForAuth(auth)
	reg := registry.GetGlobalRegistry()
	t.Cleanup(func() { reg.UnregisterClient(auth.ID) })
	models := reg.GetModelsForClient(auth.ID)
	names := make(map[string]bool)
	for _, model := range models {
		names[model.ID] = true
	}
	if len(models) != 2 || !names["native-fixture"] || !names["tenant/native-fixture"] {
		t.Fatalf("configured native models: %+v", names)
	}
}
