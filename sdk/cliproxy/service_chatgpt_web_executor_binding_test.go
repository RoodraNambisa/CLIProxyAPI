package cliproxy

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestServiceBindsChatGPTWebExecutorWithBuiltinModels(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	auth := &coreauth.Auth{
		ID:       "chatgpt-web-service-auth",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"access_token": "token", "lifecycle_state": coreauth.LifecycleStateActive},
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })

	service.ensureExecutorsForAuth(auth)
	registered, ok := service.coreManager.Executor("chatgpt-web")
	if !ok {
		t.Fatal("chatgpt web executor was not registered")
	}
	if _, ok = registered.(*executor.ChatGPTWebExecutor); !ok {
		t.Fatalf("chatgpt web executor type = %T, want *executor.ChatGPTWebExecutor", registered)
	}

	service.registerModelsForAuth(auth)
	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if !containsRegisteredModel(models, "gpt-image-2") {
		t.Fatalf("chatgpt web models = %v, want gpt-image-2", models)
	}
}

func TestServiceChatGPTWebExecutorBindingModes(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	auth := &coreauth.Auth{ID: "chatgpt-web-binding-auth", Provider: "chatgpt-web", Status: coreauth.StatusActive}

	service.ensureExecutorsForAuth(auth)
	first, ok := service.coreManager.Executor("chatgpt-web")
	if !ok || first == nil {
		t.Fatal("expected chatgpt web executor after first bind")
	}
	service.ensureExecutorsForAuth(auth)
	second, ok := service.coreManager.Executor("chatgpt-web")
	if !ok || second == nil {
		t.Fatal("expected chatgpt web executor after second bind")
	}
	if first != second {
		t.Fatal("normal binding unexpectedly replaced the chatgpt web executor")
	}

	service.ensureExecutorsForAuthWithMode(auth, true)
	third, ok := service.coreManager.Executor("chatgpt-web")
	if !ok || third == nil {
		t.Fatal("expected chatgpt web executor after forced bind")
	}
	if second == third {
		t.Fatal("forced binding did not replace the chatgpt web executor")
	}
}

func TestServiceBindsNativeAndOpenAICompatChatGPTWebExecutors(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			OpenAICompatibility: []config.OpenAICompatibility{{Name: "chatgpt-web"}},
		},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	compatAuth := &coreauth.Auth{
		ID:       "chatgpt-web-compat-auth",
		Provider: "openai-compatibility-chatgpt-web",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"compat_name":  "chatgpt-web",
			"provider_key": "openai-compatibility-chatgpt-web",
		},
	}
	nativeAuth := &coreauth.Auth{ID: "chatgpt-web-native-auth", Provider: "chatgpt-web", Status: coreauth.StatusActive}

	service.ensureExecutorsForAuth(compatAuth)
	registered, ok := service.coreManager.Executor("openai-compatibility-chatgpt-web")
	if !ok {
		t.Fatal("chatgpt-web compatibility executor was not registered under its reserved runtime key")
	}
	if _, ok = registered.(*executor.OpenAICompatExecutor); !ok {
		t.Fatalf("chatgpt-web compatibility executor type = %T, want *executor.OpenAICompatExecutor", registered)
	}
	service.ensureExecutorsForAuth(nativeAuth)
	registered, ok = service.coreManager.Executor("chatgpt-web")
	if !ok {
		t.Fatal("native chatgpt-web executor was not registered")
	}
	if _, ok = registered.(*executor.ChatGPTWebExecutor); !ok {
		t.Fatalf("native chatgpt-web executor type = %T, want *executor.ChatGPTWebExecutor", registered)
	}
}

func TestServiceRebindExecutorsDoesNotConsumeChatGPTWebDedupForDisabledAuth(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	disabled := &coreauth.Auth{
		ID:       "chatgpt-web-disabled-auth",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusDisabled,
		Disabled: true,
	}
	enabled := &coreauth.Auth{
		ID:       "chatgpt-web-enabled-auth",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
	}

	service.rebindExecutorsForAuths([]*coreauth.Auth{disabled, enabled})
	registered, ok := service.coreManager.Executor("chatgpt-web")
	if !ok {
		t.Fatal("enabled chatgpt web auth did not bind an executor after disabled auth")
	}
	if _, ok = registered.(*executor.ChatGPTWebExecutor); !ok {
		t.Fatalf("chatgpt web executor type = %T, want *executor.ChatGPTWebExecutor", registered)
	}
}

func TestServiceRebindsLegacyCompatAndNativeChatGPTWebExecutors(t *testing.T) {
	service := &Service{
		cfg: &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{
			Name: "chatgpt-web",
		}}},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	legacyCompat := &coreauth.Auth{
		ID:       "legacy-chatgpt-web-compat",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"compat_name":  "chatgpt-web",
			"provider_key": "openai-compatibility-chatgpt-web",
		},
	}
	native := &coreauth.Auth{ID: "native-chatgpt-web", Provider: "chatgpt-web", Status: coreauth.StatusActive}

	service.rebindExecutorsForAuths([]*coreauth.Auth{legacyCompat, native})
	compatExecutor, ok := service.coreManager.Executor("openai-compatibility-chatgpt-web")
	if !ok {
		t.Fatal("legacy compatibility auth did not bind its executor")
	}
	if _, ok = compatExecutor.(*executor.OpenAICompatExecutor); !ok {
		t.Fatalf("compatibility executor type = %T", compatExecutor)
	}
	nativeExecutor, ok := service.coreManager.Executor("chatgpt-web")
	if !ok {
		t.Fatal("native chatgpt web auth did not bind its executor")
	}
	if _, ok = nativeExecutor.(*executor.ChatGPTWebExecutor); !ok {
		t.Fatalf("native executor type = %T", nativeExecutor)
	}
}
