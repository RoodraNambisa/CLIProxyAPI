package cliproxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/api"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	executorhelps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type shutdownOrderingChatGPTWebExecutor struct {
	loginStarted chan struct{}
	loginExited  chan struct{}
}

type shutdownDeadlineExecutor struct {
	closeStarted chan struct{}
	closeRelease chan struct{}
	closeOnce    sync.Once
	closeErr     error
}

type accountInfoTriggerTestExecutor struct {
	shutdownOrderingChatGPTWebExecutor
	mu     sync.Mutex
	authID string
	calls  int
}

func (executor *accountInfoTriggerTestExecutor) TriggerAutomaticAccountInfoRefresh(authID string) bool {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	executor.authID = authID
	executor.calls++
	return true
}

func (executor *accountInfoTriggerTestExecutor) snapshot() (string, int) {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.authID, executor.calls
}

func (*shutdownOrderingChatGPTWebExecutor) Identifier() string { return chatgptwebauth.Provider }

func (*shutdownOrderingChatGPTWebExecutor) Execute(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (*shutdownOrderingChatGPTWebExecutor) ExecuteStream(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, nil
}

func (*shutdownOrderingChatGPTWebExecutor) Refresh(context.Context, *coreauth.Auth) (*coreauth.Auth, error) {
	return nil, nil
}

func (*shutdownOrderingChatGPTWebExecutor) CountTokens(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (*shutdownOrderingChatGPTWebExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func (executor *shutdownOrderingChatGPTWebExecutor) Login(ctx context.Context, _ chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
	close(executor.loginStarted)
	<-ctx.Done()
	close(executor.loginExited)
	return nil, ctx.Err()
}

func (*shutdownOrderingChatGPTWebExecutor) BeginLoginOperation(ctx context.Context, _ string) (context.Context, func(), error) {
	return ctx, func() {}, nil
}

func (*shutdownOrderingChatGPTWebExecutor) ReloginCurrent(context.Context, *coreauth.Auth) (*coreauth.Auth, bool, error) {
	return nil, false, context.Canceled
}

func (executor *shutdownOrderingChatGPTWebExecutor) Close() error {
	<-executor.loginExited
	return nil
}

func (*shutdownDeadlineExecutor) Identifier() string { return "shutdown-deadline" }

func (*shutdownDeadlineExecutor) Execute(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (*shutdownDeadlineExecutor) ExecuteStream(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, nil
}

func (*shutdownDeadlineExecutor) Refresh(context.Context, *coreauth.Auth) (*coreauth.Auth, error) {
	return nil, nil
}

func (*shutdownDeadlineExecutor) CountTokens(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (*shutdownDeadlineExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func (executor *shutdownDeadlineExecutor) Close() error {
	executor.closeOnce.Do(func() { close(executor.closeStarted) })
	<-executor.closeRelease
	return executor.closeErr
}

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

func TestServiceTriggersChatGPTWebAccountInfoRefreshForImage429(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	trigger := &accountInfoTriggerTestExecutor{}
	manager.RegisterExecutor(trigger)
	service := &Service{coreManager: manager}

	service.triggerChatGPTWebAccountInfoRefresh(coreauth.Result{
		AuthID:   "web-auth",
		Provider: chatgptwebauth.Provider,
		Model:    chatgptwebauth.ImageModel,
		Error:    &coreauth.Error{HTTPStatus: http.StatusTooManyRequests, Message: "quota"},
	})
	authID, calls := trigger.snapshot()
	if authID != "web-auth" || calls != 1 {
		t.Fatalf("trigger = auth %q calls %d", authID, calls)
	}

	service.triggerChatGPTWebAccountInfoRefresh(coreauth.Result{
		AuthID:   "web-auth",
		Provider: chatgptwebauth.Provider,
		Model:    "gpt-5.6",
		Error:    &coreauth.Error{HTTPStatus: http.StatusTooManyRequests},
	})
	service.triggerChatGPTWebAccountInfoRefresh(coreauth.Result{
		AuthID:   "web-auth",
		Provider: chatgptwebauth.Provider,
		Model:    chatgptwebauth.ImageModel,
		Error:    &coreauth.Error{HTTPStatus: http.StatusBadGateway},
	})
	_, calls = trigger.snapshot()
	if calls != 1 {
		t.Fatalf("non-image/non-429 results triggered refresh: calls = %d", calls)
	}
}

func TestAuthMaintenanceIgnoresChatGPTWebImage429(t *testing.T) {
	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	auth := &coreauth.Auth{
		ID:       "chatgpt-web-image-maintenance",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusError,
		FileName: filepath.Join(authDir, "chatgpt-web-image-maintenance.json"),
		Attributes: map[string]string{
			"path": filepath.Join(authDir, "chatgpt-web-image-maintenance.json"),
		},
		Metadata: map[string]any{"type": chatgptwebauth.Provider},
		LastError: &coreauth.Error{
			Code:       "chatgpt_web_image_quota",
			Message:    "image quota exhausted",
			HTTPStatus: http.StatusTooManyRequests,
		},
	}
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	current, _ := manager.GetByID(auth.ID)
	imageResult := coreauth.Result{
		AuthID:   auth.ID,
		Provider: chatgptwebauth.Provider,
		Model:    chatgptwebauth.ImageModel,
		Error: &coreauth.Error{
			Code:       "chatgpt_web_image_quota",
			Message:    "image quota exhausted",
			HTTPStatus: http.StatusTooManyRequests,
		},
	}
	deleteConfig := config.AuthMaintenanceConfig{
		Enable:            true,
		DeleteStatusCodes: []int{http.StatusTooManyRequests},
	}
	disableConfig := config.AuthMaintenanceConfig{
		Enable:             true,
		DisableStatusCodes: []int{http.StatusTooManyRequests},
	}
	if reason, eligible := authEligibleForMaintenanceDelete(current, &imageResult, deleteConfig); eligible {
		t.Fatalf("image result delete eligibility = %q", reason)
	}
	if reason, eligible := authEligibleForMaintenanceDisable(current, &imageResult, disableConfig); eligible {
		t.Fatalf("image result disable eligibility = %q", reason)
	}

	now := time.Now().UTC()
	genericImageAuth := &coreauth.Auth{
		ID:        "chatgpt-web-generic-image-maintenance",
		Provider:  chatgptwebauth.Provider,
		Status:    coreauth.StatusError,
		UpdatedAt: now.Add(time.Minute),
		FileName:  filepath.Join(authDir, "chatgpt-web-generic-image-maintenance.json"),
		Attributes: map[string]string{
			"path": filepath.Join(authDir, "chatgpt-web-generic-image-maintenance.json"),
		},
		Metadata: map[string]any{"type": chatgptwebauth.Provider},
		LastError: &coreauth.Error{
			Message:    "ordinary image rate limit",
			HTTPStatus: http.StatusTooManyRequests,
		},
		ModelStates: map[string]*coreauth.ModelState{
			chatgptwebauth.ImageModel: {
				Status:    coreauth.StatusError,
				UpdatedAt: now,
				LastError: &coreauth.Error{
					Message:    "ordinary image rate limit",
					HTTPStatus: http.StatusTooManyRequests,
				},
			},
		},
	}
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), genericImageAuth); errRegister != nil {
		t.Fatalf("register generic image auth: %v", errRegister)
	}

	service := &Service{coreManager: manager}
	if candidates := service.scanAuthMaintenanceCandidates(deleteConfig, authDir); len(candidates) != 0 {
		t.Fatalf("periodic image delete candidates = %d", len(candidates))
	}
	if candidates := service.scanAuthMaintenanceDisableCandidates(disableConfig, authDir); len(candidates) != 0 {
		t.Fatalf("periodic image disable candidates = %d", len(candidates))
	}
	textLimitedAuth := genericImageAuth.Clone()
	textLimitedAuth.LastError = &coreauth.Error{
		Message:    "text rate limit",
		HTTPStatus: http.StatusTooManyRequests,
	}
	if statusCode := authMaintenanceStatusCode(textLimitedAuth, nil); statusCode != http.StatusTooManyRequests {
		t.Fatalf("text limit maintenance status = %d, want 429", statusCode)
	}
	sameErrorTextAuth := genericImageAuth.Clone()
	sameErrorTextAuth.ModelStates["gpt-5.6"] = &coreauth.ModelState{
		Status:    coreauth.StatusError,
		UpdatedAt: now.Add(2 * time.Minute),
		LastError: &coreauth.Error{
			Message:    "ordinary image rate limit",
			HTTPStatus: http.StatusTooManyRequests,
		},
	}
	if statusCode := authMaintenanceStatusCode(sameErrorTextAuth, nil); statusCode != http.StatusTooManyRequests {
		t.Fatalf("same-message text limit maintenance status = %d, want 429", statusCode)
	}
	sameErrorImageAuth := sameErrorTextAuth.Clone()
	sameErrorImageAuth.ModelStates[chatgptwebauth.ImageModel].UpdatedAt = now.Add(3 * time.Minute)
	if statusCode := authMaintenanceStatusCode(sameErrorImageAuth, nil); statusCode != 0 {
		t.Fatalf("newer same-message image limit maintenance status = %d, want ignored", statusCode)
	}
	explicitQuotaTextAuth := sameErrorTextAuth.Clone()
	explicitQuotaTextAuth.LastError.Code = "chatgpt_web_image_quota"
	explicitQuotaTextAuth.ModelStates[chatgptwebauth.ImageModel].LastError.Code = "chatgpt_web_image_quota"
	explicitQuotaTextAuth.ModelStates["gpt-5.6"].LastError.Code = "chatgpt_web_image_quota"
	if statusCode := authMaintenanceStatusCode(explicitQuotaTextAuth, nil); statusCode != http.StatusTooManyRequests {
		t.Fatalf("newer same-code text limit maintenance status = %d, want 429", statusCode)
	}

	textResult := imageResult
	textResult.Model = "gpt-5.6"
	textResult.Error = &coreauth.Error{
		Code:       "rate_limit_exceeded",
		Message:    "text rate limit",
		HTTPStatus: http.StatusTooManyRequests,
	}
	if reason, eligible := authEligibleForMaintenanceDelete(current, &textResult, deleteConfig); !eligible || reason != "http_429" {
		t.Fatalf("text result delete eligibility = %q eligible=%v", reason, eligible)
	}
	if reason, eligible := authEligibleForMaintenanceDisable(current, &textResult, disableConfig); !eligible || reason != "http_429" {
		t.Fatalf("text result disable eligibility = %q eligible=%v", reason, eligible)
	}
}

func TestAuthMaintenanceIgnoresConfiguredChatGPTWebImageModel429(t *testing.T) {
	auth := &coreauth.Auth{
		ID:       "chatgpt-web-custom-image-maintenance",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusError,
		LastError: &coreauth.Error{
			Message:    "image quota exhausted",
			HTTPStatus: http.StatusTooManyRequests,
		},
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{
		ID:         "custom-web-image",
		UpstreamID: chatgptwebauth.ImageModel,
		Type:       registry.OpenAIImageModelType,
	}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})

	result := coreauth.Result{
		AuthID:   auth.ID,
		Provider: chatgptwebauth.Provider,
		Model:    "custom-web-image",
		Error: &coreauth.Error{
			Message:    "image quota exhausted",
			HTTPStatus: http.StatusTooManyRequests,
		},
	}
	cfg := config.AuthMaintenanceConfig{
		Enable:            true,
		DeleteStatusCodes: []int{http.StatusTooManyRequests},
	}
	if status := authMaintenanceStatusCode(auth, &result); status != 0 {
		t.Fatalf("custom image maintenance status = %d, want ignored", status)
	}
	if reason, eligible := authEligibleForMaintenanceDelete(auth, &result, cfg); eligible {
		t.Fatalf("custom image result delete eligibility = %q", reason)
	}
}

func TestServiceBindsChatGPTWebExecutorBeforeFirstCredential(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}

	service.ensureChatGPTWebExecutor(false)

	registered, ok := service.coreManager.Executor("chatgpt-web")
	if !ok {
		t.Fatal("chatgpt web executor was not registered before the first credential")
	}
	if _, ok = registered.(*executor.ChatGPTWebExecutor); !ok {
		t.Fatalf("chatgpt web executor type = %T, want *executor.ChatGPTWebExecutor", registered)
	}
}

func TestServiceSchedulesRecoveryForChatGPTWebAuthRegisteredAfterExecutor(t *testing.T) {
	jitterSeconds := 0
	cfg := &config.Config{}
	cfg.ChatGPTWeb.AccountInfo.RecoveryJitterSeconds = &jitterSeconds
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{
		cfg:         cfg,
		coreManager: manager,
	}
	service.ensureChatGPTWebExecutor(false)
	registeredExecutor, ok := manager.Executor(chatgptwebauth.Provider)
	if !ok {
		t.Fatal("ChatGPT Web executor was not registered")
	}
	chatGPTWebExecutor, ok := registeredExecutor.(*executor.ChatGPTWebExecutor)
	if !ok {
		t.Fatalf("executor type = %T", registeredExecutor)
	}
	t.Cleanup(func() {
		if errClose := chatGPTWebExecutor.Close(); errClose != nil {
			t.Errorf("Close() error = %v", errClose)
		}
	})

	resetAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	installed, errRegister := manager.Register(coreauth.WithSkipPersist(context.Background()), &coreauth.Auth{
		ID:       "chatgpt-web-late-recovery",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"type":                  chatgptwebauth.Provider,
			"access_token":          "token",
			"lifecycle_state":       coreauth.LifecycleStateActive,
			"quota_state":           string(chatgptwebauth.QuotaStateExhausted),
			"image_quota_remaining": 0,
			"image_quota_reset_at":  resetAt.Format(time.RFC3339Nano),
		},
	})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	authMaintenanceHook{service: service}.OnAuthRegistered(context.Background(), installed)
	state := chatGPTWebExecutor.AccountInfoAuthState(installed.ID)
	if !state.NextRefreshAt.Equal(resetAt) {
		t.Fatalf("recovery schedule = %v, want %v", state.NextRefreshAt, resetAt)
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

	updatedConfig := &config.Config{}
	updatedConfig.ChatGPTWeb.AutoRelogin = true
	service.cfg = updatedConfig
	service.ensureExecutorsForAuthWithMode(auth, true)
	third, ok := service.coreManager.Executor("chatgpt-web")
	if !ok || third == nil {
		t.Fatal("expected chatgpt web executor after forced bind")
	}
	if second != third {
		t.Fatal("configuration rebind replaced the chatgpt web executor")
	}
	chatGPTWebExecutor, ok := third.(*executor.ChatGPTWebExecutor)
	if !ok || !chatGPTWebExecutor.AutoReloginEnabled() {
		t.Fatal("configuration rebind did not update the ChatGPT Web executor")
	}
}

func TestBeforeStartHookUpdatesChatGPTWebExecutorSnapshot(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
		hooks: Hooks{OnBeforeStart: func(runtimeConfig *config.Config) {
			runtimeConfig.ChatGPTWeb.AutoRelogin = true
		}},
	}
	service.ensureChatGPTWebExecutor(false)

	if errApply := service.applyBeforeStartConfig(); errApply != nil {
		t.Fatalf("applyBeforeStartConfig() error = %v", errApply)
	}
	registered, ok := service.coreManager.Executor(chatgptwebauth.Provider)
	if !ok {
		t.Fatal("chatgpt web executor is missing")
	}
	chatGPTWebExecutor, ok := registered.(*executor.ChatGPTWebExecutor)
	if !ok || !chatGPTWebExecutor.AutoReloginEnabled() {
		t.Fatal("before-start hook update was not published to the ChatGPT Web executor")
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

func TestServiceRebindExecutorsResumesPendingChatGPTWebRelogin(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{
		cfg:         &config.Config{},
		coreManager: manager,
	}
	auth := &coreauth.Auth{
		ID:       "chatgpt-web-pending-relogin",
		Provider: "chatgpt-web",
		Status:   coreauth.StatusPending,
		Metadata: map[string]any{
			"email":           "pending@example.com",
			"lifecycle_state": coreauth.LifecycleStateReloginPending,
		},
	}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	service.ensureChatGPTWebExecutor(false)
	before, ok := manager.Executor("chatgpt-web")
	if !ok {
		t.Fatal("initial ChatGPT Web executor was not registered")
	}
	observed := make(chan string, 1)
	service.chatGPTWebReloginObserved = func(current *coreauth.Auth) {
		observed <- current.ID
	}

	service.rebindExecutorsForAuths(manager.List())

	after, ok := manager.Executor("chatgpt-web")
	if !ok {
		t.Fatal("replacement ChatGPT Web executor was not registered")
	}
	if before != after {
		t.Fatal("configuration rebind replaced the ChatGPT Web executor")
	}
	select {
	case authID := <-observed:
		if authID != auth.ID {
			t.Fatalf("resumed auth ID = %q, want %q", authID, auth.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("pending ChatGPT Web re-login was not resumed after executor replacement")
	}
}

func TestServiceShutdownCancelsManagementLoginBeforeClosingExecutor(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "secret")
	cfg := &config.Config{}
	cfg.RemoteManagement.SecretKey = "secret"
	cfg.RemoteManagement.AllowRemote = true
	manager := coreauth.NewManager(nil, nil, nil)
	executor := &shutdownOrderingChatGPTWebExecutor{
		loginStarted: make(chan struct{}),
		loginExited:  make(chan struct{}),
	}
	manager.RegisterExecutor(executor)
	var engine *gin.Engine
	server := api.NewServer(
		cfg,
		manager,
		nil,
		"",
		api.WithEngineConfigurator(func(configured *gin.Engine) {
			engine = configured
		}),
	)
	service := &Service{cfg: cfg, coreManager: manager, server: server}

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/v0/management/chatgpt-web/login-tasks",
		bytes.NewBufferString("shutdown@example.com---password---JBSWY3DPEHPK3PXP"),
	)
	request.Header.Set("Content-Type", "text/plain")
	request.Header.Set("X-Management-Key", "secret")
	engine.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("create login task status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	select {
	case <-executor.loginStarted:
	case <-time.After(time.Second):
		t.Fatal("login task did not start")
	}

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		shutdownDone <- service.Shutdown(ctx)
	}()
	select {
	case errShutdown := <-shutdownDone:
		if errShutdown != nil {
			t.Fatalf("Shutdown() error = %v", errShutdown)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Service.Shutdown deadlocked while a management login was active")
	}
}

func TestServiceConcurrentShutdownHonorsDeadlineAndReturnsExecutorError(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	wantCloseErr := errors.New("executor close failed")
	blocked := &shutdownDeadlineExecutor{
		closeStarted: make(chan struct{}),
		closeRelease: make(chan struct{}),
		closeErr:     wantCloseErr,
	}
	var releaseOnce sync.Once
	releaseClose := func() { releaseOnce.Do(func() { close(blocked.closeRelease) }) }
	t.Cleanup(releaseClose)
	manager.RegisterExecutor(blocked)
	service := &Service{cfg: &config.Config{}, coreManager: manager}

	firstDone := make(chan error, 1)
	go func() { firstDone <- service.Shutdown(context.Background()) }()
	select {
	case <-blocked.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("executor close did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	startedAt := time.Now()
	errShutdown := service.Shutdown(ctx)
	if !errors.Is(errShutdown, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want deadline exceeded", errShutdown)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("Shutdown() exceeded its context deadline: %v", elapsed)
	}

	finalDone := make(chan error, 1)
	go func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
		defer closeCancel()
		finalDone <- service.Shutdown(closeCtx)
	}()
	select {
	case errClose := <-finalDone:
		t.Fatalf("final Shutdown() returned before executor release: %v", errClose)
	case <-time.After(20 * time.Millisecond):
	}
	releaseClose()
	for index, done := range []<-chan error{firstDone, finalDone} {
		select {
		case errClose := <-done:
			if !errors.Is(errClose, wantCloseErr) {
				t.Fatalf("Shutdown() result %d = %v, want %v", index, errClose, wantCloseErr)
			}
		case <-time.After(time.Second):
			t.Fatalf("Shutdown() result %d did not finish", index)
		}
	}
}

func TestServiceShutdownKeepsProxyTransportUntilExecutorsClose(t *testing.T) {
	var connections atomic.Int32
	proxyServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	proxyServer.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	proxyServer.Start()
	defer proxyServer.Close()

	auth := &coreauth.Auth{ID: "shutdown-proxy-transport", ProxyURL: proxyServer.URL}
	client := executorhelps.NewProxyAwareHTTPClient(context.Background(), &config.Config{}, auth, 0)
	request := func() {
		t.Helper()
		response, errRequest := client.Get("http://upstream.invalid/test")
		if errRequest != nil {
			t.Fatalf("proxy request: %v", errRequest)
		}
		_, errRead := io.Copy(io.Discard, response.Body)
		errClose := response.Body.Close()
		if errRead != nil || errClose != nil {
			t.Fatalf("consume proxy response: %v", errors.Join(errRead, errClose))
		}
	}
	request()
	if got := connections.Load(); got != 1 {
		t.Fatalf("initial proxy connections = %d, want 1", got)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	blocked := &shutdownDeadlineExecutor{
		closeStarted: make(chan struct{}),
		closeRelease: make(chan struct{}),
	}
	var releaseOnce sync.Once
	releaseClose := func() { releaseOnce.Do(func() { close(blocked.closeRelease) }) }
	defer releaseClose()
	manager.RegisterExecutor(blocked)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- service.Shutdown(context.Background()) }()
	select {
	case <-blocked.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("executor close did not start")
	}
	time.Sleep(50 * time.Millisecond)
	request()
	if got := connections.Load(); got != 1 {
		releaseClose()
		<-shutdownDone
		t.Fatalf("proxy transport was closed before executor exit; connections = %d", got)
	}
	releaseClose()
	select {
	case errShutdown := <-shutdownDone:
		if errShutdown != nil {
			t.Fatalf("Shutdown() error = %v", errShutdown)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown() did not finish after executor exit")
	}
}

func TestServiceShutdownOnRunExitCreatesDeadlineWhenInvoked(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	blocked := &shutdownDeadlineExecutor{
		closeStarted: make(chan struct{}),
		closeRelease: make(chan struct{}),
	}
	manager.RegisterExecutor(blocked)
	service := &Service{cfg: &config.Config{}, coreManager: manager}

	time.Sleep(40 * time.Millisecond)
	startedAt := time.Now()
	errShutdown := service.shutdownOnRunExit(30 * time.Millisecond)
	if !errors.Is(errShutdown, context.DeadlineExceeded) {
		t.Fatalf("shutdownOnRunExit() error = %v, want deadline exceeded", errShutdown)
	}
	if elapsed := time.Since(startedAt); elapsed < 20*time.Millisecond {
		t.Fatalf("shutdownOnRunExit() used a deadline created before invocation: %v", elapsed)
	}
	close(blocked.closeRelease)
	if errFinal := service.Shutdown(context.Background()); errFinal != nil {
		t.Fatalf("final Shutdown() error = %v", errFinal)
	}
}
