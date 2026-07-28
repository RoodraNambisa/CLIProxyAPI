package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type chatGPTWebQuotaMutationBlockingStore struct {
	once    sync.Once
	started chan struct{}
	release chan struct{}
}

func (*chatGPTWebQuotaMutationBlockingStore) List(context.Context) ([]*Auth, error) {
	return nil, nil
}

func (store *chatGPTWebQuotaMutationBlockingStore) Save(_ context.Context, _ *Auth) (string, error) {
	block := false
	store.once.Do(func() {
		block = true
		close(store.started)
	})
	if block {
		<-store.release
	}
	return "", nil
}

func (*chatGPTWebQuotaMutationBlockingStore) Delete(context.Context, string) error {
	return nil
}

type committedImageQuotaTestError struct{}

func (committedImageQuotaTestError) Error() string                { return "image quota exhausted" }
func (committedImageQuotaTestError) StatusCode() int              { return http.StatusTooManyRequests }
func (committedImageQuotaTestError) RequestCommitted() bool       { return true }
func (committedImageQuotaTestError) ExecutionResultModel() string { return chatgptwebauth.ImageModel }
func (committedImageQuotaTestError) ExecutionResultErrorCode() string {
	return "chatgpt_web_image_quota"
}

func TestChatGPTWebImageQuotaBlocksOnlyImageModel(t *testing.T) {
	now := time.Now()
	auth := &Auth{
		ID:       "quota-scope",
		Provider: chatgptwebauth.Provider,
		Status:   StatusActive,
		Metadata: map[string]any{
			"lifecycle_state":      LifecycleStateActive,
			"quota_state":          string(chatgptwebauth.QuotaStateExhausted),
			"quota_stale":          true,
			"image_quota_reset_at": now.Add(-time.Minute).Format(time.RFC3339Nano),
		},
	}

	if blocked, _, _ := isAuthBlockedForModel(auth, chatgptwebauth.ImageModel, now); !blocked {
		t.Fatal("expired reset time automatically unblocked exhausted image quota")
	}
	if blocked, _, _ := isAuthBlockedForModel(auth, "gpt-5.6", now); blocked {
		t.Fatal("image quota blocked a text model")
	}

	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateAvailable)
	if blocked, _, _ := isAuthBlockedForModel(auth, chatgptwebauth.ImageModel, now); blocked {
		t.Fatal("stale but last-known available quota blocked the image model")
	}
	auth.Metadata["image_quota_remaining"] = 0
	if blocked, _, _ := isAuthBlockedForModel(auth, chatgptwebauth.ImageModel, now); !blocked {
		t.Fatal("zero remaining quota did not override an inconsistent available state")
	}
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)
	auth.Metadata["image_quota_remaining"] = 3
	if blocked, _, _ := isAuthBlockedForModel(auth, chatgptwebauth.ImageModel, now); blocked {
		t.Fatal("positive remaining quota did not override an inconsistent exhausted state")
	}
}

func TestManagerChatGPTWebImageQuotaFallsBackAndReturnsDedicatedError(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	executor := &authFallbackExecutor{id: chatgptwebauth.Provider}
	manager.RegisterExecutor(executor)

	highID := "quota-high-" + uuid.NewString()
	lowID := "quota-low-" + uuid.NewString()
	resetAt := time.Now().Add(2 * time.Minute)
	registerFallbackAuthForModel(t, manager, &Auth{
		ID:         highID,
		Provider:   chatgptwebauth.Provider,
		Status:     StatusActive,
		Attributes: map[string]string{"priority": "10"},
		Metadata: map[string]any{
			"lifecycle_state":      LifecycleStateActive,
			"quota_state":          string(chatgptwebauth.QuotaStateExhausted),
			"image_quota_reset_at": resetAt.Format(time.RFC3339Nano),
		},
	}, chatgptwebauth.ImageModel)
	registerFallbackAuthForModel(t, manager, &Auth{
		ID:         lowID,
		Provider:   chatgptwebauth.Provider,
		Status:     StatusActive,
		Attributes: map[string]string{"priority": "0"},
		Metadata: map[string]any{
			"lifecycle_state": LifecycleStateActive,
			"quota_state":     string(chatgptwebauth.QuotaStateAvailable),
		},
	}, chatgptwebauth.ImageModel)

	response, errExecute := manager.Execute(context.Background(), []string{chatgptwebauth.Provider}, cliproxyexecutor.Request{Model: chatgptwebauth.ImageModel}, cliproxyexecutor.Options{})
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if string(response.Payload) != lowID {
		t.Fatalf("selected auth = %q, want %q", string(response.Payload), lowID)
	}

	current, ok := manager.GetByID(lowID)
	if !ok || current == nil {
		t.Fatal("available auth disappeared")
	}
	current.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)
	current.Metadata["image_quota_reset_at"] = resetAt.Format(time.RFC3339Nano)
	if _, errUpdate := manager.Update(context.Background(), current); errUpdate != nil {
		t.Fatalf("update low auth: %v", errUpdate)
	}

	_, errExecute = manager.Execute(context.Background(), []string{chatgptwebauth.Provider}, cliproxyexecutor.Request{Model: chatgptwebauth.ImageModel}, cliproxyexecutor.Options{})
	if errExecute == nil {
		t.Fatal("Execute() error = nil, want image quota error")
	}
	var status interface{ StatusCode() int }
	if !errors.As(errExecute, &status) || status.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("status error = %v, want 429", errExecute)
	}
	if !strings.Contains(errExecute.Error(), `"code":"image_quota_exhausted"`) {
		t.Fatalf("error = %v, want image_quota_exhausted", errExecute)
	}
	var headers interface{ Headers() http.Header }
	if !errors.As(errExecute, &headers) || headers.Headers().Get("Retry-After") == "" {
		t.Fatalf("Retry-After missing: %v", errExecute)
	}
}

func TestChatGPTWebImageQuotaExhaustedErrorRetryAfter(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	errQuota := newChatGPTWebImageQuotaExhaustedError(now.Add(90*time.Second), now)

	retryAfter := errQuota.RetryAfter()
	if retryAfter == nil || *retryAfter != 90*time.Second {
		t.Fatalf("RetryAfter() = %v, want 90s", retryAfter)
	}
	if resetIn, ok := availabilityBlockerResetIn(errQuota); !ok || resetIn != 90*time.Second {
		t.Fatalf("availabilityBlockerResetIn() = %v, %v, want 90s, true", resetIn, ok)
	}
	if wait, ok := retryAfterWaitFromError(errQuota, 2*time.Minute); !ok || wait != 90*time.Second {
		t.Fatalf("retryAfterWaitFromError() = %v, %v, want 90s, true", wait, ok)
	}

	expired := newChatGPTWebImageQuotaExhaustedError(now, now)
	if retryAfter := expired.RetryAfter(); retryAfter != nil {
		t.Fatalf("expired RetryAfter() = %v, want nil", retryAfter)
	}
}

type chatGPTWebQuotaRefreshTriggerExecutor struct {
	*authFallbackExecutor
	authIDs       []string
	runtimeStates map[string]chatgptwebauth.AccountInfoAuthRuntimeState
}

type chatGPTWebRoundErrorTestExecutor struct {
	id  string
	err error

	mu           sync.Mutex
	executeCalls []string
	streamCalls  []string
}

func (executor *chatGPTWebRoundErrorTestExecutor) Identifier() string {
	return executor.id
}

func (executor *chatGPTWebRoundErrorTestExecutor) Execute(
	_ context.Context,
	auth *Auth,
	_ cliproxyexecutor.Request,
	_ cliproxyexecutor.Options,
) (cliproxyexecutor.Response, error) {
	executor.mu.Lock()
	executor.executeCalls = append(executor.executeCalls, auth.ID)
	executor.mu.Unlock()
	return cliproxyexecutor.Response{}, executor.err
}

func (executor *chatGPTWebRoundErrorTestExecutor) ExecuteStream(
	_ context.Context,
	auth *Auth,
	_ cliproxyexecutor.Request,
	_ cliproxyexecutor.Options,
) (*cliproxyexecutor.StreamResult, error) {
	executor.mu.Lock()
	executor.streamCalls = append(executor.streamCalls, auth.ID)
	executor.mu.Unlock()
	return nil, executor.err
}

func (*chatGPTWebRoundErrorTestExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (*chatGPTWebRoundErrorTestExecutor) CountTokens(
	context.Context,
	*Auth,
	cliproxyexecutor.Request,
	cliproxyexecutor.Options,
) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (*chatGPTWebRoundErrorTestExecutor) HttpRequest(
	context.Context,
	*Auth,
	*http.Request,
) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (executor *chatGPTWebRoundErrorTestExecutor) calls(stream bool) []string {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if stream {
		return append([]string(nil), executor.streamCalls...)
	}
	return append([]string(nil), executor.executeCalls...)
}

func (executor *chatGPTWebQuotaRefreshTriggerExecutor) TriggerAutomaticAccountInfoRefresh(authID string) bool {
	executor.authIDs = append(executor.authIDs, authID)
	if executor.runtimeStates == nil {
		executor.runtimeStates = make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState)
	}
	state := executor.runtimeStates[authID]
	state.Refreshing = true
	executor.runtimeStates[authID] = state
	return true
}

func (executor *chatGPTWebQuotaRefreshTriggerExecutor) AccountInfoAuthState(
	authID string,
) chatgptwebauth.AccountInfoAuthRuntimeState {
	return executor.runtimeStates[authID]
}

type chatGPTWebImageSuccessProjectionExecutor struct {
	manager                  *Manager
	markImageSuccess         bool
	confirmExhaustedInFlight bool
}

func (*chatGPTWebImageSuccessProjectionExecutor) Identifier() string {
	return chatgptwebauth.Provider
}

func (executor *chatGPTWebImageSuccessProjectionExecutor) recordImageResult(
	ctx context.Context,
	auth *Auth,
	opts cliproxyexecutor.Options,
) error {
	if executor.confirmExhaustedInFlight {
		_, current, errMutate := executor.manager.MutateRuntimeMetadataIfCurrent(ctx, auth, func(candidate *Auth) {
			candidate.Metadata["image_quota_remaining"] = 0
			candidate.Metadata["image_quota_reset_at"] = time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano)
			candidate.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)
			candidate.Metadata["quota_updated_at"] = time.Now().UTC().Format(time.RFC3339Nano)
			candidate.Metadata["quota_stale"] = false
			state := ensureModelState(candidate, chatgptwebauth.ImageModel)
			state.Status = StatusError
			state.Unavailable = true
			state.NextRetryAfter = time.Now().Add(time.Hour)
			state.Quota = QuotaState{
				Exceeded:      true,
				Reason:        "chatgpt_web_image_quota",
				NextRecoverAt: state.NextRetryAfter,
			}
		})
		if errMutate != nil {
			return errMutate
		}
		if !current {
			return errors.New("auth changed during in-flight quota update")
		}
	}
	if executor.markImageSuccess {
		state, _ := opts.Metadata[cliproxyexecutor.ImageGenerationResultStateMetadataKey].(*cliproxyexecutor.ImageGenerationResultState)
		if state != nil {
			state.MarkSucceeded()
		}
	}
	return nil
}

func (executor *chatGPTWebImageSuccessProjectionExecutor) Execute(
	ctx context.Context,
	auth *Auth,
	_ cliproxyexecutor.Request,
	opts cliproxyexecutor.Options,
) (cliproxyexecutor.Response, error) {
	if err := executor.recordImageResult(ctx, auth, opts); err != nil {
		return cliproxyexecutor.Response{}, err
	}
	return cliproxyexecutor.Response{Payload: []byte("text-result")}, nil
}

func (executor *chatGPTWebImageSuccessProjectionExecutor) ExecuteStream(
	ctx context.Context,
	auth *Auth,
	_ cliproxyexecutor.Request,
	opts cliproxyexecutor.Options,
) (*cliproxyexecutor.StreamResult, error) {
	if err := executor.recordImageResult(ctx, auth, opts); err != nil {
		return nil, err
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("text-stream-result")}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (*chatGPTWebImageSuccessProjectionExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (*chatGPTWebImageSuccessProjectionExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (*chatGPTWebImageSuccessProjectionExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

type chatGPTWebImageSuccessResultHook struct {
	NoopHook
	mu      sync.Mutex
	results []Result
}

func (hook *chatGPTWebImageSuccessResultHook) OnResult(_ context.Context, result Result) {
	hook.mu.Lock()
	hook.results = append(hook.results, result)
	hook.mu.Unlock()
}

func (hook *chatGPTWebImageSuccessResultHook) Results() []Result {
	hook.mu.Lock()
	defer hook.mu.Unlock()
	return append([]Result(nil), hook.results...)
}

func registerQuotaTestAuthForModels(t *testing.T, manager *Manager, auth *Auth, models ...string) {
	t.Helper()
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth %s: %v", auth.ID, errRegister)
	}
	modelInfos := make([]*registry.ModelInfo, 0, len(models))
	for _, model := range models {
		modelInfos = append(modelInfos, &registry.ModelInfo{ID: model})
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, modelInfos)
	manager.RefreshSchedulerEntry(auth.ID)
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})
}

func markOrdinaryChatGPTWebImageRateLimit(manager *Manager, authID string, retryAfter time.Duration) {
	manager.MarkResult(context.Background(), Result{
		AuthID:   authID,
		Provider: chatgptwebauth.Provider,
		Model:    chatgptwebauth.ImageModel,
		Error: &Error{
			Message:    "ordinary image rate limit",
			HTTPStatus: http.StatusTooManyRequests,
		},
		RetryAfter: &retryAfter,
	})
}

func assertOrdinaryImageModelCooldown(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("selection error = nil")
	}
	if strings.Contains(err.Error(), `"code":"image_quota_exhausted"`) {
		t.Fatalf("ordinary rate limit was reported as exhausted quota: %v", err)
	}
	if !strings.Contains(err.Error(), `"code":"model_cooldown"`) ||
		!strings.Contains(err.Error(), `"model":"`+chatgptwebauth.ImageModel+`"`) {
		t.Fatalf("selection error = %v, want image model cooldown", err)
	}
	var headers interface{ Headers() http.Header }
	if !errors.As(err, &headers) {
		t.Fatalf("selection error = %T %v, want headers", err, err)
	}
	if retryAfter := strings.TrimSpace(headers.Headers().Get("Retry-After")); retryAfter == "" || retryAfter == "0" {
		t.Fatalf("Retry-After = %q, want active cooldown", retryAfter)
	}
	if retryAfter := retryAfterFromError(err); retryAfter == nil || *retryAfter <= 0 {
		t.Fatalf("RetryAfter() = %v, want active cooldown", retryAfter)
	}
}

func TestManagerOrdinaryImage429CooldownBlocksDirectImageButNotText(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.SetConfig(&internalconfig.Config{FixedErrorCooldowns: []internalconfig.FixedErrorCooldownRule{{
		StatusCode:      http.StatusTooManyRequests,
		CooldownSeconds: 3600,
		Scope:           cooldownScopeAuth,
	}}})
	executor := &authFallbackExecutor{id: chatgptwebauth.Provider}
	manager.RegisterExecutor(executor)
	textModel := "ordinary-image-text-" + uuid.NewString()
	highID := "ordinary-image-direct-high-" + uuid.NewString()
	lowID := "ordinary-image-direct-low-" + uuid.NewString()
	for _, auth := range []*Auth{
		{
			ID:         highID,
			Provider:   chatgptwebauth.Provider,
			Status:     StatusActive,
			Attributes: map[string]string{"priority": "10"},
			Metadata:   map[string]any{"lifecycle_state": LifecycleStateActive},
		},
		{
			ID:         lowID,
			Provider:   chatgptwebauth.Provider,
			Status:     StatusActive,
			Attributes: map[string]string{"priority": "0"},
			Metadata:   map[string]any{"lifecycle_state": LifecycleStateActive},
		},
	} {
		registerQuotaTestAuthForModels(t, manager, auth, textModel, chatgptwebauth.ImageModel)
	}

	retryAfter := 2 * time.Minute
	markOrdinaryChatGPTWebImageRateLimit(manager, highID, retryAfter)
	current, _ := manager.GetByID(highID)
	state := current.ModelStates[chatgptwebauth.ImageModel]
	if state == nil || state.Quota.Reason != "quota" {
		t.Fatalf("ordinary rate limit state = %+v", state)
	}
	if current.CooldownScope == cooldownScopeAuth {
		t.Fatalf("ordinary image rate limit leaked to auth cooldown: scope=%q until=%s", current.CooldownScope, current.NextRetryAfter)
	}
	if current.HasAccountMaintenanceQuotaExceeded() {
		t.Fatal("ordinary image rate limit became account-maintenance quota")
	}
	capability := chatGPTWebImageCapabilityStateForAuth(current, time.Now())
	if !capability.blocked || !capability.rateLimited || capability.confirmedExhausted {
		t.Fatalf("ordinary rate limit capability = %+v", capability)
	}

	textResponse, errText := manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: textModel},
		cliproxyexecutor.Options{},
	)
	if errText != nil {
		t.Fatalf("text Execute() error = %v", errText)
	}
	if string(textResponse.Payload) != highID {
		t.Fatalf("text selected auth = %q, want %q", string(textResponse.Payload), highID)
	}

	imageResponse, errImage := manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: chatgptwebauth.ImageModel},
		cliproxyexecutor.Options{},
	)
	if errImage != nil {
		t.Fatalf("image Execute() error = %v", errImage)
	}
	if string(imageResponse.Payload) != lowID {
		t.Fatalf("image selected auth = %q, want %q", string(imageResponse.Payload), lowID)
	}

	markOrdinaryChatGPTWebImageRateLimit(manager, lowID, retryAfter)
	_, errImage = manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: chatgptwebauth.ImageModel},
		cliproxyexecutor.Options{},
	)
	assertOrdinaryImageModelCooldown(t, errImage)
}

func TestManagerOrdinaryImage429CooldownBlocksImageTool(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	executor := &authFallbackExecutor{id: chatgptwebauth.Provider}
	manager.RegisterExecutor(executor)
	model := "ordinary-image-tool-" + uuid.NewString()
	highID := "ordinary-image-tool-high-" + uuid.NewString()
	lowID := "ordinary-image-tool-low-" + uuid.NewString()
	for _, auth := range []*Auth{
		{
			ID:         highID,
			Provider:   chatgptwebauth.Provider,
			Status:     StatusActive,
			Attributes: map[string]string{"priority": "10"},
			Metadata:   map[string]any{"lifecycle_state": LifecycleStateActive},
		},
		{
			ID:         lowID,
			Provider:   chatgptwebauth.Provider,
			Status:     StatusActive,
			Attributes: map[string]string{"priority": "0"},
			Metadata:   map[string]any{"lifecycle_state": LifecycleStateActive},
		},
	} {
		registerQuotaTestAuthForModels(t, manager, auth, model, chatgptwebauth.ImageModel)
	}

	retryAfter := 90 * time.Second
	markOrdinaryChatGPTWebImageRateLimit(manager, highID, retryAfter)
	response, errExecute := manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: model, Payload: imageToolFallbackPayload(model)},
		cliproxyexecutor.Options{},
	)
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if string(response.Payload) != lowID {
		t.Fatalf("selected auth = %q, want %q", string(response.Payload), lowID)
	}

	markOrdinaryChatGPTWebImageRateLimit(manager, lowID, retryAfter)
	_, errExecute = manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: model, Payload: imageToolFallbackPayload(model)},
		cliproxyexecutor.Options{},
	)
	assertOrdinaryImageModelCooldown(t, errExecute)
}

func TestManagerNonQuotaImageModelCooldownBlocksDirectAndImageToolButNotText(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(&authFallbackExecutor{id: chatgptwebauth.Provider})
	textModel := "non-quota-image-text-" + uuid.NewString()
	highID := "non-quota-image-high-" + uuid.NewString()
	lowID := "non-quota-image-low-" + uuid.NewString()
	for _, auth := range []*Auth{
		{
			ID:         highID,
			Provider:   chatgptwebauth.Provider,
			Status:     StatusActive,
			Attributes: map[string]string{"priority": "10"},
			Metadata:   map[string]any{"lifecycle_state": LifecycleStateActive},
		},
		{
			ID:         lowID,
			Provider:   chatgptwebauth.Provider,
			Status:     StatusActive,
			Attributes: map[string]string{"priority": "0"},
			Metadata:   map[string]any{"lifecycle_state": LifecycleStateActive},
		},
	} {
		registerQuotaTestAuthForModels(t, manager, auth, textModel, chatgptwebauth.ImageModel)
	}

	retryAfter := 2 * time.Minute
	manager.MarkResult(context.Background(), Result{
		AuthID:   highID,
		Provider: chatgptwebauth.Provider,
		Model:    chatgptwebauth.ImageModel,
		Error: &Error{
			Message:    "image upstream unavailable",
			HTTPStatus: http.StatusServiceUnavailable,
		},
		RetryAfter: &retryAfter,
	})
	current, _ := manager.GetByID(highID)
	capability := chatGPTWebImageCapabilityStateForAuth(current, time.Now())
	if !capability.blocked || !capability.modelCooldown ||
		capability.rateLimited || capability.confirmedExhausted {
		t.Fatalf("503 image capability = %+v", capability)
	}

	textResponse, errText := manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: textModel},
		cliproxyexecutor.Options{},
	)
	if errText != nil {
		t.Fatalf("text Execute() error = %v", errText)
	}
	if string(textResponse.Payload) != highID {
		t.Fatalf("text selected auth = %q, want %q", string(textResponse.Payload), highID)
	}

	for name, request := range map[string]cliproxyexecutor.Request{
		"direct": {Model: chatgptwebauth.ImageModel},
		"tool":   {Model: textModel, Payload: imageToolFallbackPayload(textModel)},
	} {
		t.Run(name, func(t *testing.T) {
			response, errExecute := manager.Execute(
				context.Background(),
				[]string{chatgptwebauth.Provider},
				request,
				cliproxyexecutor.Options{},
			)
			if errExecute != nil {
				t.Fatalf("Execute() error = %v", errExecute)
			}
			if string(response.Payload) != lowID {
				t.Fatalf("selected auth = %q, want %q", string(response.Payload), lowID)
			}
		})
	}

	manager.MarkResult(context.Background(), Result{
		AuthID:   lowID,
		Provider: chatgptwebauth.Provider,
		Model:    chatgptwebauth.ImageModel,
		Error: &Error{
			Code:       "model_not_supported",
			Message:    "image model not found",
			HTTPStatus: http.StatusNotFound,
		},
	})
	for name, request := range map[string]cliproxyexecutor.Request{
		"direct": {Model: chatgptwebauth.ImageModel},
		"tool":   {Model: textModel, Payload: imageToolFallbackPayload(textModel)},
	} {
		t.Run("all_cooling_"+name, func(t *testing.T) {
			_, errExecute := manager.Execute(
				context.Background(),
				[]string{chatgptwebauth.Provider},
				request,
				cliproxyexecutor.Options{},
			)
			assertOrdinaryImageModelCooldown(t, errExecute)
		})
	}
}

func TestManagerOrdinaryImage429CooldownFallsBackAcrossProviders(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(&authFallbackExecutor{id: chatgptwebauth.Provider})
	manager.RegisterExecutor(&authFallbackExecutor{id: "xai"})
	model := "ordinary-image-mixed-" + uuid.NewString()
	webID := "ordinary-image-mixed-web-" + uuid.NewString()
	xaiID := "ordinary-image-mixed-xai-" + uuid.NewString()
	registerQuotaTestAuthForModels(t, manager, &Auth{
		ID:         webID,
		Provider:   chatgptwebauth.Provider,
		Status:     StatusActive,
		Attributes: map[string]string{"priority": "10"},
		Metadata:   map[string]any{"lifecycle_state": LifecycleStateActive},
	}, model, chatgptwebauth.ImageModel)
	registerQuotaTestAuthForModels(t, manager, &Auth{
		ID:         xaiID,
		Provider:   "xai",
		Status:     StatusActive,
		Attributes: map[string]string{"priority": "0"},
		Metadata:   map[string]any{"lifecycle_state": LifecycleStateActive},
	}, model)
	markOrdinaryChatGPTWebImageRateLimit(manager, webID, 2*time.Minute)

	response, errExecute := manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider, "xai"},
		cliproxyexecutor.Request{Model: model, Payload: imageToolFallbackPayload(model)},
		cliproxyexecutor.Options{},
	)
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if string(response.Payload) != xaiID {
		t.Fatalf("selected auth = %q, want mixed-provider fallback %q", string(response.Payload), xaiID)
	}
}

func TestManagerImageToolQuotaFallsBackToCodexDisabledImagePolicy(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.SetConfig(&internalconfig.Config{
		DisabledImageGenerationToolFallback: true,
		AuthModelExclusions: []internalconfig.AuthModelExclusionRule{{
			Providers:              []string{"codex"},
			DisableImageGeneration: true,
		}},
	})
	manager.RegisterExecutor(&authFallbackExecutor{id: chatgptwebauth.Provider})
	manager.RegisterExecutor(&authFallbackExecutor{id: "codex"})
	model := "quota-disabled-policy-" + uuid.NewString()
	webID := "quota-disabled-policy-web-" + uuid.NewString()
	codexID := "quota-disabled-policy-codex-" + uuid.NewString()
	registerQuotaTestAuthForModels(t, manager, &Auth{
		ID:         webID,
		Provider:   chatgptwebauth.Provider,
		Status:     StatusActive,
		Attributes: map[string]string{"priority": "10"},
		Metadata: map[string]any{
			"lifecycle_state": LifecycleStateActive,
			"quota_state":     string(chatgptwebauth.QuotaStateExhausted),
		},
	}, model, chatgptwebauth.ImageModel)
	registerQuotaTestAuthForModels(t, manager, &Auth{
		ID:         codexID,
		Provider:   "codex",
		Status:     StatusActive,
		Attributes: map[string]string{"priority": "0"},
	}, model)

	response, errExecute := manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider, "codex"},
		cliproxyexecutor.Request{Model: model, Payload: imageToolFallbackPayload(model)},
		cliproxyexecutor.Options{},
	)
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if string(response.Payload) != codexID {
		t.Fatalf("selected auth = %q, want Codex policy fallback %q", string(response.Payload), codexID)
	}
}

func TestManagerExpiredOrdinaryImage429WaitsForFreshQuotaRecheck(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	executor := &chatGPTWebQuotaRefreshTriggerExecutor{
		authFallbackExecutor: &authFallbackExecutor{id: chatgptwebauth.Provider},
	}
	manager.RegisterExecutor(executor)
	now := time.Now().UTC()
	stateUpdatedAt := now.Add(-2 * time.Minute)
	retryAt := now.Add(-time.Minute)
	authID := "ordinary-image-recheck-" + uuid.NewString()
	registerQuotaTestAuthForModels(t, manager, &Auth{
		ID:       authID,
		Provider: chatgptwebauth.Provider,
		Status:   StatusActive,
		Metadata: map[string]any{
			"lifecycle_state": LifecycleStateActive,
			"quota_state":     string(chatgptwebauth.QuotaStateAvailable),
			"quota_stale":     false,
			"quota_updated_at": now.Add(-90 * time.Second).
				Format(time.RFC3339Nano),
		},
		ModelStates: map[string]*ModelState{
			chatgptwebauth.ImageModel: {
				Status:         StatusError,
				Unavailable:    true,
				UpdatedAt:      stateUpdatedAt,
				NextRetryAfter: retryAt,
				Quota: QuotaState{
					Exceeded:      true,
					Reason:        "quota",
					NextRecoverAt: retryAt,
				},
			},
		},
	}, chatgptwebauth.ImageModel)

	_, errExecute := manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: chatgptwebauth.ImageModel},
		cliproxyexecutor.Options{},
	)
	if errExecute == nil || strings.Contains(errExecute.Error(), `"code":"image_quota_exhausted"`) {
		t.Fatalf("expired ordinary cooldown error = %v", errExecute)
	}
	if !strings.Contains(errExecute.Error(), `"code":"model_cooldown"`) ||
		!strings.Contains(errExecute.Error(), `"model":"`+chatgptwebauth.ImageModel+`"`) {
		t.Fatalf("expired ordinary cooldown error = %v, want pending image model recheck", errExecute)
	}
	if !chatGPTWebImageQuotaRefreshPendingError(errExecute) {
		t.Fatalf("expired ordinary cooldown error = %T %v, want refresh pending", errExecute, errExecute)
	}
	var headers interface{ Headers() http.Header }
	if !errors.As(errExecute, &headers) {
		t.Fatalf("expired ordinary cooldown error = %T %v, want headers", errExecute, errExecute)
	}
	if retryAfter := strings.TrimSpace(headers.Headers().Get("Retry-After")); retryAfter != "" {
		t.Fatalf("Retry-After = %q, want empty while refresh is pending", retryAfter)
	}
	if retryAfter := retryAfterFromError(errExecute); retryAfter != nil {
		t.Fatalf("RetryAfter() = %v, want nil while refresh is pending", retryAfter)
	}
	if len(executor.authIDs) != 1 || executor.authIDs[0] != authID {
		t.Fatalf("refresh triggers = %v", executor.authIDs)
	}
	if calls := executor.ExecuteCalls(); len(calls) != 0 {
		t.Fatalf("expired cooldown executed before recheck: %v", calls)
	}

	current, _ := manager.GetByID(authID)
	imageState := current.ModelStates[chatgptwebauth.ImageModel]
	if imageState == nil || !imageState.Unavailable || !imageState.NextRetryAfter.Equal(retryAt) {
		t.Fatalf("pending recheck changed provider cooldown: %+v", imageState)
	}
	if state := executor.AccountInfoAuthState(authID); !state.Refreshing {
		t.Fatalf("account-info runtime state = %+v, want queued refresh", state)
	}
	if _, matched, errMutate := manager.MutateRuntimeMetadataIfCurrent(
		context.Background(),
		current,
		func(candidate *Auth) {
			candidate.Metadata["quota_updated_at"] = now.Format(time.RFC3339Nano)
			candidate.Metadata["quota_stale"] = true
		},
	); errMutate != nil || !matched {
		t.Fatalf("mutate stale quota result = matched %v, error %v", matched, errMutate)
	}
	_, _ = manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: chatgptwebauth.ImageModel},
		cliproxyexecutor.Options{},
	)
	if len(executor.authIDs) != 1 {
		t.Fatalf("pending recheck duplicated refresh triggers = %v", executor.authIDs)
	}
	if calls := executor.ExecuteCalls(); len(calls) != 0 {
		t.Fatalf("stale quota result executed image request: %v", calls)
	}

	executor.runtimeStates[authID] = chatgptwebauth.AccountInfoAuthRuntimeState{}
	_, _ = manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: chatgptwebauth.ImageModel},
		cliproxyexecutor.Options{},
	)
	if len(executor.authIDs) != 2 {
		t.Fatalf("expired pending recheck triggers = %v, want two", executor.authIDs)
	}

	current, _ = manager.GetByID(authID)
	if _, matched, errMutate := manager.MutateRuntimeMetadataIfCurrent(
		context.Background(),
		current,
		func(candidate *Auth) {
			candidate.Metadata["quota_updated_at"] = time.Now().UTC().Format(time.RFC3339Nano)
			candidate.Metadata["quota_stale"] = false
		},
	); errMutate != nil || !matched {
		t.Fatalf("mutate fresh quota result = matched %v, error %v", matched, errMutate)
	}
	response, errExecute := manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: chatgptwebauth.ImageModel},
		cliproxyexecutor.Options{},
	)
	if errExecute != nil {
		t.Fatalf("Execute() after fresh quota recheck error = %v", errExecute)
	}
	if string(response.Payload) != authID {
		t.Fatalf("selected auth after recheck = %q, want %q", string(response.Payload), authID)
	}
	if len(executor.authIDs) != 2 {
		t.Fatalf("fresh quota result triggered another refresh: %v", executor.authIDs)
	}
}

func TestAuthAccountMaintenanceQuotaExcludesOnlyChatGPTWebImageProjection(t *testing.T) {
	imageQuotaAuth := &Auth{
		Provider:      chatgptwebauth.Provider,
		CooldownScope: cooldownScopeModel,
		Quota: QuotaState{
			Exceeded:    true,
			StrikeCount: 1,
		},
		ModelStates: map[string]*ModelState{
			chatgptwebauth.ImageModel: {
				Quota: QuotaState{
					Exceeded:    true,
					Reason:      "quota",
					StrikeCount: 1,
				},
			},
		},
	}
	if imageQuotaAuth.HasAccountMaintenanceQuotaExceeded() {
		t.Fatal("ChatGPT Web image model quota was eligible for account maintenance")
	}

	otherProvider := imageQuotaAuth.Clone()
	otherProvider.Provider = "codex"
	if !otherProvider.HasAccountMaintenanceQuotaExceeded() {
		t.Fatal("other provider image quota lost account-maintenance eligibility")
	}

	authWide := imageQuotaAuth.Clone()
	authWide.CooldownScope = cooldownScopeAuth
	if !authWide.HasAccountMaintenanceQuotaExceeded() {
		t.Fatal("ChatGPT Web auth-wide quota lost account-maintenance eligibility")
	}

	textQuota := imageQuotaAuth.Clone()
	textQuota.ModelStates["gpt-5.6"] = &ModelState{
		Quota: QuotaState{
			Exceeded:    true,
			Reason:      "quota",
			StrikeCount: 1,
		},
	}
	if !textQuota.HasAccountMaintenanceQuotaExceeded() {
		t.Fatal("ChatGPT Web text model quota lost account-maintenance eligibility")
	}

	unknownAuthWide := &Auth{
		Provider: chatgptwebauth.Provider,
		Quota: QuotaState{
			Exceeded:    true,
			StrikeCount: 1,
		},
	}
	if !unknownAuthWide.HasAccountMaintenanceQuotaExceeded() {
		t.Fatal("quota without image-model provenance lost account-maintenance eligibility")
	}
}

func TestManagerAccountInfoRetryDefersRepeatedQuotaRechecks(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	executor := &chatGPTWebQuotaRefreshTriggerExecutor{
		authFallbackExecutor: &authFallbackExecutor{id: chatgptwebauth.Provider},
		runtimeStates:        make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
	}
	manager.RegisterExecutor(executor)
	now := time.Now().UTC()
	stateUpdatedAt := now.Add(-2 * time.Minute)
	cooldownAt := now.Add(-time.Minute)
	authID := "ordinary-image-runtime-retry-" + uuid.NewString()
	registerQuotaTestAuthForModels(t, manager, &Auth{
		ID:       authID,
		Provider: chatgptwebauth.Provider,
		Status:   StatusActive,
		Metadata: map[string]any{
			"lifecycle_state": LifecycleStateActive,
			"quota_state":     string(chatgptwebauth.QuotaStateAvailable),
			"quota_stale":     false,
			"quota_updated_at": now.Add(-90 * time.Second).
				Format(time.RFC3339Nano),
		},
		ModelStates: map[string]*ModelState{
			chatgptwebauth.ImageModel: {
				Status:         StatusError,
				Unavailable:    true,
				UpdatedAt:      stateUpdatedAt,
				NextRetryAfter: cooldownAt,
				Quota: QuotaState{
					Exceeded:      true,
					Reason:        "quota",
					NextRecoverAt: cooldownAt,
				},
			},
		},
	}, chatgptwebauth.ImageModel)

	_, _ = manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: chatgptwebauth.ImageModel},
		cliproxyexecutor.Options{},
	)
	if len(executor.authIDs) != 1 || executor.authIDs[0] != authID {
		t.Fatalf("initial refresh triggers = %v", executor.authIDs)
	}

	manager.mu.Lock()
	current := manager.auths[authID]
	if current == nil || current.ModelStates[chatgptwebauth.ImageModel] == nil {
		manager.mu.Unlock()
		t.Fatal("image cooldown state disappeared after initial recheck")
	}
	current.ModelStates[chatgptwebauth.ImageModel].NextRetryAfter = cooldownAt
	manager.mu.Unlock()
	manager.RefreshSchedulerEntry(authID)

	nextRefreshAt := now.Add(10 * time.Minute)
	executor.runtimeStates[authID] = chatgptwebauth.AccountInfoAuthRuntimeState{
		NextRefreshAt: nextRefreshAt,
		LastError:     "rate_limited",
	}
	for attempt := 0; attempt < 3; attempt++ {
		_, errExecute := manager.Execute(
			context.Background(),
			[]string{chatgptwebauth.Provider},
			cliproxyexecutor.Request{Model: chatgptwebauth.ImageModel},
			cliproxyexecutor.Options{},
		)
		assertOrdinaryImageModelCooldown(t, errExecute)
		retryAfter := retryAfterFromError(errExecute)
		if retryAfter == nil || *retryAfter < 9*time.Minute {
			t.Fatalf("attempt %d RetryAfter() = %v, want runtime retry near 10m", attempt, retryAfter)
		}
	}
	if len(executor.authIDs) != 1 {
		t.Fatalf("repeated blocked traffic advanced account-info retry: %v", executor.authIDs)
	}
	if state := executor.AccountInfoAuthState(authID); !state.NextRefreshAt.Equal(nextRefreshAt) {
		t.Fatalf("runtime NextRefreshAt = %v, want %v", state.NextRefreshAt, nextRefreshAt)
	}
	current, _ = manager.GetByID(authID)
	imageState := current.ModelStates[chatgptwebauth.ImageModel]
	if imageState == nil || !imageState.NextRetryAfter.Equal(cooldownAt) {
		t.Fatalf("runtime retry schedule changed provider cooldown: %+v", imageState)
	}
}

func TestManagerTriggersRefreshWhenExhaustedQuotaResetIsDue(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	executor := &chatGPTWebQuotaRefreshTriggerExecutor{
		authFallbackExecutor: &authFallbackExecutor{id: chatgptwebauth.Provider},
	}
	manager.RegisterExecutor(executor)

	authID := "quota-due-" + uuid.NewString()
	registerFallbackAuthForModel(t, manager, &Auth{
		ID:       authID,
		Provider: chatgptwebauth.Provider,
		Status:   StatusActive,
		Metadata: map[string]any{
			"lifecycle_state":      LifecycleStateActive,
			"quota_state":          string(chatgptwebauth.QuotaStateExhausted),
			"image_quota_reset_at": time.Now().Add(-time.Minute).Format(time.RFC3339Nano),
		},
	}, chatgptwebauth.ImageModel)

	_, errExecute := manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: chatgptwebauth.ImageModel},
		cliproxyexecutor.Options{},
	)
	if errExecute == nil || !strings.Contains(errExecute.Error(), `"code":"image_quota_exhausted"`) {
		t.Fatalf("Execute() error = %v, want image quota exhausted", errExecute)
	}
	if len(executor.authIDs) != 1 || executor.authIDs[0] != authID {
		t.Fatalf("refresh triggers = %v", executor.authIDs)
	}

	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("exhausted auth disappeared")
	}
	current.Metadata["image_quota_reset_at"] = time.Now().Add(time.Hour).Format(time.RFC3339Nano)
	if _, errUpdate := manager.Update(context.Background(), current); errUpdate != nil {
		t.Fatalf("update future reset: %v", errUpdate)
	}
	executor.authIDs = nil
	_, _ = manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: chatgptwebauth.ImageModel},
		cliproxyexecutor.Options{},
	)
	if len(executor.authIDs) != 0 {
		t.Fatalf("future quota reset triggered an eager refresh: %v", executor.authIDs)
	}
}

func TestManagerDueImageQuotaRefreshDoesNotExposeLaterResetRetryAfter(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	executor := &chatGPTWebQuotaRefreshTriggerExecutor{
		authFallbackExecutor: &authFallbackExecutor{id: chatgptwebauth.Provider},
	}
	manager.RegisterExecutor(executor)
	now := time.Now()
	for index, resetAt := range []time.Time{now.Add(-time.Minute), now.Add(time.Hour)} {
		registerFallbackAuthForModel(t, manager, &Auth{
			ID:       fmt.Sprintf("quota-reset-%d-%s", index, uuid.NewString()),
			Provider: chatgptwebauth.Provider,
			Status:   StatusActive,
			Metadata: map[string]any{
				"lifecycle_state":      LifecycleStateActive,
				"quota_state":          string(chatgptwebauth.QuotaStateExhausted),
				"image_quota_reset_at": resetAt.Format(time.RFC3339Nano),
			},
		}, chatgptwebauth.ImageModel)
	}

	_, errExecute := manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: chatgptwebauth.ImageModel},
		cliproxyexecutor.Options{},
	)
	if errExecute == nil {
		t.Fatal("Execute() error = nil, want image quota exhausted")
	}
	var headers interface{ Headers() http.Header }
	if errors.As(errExecute, &headers) && strings.TrimSpace(headers.Headers().Get("Retry-After")) != "" {
		t.Fatalf("due refresh exposed later Retry-After: %v", headers.Headers())
	}
	if len(executor.authIDs) != 1 {
		t.Fatalf("due refresh triggers = %v, want one", executor.authIDs)
	}
}

func TestManagerInFlightImageQuotaRefreshDoesNotExposeLaterResetRetryAfter(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	executor := &chatGPTWebQuotaRefreshTriggerExecutor{
		authFallbackExecutor: &authFallbackExecutor{id: chatgptwebauth.Provider},
		runtimeStates:        make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
	}
	manager.RegisterExecutor(executor)
	now := time.Now()
	refreshingAuthID := "quota-refreshing-" + uuid.NewString()
	for index, resetAt := range []time.Time{now.Add(-time.Minute), now.Add(time.Hour)} {
		authID := fmt.Sprintf("quota-refreshing-%d-%s", index, uuid.NewString())
		if index == 0 {
			authID = refreshingAuthID
		}
		registerFallbackAuthForModel(t, manager, &Auth{
			ID:       authID,
			Provider: chatgptwebauth.Provider,
			Status:   StatusActive,
			Metadata: map[string]any{
				"lifecycle_state":      LifecycleStateActive,
				"quota_state":          string(chatgptwebauth.QuotaStateExhausted),
				"image_quota_reset_at": resetAt.Format(time.RFC3339Nano),
			},
		}, chatgptwebauth.ImageModel)
	}
	executor.runtimeStates[refreshingAuthID] = chatgptwebauth.AccountInfoAuthRuntimeState{
		Refreshing: true,
	}

	_, errExecute := manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: chatgptwebauth.ImageModel},
		cliproxyexecutor.Options{},
	)
	if errExecute == nil {
		t.Fatal("Execute() error = nil, want image quota exhausted")
	}
	var headers interface{ Headers() http.Header }
	if errors.As(errExecute, &headers) && strings.TrimSpace(headers.Headers().Get("Retry-After")) != "" {
		t.Fatalf("in-flight refresh exposed later Retry-After: %v", headers.Headers())
	}
	if len(executor.authIDs) != 0 {
		t.Fatalf("in-flight refresh triggered duplicate work: %v", executor.authIDs)
	}
}

func TestCommittedImageQuotaRequestIsNotReplayedOnAnotherAuth(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(2, 0, 2)
	firstID := "committed-quota-first-" + uuid.NewString()
	secondID := "committed-quota-second-" + uuid.NewString()
	execErr := committedImageQuotaTestError{}
	executor := &authFallbackExecutor{
		id: chatgptwebauth.Provider,
		executeErrors: map[string]error{
			firstID:  execErr,
			secondID: execErr,
		},
	}
	manager.RegisterExecutor(executor)
	for _, authID := range []string{firstID, secondID} {
		registerFallbackAuthForModel(t, manager, &Auth{
			ID:       authID,
			Provider: chatgptwebauth.Provider,
			Status:   StatusActive,
			Metadata: map[string]any{"lifecycle_state": LifecycleStateActive},
		}, chatgptwebauth.ImageModel)
	}

	_, errExecute := manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: chatgptwebauth.ImageModel},
		cliproxyexecutor.Options{},
	)
	if errExecute == nil {
		t.Fatal("Execute() error = nil")
	}
	calls := executor.ExecuteCalls()
	if len(calls) != 1 {
		t.Fatalf("committed request execute calls = %v, want one", calls)
	}
	current, ok := manager.GetByID(calls[0])
	if !ok || current == nil {
		t.Fatalf("selected auth %q disappeared", calls[0])
	}
	state := current.ModelStates[chatgptwebauth.ImageModel]
	if state == nil || !state.Quota.Exceeded ||
		state.Quota.Reason != "chatgpt_web_image_quota" {
		t.Fatalf("committed quota result was not recorded: %+v", state)
	}
}

func TestAccountInfoCooldownClearSerializesConcurrentImageQuotaResult(t *testing.T) {
	store := &chatGPTWebQuotaMutationBlockingStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := NewManager(store, &FillFirstSelector{}, nil)
	authID := "quota-clear-race-" + uuid.NewString()
	auth := &Auth{
		ID:       authID,
		FileName: authID + ".json",
		Provider: chatgptwebauth.Provider,
		Status:   StatusError,
		Metadata: map[string]any{
			"lifecycle_state": LifecycleStateActive,
			"quota_state":     string(chatgptwebauth.QuotaStateExhausted),
		},
		ModelStates: map[string]*ModelState{
			chatgptwebauth.ImageModel: {
				Status:         StatusError,
				Unavailable:    true,
				NextRetryAfter: time.Now().Add(time.Minute),
				Quota: QuotaState{
					Exceeded:    true,
					Reason:      "chatgpt_web_image_quota",
					StrikeCount: 1,
				},
			},
		},
	}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	expected, _ := manager.GetByID(authID)
	mutationDone := make(chan error, 1)
	go func() {
		_, _, errMutate := manager.MutateRuntimeMetadataAndClearModelCooldownIfCurrent(
			context.Background(),
			expected,
			chatgptwebauth.ImageModel,
			"chatgpt_web_image_quota",
			func(candidate *Auth) {
				candidate.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateAvailable)
			},
		)
		mutationDone <- errMutate
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("metadata save did not start")
	}

	resultStarted := make(chan struct{})
	resultDone := make(chan struct{})
	go func() {
		close(resultStarted)
		retryAfter := 2 * time.Minute
		manager.MarkResult(context.Background(), Result{
			AuthID:   authID,
			Provider: chatgptwebauth.Provider,
			Model:    chatgptwebauth.ImageModel,
			Error: &Error{
				Code:       "chatgpt_web_image_quota",
				Message:    "new image quota result",
				HTTPStatus: http.StatusTooManyRequests,
			},
			RetryAfter: &retryAfter,
		})
		close(resultDone)
	}()
	<-resultStarted
	select {
	case <-resultDone:
		t.Fatal("concurrent result bypassed the auth mutation sequence")
	case <-time.After(50 * time.Millisecond):
	}
	current, _ := manager.GetByID(authID)
	state := current.ModelStates[chatgptwebauth.ImageModel]
	if state == nil || state.Quota.StrikeCount != 1 {
		t.Fatalf("result mutated auth while metadata persistence was blocked: %+v", state)
	}
	close(store.release)
	if errMutate := <-mutationDone; errMutate != nil {
		t.Fatalf("metadata mutation error = %v", errMutate)
	}
	select {
	case <-resultDone:
	case <-time.After(time.Second):
		t.Fatal("concurrent result persistence did not finish")
	}
	current, _ = manager.GetByID(authID)
	state = current.ModelStates[chatgptwebauth.ImageModel]
	if state == nil || !state.Unavailable || !state.Quota.Exceeded ||
		state.Quota.Reason != "chatgpt_web_image_quota" || state.Quota.StrikeCount != 1 {
		t.Fatalf("new image quota state was cleared: %+v", state)
	}
}

func TestAccountInfoCooldownClearRejectsStaleModelObservation(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	authID := "quota-clear-stale-observation-" + uuid.NewString()
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), &Auth{
		ID:       authID,
		Provider: chatgptwebauth.Provider,
		Status:   StatusActive,
		Metadata: map[string]any{
			"lifecycle_state": LifecycleStateActive,
			"quota_state":     string(chatgptwebauth.QuotaStateUnknown),
		},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	expected, _ := manager.GetByID(authID)
	retryAfter := 2 * time.Minute
	manager.MarkResult(context.Background(), Result{
		AuthID:   authID,
		Provider: chatgptwebauth.Provider,
		Model:    chatgptwebauth.ImageModel,
		Error: &Error{
			Code:       "chatgpt_web_image_quota",
			Message:    "new image quota result",
			HTTPStatus: http.StatusTooManyRequests,
		},
		RetryAfter: &retryAfter,
	})

	current, matched, errMutate := manager.MutateRuntimeMetadataAndClearModelCooldownIfCurrent(
		context.Background(),
		expected,
		chatgptwebauth.ImageModel,
		"chatgpt_web_image_quota",
		func(candidate *Auth) {
			candidate.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateAvailable)
		},
	)
	if errMutate != nil || !matched || current == nil {
		t.Fatalf("metadata mutation = current %v matched %v error %v", current, matched, errMutate)
	}
	imageState := current.ModelStates[chatgptwebauth.ImageModel]
	if imageState == nil || !imageState.Unavailable || !imageState.Quota.Exceeded ||
		imageState.Quota.Reason != "chatgpt_web_image_quota" {
		t.Fatalf("stale observation cleared newer image quota state: %+v", imageState)
	}
}

func TestClearModelCooldownByReasonCannotPublishOverConcurrentStrike(t *testing.T) {
	store := &chatGPTWebQuotaMutationBlockingStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := NewManager(store, &FillFirstSelector{}, nil)
	authID := "direct-quota-clear-race-" + uuid.NewString()
	model := "direct-quota-clear-model-" + uuid.NewString()
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(authID, chatgptwebauth.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() {
		reg.UnregisterClient(authID)
	})
	auth := &Auth{
		ID:       authID,
		FileName: authID + ".json",
		Provider: chatgptwebauth.Provider,
		Status:   StatusError,
		Metadata: map[string]any{"lifecycle_state": LifecycleStateActive},
		ModelStates: map[string]*ModelState{
			model: {
				Status:         StatusError,
				Unavailable:    true,
				NextRetryAfter: time.Now().Add(time.Minute),
				Quota: QuotaState{
					Exceeded:    true,
					Reason:      "chatgpt_web_image_quota",
					StrikeCount: 1,
				},
			},
		},
	}
	if _, errRegister := manager.Register(WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	reg.SetModelQuotaExceeded(authID, model)
	reg.SuspendClientModel(authID, model, "chatgpt_web_image_quota")

	clearDone := make(chan bool, 1)
	go func() {
		clearDone <- manager.ClearModelCooldownByReason(
			context.Background(),
			authID,
			model,
			"chatgpt_web_image_quota",
		)
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("cooldown clear persistence did not reach barrier")
	}

	resultStarted := make(chan struct{})
	resultDone := make(chan struct{})
	go func() {
		close(resultStarted)
		retryAfter := 2 * time.Minute
		manager.MarkResult(context.Background(), Result{
			AuthID:   authID,
			Provider: chatgptwebauth.Provider,
			Model:    model,
			Error: &Error{
				Code:       "chatgpt_web_image_quota",
				Message:    "new image quota strike",
				HTTPStatus: http.StatusTooManyRequests,
			},
			RetryAfter: &retryAfter,
		})
		close(resultDone)
	}()
	<-resultStarted
	select {
	case <-resultDone:
		t.Fatal("concurrent MarkResult bypassed cooldown clear persistence")
	case <-time.After(50 * time.Millisecond):
	}
	current, _ := manager.GetByID(authID)
	state := current.ModelStates[model]
	if state == nil || !modelStateIsClean(state) {
		t.Fatalf("concurrent strike mutated the clearing auth before publication: %+v", state)
	}

	close(store.release)
	select {
	case cleared := <-clearDone:
		if !cleared {
			t.Fatal("ClearModelCooldownByReason() = false")
		}
	case <-time.After(time.Second):
		t.Fatal("cooldown clear did not finish")
	}
	select {
	case <-resultDone:
	case <-time.After(time.Second):
		t.Fatal("concurrent MarkResult did not finish")
	}

	current, _ = manager.GetByID(authID)
	state = current.ModelStates[model]
	if state == nil || !state.Unavailable || !state.Quota.Exceeded ||
		state.Quota.Reason != "chatgpt_web_image_quota" || state.Quota.StrikeCount != 1 {
		t.Fatalf("new quota strike was overwritten: %+v", state)
	}
	if count := reg.GetModelCount(model); count != 0 {
		t.Fatalf("registry model count = %d, want concurrent strike to remain published", count)
	}
	selected, errPick := manager.scheduler.pickSingle(
		context.Background(),
		chatgptwebauth.Provider,
		model,
		cliproxyexecutor.Options{},
		nil,
	)
	if errPick == nil || selected != nil {
		t.Fatalf("scheduler selected auth after concurrent strike: auth=%+v error=%v", selected, errPick)
	}
}

func TestManagerImageToolSkipsExhaustedChatGPTWebWithoutFallbackSwitch(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	executor := &authFallbackExecutor{id: chatgptwebauth.Provider}
	manager.RegisterExecutor(executor)
	model := "quota-tool-" + uuid.NewString()
	highID := "quota-tool-high-" + uuid.NewString()
	lowID := "quota-tool-low-" + uuid.NewString()

	for _, auth := range []*Auth{
		{
			ID:         highID,
			Provider:   chatgptwebauth.Provider,
			Status:     StatusActive,
			Attributes: map[string]string{"priority": "10"},
			Metadata: map[string]any{
				"lifecycle_state": LifecycleStateActive,
				"quota_state":     string(chatgptwebauth.QuotaStateExhausted),
			},
		},
		{
			ID:         lowID,
			Provider:   chatgptwebauth.Provider,
			Status:     StatusActive,
			Attributes: map[string]string{"priority": "0"},
			Metadata: map[string]any{
				"lifecycle_state": LifecycleStateActive,
				"quota_state":     string(chatgptwebauth.QuotaStateAvailable),
			},
		},
	} {
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register auth %s: %v", auth.ID, errRegister)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{
			{ID: model},
			{ID: chatgptwebauth.ImageModel},
		})
		authID := auth.ID
		t.Cleanup(func() {
			registry.GetGlobalRegistry().UnregisterClient(authID)
		})
	}

	response, errExecute := manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: model, Payload: imageToolFallbackPayload(model)},
		cliproxyexecutor.Options{},
	)
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if string(response.Payload) != lowID {
		t.Fatalf("selected auth = %q, want %q", string(response.Payload), lowID)
	}
}

func TestManagerImageToolSkipsChatGPTWebImageQuotaModelCooldown(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	executor := &authFallbackExecutor{id: chatgptwebauth.Provider}
	manager.RegisterExecutor(executor)
	model := "quota-tool-model-state-" + uuid.NewString()
	highID := "quota-tool-model-state-high-" + uuid.NewString()
	lowID := "quota-tool-model-state-low-" + uuid.NewString()

	for _, auth := range []*Auth{
		{
			ID:         highID,
			Provider:   chatgptwebauth.Provider,
			Status:     StatusActive,
			Attributes: map[string]string{"priority": "10"},
			Metadata: map[string]any{
				"lifecycle_state": LifecycleStateActive,
				"quota_state":     string(chatgptwebauth.QuotaStateAvailable),
			},
			ModelStates: map[string]*ModelState{
				chatgptwebauth.ImageModel: {
					Status:         StatusError,
					Unavailable:    true,
					NextRetryAfter: time.Now().Add(time.Minute),
					Quota:          QuotaState{Exceeded: true, Reason: "chatgpt_web_image_quota"},
				},
			},
		},
		{
			ID:         lowID,
			Provider:   chatgptwebauth.Provider,
			Status:     StatusActive,
			Attributes: map[string]string{"priority": "0"},
			Metadata: map[string]any{
				"lifecycle_state": LifecycleStateActive,
				"quota_state":     string(chatgptwebauth.QuotaStateAvailable),
			},
		},
	} {
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("register auth %s: %v", auth.ID, errRegister)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{
			{ID: model},
			{ID: chatgptwebauth.ImageModel},
		})
		authID := auth.ID
		t.Cleanup(func() {
			registry.GetGlobalRegistry().UnregisterClient(authID)
		})
	}

	response, errExecute := manager.Execute(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		cliproxyexecutor.Request{Model: model, Payload: imageToolFallbackPayload(model)},
		cliproxyexecutor.Options{},
	)
	if errExecute != nil {
		t.Fatalf("Execute() error = %v", errExecute)
	}
	if string(response.Payload) != lowID {
		t.Fatalf("selected auth = %q, want %q", string(response.Payload), lowID)
	}
}

type chatGPTWebQuotaProjectionTestError struct{}

func (chatGPTWebQuotaProjectionTestError) Error() string   { return "image quota exhausted" }
func (chatGPTWebQuotaProjectionTestError) StatusCode() int { return http.StatusTooManyRequests }
func (chatGPTWebQuotaProjectionTestError) ExecutionResultModel() string {
	return chatgptwebauth.ImageModel
}
func (chatGPTWebQuotaProjectionTestError) ExecutionResultErrorCode() string {
	return "chatgpt_web_image_quota"
}

type chatGPTWebQuotaStreamTestExecutor struct {
	bootstrap bool
}

func (*chatGPTWebQuotaStreamTestExecutor) Identifier() string {
	return chatgptwebauth.Provider
}

func (*chatGPTWebQuotaStreamTestExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (executor *chatGPTWebQuotaStreamTestExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	if executor.bootstrap {
		return nil, chatGPTWebQuotaProjectionTestError{}
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, 2)
	chunks <- cliproxyexecutor.BootstrapCommitStreamChunk()
	chunks <- cliproxyexecutor.StreamChunk{Err: chatGPTWebQuotaProjectionTestError{}}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (*chatGPTWebQuotaStreamTestExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (*chatGPTWebQuotaStreamTestExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (*chatGPTWebQuotaStreamTestExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func TestChatGPTWebImageCapabilityUsesLatestFutureRecoveryTime(t *testing.T) {
	now := time.Now().UTC()
	retryAt := now.Add(10 * time.Minute)
	recoverAt := now.Add(20 * time.Minute)
	auth := &Auth{
		Provider: chatgptwebauth.Provider,
		Metadata: map[string]any{
			"quota_state":          string(chatgptwebauth.QuotaStateExhausted),
			"image_quota_reset_at": now.Add(-time.Minute).Format(time.RFC3339Nano),
		},
		ModelStates: map[string]*ModelState{
			chatgptwebauth.ImageModel: {
				Unavailable:    true,
				NextRetryAfter: retryAt,
				Quota: QuotaState{
					Exceeded:      true,
					Reason:        "chatgpt_web_image_quota",
					NextRecoverAt: recoverAt,
				},
			},
		},
	}

	unavailable, next := chatGPTWebImageCapabilityUnavailable(auth, now)
	if !unavailable || !next.Equal(recoverAt) {
		t.Fatalf("capability unavailable=%v next=%v, want %v", unavailable, next, recoverAt)
	}
	blocked, blockNext := chatGPTWebImageQuotaBlocksModel(auth, chatgptwebauth.ImageModel, now)
	if !blocked || !blockNext.Equal(recoverAt) {
		t.Fatalf("model blocked=%v next=%v, want %v", blocked, blockNext, recoverAt)
	}
}

func TestChatGPTWebImageCapabilityMergesNonQuotaModelCooldownRecovery(t *testing.T) {
	now := time.Now().UTC()
	resetAt := now.Add(10 * time.Minute)
	retryAt := now.Add(20 * time.Minute)
	auth := &Auth{
		Provider: chatgptwebauth.Provider,
		Metadata: map[string]any{
			"quota_state":          string(chatgptwebauth.QuotaStateExhausted),
			"image_quota_reset_at": resetAt.Format(time.RFC3339Nano),
		},
		ModelStates: map[string]*ModelState{
			chatgptwebauth.ImageModel: {
				Unavailable:    true,
				NextRetryAfter: retryAt,
				Quota: QuotaState{
					Exceeded: true,
					Reason:   "fixed_error",
				},
			},
		},
	}

	unavailable, next := chatGPTWebImageCapabilityUnavailable(auth, now)
	if !unavailable || !next.Equal(retryAt) {
		t.Fatalf("capability unavailable=%v next=%v, want %v", unavailable, next, retryAt)
	}
}

func TestChatGPTWebImageQuotaReasonRemainsConfirmedExhausted(t *testing.T) {
	now := time.Now().UTC()
	auth := &Auth{
		Provider: chatgptwebauth.Provider,
		Metadata: map[string]any{
			"quota_state":      string(chatgptwebauth.QuotaStateAvailable),
			"quota_stale":      false,
			"quota_updated_at": now.Format(time.RFC3339Nano),
		},
		ModelStates: map[string]*ModelState{
			chatgptwebauth.ImageModel: {
				Unavailable:    false,
				UpdatedAt:      now.Add(-2 * time.Minute),
				NextRetryAfter: now.Add(-time.Minute),
				Quota: QuotaState{
					Exceeded:      true,
					Reason:        "chatgpt_web_image_quota",
					NextRecoverAt: now.Add(-time.Minute),
				},
			},
		},
	}

	capability := chatGPTWebImageCapabilityStateForAuth(auth, now)
	if !capability.blocked || !capability.confirmedExhausted || capability.rateLimited {
		t.Fatalf("confirmed quota capability = %+v", capability)
	}
	if !capability.refreshDue {
		t.Fatalf("expired quota-owned state did not request a remote recheck: %+v", capability)
	}
	if blocked, _ := chatGPTWebImageQuotaBlocksModel(auth, chatgptwebauth.ImageModel, now); !blocked {
		t.Fatal("quota-owned model state was ignored after aggregate availability expired")
	}
	if blocked, _ := chatGPTWebImageQuotaBlocksModel(auth, "gpt-5.6", now); blocked {
		t.Fatal("quota-owned image state blocked a text model")
	}
}

func TestChatGPTWebExpiredOrdinaryImageQuotaRequiresRemoteRecheckAfterAggregateRecovery(t *testing.T) {
	now := time.Now().UTC()
	auth := &Auth{
		Provider: chatgptwebauth.Provider,
		Metadata: map[string]any{
			"quota_state": string(chatgptwebauth.QuotaStateUnknown),
		},
		ModelStates: map[string]*ModelState{
			chatgptwebauth.ImageModel: {
				Unavailable:    false,
				UpdatedAt:      now.Add(-2 * time.Minute),
				NextRetryAfter: now.Add(-time.Minute),
				Quota: QuotaState{
					Exceeded:      true,
					Reason:        "quota",
					NextRecoverAt: now.Add(-time.Minute),
				},
			},
		},
	}

	capability := chatGPTWebImageCapabilityStateForAuth(auth, now)
	if !capability.blocked || !capability.rateLimited || !capability.refreshDue {
		t.Fatalf("expired ordinary quota capability = %+v, want blocked rate-limited recheck", capability)
	}
	if capability.confirmedExhausted {
		t.Fatalf("ordinary quota was treated as confirmed account-info exhaustion: %+v", capability)
	}
}

func TestManagerRouteAwareOrdinaryImageCooldown(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.RegisterExecutor(&authFallbackExecutor{id: chatgptwebauth.Provider})
	now := time.Now()
	retryAt := now.Add(2 * time.Minute)
	routeModel := "team/" + chatgptwebauth.ImageModel
	highID := "ordinary-quota-prefix-high-" + uuid.NewString()
	lowID := "ordinary-quota-prefix-low-" + uuid.NewString()
	registerQuotaTestAuthForModels(t, manager, &Auth{
		ID:         highID,
		Provider:   chatgptwebauth.Provider,
		Prefix:     "team",
		Status:     StatusActive,
		Attributes: map[string]string{"priority": "10"},
		Metadata:   map[string]any{"lifecycle_state": LifecycleStateActive},
		ModelStates: map[string]*ModelState{
			chatgptwebauth.ImageModel: {
				Status:         StatusError,
				Unavailable:    true,
				UpdatedAt:      now,
				NextRetryAfter: retryAt,
				Quota: QuotaState{
					Exceeded:      true,
					Reason:        "quota",
					NextRecoverAt: retryAt,
				},
			},
		},
	}, chatgptwebauth.ImageModel)
	registerQuotaTestAuthForModels(t, manager, &Auth{
		ID:         lowID,
		Provider:   chatgptwebauth.Provider,
		Prefix:     "team",
		Status:     StatusActive,
		Attributes: map[string]string{"priority": "0"},
		Metadata:   map[string]any{"lifecycle_state": LifecycleStateActive},
	}, chatgptwebauth.ImageModel)

	selected, _, errPick := manager.pickNext(
		context.Background(),
		chatgptwebauth.Provider,
		routeModel,
		cliproxyexecutor.Options{},
		nil,
	)
	if errPick != nil {
		t.Fatalf("pickNext() error = %v", errPick)
	}
	if selected == nil || selected.ID != lowID {
		t.Fatalf("selected auth = %+v, want %q", selected, lowID)
	}

	current, _ := manager.GetByID(lowID)
	current.ModelStates = map[string]*ModelState{
		chatgptwebauth.ImageModel: {
			Status:         StatusError,
			Unavailable:    true,
			UpdatedAt:      now,
			NextRetryAfter: retryAt,
			Quota: QuotaState{
				Exceeded:      true,
				Reason:        "quota",
				NextRecoverAt: retryAt,
			},
		},
	}
	if _, errUpdate := manager.Update(context.Background(), current); errUpdate != nil {
		t.Fatalf("update low auth cooldown: %v", errUpdate)
	}
	_, _, errPick = manager.pickNext(
		context.Background(),
		chatgptwebauth.Provider,
		routeModel,
		cliproxyexecutor.Options{},
		nil,
	)
	assertOrdinaryImageModelCooldown(t, errPick)
}

func TestManagerRouteAwareImageQuotaReturnsDedicatedError(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.RegisterExecutor(&authFallbackExecutor{id: chatgptwebauth.Provider})
	authID := "quota-prefix-" + uuid.NewString()
	routeModel := "team/" + chatgptwebauth.ImageModel
	registered, errRegister := manager.Register(context.Background(), &Auth{
		ID:       authID,
		Provider: chatgptwebauth.Provider,
		Prefix:   "team",
		Status:   StatusActive,
		Metadata: map[string]any{
			"lifecycle_state":      LifecycleStateActive,
			"quota_state":          string(chatgptwebauth.QuotaStateExhausted),
			"image_quota_reset_at": time.Now().Add(time.Minute).Format(time.RFC3339Nano),
		},
	})
	if errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, chatgptwebauth.Provider, []*registry.ModelInfo{
		{ID: chatgptwebauth.ImageModel},
	})
	manager.RefreshSchedulerEntry(authID)
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(authID)
	})
	if registered == nil {
		t.Fatal("registered auth = nil")
	}

	_, _, errPick := manager.pickNext(
		context.Background(),
		chatgptwebauth.Provider,
		routeModel,
		cliproxyexecutor.Options{},
		nil,
	)
	assertChatGPTWebImageQuotaError(t, errPick)

	_, _, _, errPickMixed := manager.pickNextMixed(
		context.Background(),
		[]string{chatgptwebauth.Provider},
		routeModel,
		cliproxyexecutor.Options{},
		nil,
	)
	assertChatGPTWebImageQuotaError(t, errPickMixed)
}

func TestManagerImageToolQuotaPreservesOtherProviderModelCooldown(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	now := time.Now()
	model := "mixed-image-tool-" + uuid.NewString()
	registerFallbackAuthForModel(t, manager, &Auth{
		ID:       "mixed-web-exhausted-" + uuid.NewString(),
		Provider: chatgptwebauth.Provider,
		Status:   StatusActive,
		Metadata: map[string]any{
			"lifecycle_state":      LifecycleStateActive,
			"quota_state":          string(chatgptwebauth.QuotaStateExhausted),
			"image_quota_reset_at": now.Add(time.Hour).Format(time.RFC3339Nano),
		},
	}, model)
	registerFallbackAuthForModel(t, manager, &Auth{
		ID:       "mixed-xai-cooldown-" + uuid.NewString(),
		Provider: "xai",
		Status:   StatusActive,
		ModelStates: map[string]*ModelState{
			model: {
				Status:         StatusError,
				Unavailable:    true,
				NextRetryAfter: now.Add(2 * time.Minute),
			},
		},
	}, model)

	original := newModelCooldownErrorUntil(model, "mixed", now.Add(2*time.Minute), now)
	got := manager.preferChatGPTWebImageToolQuotaError(
		original,
		[]string{chatgptwebauth.Provider, "xai"},
		model,
		cliproxyexecutor.Options{},
		nil,
		nil,
	)
	if got != original {
		t.Fatalf("mixed provider error = %T %v, want original cooldown %v", got, got, original)
	}
}

func TestManagerInFlightWebQuotaRefreshSuppressesOtherProviderRetryAfter(t *testing.T) {
	for _, imageTool := range []bool{false, true} {
		name := "direct image"
		model := chatgptwebauth.ImageModel
		if imageTool {
			name = "image tool"
			model = "mixed-image-tool-refresh-" + uuid.NewString()
		}
		t.Run(name, func(t *testing.T) {
			manager := NewManager(nil, &FillFirstSelector{}, nil)
			now := time.Now()
			webID := "mixed-web-refreshing-" + uuid.NewString()
			executor := &chatGPTWebQuotaRefreshTriggerExecutor{
				authFallbackExecutor: &authFallbackExecutor{id: chatgptwebauth.Provider},
				runtimeStates: map[string]chatgptwebauth.AccountInfoAuthRuntimeState{
					webID: {Refreshing: true},
				},
			}
			manager.RegisterExecutor(executor)
			registerFallbackAuthForModel(t, manager, &Auth{
				ID:       webID,
				Provider: chatgptwebauth.Provider,
				Status:   StatusActive,
				Metadata: map[string]any{
					"lifecycle_state":      LifecycleStateActive,
					"quota_state":          string(chatgptwebauth.QuotaStateExhausted),
					"image_quota_reset_at": now.Add(-time.Minute).Format(time.RFC3339Nano),
				},
			}, model)
			registerFallbackAuthForModel(t, manager, &Auth{
				ID:       "mixed-xai-refresh-cooldown-" + uuid.NewString(),
				Provider: "xai",
				Status:   StatusActive,
				ModelStates: map[string]*ModelState{
					model: {
						Status:         StatusError,
						Unavailable:    true,
						NextRetryAfter: now.Add(time.Hour),
					},
				},
			}, model)

			original := newModelCooldownErrorUntil(model, "mixed", now.Add(time.Hour), now)
			var got error
			if imageTool {
				got = manager.preferChatGPTWebImageToolQuotaError(
					original,
					[]string{chatgptwebauth.Provider, "xai"},
					model,
					cliproxyexecutor.Options{},
					nil,
					nil,
				)
			} else {
				got = manager.preferChatGPTWebImageQuotaError(
					original,
					[]string{chatgptwebauth.Provider, "xai"},
					model,
					cliproxyexecutor.Options{},
					nil,
					nil,
				)
			}
			if got == original {
				t.Fatal("in-flight Web refresh preserved another provider's long cooldown")
			}
			var headers interface{ Headers() http.Header }
			if errors.As(got, &headers) && strings.TrimSpace(headers.Headers().Get("Retry-After")) != "" {
				t.Fatalf("in-flight Web refresh exposed Retry-After: %v", headers.Headers())
			}
			if retryAfter, ok := availabilityBlockerResetIn(got); ok {
				t.Fatalf("availabilityBlockerResetIn() = %v, true, want no published reset", retryAfter)
			}
		})
	}
}

func TestManagerInFlightWebQuotaRefreshOverridesPriorProviderRetryAfter(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, imageTool := range []bool{false, true} {
			name := "execute/direct image"
			if stream {
				name = "stream/direct image"
			}
			if imageTool {
				name = "execute/image tool"
				if stream {
					name = "stream/image tool"
				}
			}
			t.Run(name, func(t *testing.T) {
				manager := NewManager(nil, &FillFirstSelector{}, nil)
				manager.SetRetryConfig(0, 0, 0)
				now := time.Now()
				model := chatgptwebauth.ImageModel
				request := cliproxyexecutor.Request{Model: model}
				if imageTool {
					model = "mixed-image-tool-round-" + uuid.NewString()
					request = cliproxyexecutor.Request{
						Model:   model,
						Payload: imageToolFallbackPayload(model),
					}
				}

				xaiID := "a-mixed-xai-round-" + uuid.NewString()
				xaiErr := &retryAfterStatusError{
					status:     http.StatusTooManyRequests,
					message:    "xai rate limited",
					retryAfter: time.Hour,
				}
				xaiExecutor := &chatGPTWebRoundErrorTestExecutor{
					id:  "xai",
					err: xaiErr,
				}
				manager.RegisterExecutor(xaiExecutor)
				registerFallbackAuthForModel(t, manager, &Auth{
					ID:         xaiID,
					Provider:   "xai",
					Status:     StatusActive,
					Attributes: map[string]string{"priority": "0"},
				}, model)

				webID := "z-mixed-web-round-" + uuid.NewString()
				webExecutor := &chatGPTWebQuotaRefreshTriggerExecutor{
					authFallbackExecutor: &authFallbackExecutor{id: chatgptwebauth.Provider},
					runtimeStates: map[string]chatgptwebauth.AccountInfoAuthRuntimeState{
						webID: {Refreshing: true},
					},
				}
				manager.RegisterExecutor(webExecutor)
				registerFallbackAuthForModel(t, manager, &Auth{
					ID:         webID,
					Provider:   chatgptwebauth.Provider,
					Status:     StatusActive,
					Attributes: map[string]string{"priority": "0"},
					Metadata: map[string]any{
						"lifecycle_state":      LifecycleStateActive,
						"quota_state":          string(chatgptwebauth.QuotaStateExhausted),
						"image_quota_reset_at": now.Add(-time.Minute).Format(time.RFC3339Nano),
					},
				}, model)
				runtimeReader := manager.chatGPTWebAccountInfoRuntimeStateReader()
				if runtimeReader == nil || !runtimeReader.AccountInfoAuthState(webID).Refreshing {
					t.Fatal("Web quota refresh runtime state is not visible")
				}

				var errExecute error
				if stream {
					_, errExecute = manager.ExecuteStream(
						context.Background(),
						[]string{"xai", chatgptwebauth.Provider},
						request,
						cliproxyexecutor.Options{},
					)
				} else {
					_, errExecute = manager.Execute(
						context.Background(),
						[]string{"xai", chatgptwebauth.Provider},
						request,
						cliproxyexecutor.Options{},
					)
				}
				if errExecute == nil {
					t.Fatal("execution error = nil")
				}
				if strings.Contains(errExecute.Error(), xaiErr.message) {
					t.Fatalf("execution returned prior provider error: %v", errExecute)
				}
				var headers interface{ Headers() http.Header }
				if errors.As(errExecute, &headers) &&
					strings.TrimSpace(headers.Headers().Get("Retry-After")) != "" {
					t.Fatalf("execution exposed prior provider Retry-After: %v", headers.Headers())
				}
				if calls := xaiExecutor.calls(stream); len(calls) != 1 || calls[0] != xaiID {
					t.Fatalf("xai calls = %v, want [%s]", calls, xaiID)
				}
			})
		}
	}
}

func TestManagerAccountInfoRefreshDoesNotOverrideOrdinaryImageModelCooldown(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, imageTool := range []bool{false, true} {
			name := "execute/direct image"
			if stream {
				name = "stream/direct image"
			}
			if imageTool {
				name = "execute/image tool"
				if stream {
					name = "stream/image tool"
				}
			}
			t.Run(name, func(t *testing.T) {
				manager := NewManager(nil, &FillFirstSelector{}, nil)
				manager.SetRetryConfig(0, 0, 0)
				now := time.Now()
				model := chatgptwebauth.ImageModel
				request := cliproxyexecutor.Request{Model: model}
				if imageTool {
					model = "mixed-image-tool-ordinary-cooldown-" + uuid.NewString()
					request = cliproxyexecutor.Request{
						Model:   model,
						Payload: imageToolFallbackPayload(model),
					}
				}

				xaiID := "a-mixed-xai-ordinary-cooldown-" + uuid.NewString()
				xaiErr := &retryAfterStatusError{
					status:     http.StatusTooManyRequests,
					message:    "xai temporarily unavailable",
					retryAfter: time.Minute,
				}
				xaiExecutor := &chatGPTWebRoundErrorTestExecutor{id: "xai", err: xaiErr}
				manager.RegisterExecutor(xaiExecutor)
				registerFallbackAuthForModel(t, manager, &Auth{
					ID:         xaiID,
					Provider:   "xai",
					Status:     StatusActive,
					Attributes: map[string]string{"priority": "0"},
				}, model)

				webID := "z-mixed-web-ordinary-cooldown-" + uuid.NewString()
				webExecutor := &chatGPTWebQuotaRefreshTriggerExecutor{
					authFallbackExecutor: &authFallbackExecutor{id: chatgptwebauth.Provider},
					runtimeStates: map[string]chatgptwebauth.AccountInfoAuthRuntimeState{
						webID: {Refreshing: true, NextRefreshAt: now.Add(time.Hour)},
					},
				}
				manager.RegisterExecutor(webExecutor)
				registerFallbackAuthForModel(t, manager, &Auth{
					ID:         webID,
					Provider:   chatgptwebauth.Provider,
					Status:     StatusActive,
					Attributes: map[string]string{"priority": "0"},
					Metadata: map[string]any{
						"lifecycle_state": LifecycleStateActive,
						"quota_state":     string(chatgptwebauth.QuotaStateAvailable),
					},
					ModelStates: map[string]*ModelState{
						chatgptwebauth.ImageModel: {
							Status:         StatusError,
							Unavailable:    true,
							NextRetryAfter: now.Add(5 * time.Minute),
							LastError: &Error{
								Message:    "ordinary image upstream failure",
								HTTPStatus: http.StatusServiceUnavailable,
							},
						},
					},
				}, model)

				var errExecute error
				if stream {
					_, errExecute = manager.ExecuteStream(
						context.Background(),
						[]string{"xai", chatgptwebauth.Provider},
						request,
						cliproxyexecutor.Options{},
					)
				} else {
					_, errExecute = manager.Execute(
						context.Background(),
						[]string{"xai", chatgptwebauth.Provider},
						request,
						cliproxyexecutor.Options{},
					)
				}
				if errExecute == nil || !strings.Contains(errExecute.Error(), xaiErr.message) {
					t.Fatalf("execution error = %T %v, want prior xai error", errExecute, errExecute)
				}
				if retryAfter := retryAfterFromError(errExecute); retryAfter == nil || *retryAfter != time.Minute {
					t.Fatalf("execution RetryAfter() = %v, want 1m", retryAfter)
				}
				if calls := xaiExecutor.calls(stream); len(calls) != 1 || calls[0] != xaiID {
					t.Fatalf("xai calls = %v, want [%s]", calls, xaiID)
				}
			})
		}
	}
}

func TestManagerOrdinaryImageModelCooldownWaitsWithinMaxRetryInterval(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, imageTool := range []bool{false, true} {
			name := "execute/direct image"
			if stream {
				name = "stream/direct image"
			}
			if imageTool {
				name = "execute/image tool"
				if stream {
					name = "stream/image tool"
				}
			}
			t.Run(name, func(t *testing.T) {
				manager := NewManager(nil, &FillFirstSelector{}, nil)
				manager.SetRetryConfig(0, 500*time.Millisecond, 0)
				executor := &chatGPTWebQuotaRefreshTriggerExecutor{
					authFallbackExecutor: &authFallbackExecutor{id: chatgptwebauth.Provider},
					runtimeStates:        make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
				}
				manager.RegisterExecutor(executor)
				model := chatgptwebauth.ImageModel
				request := cliproxyexecutor.Request{Model: model}
				if imageTool {
					model = "image-tool-wait-" + uuid.NewString()
					request = cliproxyexecutor.Request{
						Model:   model,
						Payload: imageToolFallbackPayload(model),
					}
				}
				authID := "web-image-wait-" + uuid.NewString()
				cooldownAt := time.Now().Add(100 * time.Millisecond)
				executor.runtimeStates[authID] = chatgptwebauth.AccountInfoAuthRuntimeState{
					Refreshing:    true,
					NextRefreshAt: time.Now().Add(time.Hour),
				}
				registerFallbackAuthForModel(t, manager, &Auth{
					ID:       authID,
					Provider: chatgptwebauth.Provider,
					Status:   StatusActive,
					Metadata: map[string]any{
						"lifecycle_state": LifecycleStateActive,
						"quota_state":     string(chatgptwebauth.QuotaStateAvailable),
					},
					ModelStates: map[string]*ModelState{
						chatgptwebauth.ImageModel: {
							Status:         StatusError,
							Unavailable:    true,
							NextRetryAfter: cooldownAt,
							LastError: &Error{
								Message:    "ordinary image cooldown",
								HTTPStatus: http.StatusServiceUnavailable,
							},
							Quota: QuotaState{
								Exceeded:      true,
								Reason:        "quota",
								NextRecoverAt: cooldownAt,
							},
						},
					},
				}, model)

				startedAt := time.Now()
				var errExecute error
				if stream {
					_, errExecute = manager.ExecuteStream(
						context.Background(),
						[]string{chatgptwebauth.Provider},
						request,
						cliproxyexecutor.Options{},
					)
				} else {
					_, errExecute = manager.Execute(
						context.Background(),
						[]string{chatgptwebauth.Provider},
						request,
						cliproxyexecutor.Options{},
					)
				}
				if elapsed := time.Since(startedAt); elapsed < 75*time.Millisecond {
					t.Fatalf("request returned after %v, want wait for provider cooldown", elapsed)
				}
				if errExecute == nil || !chatGPTWebImageQuotaRefreshPendingError(errExecute) {
					t.Fatalf("execution error = %T %v, want refresh-pending model cooldown", errExecute, errExecute)
				}
				if retryAfter := retryAfterFromError(errExecute); retryAfter != nil {
					t.Fatalf("execution RetryAfter() = %v, want nil after cooldown reaches recheck", retryAfter)
				}
				calls := executor.ExecuteCalls()
				if stream {
					calls = executor.StreamCalls()
				}
				if len(calls) != 0 {
					t.Fatalf("upstream calls = %v, want none before fresh quota confirmation", calls)
				}
			})
		}
	}

	if wait, ok := cooldownWaitFromError(
		newChatGPTWebImageRateLimitError(time.Time{}, time.Now()),
		time.Second,
	); ok || wait != 0 {
		t.Fatalf("refresh-pending cooldown wait = %v, %v, want 0, false", wait, ok)
	}
}

func assertChatGPTWebImageQuotaError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("selection error = nil")
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("selection error = %v, want 429", err)
	}
	if !strings.Contains(err.Error(), `"code":"image_quota_exhausted"`) {
		t.Fatalf("selection error = %v, want image_quota_exhausted", err)
	}
}

func TestMutateRuntimeMetadataAndClearModelCooldownPersistsOnce(t *testing.T) {
	store := &countingStore{}
	manager := NewManager(store, &FillFirstSelector{}, nil)
	authID := "quota-atomic-" + uuid.NewString()
	installed, errRegister := manager.Register(context.Background(), &Auth{
		ID:       authID,
		Provider: chatgptwebauth.Provider,
		Status:   StatusActive,
		Metadata: map[string]any{
			"quota_state":           string(chatgptwebauth.QuotaStateExhausted),
			"image_quota_remaining": 0,
		},
		ModelStates: map[string]*ModelState{
			chatgptwebauth.ImageModel: {
				Status:         StatusError,
				Unavailable:    true,
				NextRetryAfter: time.Now().Add(time.Hour),
				Quota: QuotaState{
					Exceeded: true,
					Reason:   "chatgpt_web_image_quota",
				},
			},
		},
	})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	baselineSaves := store.saveCount.Load()
	remaining := 2
	current, matched, errMutate := manager.MutateRuntimeMetadataAndClearModelCooldownIfCurrent(
		context.Background(),
		installed,
		chatgptwebauth.ImageModel,
		"chatgpt_web_image_quota",
		func(auth *Auth) {
			auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateAvailable)
			auth.Metadata["image_quota_remaining"] = remaining
		},
	)
	if errMutate != nil {
		t.Fatalf("MutateRuntimeMetadataAndClearModelCooldownIfCurrent() error = %v", errMutate)
	}
	if !matched || current == nil {
		t.Fatalf("mutation = current %v matched %v", current, matched)
	}
	if saves := store.saveCount.Load() - baselineSaves; saves != 1 {
		t.Fatalf("persistence writes = %d, want 1", saves)
	}
	if current.Metadata["quota_state"] != string(chatgptwebauth.QuotaStateAvailable) {
		t.Fatalf("quota metadata = %#v", current.Metadata)
	}
	imageState := current.ModelStates[chatgptwebauth.ImageModel]
	if imageState == nil || imageState.Status == StatusError || imageState.Unavailable || imageState.Quota.Exceeded {
		t.Fatalf("image state = %+v", imageState)
	}
}

func TestManagerProjectsImageToolQuotaFailureToImageModel(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.SetConfig(&internalconfig.Config{FixedErrorCooldowns: []internalconfig.FixedErrorCooldownRule{{
		StatusCode:      http.StatusTooManyRequests,
		CooldownSeconds: 3600,
		Scope:           cooldownScopeAuth,
	}}})
	authID := "quota-projection-" + uuid.NewString()
	model := "quota-projection-text-" + uuid.NewString()
	executor := &authFallbackExecutor{
		id:            chatgptwebauth.Provider,
		executeErrors: map[string]error{authID: chatGPTWebQuotaProjectionTestError{}},
	}
	manager.RegisterExecutor(executor)
	registered, errRegister := manager.Register(context.Background(), &Auth{
		ID:       authID,
		Provider: chatgptwebauth.Provider,
		Status:   StatusActive,
		Metadata: map[string]any{"lifecycle_state": LifecycleStateActive},
	})
	if errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, chatgptwebauth.Provider, []*registry.ModelInfo{
		{ID: model},
		{ID: chatgptwebauth.ImageModel},
	})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(authID)
	})

	_, errExecute := manager.Execute(context.Background(), []string{chatgptwebauth.Provider}, cliproxyexecutor.Request{
		Model:   model,
		Payload: imageToolFallbackPayload(model),
	}, cliproxyexecutor.Options{})
	if errExecute == nil {
		t.Fatal("Execute() error = nil")
	}

	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("auth disappeared")
	}
	if state := current.ModelStates[model]; state != nil && state.Status == StatusError {
		t.Fatalf("text model received image quota failure: %+v", state)
	}
	imageState := current.ModelStates[chatgptwebauth.ImageModel]
	if imageState == nil || imageState.Quota.Reason != "chatgpt_web_image_quota" {
		t.Fatalf("image model state = %+v", imageState)
	}
	if current.CooldownScope == cooldownScopeAuth {
		t.Fatalf("image quota fixed cooldown leaked to auth scope: %+v", current)
	}
	if current.HasAccountMaintenanceQuotaExceeded() {
		t.Fatal("image quota projection became account-maintenance quota")
	}
	if blocked, _, _ := isAuthBlockedForModel(current, model, time.Now()); blocked {
		t.Fatal("image quota fixed cooldown blocked the text model")
	}

	if !manager.ClearModelCooldownByReason(context.Background(), registered.ID, chatgptwebauth.ImageModel, "chatgpt_web_image_quota") {
		t.Fatal("ClearModelCooldownByReason() = false")
	}
	current, _ = manager.GetByID(authID)
	imageState = current.ModelStates[chatgptwebauth.ImageModel]
	if imageState == nil || imageState.Status == StatusError || imageState.Unavailable || imageState.Quota.Exceeded {
		t.Fatalf("image model state was not cleared: %+v", imageState)
	}
}

func TestManagerProjectsStreamImageQuotaFailureToImageModel(t *testing.T) {
	for _, bootstrap := range []bool{true, false} {
		name := "terminal"
		if bootstrap {
			name = "bootstrap"
		}
		t.Run(name, func(t *testing.T) {
			manager := NewManager(nil, &FillFirstSelector{}, nil)
			manager.SetRetryConfig(0, 0, 0)
			manager.RegisterExecutor(&chatGPTWebQuotaStreamTestExecutor{bootstrap: bootstrap})
			authID := "quota-stream-" + name + "-" + uuid.NewString()
			model := "quota-stream-text-" + uuid.NewString()
			if _, errRegister := manager.Register(context.Background(), &Auth{
				ID:       authID,
				Provider: chatgptwebauth.Provider,
				Status:   StatusActive,
				Metadata: map[string]any{"lifecycle_state": LifecycleStateActive},
			}); errRegister != nil {
				t.Fatalf("register auth: %v", errRegister)
			}
			registry.GetGlobalRegistry().RegisterClient(authID, chatgptwebauth.Provider, []*registry.ModelInfo{
				{ID: model},
				{ID: chatgptwebauth.ImageModel},
			})
			t.Cleanup(func() {
				registry.GetGlobalRegistry().UnregisterClient(authID)
			})

			stream, errExecute := manager.ExecuteStream(
				context.Background(),
				[]string{chatgptwebauth.Provider},
				cliproxyexecutor.Request{Model: model, Payload: imageToolFallbackPayload(model)},
				cliproxyexecutor.Options{},
			)
			if bootstrap {
				if errExecute == nil {
					t.Fatal("ExecuteStream() error = nil")
				}
			} else {
				if errExecute != nil {
					t.Fatalf("ExecuteStream() error = %v", errExecute)
				}
				sawQuotaError := false
				for chunk := range stream.Chunks {
					if cliproxyexecutor.IsBootstrapCommitStreamChunk(chunk) {
						continue
					}
					if chunk.Err == nil {
						t.Fatalf("stream chunk = %+v, want quota error", chunk)
					}
					sawQuotaError = true
				}
				if !sawQuotaError {
					t.Fatal("terminal stream did not return quota error")
				}
			}

			current, ok := manager.GetByID(authID)
			if !ok || current == nil {
				t.Fatal("auth disappeared")
			}
			if state := current.ModelStates[model]; state != nil && state.Status == StatusError {
				t.Fatalf("text model received stream image quota failure: %+v", state)
			}
			imageState := current.ModelStates[chatgptwebauth.ImageModel]
			if imageState == nil || imageState.Quota.Reason != "chatgpt_web_image_quota" {
				t.Fatalf("image model state = %+v", imageState)
			}
		})
	}
}

func TestManagerProjectsSuccessfulImageToolToImageModel(t *testing.T) {
	for _, stream := range []bool{false, true} {
		name := "nonstream"
		if stream {
			name = "stream"
		}
		t.Run(name, func(t *testing.T) {
			hook := &chatGPTWebImageSuccessResultHook{}
			manager := NewManager(nil, &FillFirstSelector{}, hook)
			manager.SetRetryConfig(0, 0, 0)
			executor := &chatGPTWebImageSuccessProjectionExecutor{
				manager:          manager,
				markImageSuccess: true,
			}
			manager.RegisterExecutor(executor)
			now := time.Now().UTC()
			model := "image-success-text-" + uuid.NewString()
			authID := "image-success-" + uuid.NewString()
			retryAt := now.Add(-time.Minute)
			registerQuotaTestAuthForModels(t, manager, &Auth{
				ID:       authID,
				Provider: chatgptwebauth.Provider,
				Status:   StatusError,
				LastError: &Error{
					Message:    "ordinary image rate limit",
					HTTPStatus: http.StatusTooManyRequests,
				},
				Metadata: map[string]any{
					"lifecycle_state":  LifecycleStateActive,
					"quota_state":      string(chatgptwebauth.QuotaStateAvailable),
					"quota_stale":      false,
					"quota_updated_at": now.Format(time.RFC3339Nano),
				},
				ModelStates: map[string]*ModelState{
					chatgptwebauth.ImageModel: {
						Status:         StatusError,
						StatusMessage:  "ordinary image rate limit",
						Unavailable:    true,
						UpdatedAt:      now.Add(-2 * time.Minute),
						NextRetryAfter: retryAt,
						LastError: &Error{
							Message:    "ordinary image rate limit",
							HTTPStatus: http.StatusTooManyRequests,
						},
						Quota: QuotaState{
							Exceeded:      true,
							Reason:        "quota",
							NextRecoverAt: retryAt,
							BackoffLevel:  4,
							StrikeCount:   5,
						},
					},
				},
			}, model, chatgptwebauth.ImageModel)

			request := cliproxyexecutor.Request{Model: model, Payload: imageToolFallbackPayload(model)}
			if stream {
				result, errExecute := manager.ExecuteStream(
					context.Background(),
					[]string{chatgptwebauth.Provider},
					request,
					cliproxyexecutor.Options{},
				)
				if errExecute != nil {
					t.Fatalf("ExecuteStream() error = %v", errExecute)
				}
				var payload strings.Builder
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatalf("stream error = %v", chunk.Err)
					}
					payload.Write(chunk.Payload)
				}
				if payload.String() != "text-stream-result" {
					t.Fatalf("stream payload = %q", payload.String())
				}
			} else {
				response, errExecute := manager.Execute(
					context.Background(),
					[]string{chatgptwebauth.Provider},
					request,
					cliproxyexecutor.Options{},
				)
				if errExecute != nil {
					t.Fatalf("Execute() error = %v", errExecute)
				}
				if string(response.Payload) != "text-result" {
					t.Fatalf("response payload = %q", response.Payload)
				}
			}

			current, ok := manager.GetByID(authID)
			if !ok || current == nil {
				t.Fatal("auth disappeared")
			}
			imageState := current.ModelStates[chatgptwebauth.ImageModel]
			if !modelStateIsClean(imageState) {
				t.Fatalf("image model state = %+v, want clean", imageState)
			}
			textState := current.ModelStates[model]
			if !modelStateIsClean(textState) {
				t.Fatalf("text model state = %+v, want clean", textState)
			}
			if current.Status != StatusActive || current.LastError != nil {
				t.Fatalf("auth status = %q last_error=%+v", current.Status, current.LastError)
			}
			results := hook.Results()
			if len(results) != 1 || !results[0].Success || results[0].Model != model {
				t.Fatalf("hook results = %+v, want one text-model success", results)
			}
		})
	}
}

func TestImageToolSuccessDoesNotOverrideConcurrentExhaustedQuota(t *testing.T) {
	for _, actualImage := range []bool{false, true} {
		name := "text only"
		if actualImage {
			name = "provider confirmed image"
		}
		t.Run(name, func(t *testing.T) {
			manager := NewManager(nil, &FillFirstSelector{}, nil)
			manager.SetRetryConfig(0, 0, 0)
			executor := &chatGPTWebImageSuccessProjectionExecutor{
				manager:                  manager,
				markImageSuccess:         actualImage,
				confirmExhaustedInFlight: true,
			}
			manager.RegisterExecutor(executor)
			model := "image-evidence-text-" + uuid.NewString()
			authID := "image-evidence-" + uuid.NewString()
			registerQuotaTestAuthForModels(t, manager, &Auth{
				ID:       authID,
				Provider: chatgptwebauth.Provider,
				Status:   StatusActive,
				Metadata: map[string]any{
					"lifecycle_state": LifecycleStateActive,
					"quota_state":     string(chatgptwebauth.QuotaStateAvailable),
					"quota_stale":     false,
				},
			}, model, chatgptwebauth.ImageModel)

			request := cliproxyexecutor.Request{Model: model}
			if actualImage {
				request.Payload = imageToolFallbackPayload(model)
			}
			if _, errExecute := manager.Execute(
				context.Background(),
				[]string{chatgptwebauth.Provider},
				request,
				cliproxyexecutor.Options{},
			); errExecute != nil {
				t.Fatalf("Execute() error = %v", errExecute)
			}

			current, _ := manager.GetByID(authID)
			quotaState := metadataString(current.Metadata["quota_state"])
			if quotaState != string(chatgptwebauth.QuotaStateExhausted) {
				t.Fatalf("success quota_state = %q, want exhausted", quotaState)
			}
			if current.Metadata["image_quota_remaining"] != 0 {
				t.Fatalf("success remaining = %#v, want zero", current.Metadata["image_quota_remaining"])
			}
			if resetAt := metadataString(current.Metadata["image_quota_reset_at"]); resetAt == "" {
				t.Fatal("success cleared the remote quota reset time")
			}
			imageState := current.ModelStates[chatgptwebauth.ImageModel]
			if imageState == nil || !imageState.Quota.Exceeded ||
				imageState.Quota.Reason != "chatgpt_web_image_quota" {
				t.Fatalf("success cleared the remote quota model state: %+v", imageState)
			}
		})
	}
}
