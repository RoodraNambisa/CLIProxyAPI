package management

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type accountInfoControllerTestExecutor struct {
	coreauth.ProviderExecutor
	mu          sync.Mutex
	snapshot    chatgptwebauth.AccountInfoRuntimeSnapshot
	diagnostics chatgptwebauth.AccountInfoDiagnosticsSnapshot
	tasks       map[string]*chatgptwebauth.AccountInfoRefreshTask
	targets     []chatgptwebauth.AccountInfoRefreshTarget
	force       bool
	updates     int
	startErr    error

	expectedAuthID     string
	invalidAuthIDCount int
}

func (executor *accountInfoControllerTestExecutor) Identifier() string {
	return chatgptwebauth.Provider
}

func (executor *accountInfoControllerTestExecutor) AccountInfoSnapshot() chatgptwebauth.AccountInfoRuntimeSnapshot {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.snapshot
}

func (executor *accountInfoControllerTestExecutor) AccountInfoDiagnosticsSnapshot() chatgptwebauth.AccountInfoDiagnosticsSnapshot {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.diagnostics
}

func (executor *accountInfoControllerTestExecutor) ClearAccountInfoDiagnostics() chatgptwebauth.AccountInfoDiagnosticsSnapshot {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	executor.diagnostics.UniqueCount = 0
	executor.diagnostics.TotalCount = 0
	executor.diagnostics.EvictedCount = 0
	executor.diagnostics.Records = nil
	return executor.diagnostics
}

func (executor *accountInfoControllerTestExecutor) AccountInfoAuthState(authID string) chatgptwebauth.AccountInfoAuthRuntimeState {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	if executor.expectedAuthID != "" && authID != executor.expectedAuthID {
		executor.invalidAuthIDCount++
	}
	return chatgptwebauth.AccountInfoAuthRuntimeState{}
}

func (executor *accountInfoControllerTestExecutor) StartAccountInfoRefreshTask(targets []chatgptwebauth.AccountInfoRefreshTarget, force bool) (*chatgptwebauth.AccountInfoRefreshTask, error) {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	executor.targets = append([]chatgptwebauth.AccountInfoRefreshTarget(nil), targets...)
	executor.force = force
	for _, target := range targets {
		if executor.expectedAuthID != "" && target.AuthID != executor.expectedAuthID {
			executor.invalidAuthIDCount++
		}
	}
	if executor.startErr != nil {
		return nil, executor.startErr
	}
	task := &chatgptwebauth.AccountInfoRefreshTask{
		ID:        "account-info-task",
		State:     chatgptwebauth.AccountInfoTaskQueued,
		Force:     force,
		CreatedAt: time.Now(),
		Total:     len(targets),
		Results:   make([]chatgptwebauth.AccountInfoRefreshResult, len(targets)),
	}
	for index, target := range targets {
		task.Results[index] = chatgptwebauth.AccountInfoRefreshResult{
			Name:      target.Name,
			AuthIndex: target.AuthIndex,
			Status:    chatgptwebauth.AccountInfoResultQueued,
		}
		if target.AuthID == "" {
			task.Results[index].Status = chatgptwebauth.AccountInfoResultFailed
			task.Results[index].Error = "credential_unavailable"
			task.Processed++
			task.Failed++
		}
	}
	if task.Processed == task.Total {
		completedAt := time.Now()
		task.State = chatgptwebauth.AccountInfoTaskCompletedWithErrors
		task.CompletedAt = &completedAt
	}
	if executor.tasks == nil {
		executor.tasks = make(map[string]*chatgptwebauth.AccountInfoRefreshTask)
	}
	executor.tasks[task.ID] = task
	return task, nil
}

func (executor *accountInfoControllerTestExecutor) AccountInfoRefreshTask(id string) (*chatgptwebauth.AccountInfoRefreshTask, bool) {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	task := executor.tasks[id]
	return task, task != nil
}

func (executor *accountInfoControllerTestExecutor) CancelAccountInfoRefreshTask(id string) (*chatgptwebauth.AccountInfoRefreshTask, bool) {
	return executor.AccountInfoRefreshTask(id)
}

func (executor *accountInfoControllerTestExecutor) UpdateConfig(*config.Config) {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	executor.updates++
}

func TestGetChatGPTWebAccountInfoReturnsDefaultsAndRuntime(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	executor := &accountInfoControllerTestExecutor{snapshot: chatgptwebauth.AccountInfoRuntimeSnapshot{
		Busy: 2, Queued: 3, Scheduled: 4, Inflight: 1, RefreshCount: 7,
	}}
	manager.RegisterExecutor(executor)
	handler := &Handler{cfg: &config.Config{}, authManager: manager}
	ctx, recorder := newChatGPTWebAccountInfoRequest(http.MethodGet, "")
	handler.GetChatGPTWebAccountInfo(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET status = %d: %s", recorder.Code, recorder.Body.String())
	}
	var response chatGPTWebAccountInfoResponse
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if !response.Config.AutoRefreshEnabled || response.Config.DiagnosticsEnabled ||
		response.Config.RefreshWorkers != 4 || response.Config.RefreshQueueSize != 256 ||
		response.Config.RefreshTTLMinutes != 15 || response.Config.PeriodicRefreshMinutes != 0 ||
		response.Config.RecoveryJitterSeconds != 30 ||
		response.Config.MaxRetries != 3 {
		t.Fatalf("defaults = %+v", response.Config)
	}
	if response.Runtime.Busy != 2 || response.Runtime.Queued != 3 ||
		response.Runtime.Scheduled != 4 || response.Runtime.RefreshCount != 7 {
		t.Fatalf("runtime = %+v", response.Runtime)
	}
}

func TestGetAndClearChatGPTWebAccountInfoDiagnostics(t *testing.T) {
	executor := &accountInfoControllerTestExecutor{diagnostics: chatgptwebauth.AccountInfoDiagnosticsSnapshot{
		Enabled:      true,
		Capacity:     chatgptwebauth.AccountInfoDiagnosticsCapacity,
		UniqueCount:  1,
		TotalCount:   3,
		EvictedCount: 2,
		Records: []chatgptwebauth.AccountInfoDiagnosticRecord{{
			ID: "diagnostic", Phase: "quota", Stage: "parse", Reason: "quota_remaining_invalid", Count: 3,
		}},
	}}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	handler := &Handler{cfg: &config.Config{}, authManager: manager}

	ctx, recorder := newChatGPTWebAccountInfoRequest(http.MethodGet, "")
	handler.GetChatGPTWebAccountInfoDiagnostics(ctx)
	if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("GET status = %d cache-control = %q body = %s", recorder.Code, recorder.Header().Get("Cache-Control"), recorder.Body.String())
	}
	var before chatgptwebauth.AccountInfoDiagnosticsSnapshot
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &before); errDecode != nil {
		t.Fatalf("decode GET diagnostics: %v", errDecode)
	}
	if before.UniqueCount != 1 || before.TotalCount != 3 || before.EvictedCount != 2 || len(before.Records) != 1 {
		t.Fatalf("GET diagnostics = %+v", before)
	}

	ctx, recorder = newChatGPTWebAccountInfoRequest(http.MethodDelete, "")
	handler.ClearChatGPTWebAccountInfoDiagnostics(ctx)
	if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("DELETE status = %d cache-control = %q body = %s", recorder.Code, recorder.Header().Get("Cache-Control"), recorder.Body.String())
	}
	var after chatgptwebauth.AccountInfoDiagnosticsSnapshot
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &after); errDecode != nil {
		t.Fatalf("decode DELETE diagnostics: %v", errDecode)
	}
	if !after.Enabled || after.Capacity != chatgptwebauth.AccountInfoDiagnosticsCapacity ||
		after.UniqueCount != 0 || after.TotalCount != 0 || after.EvictedCount != 0 || len(after.Records) != 0 {
		t.Fatalf("cleared diagnostics = %+v", after)
	}
}

func TestPatchAndStartChatGPTWebAccountInfoRefresh(t *testing.T) {
	configPath := writeTestConfigFile(t)
	manager := coreauth.NewManager(nil, nil, nil)
	executor := &accountInfoControllerTestExecutor{}
	manager.RegisterExecutor(executor)
	registered, errRegister := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "web.json",
		FileName: "web.json",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"lifecycle_state": coreauth.LifecycleStateActive},
	})
	if errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	handler := &Handler{cfg: &config.Config{}, configFilePath: configPath, authManager: manager}

	ctx, recorder := newChatGPTWebAccountInfoRequest(
		http.MethodPatch,
		`{"auto-refresh-enabled":false,"diagnostics-enabled":true,"refresh-workers":6,"periodic-refresh-minutes":60,"max-retries":1}`,
	)
	handler.PatchChatGPTWebAccountInfo(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("PATCH status = %d: %s", recorder.Code, recorder.Body.String())
	}
	resolved := handler.cfg.ChatGPTWeb.AccountInfo.Resolved()
	if resolved.AutoRefreshEnabled || !resolved.DiagnosticsEnabled ||
		resolved.RefreshWorkers != 6 || resolved.PeriodicRefreshMinutes != 60 ||
		resolved.MaxRetries != 1 || resolved.RefreshQueueSize != 256 {
		t.Fatalf("patched config = %+v", resolved)
	}
	executor.mu.Lock()
	updates := executor.updates
	executor.mu.Unlock()
	if updates != 1 {
		t.Fatalf("UpdateConfig calls = %d, want 1", updates)
	}

	ctx, recorder = newChatGPTWebAccountInfoRequest(http.MethodPost, `{"names":["web.json","web.json"],"force":true}`)
	handler.StartChatGPTWebAccountInfoRefreshTask(ctx)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("POST status = %d: %s", recorder.Code, recorder.Body.String())
	}
	executor.mu.Lock()
	targets := append([]chatgptwebauth.AccountInfoRefreshTarget(nil), executor.targets...)
	force := executor.force
	executor.mu.Unlock()
	if len(targets) != 1 || targets[0].Name != "web.json" || targets[0].AuthID != "web.json" ||
		targets[0].AuthInstanceID != registered.RuntimeInstanceID() ||
		targets[0].AuthIndex != registered.EnsureIndex() || !force {
		t.Fatalf("refresh targets = %+v force=%v", targets, force)
	}
}

func TestPutChatGPTWebAccountInfoAcceptsLegacyPayloadWithoutAutoRefresh(t *testing.T) {
	configPath := writeTestConfigFile(t)
	manager := coreauth.NewManager(nil, nil, nil)
	executor := &accountInfoControllerTestExecutor{}
	manager.RegisterExecutor(executor)
	handler := &Handler{cfg: &config.Config{}, configFilePath: configPath, authManager: manager}

	ctx, recorder := newChatGPTWebAccountInfoRequest(
		http.MethodPut,
		`{"refresh-workers":6,"refresh-queue-size":128,"refresh-ttl-minutes":30,"recovery-jitter-seconds":10,"max-retries":2}`,
	)
	handler.PutChatGPTWebAccountInfo(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("PUT status = %d: %s", recorder.Code, recorder.Body.String())
	}
	resolved := handler.cfg.ChatGPTWeb.AccountInfo.Resolved()
	if !resolved.AutoRefreshEnabled || !resolved.AutomaticRefreshEnabled() {
		t.Fatalf("legacy PUT disabled automatic refresh: %+v", resolved)
	}
	if resolved.PeriodicRefreshMinutes != 0 {
		t.Fatalf("legacy PUT periodic refresh = %d, want disabled", resolved.PeriodicRefreshMinutes)
	}
	if resolved.DiagnosticsEnabled {
		t.Fatal("legacy PUT enabled diagnostics")
	}
}

func TestPatchChatGPTWebAccountInfoRejectsNullDiagnostics(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(&accountInfoControllerTestExecutor{})
	handler := &Handler{cfg: &config.Config{}, authManager: manager}
	ctx, recorder := newChatGPTWebAccountInfoRequest(
		http.MethodPatch,
		`{"diagnostics-enabled":null}`,
	)

	handler.PatchChatGPTWebAccountInfo(ctx)

	if recorder.Code != http.StatusBadRequest ||
		!strings.Contains(recorder.Body.String(), "invalid diagnostics-enabled") {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestPatchChatGPTWebAccountInfoRejectsNullPeriodicRefresh(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(&accountInfoControllerTestExecutor{})
	handler := &Handler{cfg: &config.Config{}, authManager: manager}
	ctx, recorder := newChatGPTWebAccountInfoRequest(
		http.MethodPatch,
		`{"periodic-refresh-minutes":null}`,
	)

	handler.PatchChatGPTWebAccountInfo(ctx)

	if recorder.Code != http.StatusBadRequest ||
		!strings.Contains(recorder.Body.String(), "invalid periodic-refresh-minutes") {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestPatchChatGPTWebAccountInfoRollsBackOnPersistFailure(t *testing.T) {
	periodic := 15
	diagnostics := false
	manager := coreauth.NewManager(nil, nil, nil)
	executor := &accountInfoControllerTestExecutor{}
	manager.RegisterExecutor(executor)
	handler := &Handler{
		cfg: &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
			AccountInfo: config.ChatGPTWebAccountInfoConfig{
				DiagnosticsEnabled:     &diagnostics,
				PeriodicRefreshMinutes: &periodic,
			},
		}},
		configFilePath: filepath.Join(t.TempDir(), "missing", "config.yaml"),
		authManager:    manager,
	}
	ctx, recorder := newChatGPTWebAccountInfoRequest(
		http.MethodPatch,
		`{"diagnostics-enabled":true,"periodic-refresh-minutes":60}`,
	)

	handler.PatchChatGPTWebAccountInfo(ctx)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	if got := handler.cfg.ChatGPTWeb.AccountInfo.Resolved().PeriodicRefreshMinutes; got != periodic {
		t.Fatalf("periodic refresh after persistence failure = %d, want %d", got, periodic)
	}
	if handler.cfg.ChatGPTWeb.AccountInfo.Resolved().DiagnosticsEnabled {
		t.Fatal("diagnostics remained enabled after persistence failure")
	}
	executor.mu.Lock()
	updates := executor.updates
	executor.mu.Unlock()
	if updates != 0 {
		t.Fatalf("runtime updates after persistence failure = %d, want 0", updates)
	}
}

func TestChatGPTWebAccountInfoAutoRefreshDisabledReturnsConflict(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(&accountInfoControllerTestExecutor{
		startErr: chatgptwebauth.ErrAccountInfoAutoRefreshDisabled,
	})
	if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "web.json",
		FileName: "web.json",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"lifecycle_state": coreauth.LifecycleStateActive},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	handler := &Handler{cfg: &config.Config{}, authManager: manager}
	ctx, recorder := newChatGPTWebAccountInfoRequest(http.MethodPost, `{"names":["web.json"]}`)

	handler.StartChatGPTWebAccountInfoRefreshTask(ctx)

	if recorder.Code != http.StatusConflict ||
		!strings.Contains(recorder.Body.String(), "automatic refresh is disabled") {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestChatGPTWebAccountInfoRejectsTooManyTargetsBeforeLookup(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(&accountInfoControllerTestExecutor{})
	handler := &Handler{cfg: &config.Config{}, authManager: manager}
	names := make([]string, chatgptwebauth.AccountInfoMaxTargets+1)
	for index := range names {
		names[index] = fmt.Sprintf("missing-%03d.json", index)
	}
	body, errMarshal := json.Marshal(chatGPTWebAccountInfoRefreshRequest{Names: names})
	if errMarshal != nil {
		t.Fatalf("marshal request: %v", errMarshal)
	}
	ctx, recorder := newChatGPTWebAccountInfoRequest(http.MethodPost, string(body))
	handler.StartChatGPTWebAccountInfoRefreshTask(ctx)
	if recorder.Code != http.StatusBadRequest ||
		!strings.Contains(recorder.Body.String(), "at most 500 auth file names are allowed") {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestChatGPTWebAccountInfoTaskLimitReturnsTooManyRequests(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(&accountInfoControllerTestExecutor{
		startErr: fmt.Errorf("%w: maximum tasks", chatgptwebauth.ErrAccountInfoTaskLimitReached),
	})
	if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "web.json",
		FileName: "web.json",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"lifecycle_state": coreauth.LifecycleStateActive},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	handler := &Handler{cfg: &config.Config{}, authManager: manager}
	ctx, recorder := newChatGPTWebAccountInfoRequest(http.MethodPost, `{"names":["web.json"]}`)

	handler.StartChatGPTWebAccountInfoRefreshTask(ctx)

	if recorder.Code != http.StatusTooManyRequests ||
		!strings.Contains(recorder.Body.String(), "active task limit reached") {
		t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
	}
}

func TestChatGPTWebAccountInfoBatchKeepsValidTargetsWhenOthersAreUnavailable(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	executor := &accountInfoControllerTestExecutor{}
	manager.RegisterExecutor(executor)
	valid, errRegister := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "valid-auth",
		FileName: "valid.json",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"runtime_only": "true",
		},
		Metadata: map[string]any{"lifecycle_state": coreauth.LifecycleStateActive},
	})
	if errRegister != nil {
		t.Fatalf("register valid auth: %v", errRegister)
	}
	if _, errRegister = manager.Register(context.Background(), &coreauth.Auth{
		ID:       "wrong-provider-auth",
		FileName: "wrong-provider.json",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"runtime_only": "true",
		},
	}); errRegister != nil {
		t.Fatalf("register wrong-provider auth: %v", errRegister)
	}
	handler := &Handler{cfg: &config.Config{}, authManager: manager}

	ctx, recorder := newChatGPTWebAccountInfoRequest(
		http.MethodPost,
		`{"names":["valid.json","missing.json","wrong-provider.json"]}`,
	)
	handler.StartChatGPTWebAccountInfoRefreshTask(ctx)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("POST status = %d: %s", recorder.Code, recorder.Body.String())
	}

	var task chatgptwebauth.AccountInfoRefreshTask
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &task); errDecode != nil {
		t.Fatalf("decode task: %v", errDecode)
	}
	if task.Total != 3 || task.Processed != 2 || task.Failed != 2 || task.State != chatgptwebauth.AccountInfoTaskQueued {
		t.Fatalf("task = %+v", task)
	}
	results := make(map[string]chatgptwebauth.AccountInfoRefreshResult, len(task.Results))
	for _, result := range task.Results {
		results[result.Name] = result
	}
	if result := results["valid.json"]; result.Status != chatgptwebauth.AccountInfoResultQueued || result.Error != "" {
		t.Fatalf("valid result = %+v", result)
	}
	for _, name := range []string{"missing.json", "wrong-provider.json"} {
		result := results[name]
		if result.Status != chatgptwebauth.AccountInfoResultFailed || result.Error != "credential_unavailable" {
			t.Fatalf("%s result = %+v", name, result)
		}
	}
	if strings.Contains(recorder.Body.String(), "codex") || strings.Contains(recorder.Body.String(), "not found") {
		t.Fatalf("task response leaked lookup details: %s", recorder.Body.String())
	}

	executor.mu.Lock()
	targets := append([]chatgptwebauth.AccountInfoRefreshTarget(nil), executor.targets...)
	executor.mu.Unlock()
	if len(targets) != 3 || targets[0].AuthID != valid.ID ||
		targets[0].AuthInstanceID != valid.RuntimeInstanceID() ||
		targets[0].AuthIndex != valid.EnsureIndex() {
		t.Fatalf("targets = %+v", targets)
	}
	if targets[1].AuthID != "" || targets[1].AuthIndex != "" ||
		targets[2].AuthID != "" || targets[2].AuthIndex != "" {
		t.Fatalf("unavailable targets exposed runtime identity: %+v", targets)
	}
}

func TestChatGPTWebAccountInfoManagerReplacementIsSynchronized(t *testing.T) {
	first := coreauth.NewManager(nil, nil, nil)
	firstExecutor := &accountInfoControllerTestExecutor{expectedAuthID: "first-auth"}
	first.RegisterExecutor(firstExecutor)
	second := coreauth.NewManager(nil, nil, nil)
	secondExecutor := &accountInfoControllerTestExecutor{expectedAuthID: "second-auth"}
	second.RegisterExecutor(secondExecutor)
	for index, manager := range []*coreauth.Manager{first, second} {
		authID := "first-auth"
		if index == 1 {
			authID = "second-auth"
		}
		if _, errRegister := manager.Register(context.Background(), &coreauth.Auth{
			ID:       authID,
			FileName: "shared.json",
			Provider: chatgptwebauth.Provider,
			Status:   coreauth.StatusActive,
			Attributes: map[string]string{
				"runtime_only": "true",
			},
			Metadata: map[string]any{"lifecycle_state": coreauth.LifecycleStateActive},
		}); errRegister != nil {
			t.Fatalf("register auth: %v", errRegister)
		}
	}
	handler := &Handler{cfg: &config.Config{}, authManager: first}
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for index := 0; index < 200; index++ {
			if index%2 == 0 {
				handler.SetAuthManager(first)
			} else {
				handler.SetAuthManager(second)
			}
		}
	}()
	go func() {
		defer workers.Done()
		for index := 0; index < 200; index++ {
			ctx, _ := newChatGPTWebAccountInfoRequest(http.MethodGet, "")
			handler.GetChatGPTWebAccountInfo(ctx)
			_ = handler.authFileRuntimeSummaries()
			listRecorder := httptest.NewRecorder()
			listContext, _ := gin.CreateTestContext(listRecorder)
			listContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
			handler.ListAuthFiles(listContext)
			if listRecorder.Code != http.StatusOK {
				t.Errorf("list status = %d: %s", listRecorder.Code, listRecorder.Body.String())
				return
			}
			modelsRecorder := httptest.NewRecorder()
			modelsContext, _ := gin.CreateTestContext(modelsRecorder)
			modelsContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/models?name=shared.json", nil)
			handler.GetAuthFileModels(modelsContext)
			if modelsRecorder.Code != http.StatusOK {
				t.Errorf("models status = %d: %s", modelsRecorder.Code, modelsRecorder.Body.String())
				return
			}
			ctx, recorder := newChatGPTWebAccountInfoRequest(http.MethodPost, `{"names":["shared.json"]}`)
			handler.StartChatGPTWebAccountInfoRefreshTask(ctx)
			if recorder.Code != http.StatusAccepted {
				t.Errorf("POST status = %d: %s", recorder.Code, recorder.Body.String())
				return
			}
		}
	}()
	workers.Wait()
	for name, executor := range map[string]*accountInfoControllerTestExecutor{
		"first":  firstExecutor,
		"second": secondExecutor,
	} {
		executor.mu.Lock()
		invalidAuthIDCount := executor.invalidAuthIDCount
		executor.mu.Unlock()
		if invalidAuthIDCount != 0 {
			t.Fatalf("%s manager received %d auth IDs from the other manager", name, invalidAuthIDCount)
		}
	}
}

func TestChatGPTWebAccountInfoRejectsMalformedOrOutOfRangeConfig(t *testing.T) {
	for _, body := range []string{
		`{"refresh-workers":0}`,
		`{"refresh-queue-size":10001}`,
		`{"refresh-ttl-minutes":null}`,
		`{"unknown":1}`,
		`{"refresh-workers":2} {}`,
	} {
		t.Run(body, func(t *testing.T) {
			handler := &Handler{cfg: &config.Config{}, configFilePath: writeTestConfigFile(t)}
			ctx, recorder := newChatGPTWebAccountInfoRequest(http.MethodPatch, body)
			handler.PatchChatGPTWebAccountInfo(ctx)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestChatGPTWebAccountInfoRejectsOversizedConfigAndTaskBodies(t *testing.T) {
	configBody := `{"refresh-workers":` + strings.Repeat(" ", chatGPTWebAccountInfoMaxRequestBytes) + `6}`
	handler := &Handler{cfg: &config.Config{}, configFilePath: writeTestConfigFile(t)}
	ctx, recorder := newChatGPTWebAccountInfoRequest(http.MethodPatch, configBody)
	handler.PatchChatGPTWebAccountInfo(ctx)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized config status = %d, want 413: %s", recorder.Code, recorder.Body.String())
	}

	manager := coreauth.NewManager(nil, nil, nil)
	executor := &accountInfoControllerTestExecutor{}
	manager.RegisterExecutor(executor)
	handler = &Handler{cfg: &config.Config{}, authManager: manager}
	taskBody := `{"names":["` + strings.Repeat("a", chatGPTWebAccountInfoMaxRequestBytes) + `"]}`
	ctx, recorder = newChatGPTWebAccountInfoRequest(http.MethodPost, taskBody)
	handler.StartChatGPTWebAccountInfoRefreshTask(ctx)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized task status = %d, want 413: %s", recorder.Code, recorder.Body.String())
	}

	trailing := `{"refresh-workers":6}` + strings.Repeat(" ", chatGPTWebAccountInfoMaxRequestBytes)
	handler = &Handler{cfg: &config.Config{}, configFilePath: writeTestConfigFile(t)}
	ctx, recorder = newChatGPTWebAccountInfoRequest(http.MethodPatch, trailing)
	handler.PatchChatGPTWebAccountInfo(ctx)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized trailing config status = %d, want 413: %s", recorder.Code, recorder.Body.String())
	}

	trailing = `{"names":["web.json"]}` + strings.Repeat(" ", chatGPTWebAccountInfoMaxRequestBytes)
	manager = coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(&accountInfoControllerTestExecutor{})
	handler = &Handler{cfg: &config.Config{}, authManager: manager}
	ctx, recorder = newChatGPTWebAccountInfoRequest(http.MethodPost, trailing)
	handler.StartChatGPTWebAccountInfoRefreshTask(ctx)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized trailing task status = %d, want 413: %s", recorder.Code, recorder.Body.String())
	}
}

func TestChatGPTWebQuotaCooldownSummaryIsModelScopedAndDoesNotAutoRecover(t *testing.T) {
	now := time.Now()
	auth := &coreauth.Auth{
		ID:       "quota-summary",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"lifecycle_state":       coreauth.LifecycleStateActive,
			"quota_state":           string(chatgptwebauth.QuotaStateExhausted),
			"image_quota_remaining": -2,
			"image_quota_reset_at":  now.Add(-time.Minute).Format(time.RFC3339Nano),
		},
	}
	entry := gin.H{
		"cooldown_active":      false,
		"cooldown_model_count": 0,
	}
	applyChatGPTWebAuthFileSummary(entry, auth, now)
	if entry["cooldown_active"] != true || entry["cooldown_scope"] != "model" ||
		entry["cooldown_model_count"] != 1 {
		t.Fatalf("quota cooldown summary = %+v", entry)
	}
	if entry["image_quota_remaining"] != -2 {
		t.Fatalf("image quota remaining = %#v, want -2", entry["image_quota_remaining"])
	}
	if _, exists := entry["cooldown_until"]; exists {
		t.Fatalf("past reset time was exposed as active cooldown deadline: %+v", entry)
	}

	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateAvailable)
	auth.Metadata["image_quota_remaining"] = 4
	auth.ModelStates = map[string]*coreauth.ModelState{
		chatgptwebauth.ImageModel: {
			Status:         coreauth.StatusError,
			Unavailable:    false,
			NextRetryAfter: now.Add(-time.Minute),
			Quota: coreauth.QuotaState{
				Exceeded:      true,
				Reason:        "chatgpt_web_image_quota",
				NextRecoverAt: now.Add(-30 * time.Second),
			},
		},
	}
	entry = gin.H{"cooldown_active": false, "cooldown_model_count": 0}
	applyChatGPTWebAuthFileSummary(entry, auth, now)
	if entry["cooldown_active"] != true || entry["cooldown_scope"] != "model" ||
		entry["cooldown_model_count"] != 1 {
		t.Fatalf("reason-owned quota cooldown summary = %+v", entry)
	}
	if _, exists := entry["cooldown_until"]; exists {
		t.Fatalf("expired reason-owned cooldown exposed a deadline: %+v", entry)
	}
	credential, errParse := chatgptwebauth.ParseCredential(auth.Metadata)
	if errParse != nil {
		t.Fatalf("parse credential: %v", errParse)
	}
	modelStatus := chatGPTWebImageModelCooldownStatus(
		auth,
		credential,
		nil,
		now,
		chatgptwebauth.ImageModel,
	)
	if !modelStatus.Active || modelStatus.Scope != "model" || modelStatus.ModelCount != 1 || !modelStatus.Until.IsZero() {
		t.Fatalf("reason-owned image model cooldown = %+v", modelStatus)
	}

	auth.ModelStates[chatgptwebauth.ImageModel].Quota.Reason = "quota"
	auth.ModelStates[chatgptwebauth.ImageModel].Quota.NextRecoverAt = now.Add(-time.Minute)
	runtimeRefreshAt := now.Add(2 * time.Minute)
	runtimeState := &chatgptwebauth.AccountInfoAuthRuntimeState{NextRefreshAt: runtimeRefreshAt}
	modelStatus = chatGPTWebImageModelCooldownStatus(
		auth,
		credential,
		runtimeState,
		now,
		chatgptwebauth.ImageModel,
	)
	if !modelStatus.Active ||
		modelStatus.Scope != "model" ||
		modelStatus.ModelCount != 1 ||
		!modelStatus.Until.Equal(runtimeRefreshAt) {
		t.Fatalf("ordinary quota recheck cooldown = %+v", modelStatus)
	}

	auth.Disabled = true
	auth.Status = coreauth.StatusDisabled
	entry = gin.H{"cooldown_active": false, "cooldown_model_count": 0}
	applyChatGPTWebAuthFileSummary(entry, auth, now)
	if entry["cooldown_active"] != false {
		t.Fatalf("disabled auth was presented as cooling: %+v", entry)
	}

	auth.Disabled = false
	auth.Status = coreauth.StatusActive
	auth.Unavailable = true
	auth.CooldownScope = "auth"
	auth.NextRetryAfter = now.Add(time.Hour)
	entry = gin.H{"cooldown_active": false, "cooldown_model_count": 0}
	applyChatGPTWebAuthFileSummary(entry, auth, now)
	if entry["cooldown_scope"] != "auth" {
		t.Fatalf("auth-wide cooldown did not take precedence: %+v", entry)
	}
}

func TestGetAuthFileModelsShowsExpiredReasonOwnedChatGPTWebQuotaCooldown(t *testing.T) {
	now := time.Now()
	auth := &coreauth.Auth{
		ID:       "reason-owned-quota-models",
		FileName: "reason-owned-quota-models",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"runtime_only": "true",
		},
		Metadata: map[string]any{
			"lifecycle_state":       coreauth.LifecycleStateActive,
			"quota_state":           string(chatgptwebauth.QuotaStateAvailable),
			"image_quota_remaining": 2,
		},
		ModelStates: map[string]*coreauth.ModelState{
			chatgptwebauth.ImageModel: {
				Status:         coreauth.StatusError,
				Unavailable:    true,
				NextRetryAfter: now.Add(-time.Minute),
				Quota: coreauth.QuotaState{
					Exceeded:      true,
					Reason:        "chatgpt_web_image_quota",
					NextRecoverAt: now.Add(-time.Minute),
				},
			},
		},
	}
	manager := coreauth.NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{
		ID: chatgptwebauth.ImageModel,
	}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})
	handler := &Handler{cfg: &config.Config{}, authManager: manager}

	models := requestAuthModels(t, handler, auth.ID, 1)
	imageModel := models[chatgptwebauth.ImageModel]
	if imageModel["cooldown_active"] != true || imageModel["scope"] != "model" {
		t.Fatalf("image model cooldown = %+v", imageModel)
	}
	if _, exists := imageModel["until"]; exists {
		t.Fatalf("expired reason-owned cooldown exposed a deadline: %+v", imageModel)
	}
}

func TestGetAuthFileModelsMarksChatGPTWebImageQuotaModelAliases(t *testing.T) {
	auth := &coreauth.Auth{
		ID:       "image-quota-model-marker",
		FileName: "image-quota-model-marker",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"runtime_only": "true",
		},
		Metadata: map[string]any{
			"lifecycle_state":       coreauth.LifecycleStateActive,
			"quota_state":           string(chatgptwebauth.QuotaStateAvailable),
			"image_quota_remaining": 3,
		},
	}
	manager := coreauth.NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{
		{ID: chatgptwebauth.ImageModel, UpstreamID: chatgptwebauth.ImageModel},
		{ID: "team/" + chatgptwebauth.ImageModel, UpstreamID: chatgptwebauth.ImageModel},
		{ID: "create-image", UpstreamID: chatgptwebauth.ImageModel},
		{ID: "gemini-style-image", Name: "MODELS/" + chatgptwebauth.ImageModel},
		{ID: "text-model", UpstreamID: "gpt-5"},
	})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})
	handler := &Handler{cfg: &config.Config{}, authManager: manager}

	models := requestAuthModels(t, handler, auth.ID, 5)
	for _, modelID := range []string{
		chatgptwebauth.ImageModel,
		"team/" + chatgptwebauth.ImageModel,
		"create-image",
		"gemini-style-image",
	} {
		model := models[modelID]
		if model["image_quota_model"] != true || model["quota_state"] != string(chatgptwebauth.QuotaStateAvailable) {
			t.Fatalf("image quota model %q = %+v", modelID, model)
		}
	}
	if textModel := models["text-model"]; textModel["image_quota_model"] != nil {
		t.Fatalf("text model was marked as an image quota model: %+v", textModel)
	}
}

func TestChatGPTWebMetadataSummaryPreservesAuthTypeAndProjectsPlanType(t *testing.T) {
	entry := gin.H{"account_type": "oauth"}
	applyChatGPTWebMetadataSummary(entry, map[string]any{
		"type":            chatgptwebauth.Provider,
		"email":           "person@example.com",
		"access_token":    "token",
		"refresh_token":   "refresh",
		"plan_type":       "plus",
		"lifecycle_state": coreauth.LifecycleStateActive,
	}, coreauth.LifecycleStateActive, time.Now())
	if entry["account_type"] != "oauth" {
		t.Fatalf("account_type = %v, want oauth", entry["account_type"])
	}
	if entry["plan_type"] != "plus" {
		t.Fatalf("plan_type = %v, want plus", entry["plan_type"])
	}

	entry = gin.H{"account_type": "oauth"}
	applyChatGPTWebMetadataSummary(entry, map[string]any{
		"type":            chatgptwebauth.Provider,
		"email":           "legacy@example.com",
		"access_token":    "token",
		"refresh_token":   "refresh",
		"lifecycle_state": coreauth.LifecycleStateActive,
	}, coreauth.LifecycleStateActive, time.Now())
	if entry["account_type"] != "oauth" {
		t.Fatalf("legacy account_type = %v, want oauth", entry["account_type"])
	}
	if entry["plan_type"] != "" {
		t.Fatalf("legacy plan_type = %v, want empty", entry["plan_type"])
	}
}

func newChatGPTWebAccountInfoRequest(method, body string) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(method, "/v0/management/chatgpt-web/account-info", strings.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	return ctx, recorder
}
