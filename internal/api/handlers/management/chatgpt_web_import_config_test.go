package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestGetChatGPTWebImportReturnsDefaultsAndRuntime(t *testing.T) {
	handler := &Handler{
		cfg:                     &config.Config{},
		chatGPTWebMutationTasks: newChatGPTWebMutationTaskManager(),
	}
	t.Cleanup(func() { shutdownChatGPTWebMutationTasks(t, handler.chatGPTWebMutationTasks) })

	ctx, recorder := newChatGPTWebImportConfigRequest(http.MethodGet, "")
	handler.GetChatGPTWebImport(ctx)
	if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("GET status = %d cache-control = %q body = %s", recorder.Code, recorder.Header().Get("Cache-Control"), recorder.Body.String())
	}
	var response chatGPTWebImportConfigResponse
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatal(errDecode)
	}
	if response.Config.Workers != 4 || response.Config.ValidateModelsAfterUpload || response.Config.RefreshAccountInfoAfterUpload {
		t.Fatalf("config = %+v", response.Config)
	}
	if response.Runtime.WorkerLimit != 4 {
		t.Fatalf("runtime = %+v", response.Runtime)
	}
}

func TestPatchChatGPTWebImportPersistsAndHotUpdates(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	executor := &accountInfoControllerTestExecutor{}
	manager.RegisterExecutor(executor)
	tasks := newChatGPTWebMutationTaskManager()
	handler := &Handler{
		cfg:                     &config.Config{},
		configFilePath:          writeTestConfigFile(t),
		authManager:             manager,
		chatGPTWebMutationTasks: tasks,
	}
	t.Cleanup(func() { shutdownChatGPTWebMutationTasks(t, tasks) })

	ctx, recorder := newChatGPTWebImportConfigRequest(http.MethodPatch, `{"workers":7,"validate-models-after-upload":true}`)
	handler.PatchChatGPTWebImport(ctx)
	if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("PATCH status = %d cache-control = %q body = %s", recorder.Code, recorder.Header().Get("Cache-Control"), recorder.Body.String())
	}
	resolved := handler.cfg.ChatGPTWeb.Import.Resolved()
	if resolved.Workers != 7 || !resolved.ValidateModelsAfterUpload || resolved.RefreshAccountInfoAfterUpload {
		t.Fatalf("in-memory config = %+v", resolved)
	}
	if snapshot := tasks.importRuntimeSnapshot(); snapshot.WorkerLimit != 7 {
		t.Fatalf("runtime snapshot = %+v", snapshot)
	}
	executor.mu.Lock()
	updates := executor.updates
	executor.mu.Unlock()
	if updates != 1 {
		t.Fatalf("runtime updates = %d, want 1", updates)
	}
	persisted, errLoad := config.LoadConfig(handler.configFilePath)
	if errLoad != nil {
		t.Fatal(errLoad)
	}
	if got := persisted.ChatGPTWeb.Import.Resolved(); got != resolved {
		t.Fatalf("persisted config = %+v, want %+v", got, resolved)
	}
}

func TestPutChatGPTWebImportStrictValidation(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "missing", body: `{"workers":4}`},
		{name: "unknown", body: `{"workers":4,"validate-models-after-upload":false,"refresh-account-info-after-upload":false,"extra":true}`},
		{name: "null", body: `{"workers":null,"validate-models-after-upload":false,"refresh-account-info-after-upload":false}`},
		{name: "range", body: `{"workers":33,"validate-models-after-upload":false,"refresh-account-info-after-upload":false}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := &Handler{cfg: &config.Config{}, configFilePath: writeTestConfigFile(t)}
			ctx, recorder := newChatGPTWebImportConfigRequest(http.MethodPut, test.body)
			handler.PutChatGPTWebImport(ctx)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("PUT status = %d body = %s", recorder.Code, recorder.Body.String())
			}
			completed := make(chan struct{})
			go func() {
				getCtx, _ := newChatGPTWebImportConfigRequest(http.MethodGet, "")
				handler.GetChatGPTWebImport(getCtx)
				close(completed)
			}()
			select {
			case <-completed:
			case <-time.After(time.Second):
				t.Fatal("invalid PUT retained the handler configuration lock")
			}
		})
	}
}

func TestPatchChatGPTWebImportPersistenceFailureRollsBack(t *testing.T) {
	workers := 3
	tasks := newChatGPTWebMutationTaskManager()
	tasks.updateWorkerLimit(workers)
	executor := &accountInfoControllerTestExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	handler := &Handler{
		cfg: &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{Import: config.ChatGPTWebImportConfig{
			Workers: &workers,
		}}},
		configFilePath:          t.TempDir() + "/missing/config.yaml",
		authManager:             manager,
		chatGPTWebMutationTasks: tasks,
	}
	t.Cleanup(func() { shutdownChatGPTWebMutationTasks(t, tasks) })

	ctx, recorder := newChatGPTWebImportConfigRequest(http.MethodPatch, `{"workers":9}`)
	handler.PatchChatGPTWebImport(ctx)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("PATCH status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	if got := handler.cfg.ChatGPTWeb.Import.Resolved().Workers; got != workers {
		t.Fatalf("rolled-back workers = %d, want %d", got, workers)
	}
	if got := tasks.importRuntimeSnapshot().WorkerLimit; got != workers {
		t.Fatalf("runtime workers = %d, want %d", got, workers)
	}
	executor.mu.Lock()
	updates := executor.updates
	executor.mu.Unlock()
	if updates != 0 {
		t.Fatalf("runtime updates after persistence failure = %d", updates)
	}
}

func TestChatGPTWebImportWorkerLimitDoesNotThrottleConversionPool(t *testing.T) {
	tasks := newChatGPTWebMutationTaskManager()
	tasks.updateWorkerLimit(1)
	t.Cleanup(func() { shutdownChatGPTWebMutationTasks(t, tasks) })

	if !tasks.acquireImportSlot(t.Context()) {
		t.Fatal("failed to acquire the import worker")
	}
	defer tasks.releaseImportSlot()

	for index := range chatGPTWebConversionTaskWorkers {
		if !tasks.acquireConversionSlot(t.Context()) {
			t.Fatalf("failed to acquire conversion worker %d", index)
		}
		defer tasks.releaseConversionSlot()
	}

	if snapshot := tasks.importRuntimeSnapshot(); snapshot.ActiveWorkers != 1 || snapshot.WorkerLimit != 1 {
		t.Fatalf("import runtime snapshot = %+v", snapshot)
	}
}

func TestChatGPTWebImportWorkerLimitHotUpdateUnblocksWaitingEntry(t *testing.T) {
	tasks := newChatGPTWebMutationTaskManager()
	tasks.updateWorkerLimit(1)
	t.Cleanup(func() { shutdownChatGPTWebMutationTasks(t, tasks) })
	if !tasks.acquireImportSlot(t.Context()) {
		t.Fatal("failed to acquire the first import worker")
	}
	defer tasks.releaseImportSlot()

	acquired := make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	go func() {
		if tasks.acquireImportSlot(ctx) {
			close(acquired)
			tasks.releaseImportSlot()
		}
	}()
	select {
	case <-acquired:
		t.Fatal("second import entry bypassed the configured worker limit")
	case <-time.After(20 * time.Millisecond):
	}
	tasks.updateWorkerLimit(2)
	select {
	case <-acquired:
	case <-ctx.Done():
		t.Fatal("worker hot update did not unblock the waiting import entry")
	}
}

func TestHandlerSetConfigHotUpdatesChatGPTWebImportWorkers(t *testing.T) {
	handler := NewHandler(&config.Config{}, "", nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if errShutdown := handler.Shutdown(ctx); errShutdown != nil {
			t.Errorf("shutdown handler: %v", errShutdown)
		}
	})
	workers := 11
	handler.SetConfig(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		Import: config.ChatGPTWebImportConfig{Workers: &workers},
	}})
	if snapshot := handler.chatGPTWebMutationTasks.importRuntimeSnapshot(); snapshot.WorkerLimit != workers {
		t.Fatalf("runtime snapshot = %+v", snapshot)
	}
}

func newChatGPTWebImportConfigRequest(method, body string) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(method, "/v0/management/chatgpt-web/import", strings.NewReader(body))
	if body != "" {
		ctx.Request.Header.Set("Content-Type", "application/json")
	}
	return ctx, recorder
}

func shutdownChatGPTWebMutationTasks(t *testing.T, tasks *chatGPTWebMutationTaskManager) {
	t.Helper()
	if tasks == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if errShutdown := tasks.shutdown(ctx); errShutdown != nil {
		t.Errorf("shutdown mutation tasks: %v", errShutdown)
	}
}
