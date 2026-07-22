package management

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type sentinelSnapshotTestExecutor struct {
	coreauth.ProviderExecutor
	snapshot chatgptwebauth.SentinelRuntimeSnapshot
}

type sentinelConfigUpdateTestExecutor struct {
	coreauth.ProviderExecutor
	updates  int
	resolved config.ResolvedChatGPTWebSentinelConfig
}

type sentinelBlockingSnapshotExecutor struct {
	coreauth.ProviderExecutor
	started  chan struct{}
	release  chan struct{}
	snapshot chatgptwebauth.SentinelRuntimeSnapshot
	once     sync.Once
}

func (executor *sentinelBlockingSnapshotExecutor) Identifier() string {
	return chatgptwebauth.Provider
}

func (executor *sentinelBlockingSnapshotExecutor) SentinelSnapshot() chatgptwebauth.SentinelRuntimeSnapshot {
	executor.once.Do(func() { close(executor.started) })
	<-executor.release
	return executor.snapshot
}

func (executor *sentinelBlockingSnapshotExecutor) UpdateConfig(*config.Config) {}

func (executor *sentinelConfigUpdateTestExecutor) Identifier() string {
	return chatgptwebauth.Provider
}

func (executor *sentinelConfigUpdateTestExecutor) UpdateConfig(cfg *config.Config) {
	executor.updates++
	executor.resolved = cfg.ChatGPTWeb.Sentinel.Resolved()
}

func (executor sentinelSnapshotTestExecutor) Identifier() string {
	return chatgptwebauth.Provider
}

func (executor sentinelSnapshotTestExecutor) SentinelSnapshot() chatgptwebauth.SentinelRuntimeSnapshot {
	return executor.snapshot
}

func TestGetChatGPTWebSentinelReturnsDefaultsWithoutExecutor(t *testing.T) {
	handler := &Handler{cfg: &config.Config{}}
	ctx, recorder := newChatGPTWebSentinelRequest(http.MethodGet, "")
	handler.GetChatGPTWebSentinel(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	response := decodeChatGPTWebSentinelResponse(t, recorder)
	if !response.SDKRuntimeEnabled || response.SDKWorkers != 0 || response.SDKQueueSize != 32 || response.SDKCacheVersions != 3 {
		t.Fatalf("resolved config = %#v, want defaults", response.ResolvedChatGPTWebSentinelConfig)
	}
	if response.Initialized || response.Available || response.WorkerLimit != 0 {
		t.Fatalf("runtime state = %#v, want unavailable and uninitialized", response)
	}
}

func TestGetChatGPTWebSentinelReturnsRuntimeSnapshot(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(sentinelSnapshotTestExecutor{snapshot: chatgptwebauth.SentinelRuntimeSnapshot{
		SDKRuntimeEnabled:    true,
		SDKWorkers:           3,
		SDKQueueSize:         8,
		SDKCacheVersions:     2,
		Initialized:          true,
		Available:            true,
		WorkerLimit:          3,
		Busy:                 2,
		Queued:               4,
		SourcePending:        2,
		SourceWaiters:        5,
		BytecodeWaiters:      3,
		ObserverSessions:     1,
		SDKVersion:           "20260721",
		SDKSHA256:            "abcdef",
		SourceCacheEntries:   2,
		BytecodeCacheEntries: 1,
		FallbackCount:        7,
		LastError:            "previous failure",
	}})
	handler := &Handler{cfg: &config.Config{}, authManager: manager}
	ctx, recorder := newChatGPTWebSentinelRequest(http.MethodGet, "")
	handler.GetChatGPTWebSentinel(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	response := decodeChatGPTWebSentinelResponse(t, recorder)
	if !response.Initialized || !response.Available || response.WorkerLimit != 3 || response.Busy != 2 || response.Queued != 4 || response.ObserverSessions != 1 {
		t.Fatalf("runtime counters = %#v", response)
	}
	assertResolvedSentinelConfig(t, response.ResolvedChatGPTWebSentinelConfig, true, 3, 8, 2)
	if response.SourcePending != 2 || response.SourceWaiters != 5 || response.BytecodeWaiters != 3 {
		t.Fatalf("runtime admission counters = %#v", response)
	}
	if response.SDKVersion != "20260721" || response.SDKSHA256 != "abcdef" || response.SourceCacheEntries != 2 || response.BytecodeCacheEntries != 1 || response.FallbackCount != 7 || response.LastError != "previous failure" {
		t.Fatalf("runtime details = %#v", response)
	}
}

func TestGetChatGPTWebSentinelKeepsConfigAndRuntimeSnapshotConsistent(t *testing.T) {
	handler, _ := newPersistedChatGPTWebSentinelHandler(t, explicitSentinelConfig(true, 2, 9, 4))
	manager := coreauth.NewManager(nil, nil, nil)
	executor := &sentinelBlockingSnapshotExecutor{
		started: make(chan struct{}),
		release: make(chan struct{}),
		snapshot: chatgptwebauth.SentinelRuntimeSnapshot{
			SDKRuntimeEnabled: true,
			SDKWorkers:        2,
			SDKQueueSize:      9,
			SDKCacheVersions:  4,
			Initialized:       true,
			Available:         true,
			WorkerLimit:       2,
		},
	}
	manager.RegisterExecutor(executor)
	handler.authManager = manager

	getDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		ctx, recorder := newChatGPTWebSentinelRequest(http.MethodGet, "")
		handler.GetChatGPTWebSentinel(ctx)
		getDone <- recorder
	}()
	<-executor.started

	patchDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		ctx, recorder := newChatGPTWebSentinelRequest(http.MethodPatch, `{"sdk-runtime-enabled":false}`)
		handler.PatchChatGPTWebSentinel(ctx)
		patchDone <- recorder
	}()
	select {
	case recorder := <-patchDone:
		t.Fatalf("PATCH completed while GET runtime snapshot was blocked: %s", recorder.Body.String())
	case <-time.After(50 * time.Millisecond):
	}

	close(executor.release)
	getRecorder := <-getDone
	getResponse := decodeChatGPTWebSentinelResponse(t, getRecorder)
	if !getResponse.SDKRuntimeEnabled || !getResponse.Available || getResponse.WorkerLimit != 2 {
		t.Fatalf("GET response mixed config and runtime generations: %#v", getResponse)
	}
	patchRecorder := <-patchDone
	if patchRecorder.Code != http.StatusOK {
		t.Fatalf("PATCH status = %d, want 200: %s", patchRecorder.Code, patchRecorder.Body.String())
	}
}

func TestGetChatGPTWebSentinelUsesRuntimeConfigDuringHotReloadGap(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(sentinelSnapshotTestExecutor{snapshot: chatgptwebauth.SentinelRuntimeSnapshot{
		SDKRuntimeEnabled: true,
		SDKWorkers:        2,
		SDKQueueSize:      9,
		SDKCacheVersions:  4,
		Available:         true,
		WorkerLimit:       2,
	}})
	handler := &Handler{
		cfg:         &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{Sentinel: explicitSentinelConfig(false, 6, 12, 5)}},
		authManager: manager,
	}
	ctx, recorder := newChatGPTWebSentinelRequest(http.MethodGet, "")
	handler.GetChatGPTWebSentinel(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	response := decodeChatGPTWebSentinelResponse(t, recorder)
	assertResolvedSentinelConfig(t, response.ResolvedChatGPTWebSentinelConfig, true, 2, 9, 4)
	if !response.Available || response.WorkerLimit != 2 {
		t.Fatalf("runtime state = %#v", response)
	}
}

func TestPutChatGPTWebSentinelReplacesAllFields(t *testing.T) {
	handler, configPath := newPersistedChatGPTWebSentinelHandler(t, config.ChatGPTWebSentinelConfig{})
	body := `{"sdk-runtime-enabled":false,"sdk-workers":4,"sdk-queue-size":0,"sdk-cache-versions":5}`
	ctx, recorder := newChatGPTWebSentinelRequest(http.MethodPut, body)
	handler.PutChatGPTWebSentinel(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	assertResolvedSentinelConfig(t, handler.cfg.ChatGPTWeb.Sentinel.Resolved(), false, 4, 0, 5)
	loaded, errLoad := config.LoadConfig(configPath)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	assertResolvedSentinelConfig(t, loaded.ChatGPTWeb.Sentinel.Resolved(), false, 4, 0, 5)
}

func TestPatchChatGPTWebSentinelPreservesUnsubmittedFields(t *testing.T) {
	initial := explicitSentinelConfig(true, 2, 9, 4)
	handler, configPath := newPersistedChatGPTWebSentinelHandler(t, initial)
	ctx, recorder := newChatGPTWebSentinelRequest(http.MethodPatch, `{"sdk-workers":6}`)
	handler.PatchChatGPTWebSentinel(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("PATCH status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	assertResolvedSentinelConfig(t, handler.cfg.ChatGPTWeb.Sentinel.Resolved(), true, 6, 9, 4)
	loaded, errLoad := config.LoadConfig(configPath)
	if errLoad != nil {
		t.Fatalf("LoadConfig() error = %v", errLoad)
	}
	assertResolvedSentinelConfig(t, loaded.ChatGPTWeb.Sentinel.Resolved(), true, 6, 9, 4)
}

func TestPatchChatGPTWebSentinelAppliesRuntimeBeforeReturning(t *testing.T) {
	initial := explicitSentinelConfig(true, 2, 9, 4)
	handler, _ := newPersistedChatGPTWebSentinelHandler(t, initial)
	manager := coreauth.NewManager(nil, nil, nil)
	executor := &sentinelConfigUpdateTestExecutor{}
	manager.RegisterExecutor(executor)
	handler.authManager = manager

	ctx, recorder := newChatGPTWebSentinelRequest(http.MethodPatch, `{"sdk-workers":6}`)
	handler.PatchChatGPTWebSentinel(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("PATCH status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if executor.updates != 1 {
		t.Fatalf("runtime updates = %d, want 1", executor.updates)
	}
	assertResolvedSentinelConfig(t, executor.resolved, true, 6, 9, 4)
}

func TestChatGPTWebSentinelRejectsInvalidValuesWithoutMutation(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "negative workers", body: `{"sdk-workers":-1}`},
		{name: "too many workers", body: `{"sdk-workers":17}`},
		{name: "negative queue", body: `{"sdk-queue-size":-1}`},
		{name: "large queue", body: `{"sdk-queue-size":1025}`},
		{name: "zero cache", body: `{"sdk-cache-versions":0}`},
		{name: "large cache", body: `{"sdk-cache-versions":6}`},
		{name: "null", body: `{"sdk-workers":null}`},
		{name: "wrong type", body: `{"sdk-runtime-enabled":"yes"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			initial := explicitSentinelConfig(true, 2, 9, 4)
			handler, configPath := newPersistedChatGPTWebSentinelHandler(t, initial)
			before, errRead := os.ReadFile(configPath)
			if errRead != nil {
				t.Fatalf("ReadFile() error = %v", errRead)
			}

			ctx, recorder := newChatGPTWebSentinelRequest(http.MethodPatch, test.body)
			handler.PatchChatGPTWebSentinel(ctx)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("PATCH status = %d, want 400: %s", recorder.Code, recorder.Body.String())
			}
			assertResolvedSentinelConfig(t, handler.cfg.ChatGPTWeb.Sentinel.Resolved(), true, 2, 9, 4)
			after, errRead := os.ReadFile(configPath)
			if errRead != nil {
				t.Fatalf("ReadFile() after request error = %v", errRead)
			}
			if !bytes.Equal(before, after) {
				t.Fatalf("config file changed after invalid request\nbefore:\n%s\nafter:\n%s", before, after)
			}
		})
	}
}

func TestChatGPTWebSentinelRejectsMalformedBodies(t *testing.T) {
	tests := []struct {
		name   string
		method string
		body   string
	}{
		{name: "unknown field", method: http.MethodPatch, body: `{"sdk-workers":2,"unknown":true}`},
		{name: "trailing json", method: http.MethodPatch, body: `{"sdk-workers":2} {}`},
		{name: "null body", method: http.MethodPatch, body: `null`},
		{name: "array body", method: http.MethodPatch, body: `[]`},
		{name: "put missing field", method: http.MethodPut, body: `{"sdk-runtime-enabled":true,"sdk-workers":2,"sdk-queue-size":8}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, configPath := newPersistedChatGPTWebSentinelHandler(t, explicitSentinelConfig(true, 2, 9, 4))
			before, errRead := os.ReadFile(configPath)
			if errRead != nil {
				t.Fatalf("ReadFile() error = %v", errRead)
			}
			ctx, recorder := newChatGPTWebSentinelRequest(test.method, test.body)
			if test.method == http.MethodPut {
				handler.PutChatGPTWebSentinel(ctx)
			} else {
				handler.PatchChatGPTWebSentinel(ctx)
			}
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", recorder.Code, recorder.Body.String())
			}
			assertResolvedSentinelConfig(t, handler.cfg.ChatGPTWeb.Sentinel.Resolved(), true, 2, 9, 4)
			after, errRead := os.ReadFile(configPath)
			if errRead != nil {
				t.Fatalf("ReadFile() after request error = %v", errRead)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("config file changed after malformed request")
			}
		})
	}
}

func TestChatGPTWebSentinelRollsBackAfterPersistFailure(t *testing.T) {
	initial := explicitSentinelConfig(true, 2, 9, 4)
	handler := &Handler{cfg: &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{Sentinel: initial}}, configFilePath: filepath.Join(t.TempDir(), "missing", "config.yaml")}
	ctx, recorder := newChatGPTWebSentinelRequest(http.MethodPatch, `{"sdk-workers":6}`)
	handler.PatchChatGPTWebSentinel(ctx)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("PATCH status = %d, want 500: %s", recorder.Code, recorder.Body.String())
	}
	assertResolvedSentinelConfig(t, handler.cfg.ChatGPTWeb.Sentinel.Resolved(), true, 2, 9, 4)
}

func newPersistedChatGPTWebSentinelHandler(t *testing.T, sentinel config.ChatGPTWebSentinelConfig) (*Handler, string) {
	t.Helper()
	configPath := writeTestConfigFile(t)
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{Sentinel: sentinel}}
	return &Handler{cfg: cfg, configFilePath: configPath}, configPath
}

func newChatGPTWebSentinelRequest(method, body string) (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(method, "/v0/management/chatgpt-web/sentinel", strings.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	return ctx, recorder
}

func decodeChatGPTWebSentinelResponse(t *testing.T, recorder *httptest.ResponseRecorder) chatGPTWebSentinelResponse {
	t.Helper()
	var response chatGPTWebSentinelResponse
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("json.Unmarshal() error = %v; body=%s", errDecode, recorder.Body.String())
	}
	return response
}

func explicitSentinelConfig(enabled bool, workers, queueSize, cacheVersions int) config.ChatGPTWebSentinelConfig {
	return config.ChatGPTWebSentinelConfig{
		SDKRuntimeEnabled: &enabled,
		SDKWorkers:        &workers,
		SDKQueueSize:      &queueSize,
		SDKCacheVersions:  &cacheVersions,
	}
}

func assertResolvedSentinelConfig(t *testing.T, got config.ResolvedChatGPTWebSentinelConfig, enabled bool, workers, queueSize, cacheVersions int) {
	t.Helper()
	if got.SDKRuntimeEnabled != enabled || got.SDKWorkers != workers || got.SDKQueueSize != queueSize || got.SDKCacheVersions != cacheVersions {
		t.Fatalf("resolved config = %#v, want enabled=%v workers=%d queue=%d cache=%d", got, enabled, workers, queueSize, cacheVersions)
	}
}
